package queue

import (
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"
)

// 死信重跑与容量保护相关的可判定错误。
var (
	// ErrQueueFull 表示未结束任务数已达容量上限，本次投递被整体拒绝。
	ErrQueueFull = errors.New("queue: capacity exceeded, queue is full")
	// ErrTaskNotFound 表示按 ID 操作的任务不存在。
	ErrTaskNotFound = errors.New("queue: task not found")
	// ErrNotDead 表示尝试重跑一个并非死信状态的任务（已完成/在途/等待中等）。
	ErrNotDead = errors.New("queue: task is not in dead state")
)

// 批量重跑时单个任务未被重跑的原因，便于运维审计“为什么没动它”。
const (
	SkipNotDead      = "not_dead"      // 任务当前不是死信（已完成或在处理等）
	SkipFilterDenied = "filter_denied" // 过滤条件未命中
	SkipQueueFull    = "queue_full"    // 在途额度已满
	SkipNotFound     = "not_found"     // 指定 ID 不存在
)

// RequeueFilter 描述从死信中人工捞回任务的条件；零值字段表示不限制。
type RequeueFilter struct {
	IDs             []string      // 若非空，仅处理这些 ID；为空表示在所有死信中按条件筛选
	Owner           string        // 仅重跑最近一次由该消费者持有而失败的任务
	Reason          RecoverReason // 仅重跑以此原因进入死信的任务
	PayloadContains string        // 仅重跑 payload 包含该子串的任务
}

// RequeueSkipped 记录批量重跑中被跳过的任务及原因。
type RequeueSkipped struct {
	ID     string
	Reason string
}

// RequeueReport 汇总一次批量死信重跑的结果。
type RequeueReport struct {
	Requeued []string         // 成功重跑的任务 ID
	Skipped  []RequeueSkipped // 被跳过的任务及原因
}

// requeueLocked 把一个已确认处于死信状态的任务捞回 pending。
// 保留 Failures/LastError/TotalAttempts；重置轮内 Attempts 计数，
// 使重跑后仍按原 MaxAttempts 重新走一遍，用尽后再次进死信。
// 调用前必须持锁且已完成状态与容量校验。
func (q *Queue) requeueLocked(t *Task, now time.Time) {
	t.State = StatePending
	t.Attempts = 0
	t.Owner = ""
	t.HeartbeatedAt = time.Time{}
	t.LastReason = ReasonRequeued
	t.NotBefore = now
	t.UpdatedAt = now
	t.Requeues++
}

// RequeueDeadTask 将单个死信任务人工捞回重新投递。
// 失败经历（Failures）与累计投递次数（TotalAttempts）保留；轮内投递次数
// Attempts 重置为 0，之后按原 RetryPolicy 的 MaxAttempts 重新计数，
// 再次耗尽依旧进入死信，不会无限复活。
// 已完成、正在被处理或不存在的任务分别返回 ErrNotDead / ErrTaskNotFound；
// 若重跑会超出在途容量上限，返回 ErrQueueFull。
func (q *Queue) RequeueDeadTask(id string) error {
	q.st.mu.Lock()
	defer q.st.mu.Unlock()
	t, ok := q.st.tasks[id]
	if !ok {
		return fmt.Errorf("%w: %q", ErrTaskNotFound, id)
	}
	if t.State != StateDead {
		return fmt.Errorf("%w: %q is %s", ErrNotDead, id, t.State)
	}
	if q.capacity > 0 && activeCountLocked(q.st.tasks) >= q.capacity {
		q.logf("requeue rejected: task_id=%q reason=queue_full capacity=%d active=%d",
			id, q.capacity, activeCountLocked(q.st.tasks))
		return fmt.Errorf("%w: capacity=%d active=%d",
			ErrQueueFull, q.capacity, activeCountLocked(q.st.tasks))
	}
	now := q.now()
	attemptsBefore, totalBefore, failsBefore := t.Attempts, t.TotalAttempts, len(t.Failures)
	q.requeueLocked(t, now)
	if err := q.st.persistLocked(); err != nil {
		return err
	}
	q.logf("task requeued: task_id=%q attempts=%d->0 total_attempts=%d failures_kept=%d requeues=%d reason=%s",
		t.ID, attemptsBefore, totalBefore, failsBefore, t.Requeues, ReasonRequeued)
	return nil
}

// matchesFilter 判断死信任务是否满足过滤条件（状态为 dead 已在调用处保证）。
func matchesFilter(t *Task, f RequeueFilter) bool {
	if f.Owner != "" {
		// 最近一次持有它的消费者记录在最后一条失败经历里。
		owner := ""
		if n := len(t.Failures); n > 0 {
			owner = t.Failures[n-1].Owner
		}
		if owner != f.Owner {
			return false
		}
	}
	if f.Reason != "" && t.LastReason != f.Reason {
		return false
	}
	if f.PayloadContains != "" && !strings.Contains(t.Payload, f.PayloadContains) {
		return false
	}
	return true
}

// RequeueDead 按 filter 条件批量把死信任务捞回；语义同 RequeueDeadTask，
// 返回每个任务被重跑或被跳过的明细。指定了不存在的 ID 会计入 Skipped。
// 全过程原子：要么所有能重跑的都落盘，要么出错时不落任何变更。
func (q *Queue) RequeueDead(filter RequeueFilter) (RequeueReport, error) {
	q.st.mu.Lock()
	defer q.st.mu.Unlock()

	rep := RequeueReport{}

	// 确定待处理 ID 集合：显式 ID 优先；否则取当前全部死信任务。
	var candidates []string
	if len(filter.IDs) > 0 {
		candidates = append(candidates, filter.IDs...)
	} else {
		for id, t := range q.st.tasks {
			if t.State == StateDead {
				candidates = append(candidates, id)
			}
		}
	}
	sort.Strings(candidates)

	now := q.now()
	changed := make([]*Task, 0)
	for _, id := range candidates {
		t, ok := q.st.tasks[id]
		if !ok {
			rep.Skipped = append(rep.Skipped, RequeueSkipped{ID: id, Reason: SkipNotFound})
			continue
		}
		if t.State != StateDead {
			// 已完成或正被别的消费者处理等：明确拒绝，绝不抢占。
			rep.Skipped = append(rep.Skipped, RequeueSkipped{ID: id, Reason: SkipNotDead})
			continue
		}
		if !matchesFilter(t, filter) {
			rep.Skipped = append(rep.Skipped, RequeueSkipped{ID: id, Reason: SkipFilterDenied})
			continue
		}
		if q.capacity > 0 && activeCountLocked(q.st.tasks)+len(changed) >= q.capacity {
			rep.Skipped = append(rep.Skipped, RequeueSkipped{ID: id, Reason: SkipQueueFull})
			continue
		}
		q.requeueLocked(t, now)
		changed = append(changed, t)
		rep.Requeued = append(rep.Requeued, id)
	}

	if len(changed) > 0 {
		if err := q.st.persistLocked(); err != nil {
			return RequeueReport{}, err
		}
		for _, t := range changed {
			q.logf("task requeued: task_id=%q total_attempts=%d failures_kept=%d requeues=%d reason=%s",
				t.ID, t.TotalAttempts, len(t.Failures), t.Requeues, ReasonRequeued)
		}
	}
	return rep, nil
}

// PurgeFinished 清理已结束（done/dead）的历史任务记录；在途任务与
// 统计口径不受影响，容量额度只统计未结束任务，因此清理后不会改变在途数量。
// 返回被清理的任务数。
func (q *Queue) PurgeFinished() (int, error) {
	q.st.mu.Lock()
	defer q.st.mu.Unlock()
	n := 0
	removed := 0
	for id, t := range q.st.tasks {
		if t.State == StateDone || t.State == StateDead {
			delete(q.st.tasks, id)
			n++
			continue
		}
		removed++
	}
	if n > 0 {
		if err := q.st.persistLocked(); err != nil {
			return 0, err
		}
		q.logf("history purged: removed_finished=%d active_kept=%d", n, removed)
	}
	return n, nil
}
