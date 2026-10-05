package queue

import (
	"errors"
	"fmt"
	"log"
	"time"
)

// ErrTaskExists 表示重复投递同一 ID 的任务。
var ErrTaskExists = errors.New("queue: task already exists")

// ErrQueueFull 表示队列在途任务数已达容量上限，投递被明确拒绝（未写入任何数据）。
var ErrQueueFull = errors.New("queue: capacity reached, enqueue rejected")

// Options 是队列的可选配置；零值表示不启用对应限制。
type Options struct {
	Capacity int           // 允许同时存在的未结束（pending/in_flight/retry_wait）任务上限；0 表示不限
	LeaseTTL time.Duration // 任务被领取后的租约时长，持有者需在此期限内 Heartbeat 续期；0 使用默认值
	Logger   *log.Logger   // 状态迁移日志；nil 使用 log.Default()
}

// DefaultLeaseTTL 是未配置 LeaseTTL 时的默认租约时长。
const DefaultLeaseTTL = 30 * time.Second

// Queue 是异步任务队列的入口。所有方法可并发调用。
type Queue struct {
	st     *store
	policy RetryPolicy
	opts   Options
}

// Open 打开（或创建）位于 path 的本地队列，使用默认选项。
func Open(path string, policy RetryPolicy) (*Queue, error) {
	return OpenWithOptions(path, policy, Options{})
}

// OpenWithOptions 打开（或创建）位于 path 的本地队列，并应用选项。
func OpenWithOptions(path string, policy RetryPolicy, opts Options) (*Queue, error) {
	if policy.MaxAttempts < 1 {
		policy.MaxAttempts = 1
	}
	if opts.LeaseTTL <= 0 {
		opts.LeaseTTL = DefaultLeaseTTL
	}
	if opts.Capacity < 0 {
		opts.Capacity = 0
	}
	if opts.Logger == nil {
		opts.Logger = log.Default()
	}
	st, err := openStore(path)
	if err != nil {
		return nil, err
	}
	return &Queue{st: st, policy: policy, opts: opts}, nil
}

// Close 关闭队列并落盘。
func (q *Queue) Close() error { return q.st.close() }

// Enqueue 投递任务；同 ID 重复投递返回 ErrTaskExists，原任务状态不受影响（幂等）。
// 若未结束任务数已达 Options.Capacity，返回 ErrQueueFull，本次投递不写入任何数据。
func (q *Queue) Enqueue(id, payload string) error {
	q.st.mu.Lock()
	defer q.st.mu.Unlock()
	if _, ok := q.st.tasks[id]; ok {
		return ErrTaskExists
	}
	if q.opts.Capacity > 0 && q.inFlightCountLocked() >= q.opts.Capacity {
		q.opts.Logger.Printf("enqueue rejected id=%s reason=capacity_full capacity=%d", id, q.opts.Capacity)
		return ErrQueueFull
	}
	now := time.Now()
	q.st.tasks[id] = &Task{
		ID: id, Payload: payload, State: StatePending,
		NotBefore: now, UpdatedAt: now,
	}
	q.opts.Logger.Printf("enqueued id=%s state=%s", id, StatePending)
	return q.st.persistLocked()
}

// inFlightCountLocked 统计未结束（非 done、非 dead）任务数。调用前必须持锁。
func (q *Queue) inFlightCountLocked() int {
	n := 0
	for _, t := range q.st.tasks {
		if t.State != StateDone && t.State != StateDead {
			n++
		}
	}
	return n
}

// Claim 由消费者原子领取一个可投递任务（pending 或到期的 retry_wait）。
// 同一任务只会被一个消费者领取；领取后投递次数 +1。
func (q *Queue) Claim(consumerID string) (*Task, error) {
	q.st.mu.Lock()
	defer q.st.mu.Unlock()
	now := time.Now()
	var best *Task
	for _, t := range q.st.tasks {
		ready := t.State == StatePending ||
			(t.State == StateRetryWait && !t.NotBefore.After(now))
		if !ready {
			continue
		}
		if best == nil || t.UpdatedAt.Before(best.UpdatedAt) {
			best = t
		}
	}
	if best == nil {
		return nil, nil
	}
	best.State = StateInFlight
	best.Attempts++
	best.Owner = consumerID
	best.LeaseUntil = now.Add(q.opts.LeaseTTL)
	best.UpdatedAt = now
	if err := q.st.persistLocked(); err != nil {
		return nil, err
	}
	q.opts.Logger.Printf("claimed id=%s owner=%s attempt=%d effective_attempt=%d lease_until=%s",
		best.ID, consumerID, best.Attempts, best.EffectiveAttempts(), best.LeaseUntil.Format(time.RFC3339Nano))
	cp := *best
	return &cp, nil
}

// Heartbeat 由当前持有者续期租约，表示“持有者还活着”。
// 只要持续续期，任务不会被 RecoverStale 回收；非持有者调用返回错误。
func (q *Queue) Heartbeat(id, consumerID string) error {
	q.st.mu.Lock()
	defer q.st.mu.Unlock()
	t, ok := q.st.tasks[id]
	if !ok {
		return fmt.Errorf("queue: task %q not found", id)
	}
	if t.State != StateInFlight || t.Owner != consumerID {
		return fmt.Errorf("queue: task %q not owned by %q (state=%s)", id, consumerID, t.State)
	}
	now := time.Now()
	t.LeaseUntil = now.Add(q.opts.LeaseTTL)
	t.UpdatedAt = now
	if err := q.st.persistLocked(); err != nil {
		return err
	}
	q.opts.Logger.Printf("heartbeat id=%s owner=%s attempt=%d lease_until=%s",
		id, consumerID, t.Attempts, t.LeaseUntil.Format(time.RFC3339Nano))
	return nil
}

// Ack 确认任务完成；仅当前持有者可确认，已确认任务不会再次投递。
func (q *Queue) Ack(id, consumerID string) error {
	q.st.mu.Lock()
	defer q.st.mu.Unlock()
	t, ok := q.st.tasks[id]
	if !ok {
		return fmt.Errorf("queue: task %q not found", id)
	}
	if t.State != StateInFlight || t.Owner != consumerID {
		return fmt.Errorf("queue: task %q not owned by %q (state=%s)", id, consumerID, t.State)
	}
	t.State = StateDone
	t.Owner = ""
	t.UpdatedAt = time.Now()
	return q.st.persistLocked()
}

// Fail 报告任务失败。不可重试错误直接进入死信（ReasonNonRetry）；
// 可重试错误在次数未耗尽时进入 retry_wait（ReasonRetry），
// 达到 MaxAttempts 后进入死信（ReasonRetryExceed），失败原因保留在 LastError。
func (q *Queue) Fail(id, consumerID string, cause error) error {
	q.st.mu.Lock()
	defer q.st.mu.Unlock()
	t, ok := q.st.tasks[id]
	if !ok {
		return fmt.Errorf("queue: task %q not found", id)
	}
	if t.State != StateInFlight || t.Owner != consumerID {
		return fmt.Errorf("queue: task %q not owned by %q (state=%s)", id, consumerID, t.State)
	}
	now := time.Now()
	t.Owner = ""
	t.LeaseUntil = time.Time{}
	t.LastError = cause.Error()
	t.UpdatedAt = now

	var nr *NonRetryableError
	if errors.As(cause, &nr) {
		t.State = StateDead
		t.LastReason = ReasonNonRetry
		q.opts.Logger.Printf("dead id=%s attempt=%d reason=%s err=%q", id, t.Attempts, ReasonNonRetry, t.LastError)
		return q.st.persistLocked()
	}
	if t.EffectiveAttempts() >= q.policy.MaxAttempts {
		t.State = StateDead
		t.LastReason = ReasonRetryExceed
		q.opts.Logger.Printf("dead id=%s attempt=%d effective_attempt=%d max_attempts=%d reason=%s err=%q",
			id, t.Attempts, t.EffectiveAttempts(), q.policy.MaxAttempts, ReasonRetryExceed, t.LastError)
		return q.st.persistLocked()
	}
	t.State = StateRetryWait
	t.LastReason = ReasonRetry
	t.NotBefore = now.Add(q.policy.Backoff(t.EffectiveAttempts()))
	q.opts.Logger.Printf("retry_wait id=%s attempt=%d effective_attempt=%d reason=%s err=%q not_before=%s",
		id, t.Attempts, t.EffectiveAttempts(), ReasonRetry, t.LastError, t.NotBefore.Format(time.RFC3339Nano))
	return q.st.persistLocked()
}

// Recover 将消费者异常退出遗留的 in_flight 任务重新置为 pending，
// 原因标记为 ReasonCrash，返回恢复的任务数。
// 语义：调用方确信持有这些任务的消费者进程已经退出（如重启后的首次调用），
// 因此无视租约剩余时间直接回收；与 RecoverStale 区分。
func (q *Queue) Recover() (int, error) {
	q.st.mu.Lock()
	defer q.st.mu.Unlock()
	n := 0
	now := time.Now()
	for _, t := range q.st.tasks {
		if t.State == StateInFlight {
			t.State = StatePending
			t.Owner = ""
			t.LeaseUntil = time.Time{}
			t.LastReason = ReasonCrash
			t.NotBefore = now
			t.UpdatedAt = now
			q.opts.Logger.Printf("recovered id=%s owner_lost attempt=%d reason=%s", t.ID, t.Attempts, ReasonCrash)
			n++
		}
	}
	if n > 0 {
		if err := q.st.persistLocked(); err != nil {
			return 0, err
		}
	}
	return n, nil
}

// RecoverStale 回收租约已到期（持有者在 LeaseTTL 内未续期）的 in_flight 任务，
// 重新置为 pending，原因标记为 ReasonStale，返回回收的任务数。
// 仍在续期（LeaseUntil 未到期）的任务不受影响，不会被其他消费者抢走。
func (q *Queue) RecoverStale() (int, error) {
	q.st.mu.Lock()
	defer q.st.mu.Unlock()
	n := 0
	now := time.Now()
	for _, t := range q.st.tasks {
		if t.State == StateInFlight && !t.LeaseUntil.IsZero() && !t.LeaseUntil.After(now) {
			t.State = StatePending
			t.Owner = ""
			t.LeaseUntil = time.Time{}
			t.LastReason = ReasonStale
			t.NotBefore = now
			t.UpdatedAt = now
			q.opts.Logger.Printf("recovered id=%s owner_lost attempt=%d reason=%s", t.ID, t.Attempts, ReasonStale)
			n++
		}
	}
	if n > 0 {
		if err := q.st.persistLocked(); err != nil {
			return 0, err
		}
	}
	return n, nil
}

// RequeueDead 将指定死信任务重新放回 pending 正常投递。
// 历史保留：Attempts（累计投递次数）、LastError、LastReason 变化前的失败经历不清除；
// 通过 AttemptBase 记录基线，使重跑后仍享有完整的 MaxAttempts 重试预算，
// 用尽后再次进入死信，不会无限复活。
// 仅 StateDead 可重跑；已完成或正在处理中的任务返回错误，防止误操作。
func (q *Queue) RequeueDead(id string) error {
	q.st.mu.Lock()
	defer q.st.mu.Unlock()
	t, ok := q.st.tasks[id]
	if !ok {
		return fmt.Errorf("queue: task %q not found", id)
	}
	if t.State != StateDead {
		return fmt.Errorf("queue: task %q is %s, only dead tasks can be requeued", id, t.State)
	}
	now := time.Now()
	t.State = StatePending
	t.Owner = ""
	t.LeaseUntil = time.Time{}
	t.AttemptBase = t.Attempts
	t.Requeues++
	t.LastReason = ReasonRequeue
	t.NotBefore = now
	t.UpdatedAt = now
	if err := q.st.persistLocked(); err != nil {
		return err
	}
	q.opts.Logger.Printf("requeued id=%s requeues=%d attempts_kept=%d last_error=%q reason=%s",
		id, t.Requeues, t.Attempts, t.LastError, ReasonRequeue)
	return nil
}

// RequeueDeadIf 按条件批量重跑死信任务，返回重跑数量。
// match 接收任务快照，返回 true 表示重跑；非死信任务即使匹配也会被跳过。
// 重跑只改变任务状态，不新增或删除任务，任务总量守恒。
func (q *Queue) RequeueDeadIf(match func(t *Task) bool) (int, error) {
	q.st.mu.Lock()
	defer q.st.mu.Unlock()
	n := 0
	now := time.Now()
	for _, t := range q.st.tasks {
		if t.State != StateDead {
			continue
		}
		cp := *t
		if !match(&cp) {
			continue
		}
		t.State = StatePending
		t.Owner = ""
		t.LeaseUntil = time.Time{}
		t.AttemptBase = t.Attempts
		t.Requeues++
		t.LastReason = ReasonRequeue
		t.NotBefore = now
		t.UpdatedAt = now
		q.opts.Logger.Printf("requeued id=%s requeues=%d attempts_kept=%d last_error=%q reason=%s",
			t.ID, t.Requeues, t.Attempts, t.LastError, ReasonRequeue)
		n++
	}
	if n > 0 {
		if err := q.st.persistLocked(); err != nil {
			return 0, err
		}
	}
	return n, nil
}

// Prune 清理 UpdatedAt 早于 before 的已结束（done/dead）历史任务，返回清理数量。
// 只删除终态记录，绝不影响在途任务；清理后 Stats 中在途统计保持不变。
func (q *Queue) Prune(before time.Time) (int, error) {
	q.st.mu.Lock()
	defer q.st.mu.Unlock()
	n := 0
	for id, t := range q.st.tasks {
		if (t.State == StateDone || t.State == StateDead) && t.UpdatedAt.Before(before) {
			delete(q.st.tasks, id)
			q.opts.Logger.Printf("pruned id=%s state=%s attempts=%d", id, t.State, t.Attempts)
			n++
		}
	}
	if n > 0 {
		if err := q.st.persistLocked(); err != nil {
			return 0, err
		}
	}
	return n, nil
}

// Stats 返回各状态任务数量，用于总量守恒校验。
func (q *Queue) Stats() map[State]int {
	q.st.mu.Lock()
	defer q.st.mu.Unlock()
	m := map[State]int{}
	for _, t := range q.st.tasks {
		m[t.State]++
	}
	return m
}

// Get 返回任务快照。
func (q *Queue) Get(id string) (*Task, bool) {
	q.st.mu.Lock()
	defer q.st.mu.Unlock()
	t, ok := q.st.tasks[id]
	if !ok {
		return nil, false
	}
	cp := *t
	return &cp, true
}
