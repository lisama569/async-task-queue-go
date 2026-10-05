package queue

import (
	"fmt"
	"sort"
	"time"
)

// SweepTask 是一次失联回收扫描中某个被回收任务的快照信息。
type SweepTask struct {
	ID            string
	Owner         string
	Attempts      int
	TotalAttempts int
	IdleFor       time.Duration // 距上次领取/续期的静默时长
	LeaseTimeout  time.Duration
}

// SweepReport 汇总一次 SweepStale 的结果。
type SweepReport struct {
	Scanned   int         // 扫描时在途任务总数
	Recovered []SweepTask // 因失联被回收的任务
}

// lastLeaseSeen 返回持有者最近一次“活着”的证据时间：
// 优先用心跳续期时间；从未续期时回退到领取时刻（UpdatedAt）。
func lastLeaseSeen(t *Task) time.Time {
	if !t.HeartbeatedAt.IsZero() {
		return t.HeartbeatedAt
	}
	return t.UpdatedAt
}

// Heartbeat 由任务持有者在长任务处理期间周期性调用，表示“我还活着”。
// 只有当前持有者（且任务仍在途中）可以续期；续期会刷新判定失联的时间基准。
// 非持有者或任务不在途中返回错误，任务状态不变。
func (q *Queue) Heartbeat(id, consumerID string) error {
	q.st.mu.Lock()
	defer q.st.mu.Unlock()
	t, ok := q.st.tasks[id]
	if !ok {
		return fmt.Errorf("%w: %q", ErrTaskNotFound, id)
	}
	if t.State != StateInFlight || t.Owner != consumerID {
		return fmt.Errorf("queue: task %q not owned by %q (state=%s, owner=%q)",
			id, consumerID, t.State, t.Owner)
	}
	now := q.now()
	t.HeartbeatedAt = now
	t.UpdatedAt = now
	if err := q.st.persistLocked(); err != nil {
		return err
	}
	q.logf("task heartbeat: task_id=%q owner=%q attempts=%d total_attempts=%d reason=heartbeat",
		t.ID, t.Owner, t.Attempts, t.TotalAttempts)
	return nil
}

// SweepStale 扫描所有在途任务：持有者持续续期的不动；只有距上次
// 领取/续期超过租约超时、明显失联的任务才回收为可重新投递状态，
// 并以 ReasonLostContact 标记，与显式的进程崩溃恢复（ReasonCrash）区分开。
// 该方法不会抢占任何仍在持续续期的任务，因此同一任务任何时刻
// 至多被一个消费者执行。
func (q *Queue) SweepStale() (SweepReport, error) {
	q.st.mu.Lock()
	defer q.st.mu.Unlock()
	now := q.now()

	rep := SweepReport{}
	ids := make([]string, 0)
	for id, t := range q.st.tasks {
		if t.State == StateInFlight {
			ids = append(ids, id)
		}
	}
	sort.Strings(ids)
	rep.Scanned = len(ids)

	for _, id := range ids {
		t := q.st.tasks[id]
		idle := now.Sub(lastLeaseSeen(t))
		if idle <= q.leaseTimeout {
			// 持有者仍在续期（或领取尚在租约窗口内），绝不能抢占。
			continue
		}
		st := SweepTask{
			ID:            t.ID,
			Owner:         t.Owner,
			Attempts:      t.Attempts,
			TotalAttempts: t.TotalAttempts,
			IdleFor:       idle,
			LeaseTimeout:  q.leaseTimeout,
		}
		t.State = StatePending
		t.Owner = ""
		t.LastReason = ReasonLostContact
		t.NotBefore = now
		t.HeartbeatedAt = time.Time{}
		t.UpdatedAt = now
		rep.Recovered = append(rep.Recovered, st)
	}

	if len(rep.Recovered) > 0 {
		if err := q.st.persistLocked(); err != nil {
			return SweepReport{}, err
		}
		for _, st := range rep.Recovered {
			q.logf("task recovered: task_id=%q stale_owner=%q attempts=%d total_attempts=%d reason=%s idle=%s lease_timeout=%s",
				st.ID, st.Owner, st.Attempts, st.TotalAttempts, ReasonLostContact, st.IdleFor, st.LeaseTimeout)
		}
	}
	return rep, nil
}
