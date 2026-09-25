package queue

import (
	"errors"
	"fmt"
	"time"
)

// ErrTaskExists 表示重复投递同一 ID 的任务。
var ErrTaskExists = errors.New("queue: task already exists")

// Queue 是异步任务队列的入口。所有方法可并发调用。
type Queue struct {
	st     *store
	policy RetryPolicy
}

// Open 打开（或创建）位于 path 的本地队列。
func Open(path string, policy RetryPolicy) (*Queue, error) {
	if policy.MaxAttempts < 1 {
		policy.MaxAttempts = 1
	}
	st, err := openStore(path)
	if err != nil {
		return nil, err
	}
	return &Queue{st: st, policy: policy}, nil
}

// Close 关闭队列并落盘。
func (q *Queue) Close() error { return q.st.close() }

// Enqueue 投递任务；同 ID 重复投递返回 ErrTaskExists，原任务状态不受影响（幂等）。
func (q *Queue) Enqueue(id, payload string) error {
	q.st.mu.Lock()
	defer q.st.mu.Unlock()
	if _, ok := q.st.tasks[id]; ok {
		return ErrTaskExists
	}
	now := time.Now()
	q.st.tasks[id] = &Task{
		ID: id, Payload: payload, State: StatePending,
		NotBefore: now, UpdatedAt: now,
	}
	return q.st.persistLocked()
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
	best.UpdatedAt = now
	if err := q.st.persistLocked(); err != nil {
		return nil, err
	}
	cp := *best
	return &cp, nil
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
	t.LastError = cause.Error()
	t.UpdatedAt = now

	var nr *NonRetryableError
	if errors.As(cause, &nr) {
		t.State = StateDead
		t.LastReason = ReasonNonRetry
		return q.st.persistLocked()
	}
	if t.Attempts >= q.policy.MaxAttempts {
		t.State = StateDead
		t.LastReason = ReasonRetryExceed
		return q.st.persistLocked()
	}
	t.State = StateRetryWait
	t.LastReason = ReasonRetry
	t.NotBefore = now.Add(q.policy.Backoff(t.Attempts))
	return q.st.persistLocked()
}

// Recover 将消费者异常退出遗留的 in_flight 任务重新置为 pending，
// 原因标记为 ReasonCrash，返回恢复的任务数。
func (q *Queue) Recover() (int, error) {
	q.st.mu.Lock()
	defer q.st.mu.Unlock()
	n := 0
	now := time.Now()
	for _, t := range q.st.tasks {
		if t.State == StateInFlight {
			t.State = StatePending
			t.Owner = ""
			t.LastReason = ReasonCrash
			t.NotBefore = now
			t.UpdatedAt = now
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
