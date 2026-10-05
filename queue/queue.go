package queue

import (
	"errors"
	"fmt"
	"time"
)

// ErrTaskExists 表示重复投递同一 ID 的任务。
var ErrTaskExists = errors.New("queue: task already exists")

// activeCountLocked 统计当前“未结束”任务数：既没完成也没进死信的任务
// （pending / in_flight / retry_wait）。这些任务才占用容量额度。
// 调用前必须持锁。
func activeCountLocked(tasks map[string]*Task) int {
	n := 0
	for _, t := range tasks {
		if t.State != StateDone && t.State != StateDead {
			n++
		}
	}
	return n
}

// Queue 是异步任务队列的入口。所有方法可并发调用。
type Queue struct {
	st           *store
	policy       RetryPolicy
	capacity     int           // 0 表示不限制
	leaseTimeout time.Duration // 多久没心跳算失联
	log          Logger
	clock        Clock
}

// Open 打开（或创建）位于 path 的本地队列。
// 旧调用 Open(path, policy) 完全兼容；新特性通过 WithCapacity /
// WithLeaseTimeout / WithLogger / WithClock 等选项启用。
func Open(path string, policy RetryPolicy, opts ...Option) (*Queue, error) {
	if policy.MaxAttempts < 1 {
		policy.MaxAttempts = 1
	}
	o := options{
		leaseTimeout: defaultLeaseTimeout,
		logger:       NopLogger(),
		clock:        wallClock{},
	}
	for _, opt := range opts {
		opt(&o)
	}
	if o.leaseTimeout <= 0 {
		o.leaseTimeout = defaultLeaseTimeout
	}
	st, err := openStore(path)
	if err != nil {
		return nil, err
	}
	return &Queue{
		st:           st,
		policy:       policy,
		capacity:     o.capacity,
		leaseTimeout: o.leaseTimeout,
		log:          o.logger,
		clock:        o.clock,
	}, nil
}

// now 返回当前时间（可能来自注入的时钟）。
func (q *Queue) now() time.Time { return q.clock.Now() }

// logf 输出一条带队列事件的日志；未注入 Logger 时为静默。
func (q *Queue) logf(format string, kv ...any) { q.log.Printf(format, kv...) }

// Close 关闭队列并落盘。
func (q *Queue) Close() error { return q.st.close() }

// Enqueue 投递任务；同 ID 重复投递返回 ErrTaskExists，原任务状态不受影响（幂等）。
// 当队列配置了容量上限且未结束任务数已达上限时，返回 ErrQueueFull，
// 任务不会写入（不会出现只写一半的状态）。
func (q *Queue) Enqueue(id, payload string) error {
	q.st.mu.Lock()
	defer q.st.mu.Unlock()
	if _, ok := q.st.tasks[id]; ok {
		return ErrTaskExists
	}
	if q.capacity > 0 && activeCountLocked(q.st.tasks) >= q.capacity {
		q.logf("enqueue rejected: task_id=%q reason=queue_full capacity=%d active=%d",
			id, q.capacity, activeCountLocked(q.st.tasks))
		return fmt.Errorf("%w: capacity=%d active=%d",
			ErrQueueFull, q.capacity, activeCountLocked(q.st.tasks))
	}
	now := q.now()
	q.st.tasks[id] = &Task{
		ID: id, Payload: payload, State: StatePending,
		NotBefore: now, UpdatedAt: now,
	}
	if err := q.st.persistLocked(); err != nil {
		delete(q.st.tasks, id)
		return err
	}
	q.logf("task enqueued: task_id=%q attempts=0 total_attempts=0 reason=enqueued active=%d",
		id, activeCountLocked(q.st.tasks))
	return nil
}

// Claim 由消费者原子领取一个可投递任务（pending 或到期的 retry_wait）。
// 同一任务只会被一个消费者领取；领取后投递次数 +1。
func (q *Queue) Claim(consumerID string) (*Task, error) {
	q.st.mu.Lock()
	defer q.st.mu.Unlock()
	now := q.now()
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
	best.TotalAttempts++
	best.Owner = consumerID
	best.HeartbeatedAt = time.Time{}
	best.UpdatedAt = now
	if err := q.st.persistLocked(); err != nil {
		return nil, err
	}
	q.logf("task claimed: task_id=%q owner=%q attempts=%d total_attempts=%d last_reason=%s",
		best.ID, best.Owner, best.Attempts, best.TotalAttempts, best.LastReason)
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
	t.HeartbeatedAt = time.Time{}
	t.UpdatedAt = q.now()
	if err := q.st.persistLocked(); err != nil {
		return err
	}
	q.logf("task done: task_id=%q consumer=%q attempts=%d total_attempts=%d reason=acked",
		t.ID, consumerID, t.Attempts, t.TotalAttempts)
	return nil
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
	now := q.now()
	owner := t.Owner
	t.Owner = ""
	t.LastError = cause.Error()
	t.HeartbeatedAt = time.Time{}
	t.UpdatedAt = now

	var nr *NonRetryableError
	if errors.As(cause, &nr) {
		t.State = StateDead
		t.LastReason = ReasonNonRetry
		t.Failures = append(t.Failures, AttemptEvent{
			Attempt: t.Attempts, Owner: owner, Error: cause.Error(),
			Reason: ReasonNonRetry, At: now,
		})
		if err := q.st.persistLocked(); err != nil {
			return err
		}
		q.logf("task dead: task_id=%q owner=%q attempts=%d total_attempts=%d reason=%s error=%q requeues=%d",
			t.ID, owner, t.Attempts, t.TotalAttempts, ReasonNonRetry, cause.Error(), t.Requeues)
		return nil
	}
	if t.Attempts >= q.policy.MaxAttempts {
		t.State = StateDead
		t.LastReason = ReasonRetryExceed
		t.Failures = append(t.Failures, AttemptEvent{
			Attempt: t.Attempts, Owner: owner, Error: cause.Error(),
			Reason: ReasonRetryExceed, At: now,
		})
		if err := q.st.persistLocked(); err != nil {
			return err
		}
		q.logf("task dead: task_id=%q owner=%q attempts=%d total_attempts=%d reason=%s error=%q requeues=%d",
			t.ID, owner, t.Attempts, t.TotalAttempts, ReasonRetryExceed, cause.Error(), t.Requeues)
		return nil
	}
	t.State = StateRetryWait
	t.LastReason = ReasonRetry
	t.NotBefore = now.Add(q.policy.Backoff(t.Attempts))
	t.Failures = append(t.Failures, AttemptEvent{
		Attempt: t.Attempts, Owner: owner, Error: cause.Error(),
		Reason: ReasonRetry, At: now,
	})
	if err := q.st.persistLocked(); err != nil {
		return err
	}
	q.logf("task retry scheduled: task_id=%q owner=%q attempts=%d total_attempts=%d reason=%s error=%q not_before=%s",
		t.ID, owner, t.Attempts, t.TotalAttempts, ReasonRetry, cause.Error(), t.NotBefore.Format(time.RFC3339))
	return nil
}

// Recover 用于“消费者进程已明确退出/重启”后的显式恢复：把遗留的
// in_flight 任务重新置为 pending，原因标记为 ReasonCrash，返回恢复的任务数。
// 它与 SweepStale 的语义不同——后者只回收心跳超时、疑似失联的持有者，
// 且标记为 ReasonLostContact。两者都会重新落盘，可从任务记录区分。
func (q *Queue) Recover() (int, error) {
	q.st.mu.Lock()
	defer q.st.mu.Unlock()
	n := 0
	now := q.now()
	type rec struct {
		t     *Task
		owner string
	}
	recovered := make([]rec, 0)
	for _, t := range q.st.tasks {
		if t.State == StateInFlight {
			owner := t.Owner
			t.State = StatePending
			t.Owner = ""
			t.LastReason = ReasonCrash
			t.NotBefore = now
			t.HeartbeatedAt = time.Time{}
			t.UpdatedAt = now
			recovered = append(recovered, rec{t: t, owner: owner})
			n++
		}
	}
	if n > 0 {
		if err := q.st.persistLocked(); err != nil {
			return 0, err
		}
		for _, r := range recovered {
			q.logf("task recovered: task_id=%q stale_owner=%q attempts=%d total_attempts=%d reason=%s",
				r.t.ID, r.owner, r.t.Attempts, r.t.TotalAttempts, ReasonCrash)
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
