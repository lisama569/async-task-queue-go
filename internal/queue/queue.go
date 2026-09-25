package queue

import (
	"fmt"
	"sort"
	"sync"
	"time"
)

// Options 配置队列行为。
type Options struct {
	LeaseDuration     time.Duration // 消费者租约时长，超时未确认可被恢复重投
	Backoff           BackoffPolicy // 重试退避策略
	DefaultMaxRetries int           // 默认最大重试次数（不含首次投递）
}

// Queue 是基于本地存储的异步任务队列，投递语义为至少一次。
// 所有状态变更在单把互斥锁内完成并立即落盘，保证并发一致性与崩溃可恢复。
type Queue struct {
	mu    sync.Mutex
	store *Store
	opts  Options
	tasks map[string]*Task
	now   func() time.Time // 可注入时钟，便于测试
}

// Open 打开队列并加载本地存储中的任务。
func Open(store *Store, opts Options) (*Queue, error) {
	tasks, err := store.load()
	if err != nil {
		return nil, err
	}
	if opts.Backoff == nil {
		opts.Backoff = ExponentialBackoff(time.Second, time.Minute)
	}
	if opts.LeaseDuration <= 0 {
		opts.LeaseDuration = 30 * time.Second
	}
	return &Queue{store: store, opts: opts, tasks: tasks, now: time.Now}, nil
}

// Enqueue 投递任务；相同 ID 重复投递幂等：返回已存在的任务，created=false。
// 已确认（done）或已死信（dead）的任务重复投递不会改变其终态。
func (q *Queue) Enqueue(t Task) (*Task, bool, error) {
	if t.ID == "" {
		return nil, false, fmt.Errorf("queue: task id is required")
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	if existing, ok := q.tasks[t.ID]; ok {
		cp := *existing
		return &cp, false, nil
	}
	now := q.now()
	if t.MaxRetries == 0 {
		t.MaxRetries = q.opts.DefaultMaxRetries
	}
	t.Status = StatusPending
	t.Attempts = 0
	t.NotBefore = now
	t.CreatedAt = now
	t.UpdatedAt = now
	t.RecoverBy = ReasonNone
	q.tasks[t.ID] = &t
	if err := q.store.save(q.tasks); err != nil {
		delete(q.tasks, t.ID)
		return nil, false, err
	}
	cp := t
	return &cp, true, nil
}

// Claim 由消费者原子领取一个可投递任务（pending 且退避期满，按创建时间先入先出）。
// 领取即记录租约并递增投递次数；没有可领取任务时返回 (nil, nil)。
func (q *Queue) Claim(consumerID string) (*Task, error) {
	q.mu.Lock()
	defer q.mu.Unlock()
	now := q.now()
	var candidates []*Task
	for _, t := range q.tasks {
		if t.Status == StatusPending && !t.NotBefore.After(now) {
			candidates = append(candidates, t)
		}
	}
	if len(candidates) == 0 {
		return nil, nil
	}
	sort.Slice(candidates, func(i, j int) bool {
		return candidates[i].CreatedAt.Before(candidates[j].CreatedAt)
	})
	t := candidates[0]
	t.Status = StatusInFlight
	t.LeaseOwner = consumerID
	t.LeaseExpiry = now.Add(q.opts.LeaseDuration)
	t.Attempts++
	t.UpdatedAt = now
	if err := q.store.save(q.tasks); err != nil {
		return nil, err
	}
	cp := *t
	return &cp, nil
}

// Ack 确认任务完成；只有持有有效租约的消费者才能确认。
func (q *Queue) Ack(consumerID, taskID string) error {
	q.mu.Lock()
	defer q.mu.Unlock()
	t, err := q.ownedLocked(consumerID, taskID)
	if err != nil {
		return err
	}
	t.Status = StatusDone
	t.LeaseOwner = ""
	t.LeaseExpiry = time.Time{}
	t.UpdatedAt = q.now()
	return q.store.save(q.tasks)
}

// Nack 报告任务处理失败：可重试错误按退避策略重新排队（重试次数与等待
// 时长随 attempts 单调增加）；不可重试错误或重试耗尽则进入死信并保留原因。
func (q *Queue) Nack(consumerID, taskID string, cause error) error {
	q.mu.Lock()
	defer q.mu.Unlock()
	t, err := q.ownedLocked(consumerID, taskID)
	if err != nil {
		return err
	}
	now := q.now()
	retryable := IsRetryable(cause)
	// 已用重试次数 = 已投递次数 - 1；超过 MaxRetries 即耗尽。
	retriesUsed := t.Attempts - 1
	switch {
	case retryable && retriesUsed < t.MaxRetries:
		nextRetry := retriesUsed + 1
		t.Status = StatusPending
		t.NotBefore = now.Add(q.opts.Backoff(nextRetry))
		t.LastError = cause.Error()
		t.RecoverBy = ReasonRetryScheduled
		t.LeaseOwner = ""
		t.LeaseExpiry = time.Time{}
		t.UpdatedAt = now
	case retryable:
		t.Status = StatusDead
		t.DeadReason = fmt.Sprintf("retries_exhausted: %s", cause)
		t.LastError = cause.Error()
		t.LeaseOwner = ""
		t.LeaseExpiry = time.Time{}
		t.UpdatedAt = now
	default:
		t.Status = StatusDead
		t.DeadReason = fmt.Sprintf("non_retryable: %v", cause)
		t.LastError = fmt.Sprint(cause)
		t.LeaseOwner = ""
		t.LeaseExpiry = time.Time{}
		t.UpdatedAt = now
	}
	return q.store.save(q.tasks)
}

// Recover 将需要重新投递的在途任务重新入队，并标注可区分的恢复原因：
// 持有者不在存活集合中 → consumer_crash；租约到期 → lease_expired。
// 返回恢复的任务数。
func (q *Queue) Recover(aliveConsumers map[string]bool) int {
	q.mu.Lock()
	defer q.mu.Unlock()
	now := q.now()
	recovered := 0
	for _, t := range q.tasks {
		if t.Status != StatusInFlight {
			continue
		}
		var reason RecoverReason
		switch {
		case !aliveConsumers[t.LeaseOwner]:
			reason = ReasonConsumerCrash
		case !t.LeaseExpiry.After(now):
			reason = ReasonLeaseExpired
		default:
			continue
		}
		t.Status = StatusPending
		t.RecoverBy = reason
		t.LeaseOwner = ""
		t.LeaseExpiry = time.Time{}
		t.UpdatedAt = now
		recovered++
	}
	if recovered > 0 {
		_ = q.store.save(q.tasks)
	}
	return recovered
}

// Stats 返回各状态任务数量，用于总量守恒校验（各状态之和恒等于任务总数）。
func (q *Queue) Stats() map[TaskStatus]int {
	q.mu.Lock()
	defer q.mu.Unlock()
	stats := map[TaskStatus]int{}
	for _, t := range q.tasks {
		stats[t.Status]++
	}
	return stats
}

// Get 返回任务快照。
func (q *Queue) Get(id string) (*Task, error) {
	q.mu.Lock()
	defer q.mu.Unlock()
	t, ok := q.tasks[id]
	if !ok {
		return nil, ErrTaskNotFound
	}
	cp := *t
	return &cp, nil
}

// ownedLocked 校验任务存在、处于在途状态且由该消费者持有租约。
func (q *Queue) ownedLocked(consumerID, taskID string) (*Task, error) {
	t, ok := q.tasks[taskID]
	if !ok {
		return nil, ErrTaskNotFound
	}
	if t.Status != StatusInFlight {
		return nil, ErrTaskNotInFlight
	}
	if t.LeaseOwner != consumerID {
		return nil, ErrTaskNotOwned
	}
	return t, nil
}
