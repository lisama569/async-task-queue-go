package queue

import (
	"context"
	"time"
)

// Handler 处理任务；返回 nil 表示成功，返回 error 表示失败
// （用 NonRetryable 包装则不可重试，直接进入死信）。
type Handler func(ctx context.Context, t *Task) error

// Consumer 是并发消费者，多个 Consumer 可共享同一 Queue。
type Consumer struct {
	ID         string
	q          *Queue
	PollPeriod time.Duration // 无任务时的轮询间隔
	// HeartbeatPeriod 是处理任务期间的续期间隔；<=0 时按队列 LeaseTTL 的 1/3 自动计算。
	// 只要消费者在持续处理并续期，任务不会被 RecoverStale 回收。
	HeartbeatPeriod time.Duration
}

// NewConsumer 创建绑定到队列的消费者。
func NewConsumer(q *Queue, id string) *Consumer {
	return &Consumer{ID: id, q: q, PollPeriod: 10 * time.Millisecond}
}

// Run 循环领取并处理任务，直到 ctx 取消。
// 处理成功调用 Ack，失败调用 Fail；处理期间按 HeartbeatPeriod 自动续期，
// 长任务不会被误判为失联。ctx 取消时已领取未处理的任务
// 保持 in_flight，由下一次 Open 后的 Recover 重新投递。
func (c *Consumer) Run(ctx context.Context, h Handler) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}
		t, err := c.q.Claim(c.ID)
		if err != nil || t == nil {
			select {
			case <-ctx.Done():
				return
			case <-time.After(c.PollPeriod):
			}
			continue
		}
		done := make(chan struct{})
		go c.keepAlive(ctx, t.ID, done)
		err = h(ctx, t)
		close(done)
		if err != nil {
			_ = c.q.Fail(t.ID, c.ID, err)
		} else {
			_ = c.q.Ack(t.ID, c.ID)
		}
	}
}

// keepAlive 在处理期间周期性续期，直到 done 关闭或 ctx 取消。
func (c *Consumer) keepAlive(ctx context.Context, taskID string, done <-chan struct{}) {
	period := c.HeartbeatPeriod
	if period <= 0 {
		period = c.q.opts.LeaseTTL / 3
	}
	if period <= 0 {
		period = time.Second
	}
	tick := time.NewTicker(period)
	defer tick.Stop()
	for {
		select {
		case <-done:
			return
		case <-ctx.Done():
			return
		case <-tick.C:
			if err := c.q.Heartbeat(taskID, c.ID); err != nil {
				return // 已失去持有权（如被回收或已确认），停止续期
			}
		}
	}
}
