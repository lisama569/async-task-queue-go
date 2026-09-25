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
}

// NewConsumer 创建绑定到队列的消费者。
func NewConsumer(q *Queue, id string) *Consumer {
	return &Consumer{ID: id, q: q, PollPeriod: 10 * time.Millisecond}
}

// Run 循环领取并处理任务，直到 ctx 取消。
// 处理成功调用 Ack，失败调用 Fail；ctx 取消时已领取未处理的任务
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
		if err := h(ctx, t); err != nil {
			_ = c.q.Fail(t.ID, c.ID, err)
		} else {
			_ = c.q.Ack(t.ID, c.ID)
		}
	}
}
