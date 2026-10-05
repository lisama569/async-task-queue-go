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
	ID              string
	q               *Queue
	PollPeriod      time.Duration // 无任务时的轮询间隔
	HeartbeatPeriod time.Duration // 长任务处理期间的自动续期间隔；0 表示不自动续期
}

// NewConsumer 创建绑定到队列的消费者。
func NewConsumer(q *Queue, id string) *Consumer {
	return &Consumer{
		ID:              id,
		q:               q,
		PollPeriod:      10 * time.Millisecond,
		HeartbeatPeriod: 0,
	}
}

// heartbeat 周期为任务 t 续期，直到任务处理结束（stop 关闭）或 ctx 取消。
func (c *Consumer) heartbeat(ctx context.Context, t *Task, stop <-chan struct{}) {
	ticker := time.NewTicker(c.HeartbeatPeriod)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-stop:
			return
		case <-ticker.C:
			// 续期失败（如租约已被回收、持有者已变更）只记录不中断处理：
			// 后续 Ack/Fail 同样会因持有者校验失败而被拒绝，避免双跑。
			if err := c.q.Heartbeat(t.ID, c.ID); err != nil {
				c.q.logf("heartbeat failed: task_id=%q consumer=%q error=%q",
					t.ID, c.ID, err.Error())
				return
			}
		}
	}
}

// Run 循环领取并处理任务，直到 ctx 取消。
// 处理成功调用 Ack，失败调用 Fail；ctx 取消时已领取未处理的任务
// 保持 in_flight，由下一次 Open 后的 Recover（进程退出）或
// SweepStale（心跳超时失联）重新投递。
// 配置了 HeartbeatPeriod (>0) 时，处理期间会自动续期，长任务不会被误伤。
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
		stop := make(chan struct{})
		if c.HeartbeatPeriod > 0 {
			go c.heartbeat(ctx, t, stop)
		}
		herr := h(ctx, t)
		close(stop)
		if herr != nil {
			_ = c.q.Fail(t.ID, c.ID, herr)
		} else {
			_ = c.q.Ack(t.ID, c.ID)
		}
	}
}
