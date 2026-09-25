package queue

import (
	"context"
	"log"
	"sync"
	"time"
)

// Handler 处理任务；返回 nil 表示成功，RetryableError 表示可重试失败，
// 其他 error 表示不可重试失败（直接进入死信）。
type Handler func(ctx context.Context, t *Task) error

// Consumer 是并发消费者；多个 Consumer 可共享同一 Queue，
// 同一任务只会被一个消费者领取（租约互斥）。
type Consumer struct {
	ID         string
	queue      *Queue
	handler    Handler
	PollInterval time.Duration // 无任务时的轮询间隔
}

// NewConsumer 创建一个消费者。
func NewConsumer(id string, q *Queue, h Handler) *Consumer {
	return &Consumer{ID: id, queue: q, handler: h, PollInterval: 10 * time.Millisecond}
}

// Run 循环领取并处理任务，直到 ctx 取消且没有可领取任务。
// 调用方通过 wg 等待消费者退出；若消费者异常退出（未调用 Recover 前），
// 其在途任务可由 Queue.Recover 重新投递。
func (c *Consumer) Run(ctx context.Context, wg *sync.WaitGroup) {
	defer wg.Done()
	for {
		t, err := c.queue.Claim(c.ID)
		if err != nil {
			log.Printf("[consumer %s] claim error: %v", c.ID, err)
		}
		if t == nil {
			if ctx.Err() != nil {
				return
			}
			select {
			case <-ctx.Done():
			case <-time.After(c.PollInterval):
			}
			continue
		}
		log.Printf("[consumer %s] task=%s attempt=%d recover_by=%q",
			c.ID, t.ID, t.Attempts, t.RecoverBy)
		herr := c.handler(ctx, t)
		switch {
		case herr == nil:
			if err := c.queue.Ack(c.ID, t.ID); err != nil {
				log.Printf("[consumer %s] task=%s ack error: %v", c.ID, t.ID, err)
			}
		default:
			if err := c.queue.Nack(c.ID, t.ID, herr); err != nil {
				log.Printf("[consumer %s] task=%s nack error: %v", c.ID, t.ID, err)
			}
		}
	}
}
