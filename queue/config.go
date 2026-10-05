package queue

import "time"

// Logger 是队列内部使用的结构化日志接口，由调用方提供实现。
// 默认使用丢弃日志的 NopLogger；测试中可注入输出到 testing.T 的实现。
type Logger interface {
	// Printf 记录一条事件日志。kv 为可解释的键值对（如 task_id、attempts、reason）。
	Printf(format string, kv ...any)
}

// nopLogger 丢弃所有日志。
type nopLogger struct{}

func (nopLogger) Printf(string, ...any) {}

// NopLogger 返回一个不输出任何内容的 Logger。
func NopLogger() Logger { return nopLogger{} }

// Clock 抽象时间来源，生产环境用 time.Now，测试中可注入假时钟。
type Clock interface{ Now() time.Time }

type wallClock struct{}

func (wallClock) Now() time.Time { return time.Now() }

// Option 配置 Queue，采用函数式选项；Open(path, policy) 的旧调用方式完全兼容。
type Option func(*options)

type options struct {
	capacity     int           // 0 表示不限制
	leaseTimeout time.Duration // 心跳/领取后多久没动静算失联
	logger       Logger
	clock        Clock
}

// WithCapacity 限制队列中“未结束”（既未完成也未死信）任务的最大数量。
// 0 表示不限制。Enqueue 与死信重跑都会受该上限约束。
func WithCapacity(n int) Option {
	return func(o *options) { o.capacity = n }
}

// WithLeaseTimeout 设置持有者失联判定时长：在途任务距离上次领取/续期
// 超过该时长仍无心跳，SweepStale 才会把它回收。必须 > 0。
func WithLeaseTimeout(d time.Duration) Option {
	return func(o *options) { o.leaseTimeout = d }
}

// WithLogger 注入日志实现，默认静默。
func WithLogger(l Logger) Option {
	return func(o *options) {
		if l != nil {
			o.logger = l
		}
	}
}

// WithClock 注入时间来源，主要供测试使用。
func WithClock(c Clock) Option {
	return func(o *options) {
		if c != nil {
			o.clock = c
		}
	}
}

// defaultLeaseTimeout 是未显式配置时的失联判定时长。
const defaultLeaseTimeout = 30 * time.Second
