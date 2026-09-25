// Package queue 提供基于本地存储的异步任务队列。
package queue

import "time"

// State 表示任务的生命周期状态。
type State string

const (
	StatePending   State = "pending"    // 等待投递
	StateInFlight  State = "in_flight"  // 已被消费者领取，未确认
	StateDone      State = "done"       // 已确认完成
	StateRetryWait State = "retry_wait" // 等待重试
	StateDead      State = "dead"       // 死信
)

// RecoverReason 区分任务被重新投递的原因。
type RecoverReason string

const (
	ReasonNone        RecoverReason = ""                  // 正常首次投递
	ReasonCrash       RecoverReason = "crash_recovered"   // 消费者异常退出后恢复
	ReasonRetry       RecoverReason = "retry_scheduled"   // 可重试失败后的重投
	ReasonRetryExceed RecoverReason = "retry_exhausted"   // 重试耗尽，进入死信
	ReasonNonRetry    RecoverReason = "non_retryable"     // 不可重试失败，进入死信
)

// Task 是队列中的任务记录。
type Task struct {
	ID         string        `json:"id"`
	Payload    string        `json:"payload"`
	State      State         `json:"state"`
	Attempts   int           `json:"attempts"`    // 已投递次数
	LastReason RecoverReason `json:"last_reason"` // 最近一次状态变化原因
	LastError  string        `json:"last_error"`  // 最近一次失败原因
	NotBefore  time.Time     `json:"not_before"`  // 重试等待：该时间前不可投递
	Owner      string        `json:"owner"`       // 当前持有任务的消费者
	UpdatedAt  time.Time     `json:"updated_at"`
}

// NonRetryableError 标记不可重试的失败。
type NonRetryableError struct{ Err error }

func (e *NonRetryableError) Error() string { return e.Err.Error() }

// NonRetryable 包装 err，使其失败时直接进入死信。
func NonRetryable(err error) error {
	if err == nil {
		return nil
	}
	return &NonRetryableError{Err: err}
}
