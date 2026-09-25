// Package queue 提供基于本地存储的异步任务队列。
package queue

import (
	"errors"
	"time"
)

// TaskStatus 表示任务在生命周期中的状态。
type TaskStatus string

const (
	StatusPending   TaskStatus = "pending"    // 等待投递
	StatusInFlight  TaskStatus = "in_flight"  // 已被消费者领取，未确认
	StatusDone      TaskStatus = "done"       // 已确认完成
	StatusDead      TaskStatus = "dead"       // 进入死信队列
)

// RecoverReason 区分任务被重新投递的原因。
type RecoverReason string

const (
	ReasonNone           RecoverReason = ""                 // 非恢复投递
	ReasonConsumerCrash  RecoverReason = "consumer_crash"   // 消费者异常退出，租约未确认
	ReasonLeaseExpired   RecoverReason = "lease_expired"    // 租约超时未确认
	ReasonRetryScheduled RecoverReason = "retry_scheduled"  // 可重试失败后的再次投递
)

// Task 是队列中的任务记录。
type Task struct {
	ID          string        `json:"id"`
	Payload     string        `json:"payload"`
	Status      TaskStatus    `json:"status"`
	Attempts    int           `json:"attempts"`     // 已投递次数
	MaxRetries  int           `json:"max_retries"`  // 最大重试次数（不含首次）
	NotBefore   time.Time     `json:"not_before"`   // 退避等待：此前不可投递
	LeaseOwner  string        `json:"lease_owner"`  // 当前持有租约的消费者
	LeaseExpiry time.Time     `json:"lease_expiry"` // 租约到期时间
	LastError   string        `json:"last_error"`   // 最近一次失败原因
	DeadReason  string        `json:"dead_reason"`  // 进入死信的原因
	RecoverBy   RecoverReason `json:"recover_by"`   // 最近一次重新投递的原因
	CreatedAt   time.Time     `json:"created_at"`
	UpdatedAt   time.Time     `json:"updated_at"`
}

// RetryableError 包装可重试的失败；普通 error 视为不可重试。
type RetryableError struct{ Err error }

func (e *RetryableError) Error() string { return e.Err.Error() }
func (e *RetryableError) Unwrap() error { return e.Err }

// NewRetryableError 标记一个可重试的失败。
func NewRetryableError(err error) error {
	if err == nil {
		return nil
	}
	return &RetryableError{Err: err}
}

// IsRetryable 判定错误是否可重试。
func IsRetryable(err error) bool {
	var re *RetryableError
	return errors.As(err, &re)
}

// 队列操作错误。
var (
	ErrTaskNotFound   = errors.New("queue: task not found")
	ErrTaskNotOwned   = errors.New("queue: task not owned by this consumer")
	ErrTaskNotInFlight = errors.New("queue: task is not in flight")
)
