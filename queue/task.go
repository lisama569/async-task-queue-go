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
	ReasonCrash       RecoverReason = "crash_recovered"   // 消费者异常退出后恢复（显式恢复）
	ReasonRetry       RecoverReason = "retry_scheduled"   // 可重试失败后的重投
	ReasonRetryExceed RecoverReason = "retry_exhausted"   // 重试耗尽，进入死信
	ReasonNonRetry    RecoverReason = "non_retryable"     // 不可重试失败，进入死信
	ReasonLostContact RecoverReason = "lost_contact"      // 持有者心跳超时、疑似失联后回收
	ReasonRequeued    RecoverReason = "manually_requeued" // 人工从死信捞回重跑
)

// Task 是队列中的任务记录。
type Task struct {
	ID            string         `json:"id"`
	Payload       string         `json:"payload"`
	State         State          `json:"state"`
	Attempts      int            `json:"attempts"`       // 当前这一轮生命周期内的投递次数（重跑后重置）
	TotalAttempts int            `json:"total_attempts"` // 跨重跑累计投递次数（永不洗白）
	LastReason    RecoverReason  `json:"last_reason"`    // 最近一次状态变化原因
	LastError     string         `json:"last_error"`     // 最近一次失败原因
	NotBefore     time.Time      `json:"not_before"`     // 重试等待：该时间前不可投递
	Owner         string         `json:"owner"`          // 当前持有任务的消费者
	HeartbeatedAt time.Time      `json:"heartbeated_at"` // 持有者最近一次续期时间；零值表示从未续期
	UpdatedAt     time.Time      `json:"updated_at"`
	Failures      []AttemptEvent `json:"failures,omitempty"` // 历次失败经历（跨重跑保留）
	Requeues      int            `json:"requeues,omitempty"` // 被人工从死信重跑的次数
}

// AttemptEvent 记录一次失败投递的经历，重跑不会清空历史。
type AttemptEvent struct {
	Attempt int           `json:"attempt"` // 当时的轮内投递次数
	Owner   string        `json:"owner"`   // 当时持有任务的消费者
	Error   string        `json:"error"`   // 失败原因
	Reason  RecoverReason `json:"reason"`  // 失败后走向（retry / dead）
	At      time.Time     `json:"at"`
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
