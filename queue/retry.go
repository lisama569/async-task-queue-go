package queue

import "time"

// RetryPolicy 描述可解释的重试策略：等待时长随重试次数单调增长。
type RetryPolicy struct {
	MaxAttempts int           // 最大投递次数（含首次），超过则进入死信
	BaseBackoff time.Duration // 基础退避时长
}

// Backoff 返回第 attempt 次失败后的等待时长：base * 2^(attempt-1)，单调递增。
func (p RetryPolicy) Backoff(attempt int) time.Duration {
	if attempt < 1 {
		attempt = 1
	}
	d := p.BaseBackoff
	for i := 1; i < attempt; i++ {
		d *= 2
	}
	return d
}
