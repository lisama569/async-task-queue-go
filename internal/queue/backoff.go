package queue

import "time"

// BackoffPolicy 计算第 n 次重试前的等待时长，必须随 n 单调不减。
type BackoffPolicy func(retry int) time.Duration

// ExponentialBackoff 返回指数退避策略：base * 2^(retry-1)，封顶 max。
func ExponentialBackoff(base, max time.Duration) BackoffPolicy {
	return func(retry int) time.Duration {
		if retry < 1 {
			retry = 1
		}
		d := base
		for i := 1; i < retry; i++ {
			d *= 2
			if d >= max {
				return max
			}
		}
		if d > max {
			return max
		}
		return d
	}
}
