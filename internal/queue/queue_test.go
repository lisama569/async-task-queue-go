package queue

import (
	"errors"
	"fmt"
	"path/filepath"
	"testing"
	"time"
)

func openTestQueue(t *testing.T, opts Options) *Queue {
	t.Helper()
	store, err := NewStore(filepath.Join(t.TempDir(), "store.json"))
	if err != nil {
		t.Fatalf("NewStore: %v", err)
	}
	q, err := Open(store, opts)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	return q
}

// TestDuplicateEnqueue 重复投递同一 ID 不得造成状态错乱或重复记账。
func TestDuplicateEnqueue(t *testing.T) {
	q := openTestQueue(t, Options{LeaseDuration: time.Minute})

	_, created, err := q.Enqueue(Task{ID: "t1", Payload: "hello"})
	if err != nil || !created {
		t.Fatalf("first enqueue: created=%v err=%v", created, err)
	}
	// 重复投递：幂等，不创建新记录。
	dup, created, err := q.Enqueue(Task{ID: "t1", Payload: "changed"})
	if err != nil {
		t.Fatalf("dup enqueue: %v", err)
	}
	t.Logf("task=%s created=%v payload=%q 判定依据: 重复投递应幂等返回已存在任务", dup.ID, created, dup.Payload)
	if created {
		t.Fatalf("duplicate enqueue created a new record")
	}
	if dup.Payload != "hello" {
		t.Fatalf("duplicate enqueue overwrote payload: %q", dup.Payload)
	}

	// 领取并确认后，再次重复投递不得复活已确认任务。
	claimed, err := q.Claim("c1")
	if err != nil || claimed == nil {
		t.Fatalf("claim: %v %v", claimed, err)
	}
	t.Logf("task=%s attempt=%d 判定依据: 首次领取投递次数应为 1", claimed.ID, claimed.Attempts)
	if claimed.Attempts != 1 {
		t.Fatalf("attempts=%d, want 1", claimed.Attempts)
	}
	if err := q.Ack("c1", "t1"); err != nil {
		t.Fatalf("ack: %v", err)
	}
	if _, created, _ := q.Enqueue(Task{ID: "t1"}); created {
		t.Fatalf("enqueue after ack resurrected the task")
	}
	got, _ := q.Get("t1")
	t.Logf("task=%s status=%s 判定依据: 已确认任务重复投递后必须保持 done", got.ID, got.Status)
	if got.Status != StatusDone {
		t.Fatalf("status=%s, want done", got.Status)
	}

	// 守恒：队列中始终只有 1 条记录。
	stats := q.Stats()
	total := 0
	for _, n := range stats {
		total += n
	}
	t.Logf("stats=%v 判定依据: 总量守恒，应为 1", stats)
	if total != 1 || stats[StatusDone] != 1 {
		t.Fatalf("conservation violated: %v", stats)
	}
}

// TestRetryBackoffMonotonic 重试次数与等待时长按策略单调变化，耗尽后进入死信。
func TestRetryBackoffMonotonic(t *testing.T) {
	q := openTestQueue(t, Options{
		LeaseDuration: time.Minute,
		Backoff:       ExponentialBackoff(10*time.Millisecond, time.Second),
	})
	if _, _, err := q.Enqueue(Task{ID: "t1", MaxRetries: 3}); err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	prevDelay := time.Duration(-1)
	for i := 1; i <= 4; i++ { // 1 次首投 + 3 次重试 = 4 次投递后耗尽
		claimed, err := q.Claim("c1")
		if err != nil || claimed == nil {
			t.Fatalf("claim round %d: %v %v", i, claimed, err)
		}
		cause := NewRetryableError(fmt.Errorf("flaky failure %d", i))
		if err := q.Nack("c1", "t1", cause); err != nil {
			t.Fatalf("nack round %d: %v", i, err)
		}
		got, _ := q.Get("t1")
		delay := got.NotBefore.Sub(q.now())
		t.Logf("task=%s attempt=%d status=%s last_error=%q delay≈%s 判定依据: 退避时长应随重试次数单调不减",
			got.ID, got.Attempts, got.Status, got.LastError, delay)
		if i <= 3 {
			if got.Status != StatusPending {
				t.Fatalf("round %d: status=%s, want pending", i, got.Status)
			}
			if delay < prevDelay {
				t.Fatalf("round %d: backoff not monotonic: %s < %s", i, delay, prevDelay)
			}
			prevDelay = delay
			// 直接清零等待时间，避免测试中真实睡眠。
			q.mu.Lock()
			q.tasks["t1"].NotBefore = q.now()
			q.mu.Unlock()
		} else {
			if got.Status != StatusDead {
				t.Fatalf("round %d: status=%s, want dead", i, got.Status)
			}
			t.Logf("task=%s dead_reason=%q 判定依据: 重试耗尽应进入死信并保留原因", got.ID, got.DeadReason)
			if got.DeadReason == "" || got.LastError == "" {
				t.Fatalf("dead letter missing reason: %+v", got)
			}
		}
	}

	// 死信任务不得再被投递。
	if t2, _ := q.Claim("c1"); t2 != nil {
		t.Fatalf("dead task was redelivered")
	}
}

// TestNonRetryableGoesStraightToDead 不可重试失败与可重试失败分别处理。
func TestNonRetryableGoesStraightToDead(t *testing.T) {
	q := openTestQueue(t, Options{LeaseDuration: time.Minute})
	if _, _, err := q.Enqueue(Task{ID: "t1", MaxRetries: 5}); err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	if _, err := q.Claim("c1"); err != nil {
		t.Fatalf("claim: %v", err)
	}
	if err := q.Nack("c1", "t1", errors.New("permanent: bad payload")); err != nil {
		t.Fatalf("nack: %v", err)
	}
	got, _ := q.Get("t1")
	t.Logf("task=%s attempt=%d status=%s dead_reason=%q 判定依据: 不可重试失败应直接死信，不消耗重试",
		got.ID, got.Attempts, got.Status, got.DeadReason)
	if got.Status != StatusDead {
		t.Fatalf("status=%s, want dead", got.Status)
	}
	if got.DeadReason != "non_retryable: permanent: bad payload" {
		t.Fatalf("dead_reason=%q", got.DeadReason)
	}
	stats := q.Stats()
	if stats[StatusDead] != 1 || stats[StatusPending] != 0 {
		t.Fatalf("unexpected stats: %v", stats)
	}
}
