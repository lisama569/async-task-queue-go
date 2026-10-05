package queue

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"
)

func TestEnqueueDuplicateRejected(t *testing.T) {
	q := testQueue(t, RetryPolicy{MaxAttempts: 2, BaseBackoff: time.Millisecond})
	mustEnqueue(t, q, "t1", "p1")
	if err := q.Enqueue("t1", "p2"); !errors.Is(err, ErrTaskExists) {
		t.Fatalf("duplicate enqueue err = %v, want ErrTaskExists", err)
	}
	tk, _ := q.Get("t1")
	if tk.Payload != "p1" {
		t.Fatalf("payload changed on duplicate: %q", tk.Payload)
	}
}

func TestClaimAndAck(t *testing.T) {
	q := testQueue(t, RetryPolicy{MaxAttempts: 1})
	mustEnqueue(t, q, "t1", "p1")
	tk := mustClaim(t, q, "c1")
	if tk.ID != "t1" || tk.Attempts != 1 || tk.TotalAttempts != 1 || tk.Owner != "c1" {
		t.Fatalf("unexpected claim snapshot: %+v", tk)
	}
	if got, _ := q.Claim("c2"); got != nil {
		t.Fatalf("in-flight task must not be claimed again: %+v", got)
	}
	if err := q.Ack("t1", "c1"); err != nil {
		t.Fatalf("ack: %v", err)
	}
	assertState(t, q, "t1", StateDone, "")
}

func TestAckRejectsNonOwner(t *testing.T) {
	q := testQueue(t, RetryPolicy{MaxAttempts: 1})
	mustEnqueue(t, q, "t1", "p1")
	mustClaim(t, q, "c1")
	if err := q.Ack("t1", "c2"); err == nil {
		t.Fatal("non-owner ack must fail")
	}
	assertState(t, q, "t1", StateInFlight, "c1")
}

func TestRetryBackoffThenDead(t *testing.T) {
	policy := RetryPolicy{MaxAttempts: 3, BaseBackoff: 10 * time.Millisecond}
	clk := newFakeClock()
	q := testQueue(t, policy, WithClock(clk), WithLeaseTimeout(time.Hour))
	mustEnqueue(t, q, "t1", "p1")

	for i := 1; i <= 3; i++ {
		tk := mustClaim(t, q, fmt.Sprintf("c%d", i))
		if tk.Attempts != i {
			t.Fatalf("attempt = %d, want %d", tk.Attempts, i)
		}
		err := q.Fail("t1", tk.Owner, fmt.Errorf("boom-%d", i))
		if err != nil {
			t.Fatalf("fail: %v", err)
		}
		if i < 3 {
			assertState(t, q, "t1", StateRetryWait, "")
			// 未到退避时间不可领取。
			if got, _ := q.Claim("other"); got != nil {
				t.Fatal("retry_wait task claimed before NotBefore")
			}
			clk.Advance(policy.Backoff(i) + time.Millisecond)
		}
	}
	tk := assertState(t, q, "t1", StateDead, "")
	if tk.LastReason != ReasonRetryExceed {
		t.Fatalf("reason = %s, want %s", tk.LastReason, ReasonRetryExceed)
	}
	if tk.Attempts != 3 || tk.TotalAttempts != 3 || len(tk.Failures) != 3 {
		t.Fatalf("history wrong: attempts=%d total=%d failures=%d",
			tk.Attempts, tk.TotalAttempts, len(tk.Failures))
	}
	if tk.Failures[0].Error != "boom-1" || tk.Failures[2].Owner != "c3" {
		t.Fatalf("failure history not preserved: %+v", tk.Failures)
	}
}

func TestNonRetryableGoesStraightToDead(t *testing.T) {
	q := testQueue(t, RetryPolicy{MaxAttempts: 5})
	mustEnqueue(t, q, "t1", "p1")
	mustClaim(t, q, "c1")
	if err := q.Fail("t1", "c1", NonRetryable(errors.New("bad payload"))); err != nil {
		t.Fatalf("fail: %v", err)
	}
	if tk2 := assertState(t, q, "t1", StateDead, ""); tk2.LastReason != ReasonNonRetry {
		t.Fatalf("reason = %s", tk2.LastReason)
	}
}

func TestCrashRecoverMarksReason(t *testing.T) {
	q := testQueue(t, RetryPolicy{MaxAttempts: 3})
	mustEnqueue(t, q, "t1", "p1")
	mustClaim(t, q, "c1")
	n, err := q.Recover()
	if err != nil || n != 1 {
		t.Fatalf("recover n=%d err=%v", n, err)
	}
	tk := assertState(t, q, "t1", StatePending, "")
	if tk.LastReason != ReasonCrash {
		t.Fatalf("reason = %s, want crash", tk.LastReason)
	}
	// 恢复后可被重新领取，投递次数累加。
	tk2 := mustClaim(t, q, "c2")
	if tk2.Attempts != 2 || tk2.TotalAttempts != 2 {
		t.Fatalf("after recovery attempts=%d total=%d", tk2.Attempts, tk2.TotalAttempts)
	}
}

func TestConcurrentConsumersNoDoubleRun(t *testing.T) {
	q := testQueue(t, RetryPolicy{MaxAttempts: 1})
	const n = 50
	for i := 0; i < n; i++ {
		mustEnqueue(t, q, fmt.Sprintf("t%d", i), "p")
	}
	var mu sync.Mutex
	running := map[string]string{}
	violations := 0
	var wg sync.WaitGroup
	ctx, cancel := context.WithCancel(context.Background())
	for ci := 0; ci < 8; ci++ {
		c := NewConsumer(q, fmt.Sprintf("c%d", ci))
		c.PollPeriod = time.Millisecond
		wg.Add(1)
		go func() {
			defer wg.Done()
			c.Run(ctx, func(_ context.Context, tk *Task) error {
				mu.Lock()
				if owner, ok := running[tk.ID]; ok {
					t.Errorf("task %s running by %s and %s simultaneously", tk.ID, owner, c.ID)
					violations++
				}
				running[tk.ID] = c.ID
				mu.Unlock()
				time.Sleep(2 * time.Millisecond)
				mu.Lock()
				delete(running, tk.ID)
				mu.Unlock()
				return nil
			})
		}()
	}
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		st := q.Stats()
		if st[StateDone] == n {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	cancel()
	wg.Wait()
	if got := q.Stats()[StateDone]; got != n {
		t.Fatalf("done = %d, want %d (violations=%d)", got, n, violations)
	}
}
