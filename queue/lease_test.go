package queue

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"
)

// 长任务持续续期：租约窗口内绝不允许被 SweepStale 回收或被别人抢走。
func TestHeartbeatKeepsLongTaskInFlight(t *testing.T) {
	clk := newFakeClock()
	const lease = 100 * time.Millisecond
	q := testQueue(t, RetryPolicy{MaxAttempts: 3},
		WithClock(clk), WithLeaseTimeout(lease))
	mustEnqueue(t, q, "long", "work")
	tk := mustClaim(t, q, "worker-1")

	// 模拟长任务：每次推进 < lease，并在中途续期。
	for i := 0; i < 10; i++ {
		clk.Advance(lease / 3)
		if err := q.Heartbeat("long", "worker-1"); err != nil {
			t.Fatalf("heartbeat %d: %v", i, err)
		}
		rep, err := q.SweepStale()
		if err != nil {
			t.Fatalf("sweep: %v", err)
		}
		if len(rep.Recovered) != 0 || rep.Scanned != 1 {
			t.Fatalf("heartbeated task swept: %+v", rep)
		}
		// 别的消费者也领不走。
		if got, _ := q.Claim("worker-2"); got != nil {
			t.Fatalf("heartbeated task claimed by another consumer: %+v", got)
		}
	}
	assertState(t, q, "long", StateInFlight, "worker-1")
	if tk.Attempts != 1 {
		t.Fatalf("attempts changed by heartbeat: %d", tk.Attempts)
	}
}

// 持有者失联：超过租约没有任何心跳才回收，且标记 lost_contact。
func TestSweepReclaimsOnlyAfterLeaseTimeout(t *testing.T) {
	clk := newFakeClock()
	const lease = 100 * time.Millisecond
	q := testQueue(t, RetryPolicy{MaxAttempts: 3},
		WithClock(clk), WithLeaseTimeout(lease))
	mustEnqueue(t, q, "ghost", "work")
	mustClaim(t, q, "dead-worker")

	clk.Advance(lease) // 恰好等于超时：仍不回收（要求严格大于）
	if rep, _ := q.SweepStale(); len(rep.Recovered) != 0 {
		t.Fatalf("task reclaimed at exact lease boundary: %+v", rep)
	}
	clk.Advance(time.Millisecond)
	rep, err := q.SweepStale()
	if err != nil {
		t.Fatalf("sweep: %v", err)
	}
	if len(rep.Recovered) != 1 {
		t.Fatalf("recovered = %d, want 1", len(rep.Recovered))
	}
	st := rep.Recovered[0]
	if st.ID != "ghost" || st.Owner != "dead-worker" || st.Attempts != 1 {
		t.Fatalf("sweep detail wrong: %+v", st)
	}
	tk := assertState(t, q, "ghost", StatePending, "")
	if tk.LastReason != ReasonLostContact {
		t.Fatalf("reason = %s, want lost_contact", tk.LastReason)
	}
}

// 心跳停止（持有者卡死）后，下一次超时被回收；
// 与显式进程崩溃恢复（crash_recovered）在记录上可区分。
func TestLostContactReasonDistinctFromCrash(t *testing.T) {
	clk := newFakeClock()
	q := testQueue(t, RetryPolicy{MaxAttempts: 3},
		WithClock(clk), WithLeaseTimeout(time.Minute))

	mustEnqueue(t, q, "a", "p")
	mustEnqueue(t, q, "b", "p")
	first := mustClaim(t, q, "w1") // 先领取一个（心跳超时失联候选）
	lostID, pendingID := first.ID, "a"
	if lostID == "a" {
		pendingID = "b"
	}

	// 被领取的任务：心跳超时失联（另一个仍 pending，扫描不动它）。
	clk.Advance(time.Minute + time.Second)
	if _, err := q.SweepStale(); err != nil {
		t.Fatalf("sweep: %v", err)
	}
	ta, _ := q.Get(lostID)
	if ta.LastReason != ReasonLostContact {
		t.Fatalf("%s reason = %s", lostID, ta.LastReason)
	}

	// 再领取另一个，随后模拟进程明确退出做显式恢复。
	mustClaim(t, q, "w2")
	if _, err := q.Recover(); err != nil {
		t.Fatalf("recover: %v", err)
	}
	tb, _ := q.Get(pendingID)
	if tb.LastReason != ReasonCrash {
		t.Fatalf("%s reason = %s, want crash_recovered", pendingID, tb.LastReason)
	}
}

func TestHeartbeatRejectsNonOwnerAndWrongState(t *testing.T) {
	clk := newFakeClock()
	q := testQueue(t, RetryPolicy{MaxAttempts: 3},
		WithClock(clk), WithLeaseTimeout(time.Minute))
	mustEnqueue(t, q, "t1", "p")
	mustClaim(t, q, "owner")

	if err := q.Heartbeat("t1", "intruder"); err == nil {
		t.Fatal("non-owner heartbeat must fail")
	}
	if err := q.Heartbeat("missing", "owner"); !errors.Is(err, ErrTaskNotFound) {
		t.Fatalf("missing heartbeat err = %v, want ErrTaskNotFound", err)
	}
	if err := q.Ack("t1", "owner"); err != nil {
		t.Fatalf("ack: %v", err)
	}
	if err := q.Heartbeat("t1", "owner"); err == nil {
		t.Fatal("heartbeat on done task must fail")
	}
}

// 失联回收后只能被一个消费者重新领取；原持有者即使晚到也无法再 Ack，
// 配合消费者级自动续期，同一任务任何时刻不会被两个消费者同时执行。
func TestSweepThenNoDoubleExecution(t *testing.T) {
	clk := newFakeClock()
	q := testQueue(t, RetryPolicy{MaxAttempts: 3},
		WithClock(clk), WithLeaseTimeout(50*time.Millisecond))
	mustEnqueue(t, q, "t1", "p")
	mustClaim(t, q, "slow")

	clk.Advance(time.Second)
	if _, err := q.SweepStale(); err != nil {
		t.Fatalf("sweep: %v", err)
	}
	second := mustClaim(t, q, "fast")
	if second.Attempts != 2 || second.TotalAttempts != 2 {
		t.Fatalf("redelivery attempts=%d total=%d", second.Attempts, second.TotalAttempts)
	}
	// 原持有者的迟到确认必须被拒绝。
	if err := q.Ack("t1", "slow"); err == nil {
		t.Fatal("stale owner ack after reclamation must fail")
	}
	// 同一时刻不能有第二个领取者。
	if got, _ := q.Claim("third"); got != nil {
		t.Fatal("task claimed twice after reclamation")
	}
	if err := q.Ack("t1", "fast"); err != nil {
		t.Fatalf("new owner ack: %v", err)
	}
	assertState(t, q, "t1", StateDone, "")
}

// 消费者自动续期端到端：处理时间远超租约，靠 HeartbeatPeriod 保活，
// 处理期间并发 SweepStale 也不会把它抢走。
func TestConsumerAutoHeartbeatSurvivesLongTask(t *testing.T) {
	clk := newFakeClock()
	lease := 40 * time.Millisecond
	q := testQueue(t, RetryPolicy{MaxAttempts: 3},
		WithClock(clk), WithLeaseTimeout(lease))
	mustEnqueue(t, q, "t1", "p")

	c := NewConsumer(q, "auto")
	c.PollPeriod = time.Millisecond
	c.HeartbeatPeriod = 10 * time.Millisecond

	var wg sync.WaitGroup
	ctx, cancel := context.WithCancel(context.Background())
	wg.Add(1)
	go func() {
		defer wg.Done()
		c.Run(ctx, func(_ context.Context, tk *Task) error {
			// 用墙钟等待真实心跳 ticker，同时推进假时钟模拟现实时间流逝。
			deadline := time.Now().Add(250 * time.Millisecond)
			for time.Now().Before(deadline) {
				time.Sleep(5 * time.Millisecond)
				clk.Advance(7 * time.Millisecond)
				rep, err := q.SweepStale()
				if err != nil || len(rep.Recovered) != 0 {
					t.Errorf("long task swept while heartbeating: %+v err=%v", rep, err)
				}
			}
			if tk.Owner != "auto" {
				t.Errorf("owner changed: %s", tk.Owner)
			}
			return nil
		})
	}()

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if got := q.Stats()[StateDone]; got == 1 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	cancel()
	wg.Wait()
	if got := q.Stats()[StateDone]; got != 1 {
		t.Fatalf("done = %d, want 1", got)
	}
	ta, _ := q.Get("t1")
	if ta.TotalAttempts != 1 {
		t.Fatalf("task was redelivered despite heartbeats: total=%d", ta.TotalAttempts)
	}
}

// 未配置自动续期的消费者，任务处理超过租约且无心跳：按失联回收，可重新投递。
func TestConsumerWithoutHeartbeatIsReclaimed(t *testing.T) {
	clk := newFakeClock()
	q := testQueue(t, RetryPolicy{MaxAttempts: 3},
		WithClock(clk), WithLeaseTimeout(50*time.Millisecond))
	mustEnqueue(t, q, fmt.Sprintf("t%d", 0), "p")
	mustClaim(t, q, "stuck")
	clk.Advance(time.Second)
	rep, _ := q.SweepStale()
	if len(rep.Recovered) != 1 || rep.Recovered[0].Owner != "stuck" {
		t.Fatalf("unexpected sweep: %+v", rep)
	}
}
