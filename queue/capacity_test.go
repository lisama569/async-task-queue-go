package queue

import (
	"errors"
	"fmt"
	"testing"
	"time"
)

// 达到容量上限后新投递被明确拒绝（ErrQueueFull），任务不会被写入。
func TestEnqueueRejectedAtCapacity(t *testing.T) {
	q := testQueue(t, RetryPolicy{MaxAttempts: 1}, WithCapacity(2))
	mustEnqueue(t, q, "a", "p")
	mustEnqueue(t, q, "b", "p")

	err := q.Enqueue("c", "p")
	if !errors.Is(err, ErrQueueFull) {
		t.Fatalf("err = %v, want ErrQueueFull", err)
	}
	if _, ok := q.Get("c"); ok {
		t.Fatal("rejected task must not be stored")
	}
	st := q.Stats()
	if st[StatePending] != 2 {
		t.Fatalf("stats after reject: %v", st)
	}
}

// 完成/死信释放额度：历史任务不占容量，之后新投递成功。
func TestFinishedTasksDoNotConsumeCapacity(t *testing.T) {
	clk := newFakeClock()
	policy := RetryPolicy{MaxAttempts: 2, BaseBackoff: time.Millisecond}
	q := testQueue(t, policy, WithCapacity(2), WithClock(clk))

	mustEnqueue(t, q, "a", "p")
	mustEnqueue(t, q, "b", "p")
	// 领取并完成一个（a/b 同刻入队，按实际领取结果编排）。
	first := mustClaim(t, q, "w")
	rest := "b"
	if first.ID == "b" {
		rest = "a"
	}
	if err := q.Ack(first.ID, "w"); err != nil {
		t.Fatalf("ack: %v", err)
	}
	clk.Advance(time.Second) // 让后续投递时间戳严格晚于存量任务
	// 队列里有 1 done + 1 pending，active=1，应还能再投 1 个。
	mustEnqueue(t, q, "extra1", "p")
	if err := q.Enqueue("extra2", "p"); !errors.Is(err, ErrQueueFull) {
		t.Fatalf("err = %v, want full", err)
	}

	// 把剩余的原始任务处理到死信，释放第二个额度。
	nxt := mustClaim(t, q, "w2")
	if nxt.ID != rest {
		t.Fatalf("got %s, want %s", nxt.ID, rest)
	}
	if err := q.Fail(rest, "w2", NonRetryable(errors.New("x"))); err != nil {
		t.Fatalf("fail: %v", err)
	}
	mustEnqueue(t, q, "extra2", "p") // 现在成功
	if err := q.Enqueue("extra3", "p"); !errors.Is(err, ErrQueueFull) {
		t.Fatalf("err = %v, want full", err)
	}
}

// 重试等待、在途都算未结束、占额度；只有 done/dead 不算。
func TestInFlightAndRetryWaitCountAgainstCapacity(t *testing.T) {
	clk := newFakeClock()
	q := testQueue(t,
		RetryPolicy{MaxAttempts: 3, BaseBackoff: time.Millisecond},
		WithCapacity(2), WithClock(clk))
	mustEnqueue(t, q, "a", "p")
	mustEnqueue(t, q, "b", "p")
	inFlight := mustClaim(t, q, "w") // 一个任务变 in_flight，仍占额度
	if err := q.Enqueue("c", "p"); !errors.Is(err, ErrQueueFull) {
		t.Fatalf("in-flight should count, err=%v", err)
	}
	// 另一个任务失败进入 retry_wait，仍占额度。
	waiting := mustClaim(t, q, "w")
	if waiting.ID == inFlight.ID {
		t.Fatalf("same task claimed twice: %s", waiting.ID)
	}
	if err := q.Fail(waiting.ID, "w", errors.New("soft")); err != nil {
		t.Fatalf("fail: %v", err)
	}
	if err := q.Enqueue("c", "p"); !errors.Is(err, ErrQueueFull) {
		t.Fatalf("retry_wait should count, err=%v", err)
	}
}

// 清理历史只删 done/dead，在途任务与统计口径不能乱。
func TestPurgeFinishedKeepsActiveAndStats(t *testing.T) {
	clk := newFakeClock()
	q := testQueue(t, RetryPolicy{MaxAttempts: 2, BaseBackoff: time.Millisecond},
		WithCapacity(10), WithClock(clk))

	mustEnqueue(t, q, "d0", "p") // -> done
	mustClaim(t, q, "w")
	// 仅当领到 d0（只有一个任务）安全。
	if err := q.Ack("d0", "w"); err != nil {
		t.Fatalf("ack d0: %v", err)
	}

	mustEnqueue(t, q, "x0", "p") // -> dead (non-retryable)
	tk := mustClaim(t, q, "w")
	if tk.ID != "x0" {
		t.Fatalf("got %s", tk.ID)
	}
	if err := q.Fail("x0", "w", NonRetryable(errors.New("poison"))); err != nil {
		t.Fatalf("fail: %v", err)
	}

	mustEnqueue(t, q, "active", "p") // 保持 pending

	n, err := q.PurgeFinished()
	if err != nil || n != 2 {
		t.Fatalf("purge n=%d err=%v", n, err)
	}
	if _, ok := q.Get("d0"); ok {
		t.Fatal("done task should be purged")
	}
	if _, ok := q.Get("x0"); ok {
		t.Fatal("dead task should be purged")
	}
	st := q.Stats()
	if st[StatePending] != 1 || st[StateDone] != 0 || st[StateDead] != 0 {
		t.Fatalf("stats after purge wrong: %v", st)
	}
	// 在途任务完好可领取、可完成。
	a := mustClaim(t, q, "w")
	if a.ID != "active" {
		t.Fatalf("active task damaged: %s", a.ID)
	}
	if err := q.Ack("active", "w"); err != nil {
		t.Fatalf("ack: %v", err)
	}
	// 容量额度未受清理影响：当前 active=0，可重新投满。
	for i := 0; i < 10; i++ {
		mustEnqueue(t, q, fmt.Sprintf("n%d", i), "p")
	}
	if err := q.Enqueue("overflow", "p"); !errors.Is(err, ErrQueueFull) {
		t.Fatalf("capacity after purge err=%v", err)
	}
}

// 容量为 0（默认）表示不限制，回归旧行为。
func TestZeroCapacityMeansUnlimited(t *testing.T) {
	q := testQueue(t, RetryPolicy{MaxAttempts: 1})
	for i := 0; i < 100; i++ {
		mustEnqueue(t, q, fmt.Sprintf("t%d", i), "p")
	}
	if got := q.Stats()[StatePending]; got != 100 {
		t.Fatalf("pending=%d", got)
	}
}
