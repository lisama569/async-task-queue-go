package queue

import (
	"errors"
	"fmt"
	"testing"
	"time"
)

// failOnce 领取当前唯一可投递任务并按 cause 失败。
func failOnce(t *testing.T, q *Queue, consumer, cause string, nonRetry bool) {
	t.Helper()
	tk := mustClaim(t, q, consumer)
	var err error = errors.New(cause)
	if nonRetry {
		err = NonRetryable(err)
	}
	if e := q.Fail(tk.ID, consumer, err); e != nil {
		t.Fatalf("fail %q: %v", tk.ID, e)
	}
}

// deadTask 投递单个任务并让它重试耗尽进入死信，返回死信快照。
// clk 可为 nil（墙钟，配合较长 BaseBackoff 不现实，故用例统一传假时钟）。
func deadTask(t *testing.T, q *Queue, id, payload string, maxAttempts int, clk *fakeClock) *Task {
	t.Helper()
	mustEnqueue(t, q, id, payload)
	backoff := q.policy.BaseBackoff
	for i := 1; i <= maxAttempts; i++ {
		failOnce(t, q, fmt.Sprintf("worker-%d", i), fmt.Sprintf("err-%d", i), false)
		if i < maxAttempts {
			if clk != nil {
				clk.Advance(backoff + time.Millisecond)
			}
		}
	}
	return assertState(t, q, id, StateDead, "")
}

// 重跑保留历史：失败原因、累计投递次数、失败条数都不洗白；轮内计数重置。
func TestRequeuePreservesHistory(t *testing.T) {
	clk := newFakeClock()
	q := testQueue(t, RetryPolicy{MaxAttempts: 2, BaseBackoff: time.Millisecond},
		WithClock(clk))
	dead := deadTask(t, q, "t1", "payload-t1", 2, clk)
	if dead.TotalAttempts != 2 || len(dead.Failures) != 2 {
		t.Fatalf("precondition: total=%d failures=%d", dead.TotalAttempts, len(dead.Failures))
	}

	if err := q.RequeueDeadTask("t1"); err != nil {
		t.Fatalf("requeue: %v", err)
	}
	tk, _ := q.Get("t1")
	if tk.State != StatePending || tk.Attempts != 0 {
		t.Fatalf("after requeue state=%s attempts=%d", tk.State, tk.Attempts)
	}
	if tk.TotalAttempts != 2 {
		t.Fatalf("total attempts washed: %d", tk.TotalAttempts)
	}
	if len(tk.Failures) != 2 || tk.Failures[0].Error != "err-1" || tk.Failures[1].Error != "err-2" {
		t.Fatalf("failure history washed: %+v", tk.Failures)
	}
	if tk.LastError != "err-2" || tk.Requeues != 1 || tk.LastReason != ReasonRequeued {
		t.Fatalf("metadata wrong: last_error=%q requeues=%d reason=%s",
			tk.LastError, tk.Requeues, tk.LastReason)
	}

	// 再成功一次：累计投递次数变 3，历史仍在。
	claimed := mustClaim(t, q, "retry-worker")
	if claimed.ID != "t1" || claimed.Attempts != 1 || claimed.TotalAttempts != 3 {
		t.Fatalf("reclaimed attempts=%d total=%d", claimed.Attempts, claimed.TotalAttempts)
	}
	if err := q.Ack("t1", "retry-worker"); err != nil {
		t.Fatalf("ack: %v", err)
	}
	done, _ := q.Get("t1")
	if done.State != StateDone || done.TotalAttempts != 3 || len(done.Failures) != 2 {
		t.Fatalf("done snapshot wrong: %+v", done)
	}
}

// 重跑后继续失败：按原 MaxAttempts 重新计数，用尽后再次进死信，不能无限复活。
func TestRequeueThenExhaustAgain(t *testing.T) {
	clk := newFakeClock()
	policy := RetryPolicy{MaxAttempts: 2, BaseBackoff: time.Millisecond}
	q := testQueue(t, policy, WithClock(clk))
	deadTask(t, q, "t1", "p", 2, clk)

	if err := q.RequeueDeadTask("t1"); err != nil {
		t.Fatalf("requeue: %v", err)
	}
	for i := 1; i <= 2; i++ {
		tk := mustClaim(t, q, fmt.Sprintf("new-%d", i))
		if tk.Attempts != i {
			t.Fatalf("round attempt = %d, want %d", tk.Attempts, i)
		}
		if err := q.Fail("t1", tk.Owner, fmt.Errorf("again-%d", i)); err != nil {
			t.Fatalf("fail: %v", err)
		}
		if i < 2 {
			clk.Advance(2 * time.Millisecond)
		}
	}
	tk := assertState(t, q, "t1", StateDead, "")
	if tk.LastReason != ReasonRetryExceed || tk.Attempts != 2 {
		t.Fatalf("second round: reason=%s attempts=%d", tk.LastReason, tk.Attempts)
	}
	if tk.TotalAttempts != 4 || len(tk.Failures) != 4 || tk.Requeues != 1 {
		t.Fatalf("history after re-exhaustion: total=%d failures=%d requeues=%d",
			tk.TotalAttempts, len(tk.Failures), tk.Requeues)
	}
	if tk.Failures[2].Error != "again-1" || tk.Failures[3].Error != "again-2" {
		t.Fatalf("new failures not appended: %+v", tk.Failures)
	}

	// 可以再次人工捞回（第二次重跑），但依旧是重新计数、再耗尽再死信。
	if err := q.RequeueDeadTask("t1"); err != nil {
		t.Fatalf("second requeue: %v", err)
	}
	if tk2, _ := q.Get("t1"); tk2.Requeues != 2 || tk2.Attempts != 0 {
		t.Fatalf("second requeue state wrong: %+v", tk2)
	}
}

// 不该重跑的情况：不存在、已完成、在途中、等待重试，全部明确拒绝且状态不变。
func TestRequeueRejectsInvalidStates(t *testing.T) {
	clk := newFakeClock()
	q := testQueue(t, RetryPolicy{MaxAttempts: 2, BaseBackoff: time.Millisecond},
		WithClock(clk))

	if err := q.RequeueDeadTask("ghost"); !errors.Is(err, ErrTaskNotFound) {
		t.Fatalf("missing err=%v", err)
	}

	mustEnqueue(t, q, "done", "p")
	mustClaim(t, q, "w")
	if err := q.Ack("done", "w"); err != nil {
		t.Fatalf("ack: %v", err)
	}
	if err := q.RequeueDeadTask("done"); !errors.Is(err, ErrNotDead) {
		t.Fatalf("done task requeue err=%v", err)
	}

	mustEnqueue(t, q, "busy", "p")
	mustClaim(t, q, "w") // 保持在途
	if err := q.RequeueDeadTask("busy"); !errors.Is(err, ErrNotDead) {
		t.Fatalf("in-flight task requeue err=%v", err)
	}
	assertState(t, q, "busy", StateInFlight, "w") // 不能抢占持有者

	mustEnqueue(t, q, "waiting", "p")
	mustClaim(t, q, "w")
	if err := q.Fail("waiting", "w", errors.New("retryable")); err != nil {
		t.Fatalf("fail: %v", err)
	}
	if err := q.RequeueDeadTask("waiting"); !errors.Is(err, ErrNotDead) {
		t.Fatalf("retry_wait task requeue err=%v", err)
	}
	assertState(t, q, "waiting", StateRetryWait, "")
}

// 重跑前后任务总量守恒：重跑只是状态迁移，不新增也不消失。
func TestRequeueConservesTotal(t *testing.T) {
	q := testQueue(t, RetryPolicy{MaxAttempts: 2})
	deadTask(t, q, "d0", "p0", 2, nil)
	deadTask(t, q, "d1", "p1", 2, nil)

	before := q.Stats()
	totalBefore := 0
	for _, n := range before {
		totalBefore += n
	}
	if totalBefore != 2 || before[StateDead] != 2 {
		t.Fatalf("setup stats wrong: %v", before)
	}

	rep, err := q.RequeueDead(RequeueFilter{})
	if err != nil {
		t.Fatalf("requeue: %v", err)
	}
	if len(rep.Requeued) != 2 {
		t.Fatalf("requeued = %v", rep.Requeued)
	}
	after := q.Stats()
	totalAfter := 0
	for _, n := range after {
		totalAfter += n
	}
	if totalAfter != totalBefore {
		t.Fatalf("total changed: before=%d after=%d", totalBefore, totalAfter)
	}
	if after[StatePending] != 2 || after[StateDead] != 0 {
		t.Fatalf("stats after requeue wrong: %v", after)
	}
}

// 容量满时死信重跑也被拒绝；单任务返回 ErrQueueFull，批量计入跳过明细。
func TestRequeueRespectsCapacity(t *testing.T) {
	q := testQueue(t, RetryPolicy{MaxAttempts: 2}, WithCapacity(3))
	deadTask(t, q, "d0", "p0", 2, nil) // 此时 1 个 dead，active=0
	deadTask(t, q, "d1", "p1", 2, nil)

	// 再放 3 个未结束任务占满额度。
	mustEnqueue(t, q, "a0", "p")
	mustEnqueue(t, q, "a1", "p")
	mustEnqueue(t, q, "a2", "p")

	if err := q.RequeueDeadTask("d0"); !errors.Is(err, ErrQueueFull) {
		t.Fatalf("single requeue when full err=%v", err)
	}
	assertState(t, q, "d0", StateDead, "") // 拒绝后仍是死信

	rep, err := q.RequeueDead(RequeueFilter{})
	if err != nil {
		t.Fatalf("batch requeue: %v", err)
	}
	if len(rep.Requeued) != 0 || len(rep.Skipped) != 2 {
		t.Fatalf("when full: requeued=%v skipped=%+v", rep.Requeued, rep.Skipped)
	}
	for _, s := range rep.Skipped {
		if s.Reason != SkipQueueFull {
			t.Fatalf("skip %s reason=%s", s.ID, s.Reason)
		}
	}

	// 完成一个在途任务腾出额度后可以重跑一个。
	tk := mustClaim(t, q, "w")
	if err := q.Ack(tk.ID, "w"); err != nil {
		t.Fatalf("ack: %v", err)
	}
	if err := q.RequeueDeadTask("d0"); err != nil {
		t.Fatalf("requeue after freeing slot: %v", err)
	}
	assertState(t, q, "d0", StatePending, "")
}

// 批量按条件筛选重跑：命中的重跑，未命中的明确跳过；不存在的 ID 也报告。
func TestRequeueByFilter(t *testing.T) {
	q := testQueue(t, RetryPolicy{MaxAttempts: 2})
	deadTask(t, q, "alpha", "ALPHA-123", 2, nil) // worker-2 最后持有
	deadTask(t, q, "beta", "beta-456", 2, nil)
	// gamma：不可重试直接进死信。
	mustEnqueue(t, q, "gamma", "gamma-789")
	failOnce(t, q, "specialist", "poison", true)

	// 按 payload 子串。
	rep, err := q.RequeueDead(RequeueFilter{PayloadContains: "ALPHA"})
	if err != nil {
		t.Fatalf("requeue: %v", err)
	}
	if len(rep.Requeued) != 1 || rep.Requeued[0] != "alpha" {
		t.Fatalf("payload filter: %+v", rep)
	}

	// 按进入死信原因。
	rep, err = q.RequeueDead(RequeueFilter{Reason: ReasonNonRetry})
	if err != nil {
		t.Fatalf("requeue: %v", err)
	}
	if len(rep.Requeued) != 1 || rep.Requeued[0] != "gamma" {
		t.Fatalf("reason filter: %+v", rep)
	}

	// 显式 ID：一个已重跑（pending）跳过 not_dead，一个不存在跳过 not_found，
	// 剩余死信 beta 命中重跑。
	rep, err = q.RequeueDead(RequeueFilter{IDs: []string{"alpha", "beta", "ghost"}})
	if err != nil {
		t.Fatalf("requeue: %v", err)
	}
	if len(rep.Requeued) != 1 || rep.Requeued[0] != "beta" {
		t.Fatalf("id filter requeued: %+v", rep)
	}
	skip := map[string]string{}
	for _, s := range rep.Skipped {
		skip[s.ID] = s.Reason
	}
	if skip["alpha"] != SkipNotDead || skip["ghost"] != SkipNotFound {
		t.Fatalf("id filter skips wrong: %+v", skip)
	}
}

// 批量重跑同样保留失败历史，且只影响死信任务。
func TestRequeueBatchKeepsHistory(t *testing.T) {
	q := testQueue(t, RetryPolicy{MaxAttempts: 1})
	deadTask(t, q, "d0", "p0", 1, nil)
	rep, err := q.RequeueDead(RequeueFilter{})
	if err != nil {
		t.Fatalf("requeue: %v", err)
	}
	if len(rep.Requeued) != 1 {
		t.Fatalf("rep = %+v", rep)
	}
	tk, _ := q.Get("d0")
	if tk.TotalAttempts != 1 || len(tk.Failures) != 1 || tk.Failures[0].Error != "err-1" {
		t.Fatalf("history lost in batch: %+v", tk)
	}
}
