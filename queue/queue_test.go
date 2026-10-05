package queue

import (
	"context"
	"errors"
	"log"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

// testLogger 把队列内部日志接到 t.Log，跑测试时能看清每个任务的判定过程。
type testLogger struct{ t *testing.T }

func (l testLogger) Write(p []byte) (int, error) {
	l.t.Log(strings.TrimSpace(string(p)))
	return len(p), nil
}

func newTestQueue(t *testing.T, policy RetryPolicy, opts Options) *Queue {
	t.Helper()
	if opts.Logger == nil {
		opts.Logger = log.New(testLogger{t}, "", 0)
	}
	q, err := OpenWithOptions(filepath.Join(t.TempDir(), "queue.json"), policy, opts)
	if err != nil {
		t.Fatalf("open queue: %v", err)
	}
	t.Cleanup(func() { _ = q.Close() })
	return q
}

func totalOf(m map[State]int) int {
	n := 0
	for _, v := range m {
		n += v
	}
	return n
}

// ---------- 回归：既有主流程 ----------

func TestBasicFlow(t *testing.T) {
	q := newTestQueue(t, RetryPolicy{MaxAttempts: 3}, Options{})
	if err := q.Enqueue("t1", "hello"); err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	task, err := q.Claim("c1")
	if err != nil || task == nil {
		t.Fatalf("claim: task=%v err=%v", task, err)
	}
	if task.Attempts != 1 || task.Owner != "c1" || task.State != StateInFlight {
		t.Fatalf("unexpected task after claim: %+v", task)
	}
	// 同一任务在被持有期间不能被其他消费者领取
	if again, _ := q.Claim("c2"); again != nil {
		t.Fatalf("task claimed twice: %+v", again)
	}
	if err := q.Ack("t1", "c1"); err != nil {
		t.Fatalf("ack: %v", err)
	}
	got, _ := q.Get("t1")
	if got.State != StateDone {
		t.Fatalf("state=%s, want done", got.State)
	}
	if s := q.Stats(); s[StateDone] != 1 || totalOf(s) != 1 {
		t.Fatalf("stats=%v", s)
	}
}

func TestDuplicateEnqueue(t *testing.T) {
	q := newTestQueue(t, RetryPolicy{MaxAttempts: 1}, Options{})
	if err := q.Enqueue("t1", "a"); err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	if err := q.Enqueue("t1", "b"); !errors.Is(err, ErrTaskExists) {
		t.Fatalf("err=%v, want ErrTaskExists", err)
	}
	got, _ := q.Get("t1")
	if got.Payload != "a" || got.State != StatePending {
		t.Fatalf("original task mutated: %+v", got)
	}
}

func TestRetryBackoff(t *testing.T) {
	q := newTestQueue(t, RetryPolicy{MaxAttempts: 3, BaseBackoff: time.Hour}, Options{})
	_ = q.Enqueue("t1", "x")
	task, _ := q.Claim("c1")
	if err := q.Fail(task.ID, "c1", errors.New("boom")); err != nil {
		t.Fatalf("fail: %v", err)
	}
	got, _ := q.Get("t1")
	if got.State != StateRetryWait || got.LastReason != ReasonRetry || got.LastError != "boom" {
		t.Fatalf("unexpected: %+v", got)
	}
	if !got.NotBefore.After(time.Now()) {
		t.Fatalf("NotBefore should be in the future: %v", got.NotBefore)
	}
	// 退避期间不可投递
	if again, _ := q.Claim("c1"); again != nil {
		t.Fatalf("claimed during backoff: %+v", again)
	}
}

func TestRetryExhaustedGoesDead(t *testing.T) {
	q := newTestQueue(t, RetryPolicy{MaxAttempts: 2}, Options{})
	_ = q.Enqueue("t1", "x")
	for i := 1; i <= 2; i++ {
		task, _ := q.Claim("c1")
		if task == nil {
			t.Fatalf("round %d: no task", i)
		}
		_ = q.Fail(task.ID, "c1", errors.New("boom"))
	}
	got, _ := q.Get("t1")
	if got.State != StateDead || got.LastReason != ReasonRetryExceed {
		t.Fatalf("unexpected: %+v", got)
	}
	if got.Attempts != 2 || got.LastError != "boom" {
		t.Fatalf("history lost: %+v", got)
	}
}

func TestNonRetryableGoesDeadImmediately(t *testing.T) {
	q := newTestQueue(t, RetryPolicy{MaxAttempts: 5}, Options{})
	_ = q.Enqueue("t1", "x")
	task, _ := q.Claim("c1")
	_ = q.Fail(task.ID, "c1", NonRetryable(errors.New("bad payload")))
	got, _ := q.Get("t1")
	if got.State != StateDead || got.LastReason != ReasonNonRetry || got.Attempts != 1 {
		t.Fatalf("unexpected: %+v", got)
	}
}

func TestCrashRecover(t *testing.T) {
	q := newTestQueue(t, RetryPolicy{MaxAttempts: 3}, Options{})
	_ = q.Enqueue("t1", "x")
	if _, _ = q.Claim("c1"); true {
	}
	n, err := q.Recover()
	if err != nil || n != 1 {
		t.Fatalf("recover n=%d err=%v", n, err)
	}
	got, _ := q.Get("t1")
	if got.State != StatePending || got.LastReason != ReasonCrash {
		t.Fatalf("unexpected: %+v", got)
	}
	// 恢复后可被重新领取
	task, _ := q.Claim("c2")
	if task == nil || task.Attempts != 2 {
		t.Fatalf("reclaim after crash recover: %+v", task)
	}
}

// ---------- 续期与失联回收 ----------

func TestHeartbeatKeepsLongTaskAlive(t *testing.T) {
	q := newTestQueue(t, RetryPolicy{MaxAttempts: 3}, Options{LeaseTTL: 150 * time.Millisecond})
	_ = q.Enqueue("t1", "long")
	task, _ := q.Claim("c1")
	// 模拟长任务：持续续期 300ms，远超一个租约周期
	for i := 0; i < 6; i++ {
		time.Sleep(50 * time.Millisecond)
		if err := q.Heartbeat(task.ID, "c1"); err != nil {
			t.Fatalf("heartbeat %d: %v", i, err)
		}
	}
	// 持有者一直在续期，不允许被回收
	if n, _ := q.RecoverStale(); n != 0 {
		t.Fatalf("stale recovered %d tasks while heartbeating", n)
	}
	got, _ := q.Get("t1")
	if got.State != StateInFlight || got.Owner != "c1" {
		t.Fatalf("task stolen while heartbeating: %+v", got)
	}
	if err := q.Ack("t1", "c1"); err != nil {
		t.Fatalf("ack after heartbeats: %v", err)
	}
}

func TestStaleRecoveryWhenOwnerSilent(t *testing.T) {
	q := newTestQueue(t, RetryPolicy{MaxAttempts: 3}, Options{LeaseTTL: 60 * time.Millisecond})
	_ = q.Enqueue("t1", "x")
	if _, _ = q.Claim("c1"); true {
	}
	// 持有者无任何动静，租约到期
	time.Sleep(120 * time.Millisecond)
	n, err := q.RecoverStale()
	if err != nil || n != 1 {
		t.Fatalf("recover stale n=%d err=%v", n, err)
	}
	got, _ := q.Get("t1")
	// 失联回收必须与崩溃恢复区分开
	if got.State != StatePending || got.LastReason != ReasonStale {
		t.Fatalf("unexpected: %+v", got)
	}
	// 回收后可被其他消费者领取
	task, _ := q.Claim("c2")
	if task == nil || task.Owner != "c2" {
		t.Fatalf("reclaim after stale recovery failed")
	}
}

func TestStaleAndCrashReasonsAreDistinct(t *testing.T) {
	q := newTestQueue(t, RetryPolicy{MaxAttempts: 3}, Options{LeaseTTL: 60 * time.Millisecond})
	_ = q.Enqueue("stale", "x")
	_ = q.Enqueue("crash", "x")
	_, _ = q.Claim("c1") // 领取其中一个
	_, _ = q.Claim("c1") // 领取另一个
	time.Sleep(120 * time.Millisecond)
	if n, _ := q.RecoverStale(); n != 2 {
		t.Fatalf("stale n=%d", n)
	}
	for _, id := range []string{"stale", "crash"} {
		got, _ := q.Get(id)
		if got.LastReason != ReasonStale {
			t.Fatalf("%s reason=%s, want %s", id, got.LastReason, ReasonStale)
		}
	}
	// 再次领取后走崩溃恢复，原因应不同
	_, _ = q.Claim("c1")
	_, _ = q.Claim("c1")
	if n, _ := q.Recover(); n != 2 {
		t.Fatalf("crash n=%d", n)
	}
	for _, id := range []string{"stale", "crash"} {
		got, _ := q.Get(id)
		if got.LastReason != ReasonCrash {
			t.Fatalf("%s reason=%s, want %s", id, got.LastReason, ReasonCrash)
		}
	}
}

func TestHeartbeatRejectsNonOwner(t *testing.T) {
	q := newTestQueue(t, RetryPolicy{MaxAttempts: 3}, Options{})
	_ = q.Enqueue("t1", "x")
	_, _ = q.Claim("c1")
	if err := q.Heartbeat("t1", "c2"); err == nil {
		t.Fatalf("non-owner heartbeat should fail")
	}
	if err := q.Heartbeat("missing", "c1"); err == nil {
		t.Fatalf("heartbeat on missing task should fail")
	}
}

func TestConcurrentClaimSingleWinner(t *testing.T) {
	q := newTestQueue(t, RetryPolicy{MaxAttempts: 3}, Options{})
	_ = q.Enqueue("t1", "x")
	const workers = 8
	var wg sync.WaitGroup
	won := make(chan *Task, workers)
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			task, err := q.Claim(string(rune('a' + i)))
			if err == nil && task != nil {
				won <- task
			}
		}(i)
	}
	wg.Wait()
	close(won)
	winners := 0
	for range won {
		winners++
	}
	if winners != 1 {
		t.Fatalf("winners=%d, want exactly 1", winners)
	}
	got, _ := q.Get("t1")
	if got.Attempts != 1 {
		t.Fatalf("attempts=%d, want 1", got.Attempts)
	}
}

func TestConsumerAutoHeartbeatLongTask(t *testing.T) {
	q := newTestQueue(t, RetryPolicy{MaxAttempts: 3}, Options{LeaseTTL: 80 * time.Millisecond})
	_ = q.Enqueue("t1", "long")
	c := NewConsumer(q, "c1")
	c.HeartbeatPeriod = 20 * time.Millisecond

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	doneCh := make(chan struct{})
	go func() {
		defer close(doneCh)
		c.Run(ctx, func(ctx context.Context, task *Task) error {
			time.Sleep(250 * time.Millisecond) // 长任务，远超一个租约周期
			return nil
		})
	}()

	// 处理期间多次检查：持续续期的任务绝不能被回收
	time.Sleep(120 * time.Millisecond)
	for i := 0; i < 2; i++ {
		if n, _ := q.RecoverStale(); n != 0 {
			t.Fatalf("long task recovered while consumer alive (round %d)", i)
		}
		time.Sleep(80 * time.Millisecond)
	}
	// 等处理完成并确认
	deadline := time.After(2 * time.Second)
	for {
		got, _ := q.Get("t1")
		if got.State == StateDone {
			break
		}
		select {
		case <-deadline:
			t.Fatalf("task not done in time: %+v", got)
		case <-time.After(20 * time.Millisecond):
		}
	}
	cancel()
	<-doneCh
}

// ---------- 死信重跑 ----------

func driveToDead(t *testing.T, q *Queue, id, consumer string, rounds int) {
	t.Helper()
	for i := 0; i < rounds; i++ {
		task, err := q.Claim(consumer)
		if err != nil || task == nil {
			t.Fatalf("round %d: claim failed task=%v err=%v", i, task, err)
		}
		if task.ID != id {
			t.Fatalf("round %d: got %s, want %s", i, task.ID, id)
		}
		if err := q.Fail(id, consumer, errors.New("still broken")); err != nil {
			t.Fatalf("round %d: fail: %v", i, err)
		}
	}
}

func TestRequeueDeadPreservesHistory(t *testing.T) {
	q := newTestQueue(t, RetryPolicy{MaxAttempts: 2}, Options{})
	_ = q.Enqueue("t1", "x")
	driveToDead(t, q, "t1", "c1", 2)

	before, _ := q.Get("t1")
	if before.State != StateDead || before.Attempts != 2 {
		t.Fatalf("precondition: %+v", before)
	}
	if err := q.RequeueDead("t1"); err != nil {
		t.Fatalf("requeue: %v", err)
	}
	got, _ := q.Get("t1")
	if got.State != StatePending || got.LastReason != ReasonRequeue || got.Requeues != 1 {
		t.Fatalf("unexpected after requeue: %+v", got)
	}
	// 历史保留：累计投递次数与失败原因不被洗白
	if got.Attempts != 2 || got.LastError != "still broken" {
		t.Fatalf("history wiped: %+v", got)
	}
}

func TestRequeueThenExhaustAgain(t *testing.T) {
	q := newTestQueue(t, RetryPolicy{MaxAttempts: 2}, Options{})
	_ = q.Enqueue("t1", "x")
	driveToDead(t, q, "t1", "c1", 2)
	if err := q.RequeueDead("t1"); err != nil {
		t.Fatalf("requeue: %v", err)
	}
	// 重跑后重新享有完整的重试预算：第一次失败应进 retry_wait 而非直接死信
	task, _ := q.Claim("c1")
	if task == nil {
		t.Fatalf("no task after requeue")
	}
	_ = q.Fail(task.ID, "c1", errors.New("still broken"))
	got, _ := q.Get("t1")
	if got.State != StateRetryWait {
		t.Fatalf("state=%s, want retry_wait (fresh budget after requeue)", got.State)
	}
	// 再次用尽后重新进死信，不能无限复活
	driveToDead(t, q, "t1", "c1", 1)
	got, _ = q.Get("t1")
	if got.State != StateDead || got.LastReason != ReasonRetryExceed {
		t.Fatalf("unexpected: %+v", got)
	}
	if got.Attempts != 4 || got.Requeues != 1 {
		t.Fatalf("attempts=%d requeues=%d, want 4/1", got.Attempts, got.Requeues)
	}
}

func TestRequeueRejectsNonDead(t *testing.T) {
	q := newTestQueue(t, RetryPolicy{MaxAttempts: 3}, Options{})
	_ = q.Enqueue("done-task", "x")
	_ = q.Enqueue("running-task", "x")
	_ = q.Enqueue("pending-task", "x")

	t1, _ := q.Claim("c1")
	_ = q.Ack(t1.ID, "c1") // done-task -> done
	t2, _ := q.Claim("c1")
	if t2.ID != "running-task" {
		t.Fatalf("unexpected claim order: %s", t2.ID)
	} // running-task -> in_flight

	for _, id := range []string{"done-task", "running-task", "pending-task"} {
		if err := q.RequeueDead(id); err == nil {
			t.Fatalf("requeue %s should be rejected", id)
		}
	}
	if err := q.RequeueDead("missing"); err == nil {
		t.Fatalf("requeue missing task should be rejected")
	}
	// 被拒绝的重跑不得改变任何状态
	if s := q.Stats(); s[StateDone] != 1 || s[StateInFlight] != 1 || s[StatePending] != 1 {
		t.Fatalf("stats mutated: %v", s)
	}
}

func TestRequeueDeadIfConservesTotal(t *testing.T) {
	q := newTestQueue(t, RetryPolicy{MaxAttempts: 1}, Options{})
	_ = q.Enqueue("d1", "fix-me")
	_ = q.Enqueue("d2", "fix-me")
	_ = q.Enqueue("d3", "other")
	_ = q.Enqueue("ok", "ok")
	for _, id := range []string{"d1", "d2", "d3"} {
		driveToDead(t, q, id, "c1", 1)
	}
	task, _ := q.Claim("c1")
	_ = q.Ack(task.ID, "c1") // ok -> done

	before := totalOf(q.Stats())
	n, err := q.RequeueDeadIf(func(task *Task) bool { return task.Payload == "fix-me" })
	if err != nil || n != 2 {
		t.Fatalf("requeueIf n=%d err=%v", n, err)
	}
	after := q.Stats()
	// 总量守恒：不增不减，只有状态迁移
	if totalOf(after) != before {
		t.Fatalf("total changed: before=%d after=%d (%v)", before, totalOf(after), after)
	}
	if after[StatePending] != 2 || after[StateDead] != 1 || after[StateDone] != 1 {
		t.Fatalf("unexpected stats: %v", after)
	}
	// 不匹配的死信保持原样
	got, _ := q.Get("d3")
	if got.State != StateDead || got.Requeues != 0 {
		t.Fatalf("d3 should stay dead: %+v", got)
	}
}

// ---------- 容量上限 ----------

func TestCapacityRejectsEnqueue(t *testing.T) {
	q := newTestQueue(t, RetryPolicy{MaxAttempts: 3}, Options{Capacity: 2})
	if err := q.Enqueue("a", "x"); err != nil {
		t.Fatalf("enqueue a: %v", err)
	}
	if err := q.Enqueue("b", "x"); err != nil {
		t.Fatalf("enqueue b: %v", err)
	}
	// 到顶后明确拒绝，且不能写进去一半
	if err := q.Enqueue("c", "x"); !errors.Is(err, ErrQueueFull) {
		t.Fatalf("err=%v, want ErrQueueFull", err)
	}
	if _, ok := q.Get("c"); ok {
		t.Fatalf("rejected task should not exist")
	}
	if s := q.Stats(); totalOf(s) != 2 {
		t.Fatalf("stats=%v, want total 2", s)
	}
	// 在途（in_flight / retry_wait）同样占额度
	claimed, _ := q.Claim("c1")
	if claimed == nil {
		t.Fatalf("claim failed")
	}
	if err := q.Enqueue("d", "x"); !errors.Is(err, ErrQueueFull) {
		t.Fatalf("err=%v, want ErrQueueFull while one in_flight", err)
	}
}

func TestCapacityIgnoresTerminalStates(t *testing.T) {
	q := newTestQueue(t, RetryPolicy{MaxAttempts: 1}, Options{Capacity: 1})
	_ = q.Enqueue("a", "x")
	task, _ := q.Claim("c1")
	_ = q.Ack(task.ID, "c1") // a -> done，释放额度
	if err := q.Enqueue("b", "x"); err != nil {
		t.Fatalf("enqueue after done: %v", err)
	}
	task, _ = q.Claim("c1")
	_ = q.Fail(task.ID, "c1", errors.New("boom")) // b -> dead，释放额度
	if err := q.Enqueue("c", "x"); err != nil {
		t.Fatalf("enqueue after dead: %v", err)
	}
	if s := q.Stats(); s[StatePending] != 1 || s[StateDone] != 1 || s[StateDead] != 1 {
		t.Fatalf("stats=%v", s)
	}
}

func TestPruneKeepsInFlightAndStats(t *testing.T) {
	q := newTestQueue(t, RetryPolicy{MaxAttempts: 1}, Options{})
	_ = q.Enqueue("done-task", "x")
	_ = q.Enqueue("dead-task", "x")
	_ = q.Enqueue("running-task", "x")
	_ = q.Enqueue("pending-task", "x")

	t1, _ := q.Claim("c1")
	_ = q.Ack(t1.ID, "c1")
	t2, _ := q.Claim("c1")
	_ = q.Fail(t2.ID, "c1", errors.New("boom"))
	t3, _ := q.Claim("c1")
	if t3.ID != "running-task" {
		t.Fatalf("unexpected claim: %s", t3.ID)
	}

	// 清理全部历史（截止时间取未来）
	n, err := q.Prune(time.Now().Add(time.Minute))
	if err != nil || n != 2 {
		t.Fatalf("prune n=%d err=%v, want 2", n, err)
	}
	s := q.Stats()
	if s[StateInFlight] != 1 || s[StatePending] != 1 || totalOf(s) != 2 {
		t.Fatalf("in-flight tasks disturbed: %v", s)
	}
	if _, ok := q.Get("done-task"); ok {
		t.Fatalf("pruned task still present")
	}
	// 在途任务仍可正常确认
	if err := q.Ack("running-task", "c1"); err != nil {
		t.Fatalf("ack after prune: %v", err)
	}
}
