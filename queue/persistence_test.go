package queue

import (
	"testing"
	"time"
)

// 新字段（心跳、累计投递、失败历史、重跑次数）落盘后重开必须保留；
// 旧格式（缺少这些字段）也能正常加载，零值安全，兼容升级。
func TestPersistNewFieldsAcrossReopen(t *testing.T) {
	clk := newFakeClock()
	policy := RetryPolicy{MaxAttempts: 3, BaseBackoff: time.Millisecond}
	path := t.TempDir() + "/q.json"

	q, err := Open(path, policy, WithClock(clk), WithLeaseTimeout(time.Minute))
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	mustEnqueue(t, q, "t1", "p1")
	mustClaim(t, q, "w1")
	if err := q.Heartbeat("t1", "w1"); err != nil {
		t.Fatalf("heartbeat: %v", err)
	}
	clk.Advance(time.Minute + time.Second)
	if _, err := q.SweepStale(); err != nil {
		t.Fatalf("sweep: %v", err)
	}
	// 重跑前置：让它先死信再人工捞回。
	mustClaim(t, q, "w2")
	if err := q.Fail("t1", "w2", errBoom("first")); err != nil {
		t.Fatalf("fail: %v", err)
	}
	clk.Advance(time.Millisecond * 2)
	mustClaim(t, q, "w3")
	if err := q.Fail("t1", "w3", errBoom("second")); err != nil {
		t.Fatalf("fail: %v", err)
	}
	if err := q.RequeueDeadTask("t1"); err != nil {
		t.Fatalf("requeue: %v", err)
	}
	if err := q.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	// 重新打开同一文件。
	clk2 := newFakeClock()
	q2, err := Open(path, policy, WithClock(clk2), WithLeaseTimeout(time.Minute))
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	got, ok := q2.Get("t1")
	if !ok {
		t.Fatal("task lost after reopen")
	}
	if got.State != StatePending || got.TotalAttempts != 3 || got.Attempts != 0 {
		t.Fatalf("counters after reopen: state=%s attempts=%d total=%d",
			got.State, got.Attempts, got.TotalAttempts)
	}
	if len(got.Failures) != 2 || got.Requeues != 1 {
		t.Fatalf("history after reopen: failures=%d requeues=%d",
			len(got.Failures), got.Requeues)
	}
	if got.LastReason != ReasonRequeued {
		t.Fatalf("reason after reopen = %s", got.LastReason)
	}
}

type boom string

func (e boom) Error() string { return string(e) }

func errBoom(s string) error { return boom(s) }
