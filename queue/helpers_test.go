package queue

import (
	"sync"
	"testing"
	"time"
)

// fakeClock 是可手动推进的时钟，供续期/失联类用例做确定性判断。
type fakeClock struct {
	mu sync.Mutex
	t  time.Time
}

func newFakeClock() *fakeClock {
	return &fakeClock{t: time.Date(2026, 10, 5, 10, 0, 0, 0, time.UTC)}
}

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}

func (c *fakeClock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.t = c.t.Add(d)
}

// testLogger 把队列事件写到 testing 日志（go test -v 或失败时可见），
// 每个用例可清楚看到任务是谁、投递了几次、为何这么判定。
type testLogger struct{ t *testing.T }

func (l testLogger) Printf(format string, kv ...any) {
	l.t.Helper()
	l.t.Logf("[queue-event] "+format, kv...)
}

// testQueue 打开一个使用临时文件、假时钟与测试日志的队列。
func testQueue(t *testing.T, policy RetryPolicy, opts ...Option) *Queue {
	t.Helper()
	path := t.TempDir() + "/queue.json"
	all := append([]Option{WithLogger(testLogger{t})}, opts...)
	q, err := Open(path, policy, all...)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { _ = q.Close() })
	return q
}

// mustEnqueue 投递任务，失败即终止用例。
func mustEnqueue(t *testing.T, q *Queue, id, payload string) {
	t.Helper()
	if err := q.Enqueue(id, payload); err != nil {
		t.Fatalf("enqueue %q: %v", id, err)
	}
}

// mustClaim 领取任务，期望必然领到。
func mustClaim(t *testing.T, q *Queue, consumer string) *Task {
	t.Helper()
	tk, err := q.Claim(consumer)
	if err != nil {
		t.Fatalf("claim by %q: %v", consumer, err)
	}
	if tk == nil {
		t.Fatalf("claim by %q: got no task", consumer)
	}
	return tk
}

// assertState 校验任务状态与持有者。
func assertState(t *testing.T, q *Queue, id string, st State, owner string) *Task {
	t.Helper()
	tk, ok := q.Get(id)
	if !ok {
		t.Fatalf("task %q missing", id)
	}
	if tk.State != st {
		t.Fatalf("task %q state = %s, want %s", id, tk.State, st)
	}
	if owner != "" && tk.Owner != owner {
		t.Fatalf("task %q owner = %q, want %q", id, tk.Owner, owner)
	}
	return tk
}
