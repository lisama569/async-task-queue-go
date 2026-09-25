package queue

import (
	"context"
	"fmt"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

// TestConsumerCrashRecovery 消费者异常退出后，未确认任务必须可重新投递，
// 且恢复原因可区分（consumer_crash / lease_expired）；重启后状态一致。
func TestConsumerCrashRecovery(t *testing.T) {
	dir := t.TempDir()
	storePath := filepath.Join(dir, "store.json")

	store, err := NewStore(storePath)
	if err != nil {
		t.Fatalf("NewStore: %v", err)
	}
	q, err := Open(store, Options{LeaseDuration: 50 * time.Millisecond})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	for _, id := range []string{"crash-task", "lease-task", "done-task"} {
		if _, _, err := q.Enqueue(Task{ID: id}); err != nil {
			t.Fatalf("enqueue %s: %v", id, err)
		}
	}

	// 消费者 c1 领取两个任务后“崩溃”（不确认、不归还租约）。
	c1, _ := q.Claim("c1")
	c2, _ := q.Claim("c1")
	if c1 == nil || c2 == nil {
		t.Fatalf("claims: %v %v", c1, c2)
	}
	// 第三个任务正常完成。
	c3, _ := q.Claim("c2")
	if err := q.Ack("c2", c3.ID); err != nil {
		t.Fatalf("ack: %v", err)
	}

	// 模拟进程重启：从同一存储重新打开队列。
	store2, _ := NewStore(storePath)
	q2, err := Open(store2, Options{LeaseDuration: 50 * time.Millisecond})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	stats := q2.Stats()
	t.Logf("重启后 stats=%v 判定依据: 崩溃前状态须从本地存储完整恢复", stats)
	if stats[StatusInFlight] != 2 || stats[StatusDone] != 1 {
		t.Fatalf("state lost after restart: %v", stats)
	}

	// 恢复：c1 已死亡（consumer_crash），c2 存活。
	n := q2.Recover(map[string]bool{"c2": true})
	t.Logf("recovered=%d 判定依据: c1 持有的 2 个在途任务应被恢复", n)
	if n != 2 {
		t.Fatalf("recovered=%d, want 2", n)
	}
	for _, id := range []string{c1.ID, c2.ID} {
		got, _ := q2.Get(id)
		t.Logf("task=%s attempts=%d status=%s recover_by=%q 判定依据: 持有者死亡应标记 consumer_crash",
			got.ID, got.Attempts, got.Status, got.RecoverBy)
		if got.Status != StatusPending || got.RecoverBy != ReasonConsumerCrash {
			t.Fatalf("task=%s status=%s recover_by=%q", got.ID, got.Status, got.RecoverBy)
		}
	}

	// 已确认任务不得被恢复或重复投递。
	got, _ := q2.Get(c3.ID)
	if got.Status != StatusDone {
		t.Fatalf("acked task changed state: %s", got.Status)
	}

	// 新消费者重新领取被恢复的任务：投递次数单调递增。
	reclaimed, err := q2.Claim("c3")
	if err != nil || reclaimed == nil {
		t.Fatalf("reclaim: %v %v", reclaimed, err)
	}
	t.Logf("task=%s attempts=%d recover_by=%q 判定依据: 恢复后重投，投递次数应递增",
		reclaimed.ID, reclaimed.Attempts, reclaimed.RecoverBy)
	if reclaimed.Attempts != 2 {
		t.Fatalf("attempts=%d, want 2", reclaimed.Attempts)
	}
}

// TestLeaseExpiredRecovery 持有者存活但租约超时的任务按 lease_expired 恢复。
func TestLeaseExpiredRecovery(t *testing.T) {
	q := openTestQueue(t, Options{LeaseDuration: 20 * time.Millisecond})
	if _, _, err := q.Enqueue(Task{ID: "slow-task"}); err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	if _, err := q.Claim("c1"); err != nil {
		t.Fatalf("claim: %v", err)
	}
	time.Sleep(30 * time.Millisecond) // 等待租约过期

	n := q.Recover(map[string]bool{"c1": true}) // c1 仍存活，但租约已过期
	got, _ := q.Get("slow-task")
	t.Logf("task=%s attempts=%d status=%s recover_by=%q 判定依据: 存活持有者租约超时应标记 lease_expired",
		got.ID, got.Attempts, got.Status, got.RecoverBy)
	if n != 1 || got.RecoverBy != ReasonLeaseExpired {
		t.Fatalf("recovered=%d recover_by=%q", n, got.RecoverBy)
	}
}

// TestConcurrentConsumers 多消费者并发领取：同一任务不得被重复执行，总量守恒。
func TestConcurrentConsumers(t *testing.T) {
	q := openTestQueue(t, Options{LeaseDuration: time.Minute})

	const numTasks = 200
	for i := 0; i < numTasks; i++ {
		if _, _, err := q.Enqueue(Task{ID: fmt.Sprintf("t-%03d", i)}); err != nil {
			t.Fatalf("enqueue: %v", err)
		}
	}

	var mu sync.Mutex
	executed := map[string]int{} // task id -> 执行次数
	handler := func(ctx context.Context, task *Task) error {
		mu.Lock()
		executed[task.ID]++
		mu.Unlock()
		return nil
	}

	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	const numConsumers = 8
	for i := 0; i < numConsumers; i++ {
		c := NewConsumer(fmt.Sprintf("c%d", i), q, handler)
		wg.Add(1)
		go c.Run(ctx, &wg)
	}
	// 等待全部任务完成。
	deadline := time.Now().Add(10 * time.Second)
	for {
		if q.Stats()[StatusDone] == numTasks {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("timeout waiting for completion: %v", q.Stats())
		}
		time.Sleep(5 * time.Millisecond)
	}
	cancel()
	wg.Wait()

	// 幂等性：每个任务恰好执行一次。
	for id, count := range executed {
		if count != 1 {
			t.Fatalf("task=%s executed %d times 判定依据: 租约互斥下不得重复执行", id, count)
		}
	}
	t.Logf("executed=%d 判定依据: 每个任务恰好执行一次", len(executed))
	if len(executed) != numTasks {
		t.Fatalf("executed=%d, want %d", len(executed), numTasks)
	}

	// 守恒：done == 总数，无丢失无重复记账。
	stats := q.Stats()
	total := 0
	for _, n := range stats {
		total += n
	}
	t.Logf("stats=%v total=%d 判定依据: 完成/重试/死信之和必须等于投递总量", stats, total)
	if total != numTasks || stats[StatusDone] != numTasks {
		t.Fatalf("conservation violated: %v", stats)
	}
}
