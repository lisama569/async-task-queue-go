# async-task-queue-go

基于本地 JSON 文件的异步任务队列，支持多消费者并发、可解释的重试策略、
租约续期与失联回收、死信人工重跑、容量上限保护。所有方法可并发调用，
每次状态变更先落盘（临时文件 + rename）再返回，崩溃后可从磁盘恢复。

## 基本用法

```go
q, _ := queue.OpenWithOptions("queue.json", queue.RetryPolicy{
    MaxAttempts: 3,
    BaseBackoff: time.Second,
}, queue.Options{
    Capacity: 1000,            // 未结束任务上限，0 表示不限
    LeaseTTL: 30 * time.Second, // 持有者租约时长
})

_ = q.Enqueue("task-1", "payload")

c := queue.NewConsumer(q, "consumer-1")
c.Run(ctx, func(ctx context.Context, t *queue.Task) error {
    // 处理任务；返回 error 表示失败，queue.NonRetryable(err) 表示不可重试
    return nil
})
```

## 续期与失联回收规则

任务被 `Claim` 领取后进入 `in_flight`，同时获得一个租约（`LeaseUntil = 领取时间 + LeaseTTL`）。

- **续期（Heartbeat）**：持有者在处理期间调用 `q.Heartbeat(id, consumerID)` 续期，
  表示“持有者还活着”。`Consumer.Run` 会在处理期间按 `HeartbeatPeriod`
  （默认 `LeaseTTL/3`）自动续期，长任务无需手动处理。
- **失联回收（RecoverStale）**：只有租约**已到期**（持有者在整个 LeaseTTL 内
  没有任何续期）的 `in_flight` 任务才会被回收为 `pending`，
  原因记为 `ReasonStale`（`lease_expired`）。只要持有方持续续期，
  任务绝不会被其他消费者抢走。
- **崩溃恢复（Recover）**：调用方确信消费者进程已退出时使用（典型场景：
  进程重启后、开始消费前调用一次）。它无视租约剩余时间，把所有 `in_flight`
  任务直接重置为 `pending`，原因记为 `ReasonCrash`（`crash_recovered`）。
- 两种恢复在任务记录的 `LastReason` 字段和日志中明确区分：
  `lease_expired` = 持有者失联被回收；`crash_recovered` = 进程退出后恢复。
- 同一任务任何时刻只会被一个消费者持有：`Claim` 在单锁内原子完成，
  多消费者并发领取同一任务时只有一个赢家。

## 死信重跑规则

- `q.RequeueDead(id)` 把单个死信任务重新放回 `pending`；
  `q.RequeueDeadIf(match)` 按条件批量重跑（`match` 收到任务快照，返回 true 即重跑）。
- **历史保留**：重跑不清除失败经历。`Attempts`（累计投递次数）、`LastError`
  （最近一次失败原因）原样保留，`Requeues` 记录被重跑的次数。
- **重试预算重置**：通过内部基线 `AttemptBase` 实现——重跑后再次失败时，
  仍按 `MaxAttempts` 重新计算本轮预算；用尽后再次进入死信
  （`ReasonRetryExceed`），不会无限复活。
- **防误操作**：只有 `dead` 状态可重跑；已完成（`done`）、处理中
  （`in_flight`）、等待中（`pending`/`retry_wait`）的任务重跑会返回错误，
  且不会改变任何状态。
- **总量守恒**：重跑只做状态迁移，不新增也不删除任务，
  `Stats()` 各状态之和保持不变。

## 容量语义

- `Options.Capacity` 限制**未结束**任务（`pending` + `in_flight` + `retry_wait`）
  的总数；0 表示不限制。
- 达到上限后 `Enqueue` 返回 `ErrQueueFull`（可用 `errors.Is` 判断），
  本次投递不写入任何数据，不存在“写进去一半”的情况。
- 已完成（`done`）和已死信（`dead`）的历史任务**不占额度**。
- `q.Prune(before)` 只清理 `UpdatedAt` 早于 `before` 的终态历史记录，
  绝不影响在途任务，清理后在途统计（`Stats()`）保持不变。

## 兼容范围

- `Open(path, policy)` 保持原有签名与行为，等价于 `OpenWithOptions` 传零值
  `Options`（不限容量、默认 30s 租约、日志输出到 `log.Default()`）。
- 旧版存储文件可直接打开：新增字段（`lease_until`、`attempt_base`、`requeues`）
  缺省为零值，语义等价于“未续期 / 未重跑”。
- 零值 `AttemptBase` 下 `EffectiveAttempts() == Attempts`，重试判定与旧版一致。

## 本地验证

```bash
# 全量测试（含回归 + 续期/回收 + 死信重跑 + 容量）
go test ./...

# 查看每个任务的判定日志（谁领取、第几次投递、为何回收/重跑/拒绝）
go test ./queue/ -v

# 并发安全性
go test ./queue/ -race -count=1
```

测试日志示例（每个任务的状态迁移都可追溯）：

```
claimed id=t1 owner=c1 attempt=2 effective_attempt=1 lease_until=...
heartbeat id=t1 owner=c1 attempt=2 lease_until=...
recovered id=t1 owner_lost attempt=2 reason=lease_expired
requeued id=t1 requeues=1 attempts_kept=2 last_error="still broken" reason=dead_requeued
enqueue rejected id=c reason=capacity_full capacity=2
```
