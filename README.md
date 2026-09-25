# async-task-queue-go

基于本地存储（JSON 文件）的异步任务队列，支持生产者投递与并发消费者处理。

## 结构

```
internal/queue/
  types.go      任务模型、状态机、可重试错误标记
  backoff.go    重试退避策略（指数退避，单调不减，可封顶）
  store.go      本地 JSON 持久化（写临时文件 + 原子重命名）
  queue.go      队列核心：入队 / 领取 / 确认 / 失败 / 恢复 / 统计
  consumer.go   并发消费者循环
```

## 投递语义

- **至少一次（at-least-once）**：任务被确认（`Ack`）前可能被投递多次，
  处理器需保证幂等。
- **重复投递幂等**：`Enqueue` 以任务 ID 去重，相同 ID 重复投递返回已存在
  任务，不创建新记录、不覆盖已有内容；已确认（`done`）或已死信（`dead`）
  的任务重复投递不会改变其终态。
- **已确认任务不得再次投递**：`done` / `dead` 为终态，不会被 `Claim` 取出。
- **未确认任务可恢复**：消费者异常退出（进程重启、崩溃）后，其在途
  （`in_flight`）任务由 `Recover` 重新入队，恢复原因可区分：
  - `consumer_crash`：租约持有者不在存活消费者集合中；
  - `lease_expired`：持有者存活但租约超时未确认；
  - `retry_scheduled`：可重试失败后的计划内重投。
- 每次 `Claim` 使 `attempts`（投递次数）单调递增，重投原因记录在
  `recover_by` 字段。

## 重试与死信规则

- 处理器返回 `queue.NewRetryableError(err)` 表示**可重试失败**：
  任务按退避策略重新排队，等待时长为 `base * 2^(retry-1)`（封顶 `max`），
  随重试次数单调不减；重试次数随 `attempts` 单调递增。
- 返回其他 error 表示**不可重试失败**：不消耗重试次数，直接进入死信，
  `dead_reason` 为 `non_retryable: <原因>`。
- 可重试失败超过 `MaxRetries` 上限后进入死信，`dead_reason` 为
  `retries_exhausted: <原因>`；两类失败原因可明确区分。
- 死信任务保留 `last_error` 与 `dead_reason`，不再被投递。

## 并发一致性与守恒

- 所有状态变更在单把互斥锁内完成并立即落盘；`Claim` 原子地设置租约，
  多消费者并发领取时同一任务只会被一个消费者取走，不会重复执行。
- 每个任务在任意时刻恰好处于一个状态（`pending` / `in_flight` / `done` /
  `dead`），因此 `Stats()` 各状态之和恒等于投递总量：完成、重试与死信
  不会丢失或重复记账。

## 本地验证

```bash
# 全部测试
go test ./... -v

# 竞态检测
go test ./... -race -count=1
```

测试覆盖：

| 测试 | 场景 |
| --- | --- |
| `TestDuplicateEnqueue` | 重复投递幂等、已确认任务不复活、总量守恒 |
| `TestRetryBackoffMonotonic` | 退避时长单调不减、重试耗尽进入死信并保留原因 |
| `TestNonRetryableGoesStraightToDead` | 不可重试失败直接死信，与可重试失败区分 |
| `TestConsumerCrashRecovery` | 消费者崩溃 + 进程重启后从本地存储恢复，原因标记 `consumer_crash` |
| `TestLeaseExpiredRecovery` | 存活消费者租约超时，原因标记 `lease_expired` |
| `TestConcurrentConsumers` | 8 消费者并发处理 200 任务，每任务恰好执行一次，总量守恒 |

测试日志会打印任务标识、投递次数（attempts）与判定依据，便于核对结论。
