# 本地异步任务队列（async-task-queue-go）

基于单个本地 JSON 文件持久化的轻量异步任务队列，支持多消费者并发领取、
指数退避重试、死信、进程崩溃恢复，并在本轮补齐了三块生产化能力：

1. **长任务续期（心跳租约）与失联回收**
2. **死信人工重跑（保留失败历史、受重试上限约束）**
3. **在途容量上限保护**

所有变更都在同一把队列锁内“先改内存、再原子落盘（临时文件 + rename）”，
因此对外的状态迁移是原子的；任务总量在任何操作前后守恒。

---

## 快速开始

```go
q, err := queue.Open("./tasks.json", queue.RetryPolicy{
    MaxAttempts: 3,
    BaseBackoff: 100 * time.Millisecond,
},
    queue.WithCapacity(10_000),            // 可选：在途容量上限
    queue.WithLeaseTimeout(30*time.Second),// 可选：失联判定时长，默认 30s
    queue.WithLogger(myLogger),            // 可选：注入日志实现
)
if err != nil { panic(err) }
defer q.Close()

_ = q.Enqueue("order-1", `{"order_id":1}`)

c := queue.NewConsumer(q, "consumer-1")
c.HeartbeatPeriod = 10 * time.Second // 长任务自动续期；0 表示不自动续期
c.Run(ctx, func(ctx context.Context, t *queue.Task) error {
    // 业务处理；返回 queue.NonRetryable(err) 直接进死信
    return doWork(t.Payload)
})
```

---

## 1. 续期与失联回收

### 背景：为什么不能把“处理久”当成“消费者挂了”

旧实现里任务一旦被领取就处于 `in_flight`，任何恢复动作都会把它重新投出。
耗时较长的任务因此可能被两个消费者同时执行。本轮引入“持有者还活着”的
续期信号（心跳租约）。

### 规则

- 任务被 `Claim` 领取后，记录持有者 `Owner` 与领取时刻。
- 处理方在长任务期间周期性调用 **`Heartbeat(id, consumerID)`**，
  刷新“最近一次活着”的时间（`heartbeated_at`）。消费者可设置
  `Consumer.HeartbeatPeriod > 0` 自动续期。
- **`SweepStale()`** 扫描所有在途任务，对每个任务计算静默时长
  `idle = now - max(最近一次心跳, 领取时刻)`：
  - `idle <= LeaseTimeout`：持有者仍在续期（或还在租约窗口内），
    **绝不回收、绝不允许别的消费者抢走**；
  - `idle >  LeaseTimeout`：持有者长时间无任何动静、明显失联，
    才把任务重新置为 `pending` 并标记原因 `lost_contact`。
- 进程“明确退出/重启”后的批量恢复仍走 **`Recover()`**，标记原因
  `crash_recovered`。两种恢复在任务记录（`last_reason`）和日志里都可区分：

  | 场景 | 入口 | `last_reason` |
  | --- | --- | --- |
  | 消费者进程明确退出/重启，启动时统一恢复 | `Recover()` | `crash_recovered` |
  | 进程还在，但持有者卡死/网络失联，心跳超时 | `SweepStale()` | `lost_contact` |

- **并发安全保证**：领取、续期、回收、确认都在同一把队列锁内做持有者校验。
  同一任务任何时刻只有一个合法持有者；失联回收后原持有者的迟到
  `Ack/Fail/Heartbeat` 会因持有者不匹配被拒绝，因此不会双跑。
- 续期是非强制的：短任务不需要心跳，租约窗口内的任务也不会被回收。

> 生产部署建议：`HeartbeatPeriod` 取 `LeaseTimeout` 的 1/3 左右，
> 并由一个定时器周期性调用 `SweepStale()`（例如每 `LeaseTimeout/2` 一次）。

---

## 2. 死信人工重跑

### 入口

- 单个：`q.RequeueDeadTask(id) error`
- 按条件批量：`q.RequeueDead(filter RequeueFilter) (RequeueReport, error)`

```go
rep, err := q.RequeueDead(queue.RequeueFilter{
    Reason:          queue.ReasonRetryExceed, // 只看重试耗尽的
    PayloadContains: "tenant-42",             // 且 payload 含该子串
    // Owner: "consumer-7",                   // 或按最后持有者筛
    // IDs:   []string{"t1","t2"},            // 或显式指定 ID
})
// rep.Requeued / rep.Skipped 给出每个任务的处置与原因
```

### 规则

- **只有 `dead` 状态的任务能被重跑**。已完成（`done`）、正在被别的消费者
  处理（`in_flight`）、等待重试（`retry_wait`）一律拒绝：
  - 单个操作返回可 `errors.Is` 判定的 `ErrNotDead` / `ErrTaskNotFound`；
  - 批量操作不报错，而是在 `RequeueReport.Skipped` 中给出每个任务被跳过的
    原因：`not_dead` / `not_found` / `filter_denied` / `queue_full`。
- **历史不洗白**：
  - `Failures` 保留全部失败经历（每次的轮内投递序号、持有者、失败原因、
    时间、走向）；
  - `TotalAttempts` 是跨重跑的累计投递次数，只增不减；
  - `LastError` 等现场信息保留。
- **重新计数、可再次死信**：重跑把**本轮** `Attempts` 重置为 0，任务回到
  `pending`，之后仍按原 `RetryPolicy.MaxAttempts` 重新走一遍；再次用尽
  依旧进入死信（`retry_exhausted`），不会因为能重跑就无限复活。
  `Requeues` 记录被人工捞回的次数。
- **总量守恒**：重跑只是 `dead -> pending` 的状态迁移，不新增、不删除任务。
- **容量约束**：重跑同样占用在途额度，额度满时单个返回 `ErrQueueFull`，
  批量在 `Skipped` 中标 `queue_full`。批量操作整体原子落盘。

---

## 3. 容量上限语义

- `WithCapacity(n)`（`n > 0`）限制队列中**未结束**任务数量：
  状态为 `pending` / `in_flight` / `retry_wait` 的任务计数；
  `done` 与 `dead` 的历史任务**不占额度**。
- 到上限后：
  - `Enqueue` 返回 `ErrQueueFull`（可用 `errors.Is` 与 `ErrTaskExists` 区分），
    任务**不会写入**，不存在“写进去一半”；
  - 死信重跑同样被拒绝（见上）。
- `WithCapacity(0)`（默认）表示不限制，与历史行为一致。
- `PurgeFinished()` 可清理 `done/dead` 历史记录：只删已结束任务，
  在途任务与 `Stats()` 统计不受影响；清理不改变在途计数，因此容量额度不乱。

---

## 状态机与关键字段

```
pending ──Claim──▶ in_flight ──Ack──▶ done
  ▲                  │  └─Fail(可重试, 未耗尽)──▶ retry_wait ──(到期 Claim)──┐
  │                  ├─Fail(不可重试)──────────────────────────────▶ dead   │
  │                  └─Fail(耗尽)──────────────────────────────────▶ dead   │
  │                                                                         │
  ├──── Recover()        进程明确退出：in_flight → pending (crash_recovered)│
  ├──── SweepStale()     心跳超时失联：in_flight → pending (lost_contact)   │
  └──── RequeueDead*()   人工捞回：dead → pending (manually_requeued) ◀─────┘
```

| 字段 | 含义 |
| --- | --- |
| `Attempts` | 当前这一轮生命周期内的投递次数；死信重跑后重置为 0 |
| `TotalAttempts` | 跨重跑累计投递次数，永不重置 |
| `Failures` | 历次失败经历列表，重跑不清空 |
| `Requeues` | 被人工从死信重跑的次数 |
| `HeartbeatedAt` | 持有者最近续期时间；零值表示从未续期 |
| `Owner` | 当前持有者；非在途时为空 |
| `LastReason` | 最近一次状态变化原因（见下表） |

`LastReason` 取值：`""`（首次投递）、`retry_scheduled`、`retry_exhausted`、
`non_retryable`、`crash_recovered`、`lost_contact`、`manually_requeued`。

---

## 日志

通过 `WithLogger` 注入实现 `Printf(format string, kv ...any)` 的日志器；
默认静默。队列在领取、续期、成功、重试、进死信、两种回收、人工重跑、
容量拒绝、历史清理时都会打点，日志里可直接读出：

- `task_id`：任务是谁；
- `owner` / `consumer`：当前/原持有者是谁；
- `attempts` 与 `total_attempts`：本轮与累计各被投递了几次；
- `reason` / `error`：为什么这么判定（续期、重试、耗尽、不可重试、
  崩溃恢复、失联、人工重跑、容量满）；
- 失联回收还会输出 `idle` 与 `lease_timeout`，便于核对回收是否合理。

---

## 兼容范围

- **API 向后兼容**：`Open(path, policy)` 旧签名保持可用，新能力全部通过
  函数式选项（`WithCapacity` / `WithLeaseTimeout` / `WithLogger` /
  `WithClock`）启用；`Consumer` 零值即可用，`HeartbeatPeriod` 默认关闭。
- **数据文件向后兼容**：新字段（`total_attempts`、`heartbeated_at`、
  `failures`、`requeues`）在旧 JSON 中缺失时按零值安全加载；
  新文件含这些字段，不影响人工阅读。
- **行为边界**：
  - 本队列是单机本地队列，依赖单进程内的互斥锁；不支持多进程并发打开
    同一个数据文件（多消费者请在同一进程内创建多个 `Consumer`）。
  - `SweepStale` 需要调用方（或某个看守协程）周期性触发；`Recover` 用于
    进程重启后的显式恢复。
  - 续期不能阻止业务真正卡死造成的超时：超过 `LeaseTimeout` 仍无心跳即判失联。

---

## 本地验证

```bash
# 1. 全量单测（27 个用例，含竞态检测建议加 -race）
go test -race -count=1 ./...

# 2. 查看每个任务“是谁、投递几次、为什么这么判定”的事件日志
go test -race -v ./...

# 3. 稳定性重复执行（验证无 map 顺序/时序抖动）
go test -count=5 ./...
```

测试覆盖的重点场景：

- **续期与失联**：持续心跳的长任务不被回收/不被别的消费者抢走；
  超过租约无心跳才回收且标记 `lost_contact`；与 `crash_recovered` 区分；
  非持有者/错误状态续期被拒；回收后原持有者迟到 Ack 被拒、无双跑；
  消费者自动续期端到端保活。
- **死信重跑**：失败原因、失败条数、累计投递次数保留；轮内计数重置；
  重跑后再次耗尽重新进死信且历史叠加；已完成/在途/等待重试/不存在的
  重跑被明确拒绝；按 payload/原因/ID 条件筛选及跳过明细；总量守恒。
- **容量**：到顶 `Enqueue` 与重跑返回 `ErrQueueFull` 且不写入；
  done/dead 释放额度；in_flight/retry_wait 占额度；默认 0 不限制；
  `PurgeFinished` 只清历史、不动在途与统计。
- **持久化回归**：新字段落盘后重开保留。
