# go-config-rollout

按波次（wave）推进、进程重启后仍可继续的配置发布服务。纯 Go 标准库实现，无外部依赖。

开发环境：Go 1.23.0。

## 核心概念

- **配置包（Config）**：由内容的 SHA-256 不可变摘要（digest）标识。相同摘要重复登记且内容一致时幂等；内容不一致返回 `ErrConfigExists`；声明摘要与内容不符返回 `ErrInvalidDigest`。
- **发布（Release）**：创建时把目标节点集合**冻结**并切成有顺序的波次。一个节点在本次发布中恰好出现一次（重复节点返回 `ErrDuplicateNode`），每波所需成功数 `ceil(MinSuccessRatio × 波次大小)` 在创建时一并冻结。
- **发布策略（WavePolicy）**：
  - `MinSuccessRatio` ∈ (0,1]：当前波成功数达到门槛后，下一波才**原子开放**（同一事务内翻转本波全部节点状态、记录开波时间、追加 `wave.opened` 事件）。
  - `MaxFailures`：波次内失败数**超过**该值立即暂停（`failure_threshold`）。
  - `WaveTimeout`：当前波等待成功门槛的最长时间，超时暂停（`timeout`）；恢复后给予全新的超时窗口。
  - `Health`：健康观测策略（见下节），同样在创建时冻结；零值表示不启用（波次达标后立即推进）。
- **节点已应用版本（NodeState）**：跨发布维护单调的应用序号。新发布的成功回执才会推进它；旧发布的迟到回执不会覆盖节点较新的已应用版本。

## 健康观测与自动回滚

启用 `HealthPolicy` 后，波次成功门槛达成不再直接开放下一波，而是进入**观察状态**（`wave.observing` 事件）：

- **观察窗口（ObserveWindow）**：当前波次只有完整通过观察窗口才允许开放下一波（末波则完成发布）。窗口未走完时下一波节点仍取不到配置。
- **健康样本（HealthSample）**：携带唯一事件号（EventID）、采样时间、节点与配置摘要。
  - 幂等键为 **（发布, 事件号）**：相同事件重放幂等成功；同号异内容返回 `ErrSampleConflict`。
  - 样本必须归属该发布（节点在发布内）且摘要等于发布配置摘要（`ErrDigestMismatch`），因此**不能跨发布或跨配置使用**；不同发布的事件号空间相互独立。
  - 允许**乱序到达**：评估按采样时间归位，只统计当前波次节点、且采样时间不早于本波开放时间的样本。
- **失败判定**：窗口内样本数达到 `MinSamples` 且不健康占比 ≥ `FailureThreshold` 时触发自动回滚；样本不足时不做失败判定（窗口走完即放行）。
- **自动回滚**：触发时一次性生成回滚计划（`Active → RollingBack`），把每个已成功节点恢复到其**发布前**的配置摘要（`NodeLease.PreDigest`，在节点首次成功时冻结）。对每个已应用节点恰好生成一条 `node.rollback` 通知（携带目标摘要）。
- **回滚确认（AckRollback）**：节点确认已恢复（摘要必须与计划目标一致，重复确认幂等）；节点的已应用版本随之恢复到发布前摘要（若节点已被更新发布接管则保持单调、不覆盖）。全部确认后发布进入 `RolledBack` 终态，`release.rolled_back` 终态通知只写出一次。
- **单调状态**：`RollingBack` 是单向状态——不可暂停、不可恢复（`ErrRollbackInProgress`）、不再开放新波次、不再接受应用回执（**迟到成功回执不得覆盖回滚结果**）。人工取消仍然合法（`RollingBack → Cancelled`），但不再生成补偿通知（回滚通知已覆盖全部成功节点）。自动回滚与人工取消/恢复竞争时，所有变更串行提交，最终只留下一条单调状态序列。
- **查询**：`GetProgress` 返回的 `Progress.Health` 展示健康证据与阈值计算（样本数、不健康数、占比、冻结的阈值参数、判定结论）；`Progress.Rollback` 展示每个节点的前后版本（From/To 摘要）与回滚进度（done/total）。

## 状态机

```
                 pause (手动/失败阈值/超时)
        Active ───────────────────────────► Paused
          │  ▲                                │
          │  └──────────── resume ────────────┘
          │
          ├──── 健康失败阈值 ──► RollingBack ── 全部节点恢复 ──► RolledBack（终态）
          │                           │
          ├──── cancel ───► Cancelled ◄────────┘（cancel，终态）
          │
          └── 末波通过观察窗口 ──► Completed（终态）
```

`Cancelled` / `Completed` / `RolledBack` 为终态，任何后续变更返回 `ErrTerminal`。所有变更在单个存储事务内完成并串行化，因此**暂停、恢复、取消、自动回滚并发发生时，最终只留下一条合法且单调的状态序列**（事件序号全局递增）。

## 回执（Receipt）语义

节点回执会重复、乱序、迟到。幂等键为 **（发布, 节点, 配置摘要）**：

| 情形 | 处理 |
|---|---|
| 节点已有终态结果（成功/失败）后再来任意回执 | 幂等忽略（`ReceiptOutcome.Ignored = true`），首个结果保持不变，不重复计数、不重复发事件 |
| 节点不属于该发布 | `ErrNodeNotInRelease` |
| 回执摘要 ≠ 发布摘要 | `ErrDigestMismatch` |
| 节点属于旧波次（非当前开放波）且尚无终态结果 | `ErrWaveNotOpen`，不能推进当前流程 |
| 发布处于暂停 | `ErrNotActive` |
| 发布正在自动回滚 | `ErrNotActive`（回执不再被接受，迟到回执不覆盖回滚结果） |
| 发布已取消/完成/已回滚 | `ErrTerminal` |
| 节点已应用更新发布（seq 更大）的配置，旧发布回执迟到 | `ErrStaleReceipt`：不计数、不推进、不覆盖节点当前已应用版本 |

只有**当前开放波次**中、尚无终态结果的节点可以确认应用结果。

## 取消语义

- 取消后，所有尚未成功的节点不能再取得配置（`GetConfigForNode` 返回 `ErrConfigNotAvailable`）。
- 已经成功的节点保留审计结果（波次计数与节点已应用版本都不回滚）。
- 取消迁移（Active/Paused → Cancelled）只发生一次；迁移时对当时已成功的每个节点**恰好生成一条** `node.compensation` 补偿通知（outbox 事件）。

## 持久化与 outbox

- 状态与 outbox 事件在**同一事务**中提交。`NewFileStore(path)` 使用「同目录临时文件 + fsync + rename」原子落盘，进程重启后重新打开即可从断点继续（发布进度、暂停状态、已应用版本、事件投递标记全部恢复）。
- outbox 事件只追加、不删除（同时充当审计流）。`DispatchOutbox` 按全局序号顺序投递，每条成功后标记 `Delivered`；sink 返回错误即停在断点，下次调用继续。投递为至少一次语义，**sink 需按事件键幂等**。

## 快速上手

```go
store, _ := configrollout.NewFileStore("var/rollout/state.json")
svc := configrollout.NewService(store, nil) // nil 使用系统 UTC 时钟

// 1. 登记配置
sum := sha256.Sum256([]byte("...config bytes..."))
digest := hex.EncodeToString(sum[:])
svc.RegisterConfig(digest, configBytes)

// 2. 创建发布：冻结 5 个节点，每波 2 个，100% 成功门槛、容忍 1 个失败、每波 10 分钟超时；
//    并冻结健康策略：每波达标后观察 5 分钟，至少 2 条样本，不健康占比 ≥50% 自动回滚
svc.CreateRelease(configrollout.CreateReleaseInput{
    ID:       "rel-20260925-01",
    Digest:   digest,
    Targets:  []string{"n1", "n2", "n3", "n4", "n5"},
    WaveSize: 2,
    Policy: configrollout.WavePolicy{
        MinSuccessRatio: 1,
        MaxFailures:     1,
        WaveTimeout:     10 * time.Minute,
        Health: configrollout.HealthPolicy{
            ObserveWindow:    5 * time.Minute,
            MinSamples:       2,
            FailureThreshold: 0.5,
        },
    },
})

// 3. 节点拉取配置（仅当前开放波次可取到；取消后未成功节点取不到）
cfg, err := svc.GetConfigForNode("rel-20260925-01", "n1")

// 4. 节点回执（重复/乱序/迟到均安全）
svc.AckReceipt(configrollout.Receipt{
    ReleaseID: "rel-20260925-01", NodeID: "n1", Digest: digest, Success: true,
})

// 4b. 上报健康样本（事件号唯一；重放幂等，乱序按采样时间归位）
svc.RecordSample("rel-20260925-01", configrollout.HealthSample{
    EventID: "evt-n1-0001", NodeID: "n1", Digest: digest,
    Healthy: true, SampledAt: time.Now().UTC(),
})

// 5. 运维操作与查询
svc.Pause("rel-20260925-01")
svc.Resume("rel-20260925-01")
svc.Cancel("rel-20260925-01")
progress, _ := svc.GetProgress("rel-20260925-01")
// progress.Health：健康证据与阈值计算；progress.Rollback：每节点前后版本与回滚进度

// 5b. 若触发了自动回滚（状态 rolling_back），节点恢复后确认：
svc.AckRollback("rel-20260925-01", "n1", preReleaseDigest)

// 6. 周期驱动超时与健康观测评估（也可依赖回执/样本/查询时惰性触发）
go func() {
    t := time.NewTicker(time.Minute)
    for range t.C {
        svc.TickTimeout("rel-20260925-01")
    }
}()

// 7. 派发 outbox（补偿通知等），建议 sink 内做幂等
svc.DispatchOutbox(ctx, func(ev configrollout.Event) error {
    return notifyDownstream(ev)
})
```

## 错误一览

所有业务错误均为包级哨兵错误，用 `errors.Is` 判定：

| 错误 | 含义 |
|---|---|
| `ErrConfigNotFound` / `ErrConfigExists` / `ErrInvalidDigest` | 配置未登记 / 同摘要内容冲突 / 摘要不匹配 |
| `ErrReleaseNotFound` / `ErrReleaseExists` / `ErrInvalidReleaseID` | 发布不存在 / ID 已存在 / ID 为空 |
| `ErrInvalidWaveSize` / `ErrEmptyTargets` / `ErrDuplicateNode` / `ErrInvalidPolicy` | 创建参数非法 |
| `ErrNotActive` / `ErrNotPaused` / `ErrAlreadyPaused` / `ErrTerminal` | 状态迁移不合法 |
| `ErrRollbackInProgress` / `ErrNoRollback` | 回滚期间禁止暂停/恢复 / 当前无进行中的回滚 |
| `ErrInvalidSample` / `ErrSampleConflict` | 健康样本缺事件号 / 同号异内容冲突 |
| `ErrNodeNotInRelease` / `ErrWaveNotOpen` / `ErrDigestMismatch` / `ErrStaleReceipt` | 回执被拒绝的各类原因 |
| `ErrConfigNotAvailable` | 节点当前不能取得该配置（波次未开放/暂停/取消后） |
| `ErrNodeNeverApplied` / `ErrEventNotFound` | 查询无结果 |

## 测试

```
go test -race ./...
```

测试覆盖（语句覆盖率约 91%）：

- 创建发布时的目标冻结、有序分波、节点全局唯一、参数校验（含健康策略冻结与校验）；
- 门槛达标后下一波原子开放、末波完成；未来波次回执/取配置被拒；
- 回执重复、乱序（先失败后成功 / 先成功后失败）、迟到（旧波次）、并发重复回执的精确一次计数；
- 失败超阈值暂停、等待超时暂停（含恢复后重置超时窗口）、达标后不误判超时；
- 观察窗口门禁：达标后进入观察状态、窗口未走完不开新波、窗口走完才推进/完成；
- 健康样本：事件号重放幂等、同号异内容冲突、跨发布/跨配置拒绝、乱序样本按采样时间归位、样本不足不判定失败；
- 健康失败阈值触发自动回滚：回滚计划只生成一次、回滚通知每节点恰好一条、每节点前后版本（PreDigest）正确；
- 回滚确认恢复节点发布前版本、全部确认后进入 RolledBack 终态、终态通知只写出一次；
- 迟到成功回执不覆盖回滚结果；回滚期间不开新波、不暂停/恢复、回执被拒；
- 自动回滚与人工取消/恢复并发竞争后只剩一条合法单调状态序列，回滚/终态通知不重复；
- 回滚中取消：合法迁移但不再生成补偿通知；
- 暂停/恢复/取消并发竞争后只剩一条合法单调状态序列，取消事件唯一；
- 取消后未成功节点取不到配置、成功节点审计保留、补偿通知每个成功节点恰好一条；
- 旧发布迟到回执不推进旧发布、不覆盖节点较新的已应用版本（`ErrStaleReceipt`）；
- 基于 JSON 文件存储的进程重启恢复（进度、暂停、投递标记、回滚计划与逐项进度、样本证据），重启后继续推进至完成/回滚完成；
- outbox 顺序投递、sink 失败断点续投。
