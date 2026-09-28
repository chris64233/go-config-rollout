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
- **健康策略（HealthPolicy）**：创建发布时与 WavePolicy 一并**冻结**（零值表示不启用，保持旧行为）：
  - `ObserveWindow`：波次成功门槛达标后进入**观察期**，只有通过完整观察窗口才允许开启下一波（末波也需通过观察才能完成发布）。
  - `MinSamples`：观察期内每个已成功节点必须上报的最少健康样本数；窗口届满但样本不足时继续等待（不算超时）。
  - `MaxUnhealthy`：当前波已成功节点中允许的不健康节点数，**超过**即触发自动回滚（见下文）。
  - `Quarantine`：隔离闸门（零值表示不允许隔离），与健康策略一并冻结：
    - `MaxNodes`：本次发布允许隔离的最大节点数，超过即触发现有整体回滚。
    - `MinHealthyCoverage` ∈ (0,1]：未隔离且已有健康结论的节点中，健康占比不得低于它，否则同样整体回滚。
- **节点已应用版本（NodeState）**：跨发布维护单调的应用序号。新发布的成功回执才会推进它；旧发布的迟到回执不会覆盖节点较新的已应用版本。
- **隔离（Quarantine）**：健康观察期操作员可依据一组健康样本把节点移出正常轨道。隔离决定绑定（发布、节点、配置摘要、隔离时波次、依据样本快照），落定即冻结。
- **补跑（Catch-up）**：隔离节点修复后可创建补跑计划，从节点**当前配置**开始、按原发布波次顺序恢复到目标摘要；计划携带单调递增的 fencing token，并发领取/上报时状态只能向前。

## 状态机

```
                 pause (手动/失败阈值/超时)
        Active ───────────────────────────► Paused
          │  ▲                                │
          │  └──────────── resume ────────────┘
          │
          ├──── cancel ───► Cancelled   （终态）
          │
          ├──── 健康失败阈值 ──► RollingBack ──全部节点恢复──► RolledBack （终态）
          │
          └── 末波通过观察窗口 ─► Completed   （终态）
```

`Cancelled` / `Completed` / `RolledBack` 为终态，任何后续变更返回 `ErrTerminal`。`RollingBack` 不接受暂停/恢复/取消/回执/样本（`ErrRollbackInProgress`）：**回滚与开新波次互斥**。所有变更在单个存储事务内完成并串行化，因此**自动回滚、人工取消、恢复并发发生时，最终只留下一条合法且单调的状态序列**（事件序号全局递增），不会一边继续开新波次一边回滚。

## 回执（Receipt）语义

节点回执会重复、乱序、迟到。幂等键为 **（发布, 节点, 配置摘要）**：

| 情形 | 处理 |
|---|---|
| 节点已有终态结果（成功/失败）后再来任意回执 | 幂等忽略（`ReceiptOutcome.Ignored = true`），首个结果保持不变，不重复计数、不重复发事件 |
| 节点不属于该发布 | `ErrNodeNotInRelease` |
| 回执摘要 ≠ 发布摘要 | `ErrDigestMismatch` |
| 节点属于旧波次（非当前开放波）且尚无终态结果 | `ErrWaveNotOpen`，不能推进当前流程 |
| 发布处于暂停 | `ErrNotActive` |
| 发布已取消/完成 | `ErrTerminal` |
| 节点已应用更新发布（seq 更大）的配置，旧发布回执迟到 | `ErrStaleReceipt`：不计数、不推进、不覆盖节点当前已应用版本 |

只有**当前开放波次**中、尚无终态结果的节点可以确认应用结果。

## 取消语义

- 取消后，所有尚未成功的节点不能再取得配置（`GetConfigForNode` 返回 `ErrConfigNotAvailable`）。
- 已经成功的节点保留审计结果（波次计数与节点已应用版本都不回滚）。
- 取消迁移（Active/Paused → Cancelled）只发生一次；迁移时对当时已成功的每个节点**恰好生成一条** `node.compensation` 补偿通知（outbox 事件）。

## 健康样本（HealthSample）语义

节点应用成功回执落定后进入观察期，通过 `ReportHealth` 上报健康样本。样本字段：唯一事件号 `EventID`、采样时间 `SampledAt`、节点、配置摘要、健康与否。

| 情形 | 处理 |
|---|---|
| 相同事件号 + 相同内容重放 | 幂等忽略（`HealthOutcome.Duplicate = true`），不重复计数 |
| 相同事件号 + 不同内容 | `ErrSampleConflict`（同号异内容冲突） |
| 跨发布（发布不存在 / 节点不属于该发布） | `ErrReleaseNotFound` / `ErrNodeNotInRelease` |
| 跨配置（样本摘要 ≠ 发布摘要） | `ErrDigestMismatch` |
| 采样时间乱序到达 | 接受；样本只按事件号去重，采样时间仅作证据留存 |
| 发布未启用健康策略 | `ErrHealthNotEnabled` |
| 发布暂停中 | 接受并记录证据，恢复（`Resume`）时一次性结算 |
| 发布回滚中 / 终态 | `ErrRollbackInProgress` / `ErrTerminal` |

节点健康判定：已成功节点存在任一失败样本即**不健康**；样本数达到 `MinSamples` 且无失败样本为健康；否则仍在观察中。

## 自动回滚语义

- 当前波已成功节点中，不健康节点数**超过**冻结的 `MaxUnhealthy` 时，在同一事务内**一次性**生成回滚计划（`rollback.started` 事件恰好一条），发布进入 `RollingBack`，此后不再开放任何新波次。
- 回滚计划对当时已成功应用配置的**每个节点恰好写出一条** `node.rollback` 通知，目标摘要为该节点**发布前**的配置摘要（在成功回执落定时捕获并冻结在 lease 上；空串表示此前无版本）。
- 节点通过 `ConfirmRollback(releaseID, nodeID, digest)` 确认恢复：摘要必须等于计划目标（`ErrDigestMismatch`），重复确认幂等；确认后节点已应用版本单调地恢复到发布前摘要，并写出 `node.restored`。
- 全部节点恢复后发布进入终态 `RolledBack`，`release.rolledback` 终态通知**恰好写出一次**（含进程重启后）。
- 回滚完成后到达的**迟到成功回执不得覆盖回滚结果**：已有终态结果的节点回执被幂等忽略，不触碰节点已应用版本。
- 回滚与取消互斥：`RollingBack` 中 `Cancel` 返回 `ErrRollbackInProgress`；已 `Cancelled` 的发布不会再做健康评估。

## 隔离（Quarantine）语义

健康观察期间，操作员通过 `QuarantineNode(QuarantineInput{ReleaseID, NodeID, EvidenceEventIDs, Reason})` 依据一组健康样本把节点标记为隔离。

- **绑定与冻结**：隔离决定绑定（发布、节点、配置摘要、隔离时所在波次），并冻结依据样本事件号快照与当时的样本计数。决定只允许在发布 `Active`、节点属于当前波次且已成功时作出；重复隔离幂等返回（`QuarantineOutcome.AlreadyQuarantined = true`），**任何迟到输入都不改写已形成的决定**。
- **依据校验**：必须携带至少一条依据（`ErrQuarantineNoEvidence`）；每条事件号都必须是已落库、且属于该（发布, 节点, 配置摘要）的样本，否则 `ErrQuarantineEvidenceMismatch`。
- **隔离后的影响**：
  - 该节点**不计入后续健康门槛**——不计入不健康节点数、不要求补齐 `MinSamples`、不计入健康覆盖率；
  - 该节点**不能再接收本次发布的新操作**：`GetConfigForNode` 返回 `ErrConfigNotAvailable`；
  - 健康样本迟到或重复到达时照常只追加到样本表（回滚/终态后按原规则拒绝），但**不会重新改写隔离决定或回滚决定**。
- **隔离不能掩盖大面积故障**。创建发布时冻结 `MaxNodes` 与 `MinHealthyCoverage`，每次隔离决策后在同一事务内结算：
  - 已隔离节点数 **超过** `MaxNodes`；或
  - 未隔离且已有健康结论（样本达标或含失败样本）的节点中，健康覆盖率 **低于** `MinHealthyCoverage`；

  任一成立即触发现有的整体回滚（`QuarantineOutcome.RollbackStarted = true`），被隔离节点同样进入回滚计划、恢复到各自发布前摘要。窗口届满时覆盖率分母即全部未隔离已成功节点。

## 补跑（Catch-up）语义

问题处理后，通过 `CreateCatchupPlan(releaseID, nodeID)` 为隔离节点创建补跑计划。

- **从当前配置开始、按波次顺序恢复**：计划创建时冻结起点 `FromDigest`（节点当前实际配置）、目标 `TargetDigest`（原发布摘要）、节点原波次 `OriginalWave` 与有序路径 `Path`；节点已在目标上时路径退化为单元素。
- **每个隔离节点至多一个未作废计划**：重复创建返回 `ErrCatchupExists`；仅未隔离节点返回 `ErrNodeNotQuarantined`。
- **领取与 fencing**：`ClaimCatchup(planID, claimant)` 返回单调递增的 token 与计划快照；`ReportCatchup` / `GetCatchupConfig` 必须携带它。每次重新领取（失败后）换发新 token，**旧领取者的迟到成功以 `ErrCatchupFencing` 拒绝**，不能覆盖后来的状态。
- **成功不写发布终态**：补跑成功只把节点已应用版本单调推进到目标摘要并写出 `catchup.succeeded`，**绝不重复写出** `release.completed` / `release.rolledback`。失败把任务退回 `failed`，可重新领取重试。
- **无法继续的条件**（领取/成功上报时重新判定，命中即在同一事务作废计划并写出 `catchup.aborted`）：
  - 整体发布已回滚（`release_rolled_back`）——回滚/取消在同一事务作废旧补跑；
  - 发布已取消（`release_cancelled`）；
  - 节点被更新的有效发布纳入目标（`superseded_by_new_release`）——新发布创建事务内立即作废；
  - 节点基线漂移：当前配置既非计划起点也非目标（`node_baseline_changed`）。

  已作废计划不可复活，旧上报/取配置一律 `ErrCatchupAborted`。

## 查询

- `GetProgress(releaseID)`：波次进度（查询时顺带完成超时/健康惰性评估）。
- `GetHealth(releaseID)`：健康视图，包含冻结的健康策略、观察窗口起止（`ThresholdMetAt` / `WindowEndsAt`）、阈值计算（`UnhealthyNodes` vs `MaxUnhealthy`、`ThresholdBreached`）、每个节点的健康证据（样本数/失败样本数/健康判定）与前后版本（`PrevDigest` / `CurrentDigest`）、回滚计划与进度（`Rollback.Restored / Total` 及逐节点恢复项）。第三轮新增：
  - **隔离闸门与覆盖率**：`QuarantineEnabled` / `QuarantinePolicy`、`QuarantinedNodes` / `MaxQuarantined`、`HealthyCoverage`（含 `CoverageNumerator / CoverageDenominator`）、`MinHealthyCoverage`；
  - **隔离依据**：每个节点的 `Quarantined` 标记与 `Quarantine` 子视图（波次、摘要、原因、冻结的依据事件号 `EvidenceEventIDs`、隔离时样本计数）；
  - **节点实际配置**：每个节点的 `CurrentDigest`（跨发布最新值）；
  - **补跑进度**：`Catchups[]` 逐计划列出状态、起点/目标/路径、fencing、领取者、成功时间，以及 `NodeCurrentDigest` 与无法继续时的 `BlockedReason`。
- `GetCatchupPlan(planID)`：单个补跑计划的最新快照（状态、进度、作废原因）。
- `NodeAppliedVersion(nodeID)`：节点当前已应用版本（审计用）。

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
//    并冻结健康策略：达标后观察 5 分钟、每节点至少 2 条样本、不允许不健康节点
svc.CreateRelease(configrollout.CreateReleaseInput{
    ID:       "rel-20260925-01",
    Digest:   digest,
    Targets:  []string{"n1", "n2", "n3", "n4", "n5"},
    WaveSize: 2,
    Policy: configrollout.WavePolicy{
        MinSuccessRatio: 1,
        MaxFailures:     1,
        WaveTimeout:     10 * time.Minute,
    },
    Health: configrollout.HealthPolicy{
        ObserveWindow: 5 * time.Minute,
        MinSamples:    2,
        MaxUnhealthy:  0,
        // 隔离闸门：最多隔离 1 个节点，未隔离节点健康覆盖率不得低于 50%
        Quarantine: configrollout.QuarantinePolicy{
            MaxNodes:           1,
            MinHealthyCoverage: 0.5,
        },
    },
})

// 3. 节点拉取配置（仅当前开放波次可取到；取消后未成功节点取不到）
cfg, err := svc.GetConfigForNode("rel-20260925-01", "n1")

// 4. 节点回执（重复/乱序/迟到均安全）
svc.AckReceipt(configrollout.Receipt{
    ReleaseID: "rel-20260925-01", NodeID: "n1", Digest: digest, Success: true,
})

// 5. 观察期内上报健康样本（事件号幂等；触发阈值会自动生成回滚计划）
svc.ReportHealth(configrollout.HealthSample{
    EventID: "evt-0001", ReleaseID: "rel-20260925-01", NodeID: "n1",
    Digest: digest, Healthy: true, SampledAt: time.Now().UTC(),
})

// 6. 运维操作与查询
svc.Pause("rel-20260925-01")
svc.Resume("rel-20260925-01")
svc.Cancel("rel-20260925-01")
progress, _ := svc.GetProgress("rel-20260925-01")
health, _ := svc.GetHealth("rel-20260925-01") // 健康证据/阈值计算/前后版本/回滚进度

// 7. 若触发了自动回滚：节点按 node.rollback 通知恢复后确认
svc.ConfirmRollback("rel-20260925-01", "n1", prevDigest)

// 7b. 健康观察期隔离异常节点（携带依据样本事件号）；闸门失守会触发整体回滚
svc.QuarantineNode(configrollout.QuarantineInput{
    ReleaseID:        "rel-20260925-01",
    NodeID:           "n1",
    EvidenceEventIDs: []string{"evt-0001"},
})

// 7c. 节点修复后创建补跑计划 -> 领取（拿 fencing token）-> 取配置 -> 上报
plan, _ := svc.CreateCatchupPlan("rel-20260925-01", "n1")
token, _, _ := svc.ClaimCatchup(plan.ID, "repair-worker-1")
_, _ = svc.GetCatchupConfig(plan.ID, token)
svc.ReportCatchup(configrollout.CatchupReport{
    PlanID: plan.ID, NodeID: "n1", Token: token, Success: true, Digest: digest,
})

// 8. 周期驱动超时与健康评估（也可依赖回执/样本/查询时惰性触发）
go func() {
    t := time.NewTicker(time.Minute)
    for range t.C {
        svc.TickTimeout("rel-20260925-01")
    }
}()

// 9. 派发 outbox（补偿/回滚通知等），建议 sink 内做幂等
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
| `ErrNodeNotInRelease` / `ErrWaveNotOpen` / `ErrDigestMismatch` / `ErrStaleReceipt` | 回执被拒绝的各类原因 |
| `ErrConfigNotAvailable` | 节点当前不能取得该配置（波次未开放/暂停/取消后） |
| `ErrNodeNeverApplied` / `ErrEventNotFound` | 查询无结果 |
| `ErrInvalidSample` / `ErrSampleConflict` / `ErrHealthNotEnabled` | 样本缺事件号或采样时间 / 同号异内容冲突 / 发布未启用健康策略 |
| `ErrRollbackInProgress` / `ErrNoRollback` | 回滚中拒绝变更（暂停/恢复/取消/回执/样本）/ 发布无回滚计划 |
| `ErrQuarantineNotEnabled` / `ErrNodeNotQuarantined` | 发布未配置隔离闸门 / 节点未隔离（重复隔离走幂等 outcome，不报错） |
| `ErrQuarantineNoEvidence` / `ErrQuarantineEvidenceMismatch` | 隔离无依据样本 / 依据不属于该（发布, 节点, 摘要） |
| `ErrCatchupNotFound` / `ErrCatchupExists` | 补跑计划不存在 / 隔离节点已有未作废计划 |
| `ErrCatchupNotClaimable` / `ErrCatchupFencing` / `ErrCatchupAborted` | 计划当前不可领取（成功/运行中）/ 领取 token 过期（旧领取者迟到上报）/ 计划已作废（回滚、取消、被取代、基线漂移） |

## 测试

```
go test -race ./...
```

测试覆盖（语句覆盖率约 91%）：

- 创建发布时的目标冻结、有序分波、节点全局唯一、参数校验（含健康策略冻结与非法值）；
- 门槛达标后下一波原子开放、末波完成；未来波次回执/取配置被拒；
- 回执重复、乱序（先失败后成功 / 先成功后失败）、迟到（旧波次）、并发重复回执的精确一次计数；
- 失败超阈值暂停、等待超时暂停（含恢复后重置超时窗口）、达标后不误判超时；
- 暂停/恢复/取消并发竞争后只剩一条合法单调状态序列，取消事件唯一；
- 取消后未成功节点取不到配置、成功节点审计保留、补偿通知每个成功节点恰好一条；
- 旧发布迟到回执不推进旧发布、不覆盖节点较新的已应用版本（`ErrStaleReceipt`）；
- 健康观察窗口门禁：达标后须窗口届满且每节点样本数达标才开下一波；末波须通过观察才能完成；
- 健康样本：事件号幂等重放、同号异内容冲突、跨发布/跨配置拒绝、乱序到达接受、暂停期只记录不评估（恢复时结算）；
- 健康阈值触发自动回滚：计划一次性生成、每节点恢复到各自发布前摘要、逐节点确认、终态唯一；
- 回滚与取消/恢复并发竞争：回滚与取消互斥、回滚期间不开新波次、事件序列单调合法；
- 回滚完成后迟到成功回执不覆盖回滚结果；回滚通知与终态通知重启后仍各只写出一次；
- 健康查询视图：证据、阈值计算、每节点前后版本、回滚进度；
- 基于 JSON 文件存储的进程重启恢复（进度、暂停、投递标记、回滚计划），重启后继续推进至完成；
- outbox 顺序投递、sink 失败断点续投；
- 隔离策略冻结与非法值（必须随健康观察一并启用、`MaxNodes≥1`、覆盖率 ∈ (0,1]）；
- 隔离决定绑定（发布/节点/摘要/波次）与依据冻结：重复隔离幂等、迟到样本（含失败样本）不改写决定、被隔离节点取不到配置；
- 被隔离节点排除出门槛：不要求补齐样本、同伴健康且窗口届满即可推进/完成、覆盖率视图正确；
- 隔离状态守卫（暂停中/非当前波/非成功节点拒绝）；
- 隔离闸门：超 `MaxNodes` 或覆盖率低于 `MinHealthyCoverage` 时**同一事务**触发整体回滚，被隔离节点同样恢复到发布前摘要；
- 回滚决定不被迟到样本改写（回滚中/终态拒绝样本与隔离操作）；
- 补跑创建冻结：从节点当前配置开始、按原波次锚定、目标为发布摘要，重复创建/非隔离节点/终态后创建均被拒；
- 补跑 fencing：未领取不上报、错误摘要/错误节点拒绝、成功不写发布终态通知、失败可重新领取换发新 token、旧领取者迟到成功被 `ErrCatchupFencing` 拒绝；
- 补跑失效：回滚/取消同事务作废（含旧领取者迟到成功被拦），被更新发布接管时在创建事务内立即作废，基线漂移领取时作废并展示阻塞原因；
- 补跑并发：多路领取/失败/成功并发后计划恰好成功一次、`catchup.succeeded` 恰好一条、版本单调；隔离/失败样本/取消并发下事件序列合法且回滚与取消互斥；
- 隔离/回滚/补跑重启持久化：依据快照、计划状态、fencing 与事件唯一性在重开存储后保持；
- 健康视图扩展：隔离标记与依据、覆盖率分子分母、节点实际配置、补跑进度与阻塞原因。
