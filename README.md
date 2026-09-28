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
- **隔离策略（QuarantinePolicy）**：随健康观察一并**冻结**（零值表示不允许隔离；配置隔离但未启用健康观察返回 `ErrInvalidPolicy`）。隔离用于把观察期内发现的异常节点移出后续健康门槛，但隔离不能掩盖大面积故障，因此同时冻结两条硬限：
  - `MaxQuarantined`：整个发布允许隔离的**最大累计节点数**（跨波次），超过立即整体回滚。
  - `MinHealthyCoverage` ∈ (0,1]：健康覆盖率下限。覆盖率 = 当前观察波中「未隔离的已成功节点」÷「波内全部已成功节点」；低于下限立即整体回滚。
- **节点已应用版本（NodeState）**：跨发布维护单调的应用序号。新发布的成功回执才会推进它；旧发布的迟到回执不会覆盖节点较新的已应用版本。

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

## 节点隔离（Quarantine）语义

健康观察期间，操作员可通过 `QuarantineNode(QuarantineInput{ReleaseID, NodeID, Basis, Reason})` 把当前观察波中**已成功应用**的节点标记为隔离。

- **隔离决定不可变并绑定四要素**：发布、节点、配置摘要（恒等于发布摘要）、当前波次；`Basis` 是一组已落库健康样本的事件号作为依据（必须存在且属于该节点，空依据 `ErrQuarantineBasisMissing`、依据未知/属于他人节点 `ErrQuarantineBasisUnknown`）。决定写出 `node.quarantined` 事件，每个节点每次发布**恰好一条**；重复隔离幂等返回 `Already`。
- **被隔离节点不再计入后续健康门槛**：既不算不健康节点，也不要求其样本数达标；波次可仅凭其余健康节点通过观察。被隔离节点也**不能继续接收本次发布的新操作**：新事件号的健康样本返回 `ErrNodeQuarantined`，再次拉取配置同样被拒。
- **迟到/重复样本不改写决定**：隔离依据样本的同事件号重放仍幂等（`Duplicate`），但新事件号样本一律拒收；隔离/回滚一旦形成即定格。
- **隔离不能掩盖大面积故障**：`QuarantineNode` 在同一事务内结算冻结的两条限制——累计隔离数超过 `MaxQuarantined`，或当前波健康覆盖率低于 `MinHealthyCoverage`——任一越界立即触发现有整体回滚流程（回滚原因为 `quarantine_limit`）。整体回滚**不豁免隔离节点**：它们同样出现在回滚计划中，回到各自发布前摘要。
- 只能隔离**当前观察波**中已成功的节点（旧波次已离开观察期、未来波次尚未应用，均返回 `ErrQuarantineNodeNotSucceeded`）；`Paused` 期间可记录隔离决定，`Resume` 时按冻结限制统一结算（可能立即回滚）。

## 补跑（Catch-up）语义

隔离节点的问题处理完成后，操作员通过 `CreateCatchupPlan(releaseID)`（或重建 `RebuildCatchupPlan`）为隔离节点创建**有序补跑计划**。

- **从当前配置开始、按原波次顺序回到目标摘要**：创建计划时冻结每个节点的起始配置（节点当前实际摘要 `StartDigest` 与应用序号基线 `BaselineSeq`），目标固定为原发布摘要 `TargetDigest`；补跑项严格按原发布波次升序领取，前序波次未全部成功时后序波次被屏障挡住（`ErrNoCatchupWork` / 视图 `BlockedReason=earlier_wave_unfinished`）。
- **租约与 epoch**：`ClaimCatchup(releaseID, workerID, leaseTTL)` 领取下一可执行项并返回带 `Epoch` 的租约；租约过期后项可被重新领取（epoch 加一，写出新的 `node.catchup` 通知）。`CompleteCatchup` 必须携带领取时的 epoch：**旧领取者的迟到成功一律 `ErrClaimLost`，不能覆盖后来的重新领取/回滚**。
- **失败屏障与重建**：补跑失败使该项形成波次屏障（`ErrCatchupBlocked`），需先修复再 `RebuildCatchupPlan`：旧计划写出 `catchup.aborted`（`plan_replaced`）并以节点最新基线建新计划；已成功节点不重跑。
- **三种作废原因（旧补跑不得执行）**，每种都写出恰好一条 `catchup.aborted`：
  - `release_rolled_back`：整体发布已回滚（健康阈值或隔离限制触发）；`Cancel` 也按此作废。
  - `release_superseded`：节点基线已变化——被更新发布成功推进（`AckReceipt` 同事务作废旧计划），或领取/回报时检测到节点 `AppliedSeq` 偏离创建时基线（`ErrBaselineChanged`）。
  - `plan_replaced`：被更新的补跑计划取代。
- **状态只向前、终态通知不重复**：补跑成功单调推进节点已应用版本到目标摘要并标记隔离记录 `Remediated`；全部项成功后计划进入 `done`，写出恰好一条 `catchup.completed`。**它不是发布终态通知**——`release.completed` / `release.rolledback` 仍只由主流程写出一次，补跑不会重复写出。重复成功回报幂等忽略。
- 计划作废后领取/回报返回 `ErrCatchupNotActive`；已存在未结束计划时重复创建返回 `ErrCatchupActive`。

## 查询

- `GetProgress(releaseID)`：波次进度（查询时顺带完成超时/健康惰性评估）。
- `GetHealth(releaseID)`：健康视图，包含冻结的健康策略、观察窗口起止（`ThresholdMetAt` / `WindowEndsAt`）、阈值计算（`UnhealthyNodes` vs `MaxUnhealthy`、`ThresholdBreached`）、每个节点的健康证据（样本数/失败样本数/健康判定，隔离节点显示为 `quarantined` 与隔离依据 `Basis`）与前后版本（`PrevDigest` / `CurrentDigest`）、回滚计划与进度（`Rollback.Restored / Total` 及逐节点恢复项），并内嵌：
  - `Quarantine`：冻结的隔离策略、累计隔离数、当前波覆盖率计算（`WaveSucceeded` / `WaveCovered` / `Coverage` / `LimitBreached`）与逐节点隔离记录（绑定波次/摘要/依据/时间）。
  - `Catchup`：补跑计划的 attempt、状态、成功/失败/总数、作废原因与逐节点项（起始/目标摘要、节点**实际配置** `CurrentDigest`、`BaselineChanged`、租约领取者与到期时间、以及**无法继续的原因** `BlockedReason`：`failed` / `earlier_wave_unfinished` / `lease_expired_reclaimable` / `baseline_changed` / `plan_aborted:<reason>`）。
- `GetCatchup(releaseID)`：单独获取补跑计划视图（查询时顺带惰性作废已随回滚/取消失效的计划）。
- `NodeAppliedVersion(nodeID)`：节点当前已应用版本（审计用）。

## 持久化与 outbox

- 状态与 outbox 事件在**同一事务**中提交。`NewFileStore(path)` 使用「同目录临时文件 + fsync + rename」原子落盘，进程重启后重新打开即可从断点继续（发布进度、暂停状态、已应用版本、事件投递标记全部恢复）。
- outbox 事件只追加、不删除（同时充当审计流）。`DispatchOutbox` 按全局序号顺序投递，每条成功后标记 `Delivered`；sink 返回错误即停在断点，下次调用继续。投递为至少一次语义，**sink 需按事件键幂等**。
- 本次新增事件：`node.quarantined`（隔离决定，每节点每发布恰好一条）、`catchup.created` / `catchup.aborted` / `catchup.completed`（计划生命周期，`Attempt` 标识计划版本）、`node.catchup`（领取通知，携带 `Epoch`，重领写新事件，按（发布,节点,epoch）幂等）、`node.catchup_succeeded` / `node.catchup_failed`。补跑事件**不改变发布主状态**，`release.completed` / `release.rolledback` 不会因为补跑出现第二次。

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
    },
    // 隔离策略随健康观察冻结：最多隔离 2 个节点、当前波健康覆盖率不低于 60%
    Quarantine: configrollout.QuarantinePolicy{
        MaxQuarantined:     2,
        MinHealthyCoverage: 0.6,
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

// 5b. 依据一组健康样本隔离当前观察波中异常的 n1
svc.QuarantineNode(configrollout.QuarantineInput{
    ReleaseID: "rel-20260925-01", NodeID: "n1",
    Basis:  []string{"evt-0001"},
    Reason: "high error rate",
})

// 5c. 问题修复后为隔离节点创建补跑计划，worker 按原波次顺序领取/回报
plan, skips, err := svc.CreateCatchupPlan("rel-20260925-01")
_ = plan
_ = skips
//    补跑失败后修复并重建（旧计划 plan_replaced 作废，已成功节点不重跑）
// svc.RebuildCatchupPlan("rel-20260925-01")
lease, err := svc.ClaimCatchup("rel-20260925-01", "worker-1", 5*time.Minute)
if err == nil {
    target, _ := svc.GetCatchupConfig("rel-20260925-01", lease.NodeID) // 持有效租约才能取配置
    _ = target
    // ...worker 从 lease.FromDigest 施加到 lease.ToDigest...
    svc.CompleteCatchup(configrollout.CatchupResult{
        ReleaseID: "rel-20260925-01", NodeID: lease.NodeID,
        Epoch: lease.Epoch, Digest: lease.ToDigest, Success: true,
    })
}
catchupView, _ := svc.GetCatchup("rel-20260925-01") // 补跑进度与无法继续的原因
_ = catchupView

// 6. 运维操作与查询
svc.Pause("rel-20260925-01")
svc.Resume("rel-20260925-01")
svc.Cancel("rel-20260925-01")
progress, _ := svc.GetProgress("rel-20260925-01")
health, _ := svc.GetHealth("rel-20260925-01") // 健康证据/隔离依据/覆盖率/实际配置/补跑进度

// 7. 若触发了自动回滚：节点按 node.rollback 通知恢复后确认
svc.ConfirmRollback("rel-20260925-01", "n1", prevDigest)

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
| `ErrRollbackInProgress` / `ErrNoRollback` | 回滚中拒绝变更（暂停/恢复/取消/回执/样本/补跑）/ 发布无回滚计划 |
| `ErrQuarantineNotEnabled` / `ErrAlreadyQuarantined` / `ErrNodeQuarantined` | 未启用隔离 / 节点已被隔离（幂等）/ 已隔离节点不能再接收新操作 |
| `ErrQuarantineBasisMissing` / `ErrQuarantineBasisUnknown` / `ErrQuarantineNodeNotSucceeded` | 隔离依据为空 / 依据样本不存在或属于他人节点 / 节点不是当前观察波已成功节点 |
| `ErrNoCatchup` / `ErrCatchupActive` / `ErrCatchupNotActive` / `ErrNoCatchupWork` | 无补跑计划（无隔离或计划不存在）/ 已有活跃计划 / 计划已结束或作废 / 当前无可领取项 |
| `ErrCatchupEntryMissing` / `ErrCatchupNotClaimed` / `ErrClaimLost` / `ErrCatchupBlocked` | 节点不在计划 / 项未领取 / 租约 epoch 失效（旧领取者迟到）/ 前序波次有失败屏障 |
| `ErrBaselineChanged` | 创建补跑后节点基线已变化（被新发布推进/带外改动），旧补跑作废不得执行 |

## 测试

```
go test -race ./...
```

测试覆盖（语句覆盖率约 91%）：

- 创建发布时的目标冻结、有序分波、节点全局唯一、参数校验（含健康/隔离策略冻结与非法值）；
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
- **节点隔离**：策略冻结/校验、隔离决定绑定（发布/节点/摘要/波次/依据）、依据缺失与归属校验、重复隔离幂等、依据样本重放不改写决定、新样本/取配置对隔离节点关闭；
- **隔离移出健康门槛**：被隔离节点既不算不健康也不要求样本，其余节点可正常通过观察；
- **隔离不掩盖大面积故障**：累计隔离数超 `MaxQuarantined`、覆盖率低于 `MinHealthyCoverage` 均在同事务整体回滚（`quarantine_limit`），回滚计划不豁免隔离节点；暂停期隔离恢复时结算；隔离/回滚/样本并发下状态序列单调、回滚至多一次；
- **补跑计划**：从节点当前配置与基线冻结、严格按原波次顺序领取（前序波次进行中/租约有效不得跳波）、并发领取恰好一个成功、持租约才能取配置；
- **补跑租约**：过期可重领（epoch 递增）、旧领取者迟到成功 `ErrClaimLost` 不覆盖、成功摘要必须等于目标、重复成功幂等且不重复写终态通知；
- **补跑失败屏障与重建**：失败项挡住后序波次，重建作废旧计划（`plan_replaced`）、排除已成功节点、重建后有序跑完；
- **补跑作废三因**：整体回滚/取消（`release_rolled_back`）、被新发布推进或基线漂移（`release_superseded`，含领取与回报两个时刻的检测）、被新计划取代；旧领取者迟到成功一律拒绝且节点版本不被覆盖；
- **补跑查询/重启**：进度、起始/目标/实际配置、基线漂移、波次屏障与各类 `BlockedReason`；计划/attempt/epoch/租约与终态通知在进程重启后恢复且仍只一次；
- 基于 JSON 文件存储的进程重启恢复（进度、暂停、投递标记、回滚计划），重启后继续推进至完成；
- outbox 顺序投递、sink 失败断点续投。
