# go-config-rollout

按**波次（wave）**推进、**进程重启后可继续**的配置发布服务。配置包由不可变摘要（digest）标识；
创建发布时冻结目标节点并划分为有顺序的波次，一个节点在一次发布中只出现一次。

开发环境：Go 1.23.0，无第三方依赖。

## 核心能力

- **配置登记**：`RegisterConfig` 登记不可变配置；同一摘要重复登记相同内容幂等，内容不同返回冲突错误。
- **发布创建**：`CreateRollout` 冻结节点集合与波次划分，第一波随创建**原子开放**。
- **按波次推进**：只有当前开放波次的节点能拉取配置（`FetchConfig`）与回执（`RecordReceipt`）；
  当前波成功数达到门槛后，下一波才原子开放；全部波次达标后发布成功。
- **暂停条件**：失败数超过 `MaxFailures`、或当前波等待超过 `WaveTimeout`，发布自动暂停；
  也支持人工 `Pause` / `Resume`。恢复后重新计算完整的超时窗口。
- **取消**：`Cancel` 后任何节点都无法再取得配置或上报回执；已成功节点的审计结果原样保留，
  并为每个已成功节点向 outbox 写入**恰好一次**补偿通知。
- **进度查询**：`GetProgress` 返回每波状态（pending/open/done/interrupted）、成功/失败/待确认计数
  与完整状态迁移历史；另有 `GetNodeResult`、`GetApplied`、`ListRolloutIDs`。
- **持久化与 outbox**：业务状态与通知 outbox 在同一快照中原子提交（默认 JSON 文件，临时文件 +
  rename），重启后 `Start` 加载即可继续；`PendingOutbox` / `MarkOutboxPublished` 供投递器使用。

## 快速开始

```go
store := configrollout.NewFileStore("data/rollout.json")
svc := configrollout.NewService(store, nil) // nil = 系统时钟
ctx := context.Background()
if err := svc.Start(ctx); err != nil { /* 进程重启后从快照继续 */ }

// 1. 登记配置（digest 由调用方计算，如 sha256:...）
svc.RegisterConfig(ctx, "sha256:abc", configBytes)

// 2. 创建两波发布：第一波 2 节点，第二波 3 节点；每波 1 个成功即推进；
//    最多容忍 1 个失败；单波超时 10 分钟
r, err := svc.CreateRollout(ctx, configrollout.CreateRolloutInput{
    ID:               "rollout-2026-09-25",
    Digest:           "sha256:abc",
    Waves:            [][]string{{"node-1", "node-2"}, {"node-3", "node-4", "node-5"}},
    SuccessThreshold: 1,
    MaxFailures:      1,
    WaveTimeout:      10 * time.Minute,
})

// 3. 节点拉取配置（仅当前波节点成功）
cfg, err := svc.FetchConfig(ctx, r.ID, "node-1")

// 4. 节点应用后回执（可重复、乱序、迟到，服务幂等处理）
svc.RecordReceipt(ctx, configrollout.Receipt{
    RolloutID: r.ID, NodeID: "node-1", Digest: "sha256:abc",
    ReceiptID: "node-local-receipt-id", Result: configrollout.ResultSucceeded,
})

// 5. 查询进度
p, _ := svc.GetProgress(ctx, r.ID)

// 6. 人工控制
svc.Pause(ctx, r.ID)
svc.Resume(ctx, r.ID)
svc.Cancel(ctx, r.ID)
```

超时由两种途径触发：回执/拉取时惰性检测，或由后台定时器主动扫描：

```go
go svc.RunTimeoutSweeper(ctx, 30*time.Second) // 进程重启后重新启动即可
```

## 状态机

```
                 成功门槛达标（最后一波）
        ┌──────────────────────────────► succeeded（终态）
        │
active ◄┄► paused ──取消──► cancelled（终态）
  │  ▲       │  ▲
  └──┘       └──┘
  恢复        暂停/恢复
active/paused ──取消──► cancelled
```

每次迁移都追加一条不可变的 `StatusEvent`（from/to/reason/时间）。暂停、恢复、取消即使高并发
同时发生，锁串行化后也只会留下一条首尾相接、单调合法的状态序列；终态上的重复调用是幂等空操作。

## 回执语义（重复、乱序、迟到）

回执校验顺序如下，命中即返回，不会产生任何副作用：

1. 回执摘要 ≠ 本次发布摘要 → `ErrDigestMismatch`（旧配置的串扰/迟到回执）。
2. 该节点在本次发布**已有回执** → 幂等返回首条记录；重复回执即使结果相反（成功 vs 失败）
   也不能覆盖审计结果或改变计数。
3. 节点已在**更新的发布**（seq 更大）中成功应用 → `ErrStaleReceipt`；旧发布回执既不能推进
   旧流程，也不能覆盖节点上较新的“已应用版本”（`NodeLastApplied` 只随成功单调推进）。
4. 节点不属于本发布 / 不属于当前开放波次 / 发布暂停或已取消 → 对应明确错误。
   注意：波次达标关闭后，该波中**从未回执过**的节点的迟到首条回执会被拒绝（`ErrWaveNotOpen`）；
   而已有回执节点的重复投递仍按第 2 条幂等返回。

## 取消后的补偿

取消时，为发布中每个状态为 succeeded 的节点写入一条 `node.compensation` outbox 消息，
去重键为 `compensation:<rolloutID>:<nodeID>`。outbox 以 Key 全局去重：

- 取消与暂停/恢复/回执并发时，只有真正落在“取消前”的成功回执会产生补偿；
- 重复调用取消、进程重启后再取消，补偿都不会重复生成；
- 投递器至少投递一次，`MarkOutboxPublished` 幂等确认。

## 错误一览

| 错误 | 含义 |
| --- | --- |
| `ErrInvalidDigest` / `ErrConfigNotFound` / `ErrConfigConflict` | 配置摘要非法 / 未登记 / 同摘要不同内容 |
| `ErrEmptyWaves` / `ErrEmptyNode` / `ErrDuplicateNode` | 创建发布参数非法；节点跨波次重复 |
| `ErrInvalidThreshold` | 成功门槛或超时参数非法 |
| `ErrRolloutNotFound` / `ErrNodeNotInRollout` | 发布或节点不存在 |
| `ErrDigestMismatch` / `ErrStaleReceipt` | 回执摘要不符 / 节点已应用更新版本 |
| `ErrWaveNotOpen` | 节点所属波次不是当前开放波次 |
| `ErrRolloutPaused` / `ErrRolloutCancelled` / `ErrAlreadyFinished` | 当前发布状态不允许该操作 |
| `ErrInvalidResult` | 回执结果非法 |

所有错误均为哨兵错误，使用 `errors.Is` 判定。

## 持久化模型

- `FileStore`（默认）：JSON 快照写临时文件后 `rename`，崩溃时磁盘上只有旧快照或新快照；
  生产环境可替换为实现 `Store` 接口（单条 `Load`/`Save`）的数据库实现，
  例如在同一事务内写状态表与 outbox 表。
- `MemoryStore`：测试用。
- 状态与 outbox 同快照提交，天然满足“状态变更与其通知原子一致”；重启后待发 outbox 不丢失。

## 测试

```bash
go test -race ./...
```

覆盖内容：配置登记幂等/冲突、节点冻结与重复校验、波次门槛原子推进、部分门槛下旧波迟到回执
拒绝、失败阈值暂停、超时暂停与恢复重计时、重复/乱序/摘要不符/跨发布陈旧回执、暂停-恢复-取消
状态单调、取消后未开始节点被拦截与补偿仅一次、20 轮控制操作与回执并发竞争（`-race`）、
基于文件存储的进程重启恢复、outbox 生命周期与进度计数。
