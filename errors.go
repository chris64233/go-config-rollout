package configrollout

import "errors"

// 明确的业务错误，使用 errors.Is 判定。
var (
	// ErrConfigNotFound：配置摘要未登记。
	ErrConfigNotFound = errors.New("config not found")
	// ErrConfigExists：配置摘要已登记（登记天然幂等，但内容不一致时报错）。
	ErrConfigExists = errors.New("config already registered with different content")
	// ErrInvalidDigest：登记内容的摘要与声明摘要不一致。
	ErrInvalidDigest = errors.New("config digest does not match content")

	// ErrReleaseNotFound：发布不存在。
	ErrReleaseNotFound = errors.New("release not found")
	// ErrReleaseExists：发布 ID 已存在。
	ErrReleaseExists = errors.New("release already exists")
	// ErrInvalidReleaseID：发布 ID 为空。
	ErrInvalidReleaseID = errors.New("invalid release id")
	// ErrInvalidWaveSize：创建发布时波次切分参数非法。
	ErrInvalidWaveSize = errors.New("invalid wave size")
	// ErrEmptyTargets：目标节点集合为空。
	ErrEmptyTargets = errors.New("empty target nodes")
	// ErrDuplicateNode：目标节点在本次发布中重复出现。
	ErrDuplicateNode = errors.New("duplicate node in release targets")
	// ErrInvalidPolicy：发布策略参数非法。
	ErrInvalidPolicy = errors.New("invalid rollout policy")

	// ErrNotPaused：只能对处于 Paused 状态的发布执行恢复。
	ErrNotPaused = errors.New("release is not paused")
	// ErrAlreadyPaused：发布已经处于暂停状态，重复暂停无效。
	ErrAlreadyPaused = errors.New("release already paused")
	// ErrNotActive：回执只在发布 Active 时被接受。
	ErrNotActive = errors.New("release is not active")
	// ErrTerminal：终态发布（Cancelled/Completed）不可再变更。
	ErrTerminal = errors.New("release already finished")

	// ErrNodeNotInRelease：回执节点不属于该发布。
	ErrNodeNotInRelease = errors.New("node is not part of the release")
	// ErrWaveNotOpen：节点所在波次尚未开放，其回执无效。
	ErrWaveNotOpen = errors.New("node's wave is not the current open wave")
	// ErrDigestMismatch：回执声明的配置摘要与发布摘要不一致。
	ErrDigestMismatch = errors.New("receipt digest does not match release digest")
	// ErrNodeAlreadyDone：节点已有终态结果，重复回执被幂等忽略（成功保持成功）。
	ErrNodeAlreadyDone = errors.New("node already has a terminal result")
	// ErrStaleReceipt：节点已应用更新发布的配置，来自旧发布的迟到回执
	// 既不计数推进，也不覆盖节点当前已应用版本。
	ErrStaleReceipt = errors.New("stale receipt from an older release")

	// ErrConfigNotAvailable：取消后，尚未开始（未成功）的节点不能再取得配置。
	ErrConfigNotAvailable = errors.New("config is no longer available for this node")
	// ErrNodeNeverApplied：节点尚未在任何发布中成功应用过配置。
	ErrNodeNeverApplied = errors.New("node has never applied a config")
	// ErrEventNotFound：outbox 事件序号不存在。
	ErrEventNotFound = errors.New("outbox event not found")

	// ErrInvalidSample：健康样本缺少事件号或采样时间。
	ErrInvalidSample = errors.New("invalid health sample")
	// ErrSampleConflict：相同事件号的样本内容不一致（同号异内容）。
	ErrSampleConflict = errors.New("health sample conflicts with an existing event id")
	// ErrHealthNotEnabled：发布未启用健康观察，不接受健康样本。
	ErrHealthNotEnabled = errors.New("release has no health policy")
	// ErrRollbackInProgress：发布正在自动回滚，暂停/恢复/取消/回执/样本
	// 均不可再变更发布（回滚与开新波次互斥，保证状态序列单调）。
	ErrRollbackInProgress = errors.New("release is rolling back")
	// ErrNoRollback：发布没有回滚计划，无从确认恢复。
	ErrNoRollback = errors.New("release has no rollback plan")

	// ErrQuarantineNotEnabled：发布未配置隔离闸门（MaxNodes=0），不允许隔离。
	ErrQuarantineNotEnabled = errors.New("release has no quarantine policy")
	// ErrNodeNotQuarantined：节点未被隔离，不能为其创建补跑计划。
	ErrNodeNotQuarantined = errors.New("node is not quarantined")
	// ErrQuarantineNoEvidence：隔离请求未携带任何健康样本依据。
	ErrQuarantineNoEvidence = errors.New("quarantine requires at least one health sample")
	// ErrQuarantineEvidenceMismatch：依据样本不属于该（发布, 节点, 摘要），
	// 或引用了尚不存在的事件号。
	ErrQuarantineEvidenceMismatch = errors.New("quarantine evidence does not belong to this node/release")

	// ErrCatchupNotFound：补跑计划不存在。
	ErrCatchupNotFound = errors.New("catchup plan not found")
	// ErrCatchupNotClaimable：计划当前不可领取（已成功/已作废，或已被他人
	// 持有且尚未失败）。
	ErrCatchupNotClaimable = errors.New("catchup plan is not claimable")
	// ErrCatchupFencing：上报携带的 fencing token 已过期（旧领取者的迟到
	// 上报），不能覆盖后来的状态。
	ErrCatchupFencing = errors.New("catchup claim token is stale")
	// ErrCatchupAborted：补跑计划已作废，不能再执行/上报。
	ErrCatchupAborted = errors.New("catchup plan has been aborted")
	// ErrCatchupExists：该隔离节点已有（未作废的）补跑计划。
	ErrCatchupExists = errors.New("catchup plan already exists for quarantined node")
)
