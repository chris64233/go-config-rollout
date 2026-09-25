package configrollout

import "errors"

// 所有业务错误均为哨兵错误，可用 errors.Is 判定。
var (
	// ErrInvalidDigest 配置摘要非法（空串等）。
	ErrInvalidDigest = errors.New("config digest is invalid")
	// ErrConfigNotFound 配置未登记。
	ErrConfigNotFound = errors.New("config not found")
	// ErrConfigConflict 同一摘要已登记但内容不同；配置不可变。
	ErrConfigConflict = errors.New("config already registered with different content")

	// ErrRolloutNotFound 发布不存在。
	ErrRolloutNotFound = errors.New("rollout not found")
	// ErrEmptyWaves 波次为空。
	ErrEmptyWaves = errors.New("waves must not be empty")
	// ErrEmptyNode 节点 ID 为空。
	ErrEmptyNode = errors.New("node id must not be empty")
	// ErrDuplicateNode 同一节点在一次发布中出现了不止一次。
	ErrDuplicateNode = errors.New("node appears more than once in rollout")
	// ErrInvalidThreshold 成功门槛非法（小于 0 或大于波次节点数）。
	ErrInvalidThreshold = errors.New("success threshold is invalid")
	// ErrInvalidResult 回执结果非法。
	ErrInvalidResult = errors.New("receipt result must be succeeded or failed")
	// ErrDigestMismatch 回执摘要与发布摘要不一致，属于旧发布/旧配置的迟到回执。
	ErrDigestMismatch = errors.New("receipt digest does not match rollout digest")
	// ErrNodeNotInRollout 节点不属于本次发布。
	ErrNodeNotInRollout = errors.New("node is not part of the rollout")
	// ErrWaveNotOpen 节点所属波次不是当前开放波次（旧波次已关闭或新波次未开放）。
	ErrWaveNotOpen = errors.New("node's wave is not currently open")
	// ErrStaleReceipt 节点已在更新的发布中应用了更新版本，旧发布回执不能覆盖。
	ErrStaleReceipt = errors.New("stale receipt: node has already applied a newer version")

	// ErrRolloutPaused 发布已暂停，暂停期间不接受新回执、不发放配置。
	ErrRolloutPaused = errors.New("rollout is paused")
	// ErrRolloutCancelled 发布已取消；尚未开始的节点不能再取得配置。
	ErrRolloutCancelled = errors.New("rollout is cancelled")
	// ErrAlreadyFinished 发布已处于成功终态。
	ErrAlreadyFinished = errors.New("rollout already finished")
)
