package configrollout

import "time"

// ReleaseState 是发布的生命周期状态。
//
// 合法迁移：
//
//	Active --pause/失败阈值/超时--> Paused
//	Paused --resume--------------> Active
//	Active | Paused --cancel----> Cancelled
//	Active --健康失败阈值--------> RollingBack（生成一次性回滚计划）
//	RollingBack --全部节点恢复---> RolledBack
//	Active ----------------------> Completed（最后一波通过观察窗口）
//
// Cancelled / Completed / RolledBack 为终态。RollingBack 不接受暂停/恢复/
// 取消/回执：回滚与开新波次互斥，状态序列保持单调。
type ReleaseState string

const (
	StateActive      ReleaseState = "active"
	StatePaused      ReleaseState = "paused"
	StateRollingBack ReleaseState = "rolling_back"
	StateCancelled   ReleaseState = "cancelled"
	StateCompleted   ReleaseState = "completed"
	StateRolledBack  ReleaseState = "rolled_back"
)

// NodeResult 是节点在某次发布中的结果。
type NodeResult string

const (
	// ResultPending：节点所在波次尚未开放。
	ResultPending NodeResult = "pending"
	// ResultInProgress：波次已开放，等待节点回执。
	ResultInProgress NodeResult = "in_progress"
	ResultSucceeded  NodeResult = "succeeded"
	ResultFailed     NodeResult = "failed"
)

// PauseReason 记录发布被暂停的原因。
type PauseReason string

const (
	PauseManual           PauseReason = "manual"
	PauseFailureThreshold PauseReason = "failure_threshold"
	PauseTimeout          PauseReason = "timeout"
)

// Outbox 事件类型。outbox 同时充当审计事件流，事件只追加、不修改。
const (
	EventReleaseCreated   = "release.created"
	EventWaveOpened       = "wave.opened"
	EventReleasePaused    = "release.paused"
	EventReleaseResumed   = "release.resumed"
	EventReleaseCancelled = "release.cancelled"
	EventReleaseCompleted = "release.completed"
	EventNodeSucceeded    = "node.succeeded"
	EventNodeFailed       = "node.failed"
	// EventNodeCompensation 是取消发布时，针对已经成功应用配置的节点
	// 生成的补偿通知（例如通知回滚）。它只在 Active/Paused -> Cancelled
	// 这唯一一次状态迁移中生成，因此每个成功节点恰好一条。
	EventNodeCompensation = "node.compensation"
	// EventRollbackStarted 是健康失败阈值触发、回滚计划生成的事件，
	// 每次发布至多一条（Active -> RollingBack 只发生一次）。
	EventRollbackStarted = "rollback.started"
	// EventNodeRollback 是回滚通知：针对每个已应用节点恰好写出一次，
	// Digest 字段为该节点应恢复到的发布前配置摘要（可能为空串）。
	EventNodeRollback = "node.rollback"
	// EventNodeRestored 是节点确认已恢复到目标摘要的事件。
	EventNodeRestored = "node.restored"
	// EventReleaseRolledBack 是发布回滚完成的终态通知，恰好写出一次。
	EventReleaseRolledBack = "release.rolledback"

	// EventNodeQuarantined 是节点被隔离的事件：隔离决定绑定发布、节点、
	// 配置摘要与当前波次，Basis（样本事件号）作为证据一并记录。
	// 隔离决定一旦写出即不可变，迟到/重复样本不能改写它。
	EventNodeQuarantined = "node.quarantined"
	// EventCatchupCreated 是为隔离节点创建补跑计划的事件，每次发布至多
	// 存在一个活跃计划；重建（旧计划作废后）会写出新事件并带新的 attempt。
	EventCatchupCreated = "catchup.created"
	// EventNodeCatchup 是补跑领取通知：把某节点当前配置 -> 目标摘要的
	// 补跑项分发给领取者；租约过期被重新领取时会以新的 epoch 再写一条，
	// sink 需按（发布, 节点, epoch）幂等。
	EventNodeCatchup = "node.catchup"
	// EventNodeCatchupSucceeded 是节点补跑成功确认事件。
	EventNodeCatchupSucceeded = "node.catchup_succeeded"
	// EventNodeCatchupFailed 是节点补跑失败事件；失败项形成波次屏障，
	// 后续波次在计划重建前不可领取。
	EventNodeCatchupFailed = "node.catchup_failed"
	// EventCatchupCompleted 是补跑计划全部完成的通知，每个计划恰好一条；
	// 它不是发布终态通知（release.completed 仍只由主流程写出一次）。
	EventCatchupCompleted = "catchup.completed"
	// EventCatchupAborted 是补跑计划无法继续的通知（发布已回滚/被新发布
	// 取代/被更新的计划取代），每个计划恰好一条，Reason 给出原因。
	EventCatchupAborted = "catchup.aborted"
)

// Config 是由不可变摘要标识的配置包。
type Config struct {
	// Digest 是 Content 的 SHA-256 十六进制摘要，全局唯一、不可变。
	Digest    string    `json:"digest"`
	Content   []byte    `json:"content"`
	CreatedAt time.Time `json:"created_at"`
}

// WavePolicy 描述波次推进策略，在创建发布时冻结。
type WavePolicy struct {
	// MinSuccessRatio 位于 (0,1]，每波需要的成功数为 ceil(ratio*波次大小)。
	MinSuccessRatio float64 `json:"min_success_ratio"`
	// MaxFailures 是波次内允许的失败数；失败数超过它立即暂停。
	MaxFailures int `json:"max_failures"`
	// WaveTimeout 是单波等待成功门槛的最长时间，超时暂停。
	WaveTimeout time.Duration `json:"wave_timeout"`
}

// NodeLease 是节点在某次发布中的波次归属与结果。
type NodeLease struct {
	Wave   int        `json:"wave"`
	Result NodeResult `json:"result"`

	// PrevDigest / PrevReleaseID 是节点本次成功应用之前的已应用版本，
	// 在成功回执落定的那一刻捕获并冻结，供自动回滚恢复使用。
	PrevDigest    string `json:"prev_digest,omitempty"`
	PrevReleaseID string `json:"prev_release_id,omitempty"`
}

// HealthPolicy 是健康观察策略，在创建发布时与 WavePolicy 一并冻结。
// 零值表示不启用健康观察（波次达标后立即推进，保持旧行为）。
type HealthPolicy struct {
	// ObserveWindow 是波次达标后的观察窗口：窗口未届满不得开启下一波。
	ObserveWindow time.Duration `json:"observe_window"`
	// MinSamples 是每个已成功节点在窗口内必须上报的最少健康样本数。
	MinSamples int `json:"min_samples"`
	// MaxUnhealthy 是当前波内允许的不健康节点数；不健康节点数超过它
	// 立即生成回滚计划并进入 RollingBack。
	MaxUnhealthy int `json:"max_unhealthy"`
}

// QuarantinePolicy 是节点隔离策略，在创建发布时冻结（与 HealthPolicy 同生
// 效：健康观察启用时才允许隔离）。隔离不能掩盖大面积故障，因此同时冻结
// 两条硬限：超过任一限制，仍触发现有的整体回滚。
type QuarantinePolicy struct {
	// MaxQuarantined 是整个发布允许隔离的最大节点数（累计、跨波次），
	// 隔离后节点总数超过它立即整体回滚。
	MaxQuarantined int `json:"max_quarantined"`
	// MinHealthyCoverage 是健康覆盖率下限，取值 (0,1]。覆盖率定义为
	// 当前观察波中「未隔离的已成功节点」占「波内全部已成功节点」的比例；
	// 隔离导致覆盖率低于该值时立即整体回滚。
	MinHealthyCoverage float64 `json:"min_healthy_coverage"`
}

// NodeHealth 是节点在观察期的健康判定。
type NodeHealth string

const (
	// HealthUnknown：节点尚未成功应用，不参与健康判定。
	HealthUnknown NodeHealth = "unknown"
	// HealthObserving：已成功但样本数不足 MinSamples。
	HealthObserving NodeHealth = "observing"
	// HealthHealthy：样本数达标且无失败样本。
	HealthHealthy NodeHealth = "healthy"
	// HealthUnhealthy：存在至少一条失败样本。
	HealthUnhealthy NodeHealth = "unhealthy"
	// HealthQuarantined：节点已被操作员隔离，不再参与健康门槛。
	HealthQuarantined NodeHealth = "quarantined"
)

// HealthSampleRecord 是一条已落库的健康样本。样本以 EventID 为幂等键：
// 相同事件号相同内容的重放幂等忽略；相同事件号不同内容属于冲突。
// 样本只允许属于其声明的发布与配置摘要，不能跨发布/跨配置使用。
type HealthSampleRecord struct {
	EventID    string    `json:"event_id"`
	NodeID     string    `json:"node_id"`
	Digest     string    `json:"digest"`
	Healthy    bool      `json:"healthy"`
	SampledAt  time.Time `json:"sampled_at"`
	ReceivedAt time.Time `json:"received_at"`
}

// RollbackReason 标识回滚计划的触发原因。
const (
	RollbackReasonHealthThreshold = "health_threshold"
	// RollbackReasonQuarantineLimit：隔离数量/健康覆盖率超过创建时冻结
	// 的限制 —— 隔离不能掩盖大面积故障，仍走整体回滚。
	RollbackReasonQuarantineLimit = "quarantine_limit"
)

// QuarantineRecord 是一条不可变的隔离决定，绑定发布、节点、配置摘要与
// 隔离时刻所在波次；Basis 为操作员引用的健康样本事件号（证据，只增不改）。
type QuarantineRecord struct {
	NodeID        string    `json:"node_id"`
	Wave          int       `json:"wave"`
	Digest        string    `json:"digest"`
	Basis         []string  `json:"basis"`
	Reason        string    `json:"reason,omitempty"`
	QuarantinedAt time.Time `json:"quarantined_at"`
	// Remediated 标记该隔离节点已通过某轮补跑成功恢复到目标摘要；
	// 重建补跑计划时不再纳入该节点。
	Remediated bool `json:"remediated,omitempty"`
}

// RollbackEntry 是回滚计划中单个节点的恢复项。
type RollbackEntry struct {
	NodeID string `json:"node_id"`
	Wave   int    `json:"wave"`
	// FromDigest 是发布应用的配置摘要；ToDigest 是发布前版本（空串表示
	// 节点此前从未应用过任何配置，恢复即回到“无版本”）。
	FromDigest string `json:"from_digest"`
	ToDigest   string `json:"to_digest"`
	// Notified 标记回滚通知事件已写出（每个节点恰好一次）。
	Notified   bool      `json:"notified"`
	Restored   bool      `json:"restored"`
	RestoredAt time.Time `json:"restored_at,omitempty"`
}

// RollbackPlan 是健康阈值触发时一次性生成的回滚计划。
type RollbackPlan struct {
	Reason    string                    `json:"reason"`
	CreatedAt time.Time                 `json:"created_at"`
	Entries   map[string]*RollbackEntry `json:"entries"`
}

// CatchupState 是补跑项/补跑计划的状态，只能向前推进。
type CatchupState string

const (
	// CatchupPending：项尚未被领取（或租约已过期等待重新领取）。
	CatchupPending CatchupState = "pending"
	// CatchupClaimed：项已被某个领取者持有效租约认领。
	CatchupClaimed CatchupState = "claimed"
	// CatchupSucceeded：节点已确认补跑成功（终态）。
	CatchupSucceeded CatchupState = "succeeded"
	// CatchupFailed：节点补跑失败（终态；形成波次屏障，计划需重建才能继续）。
	CatchupFailed CatchupState = "failed"
	// CatchupAborted：计划整体作废（发布已回滚/被新发布取代/被新计划取代）。
	CatchupAborted CatchupState = "aborted"
	// CatchupDone：计划全部项成功（终态）。
	CatchupDone CatchupState = "done"
)

// CatchupEntry 是补跑计划中单个节点的恢复项。补跑必须从该节点创建计划
// 时的当前配置开始，因此 StartDigest 在创建计划时捕获冻结；目标固定为
// 原发布摘要 TargetDigest。
type CatchupEntry struct {
	NodeID string `json:"node_id"`
	// Wave 是该节点在原发布中的波次；补跑严格按波次顺序领取。
	Wave int `json:"wave"`
	// StartDigest 是创建补跑计划时节点的实际配置摘要（可能不是发布
	// 摘要，例如节点已被隔离、问题修复后处于修复版本）。
	StartDigest string `json:"start_digest"`
	// StartReleaseID 是创建计划时节点当前配置来自哪个发布（审计用）。
	StartReleaseID string `json:"start_release_id,omitempty"`
	// TargetDigest 固定为原发布目标摘要。
	TargetDigest string `json:"target_digest"`

	State CatchupState `json:"state"`

	// ClaimEpoch 在每次（重新）领取时加一；领取通知携带 epoch，
	// 完成确认必须匹配当前 epoch，旧领取者的迟到成功不能覆盖新状态。
	ClaimEpoch int64     `json:"claim_epoch"`
	ClaimedBy  string    `json:"claimed_by,omitempty"`
	LeaseUntil time.Time `json:"lease_until,omitempty"`
	ClaimedAt  time.Time `json:"claimed_at,omitempty"`

	// BaselineAt 创建计划时节点当前应用版本的 AppliedSeq，作为后续
	// “节点基线是否变化”的比对基准。
	BaselineSeq int64 `json:"baseline_seq"`

	FinishedAt time.Time `json:"finished_at,omitempty"`
}

// CatchupAbortReason 标识补跑计划无法继续的原因。
const (
	CatchupAbortRolledBack = "release_rolled_back"
	CatchupAbortSuperseded = "release_superseded"
	CatchupAbortReplaced   = "plan_replaced"
)

// CatchupPlan 是为隔离节点创建的有序补跑计划。
type CatchupPlan struct {
	Attempt   int64                    `json:"attempt"`
	CreatedAt time.Time                `json:"created_at"`
	Entries   map[string]*CatchupEntry `json:"entries"`
	// Waves 为按升序去重的波次列表，供按波次顺序领取/查询。
	Waves []int `json:"waves"`

	State       CatchupState `json:"state"`
	AbortReason string       `json:"abort_reason,omitempty"`
	AbortedAt   time.Time    `json:"aborted_at,omitempty"`
	CompletedAt time.Time    `json:"completed_at,omitempty"`
}

// Release 是一次配置发布的完整持久化状态。
type Release struct {
	ID     string `json:"id"`
	Seq    int64  `json:"seq"` // 单调递增的创建序号，用于判定发布新旧
	Digest string `json:"digest"`

	// Waves 是被冻结目标集合的有序切分；一个节点在本次发布中只出现一次。
	Waves [][]string `json:"waves"`
	// Nodes 是节点 -> 波次归属/结果，冻结后不再变化。
	Nodes map[string]*NodeLease `json:"nodes"`

	Policy          WavePolicy `json:"policy"`
	RequiredSuccess []int      `json:"required_success"` // 每波所需成功数（冻结）

	// Health 是创建时冻结的健康观察策略；HealthEnabled 标记是否启用。
	Health        HealthPolicy `json:"health"`
	HealthEnabled bool         `json:"health_enabled"`
	// Quarantine 是创建时冻结的隔离策略；QuarantineEnabled 标记是否启用
	// （随健康观察一并启用）。
	Quarantine        QuarantinePolicy `json:"quarantine"`
	QuarantineEnabled bool             `json:"quarantine_enabled"`
	// ThresholdMetAt 是当前波成功门槛首次达标的时间（观察窗口起点）；
	// 零值表示当前波尚未达标。
	ThresholdMetAt time.Time `json:"threshold_met_at,omitempty"`
	// Samples 以事件号为键存放已接受的健康样本（只增不改）。
	Samples map[string]*HealthSampleRecord `json:"samples,omitempty"`
	// Quarantined 以节点 ID 为键存放不可变隔离决定（只增不改；一个节点
	// 在一次发布中至多被隔离一次）。
	Quarantined map[string]*QuarantineRecord `json:"quarantined,omitempty"`
	// Rollback 是健康阈值/隔离限制触发时一次性生成的回滚计划，未触发为 nil。
	Rollback *RollbackPlan `json:"rollback,omitempty"`
	// Catchup 是隔离节点的补跑计划；旧计划作废（aborted/completed）后
	// 可重建，因此单独用 CatchupAttempt 标识当前计划的版本。
	Catchup        *CatchupPlan `json:"catchup,omitempty"`
	CatchupAttempt int64        `json:"catchup_attempt,omitempty"`

	State        ReleaseState `json:"state"`
	PauseReason  PauseReason  `json:"pause_reason"`
	CurrentWave  int          `json:"current_wave"`
	WaveOpenedAt time.Time    `json:"wave_opened_at"`

	CreatedAt time.Time `json:"created_at"`
	// Version 在每次状态变更时加一，配合只追加的 outbox 保证状态序列单调。
	Version int64 `json:"version"`
}

// NodeState 是节点跨发布的“当前已应用版本”。
type NodeState struct {
	NodeID string `json:"node_id"`

	// AppliedSeq 是全局单调的应用序号；AppliedReleaseSeq 标识该应用来自哪个发布。
	AppliedSeq        int64     `json:"applied_seq"`
	AppliedReleaseSeq int64     `json:"applied_release_seq"`
	AppliedReleaseID  string    `json:"applied_release_id"`
	AppliedDigest     string    `json:"applied_digest"`
	AppliedAt         time.Time `json:"applied_at"`
}

// Event 是一条 outbox / 审计事件，只追加。
type Event struct {
	Seq       int64     `json:"seq"`
	Type      string    `json:"type"`
	At        time.Time `json:"at"`
	ReleaseID string    `json:"release_id,omitempty"`
	Wave      int       `json:"wave,omitempty"`
	NodeID    string    `json:"node_id,omitempty"`
	Digest    string    `json:"digest,omitempty"`
	Reason    string    `json:"reason,omitempty"`

	// Delivered 由 outbox 派发器在成功投递后置位；重启后仍未投递的事件可继续投递。
	Delivered bool `json:"delivered"`

	// Epoch 用于补跑领取通知（node.catchup）：租约过期重新领取会写出
	// 带新 epoch 的新通知，sink 按（发布, 节点, epoch）幂等。
	Epoch int64 `json:"epoch,omitempty"`
	// Attempt 标识补跑计划的版本（catchup.* 事件）。
	Attempt int64 `json:"attempt,omitempty"`
	// ToDigest 在补跑/回滚通知中携带目标摘要（Digest 字段为起始摘要）。
	ToDigest string `json:"to_digest,omitempty"`
}

// State 是持久化的完整快照（状态 + outbox）。
type State struct {
	NextReleaseSeq int64 `json:"next_release_seq"`
	EventSeq       int64 `json:"event_seq"`
	AppliedSeq     int64 `json:"applied_seq"`

	Configs    map[string]*Config    `json:"configs"`
	Releases   map[string]*Release   `json:"releases"`
	NodeStates map[string]*NodeState `json:"node_states"`
	Events     []*Event              `json:"events"`
}

func newState() *State {
	return &State{
		Configs:    map[string]*Config{},
		Releases:   map[string]*Release{},
		NodeStates: map[string]*NodeState{},
	}
}

// clone 返回深拷贝，事务在拷贝上修改，提交失败不会污染已提交状态。
func (s *State) clone() *State {
	c := &State{
		NextReleaseSeq: s.NextReleaseSeq,
		EventSeq:       s.EventSeq,
		AppliedSeq:     s.AppliedSeq,
		Configs:        make(map[string]*Config, len(s.Configs)),
		Releases:       make(map[string]*Release, len(s.Releases)),
		NodeStates:     make(map[string]*NodeState, len(s.NodeStates)),
		Events:         make([]*Event, 0, len(s.Events)),
	}
	for d, cfg := range s.Configs {
		cc := *cfg
		cc.Content = append([]byte(nil), cfg.Content...)
		c.Configs[d] = &cc
	}
	for id, r := range s.Releases {
		rc := *r
		rc.Waves = make([][]string, len(r.Waves))
		for i, w := range r.Waves {
			rc.Waves[i] = append([]string(nil), w...)
		}
		rc.Nodes = make(map[string]*NodeLease, len(r.Nodes))
		for n, l := range r.Nodes {
			lc := *l
			rc.Nodes[n] = &lc
		}
		rc.RequiredSuccess = append([]int(nil), r.RequiredSuccess...)
		if r.Samples != nil {
			rc.Samples = make(map[string]*HealthSampleRecord, len(r.Samples))
			for k, v := range r.Samples {
				vc := *v
				rc.Samples[k] = &vc
			}
		}
		if r.Quarantined != nil {
			rc.Quarantined = make(map[string]*QuarantineRecord, len(r.Quarantined))
			for k, q := range r.Quarantined {
				qc := *q
				qc.Basis = append([]string(nil), q.Basis...)
				rc.Quarantined[k] = &qc
			}
		}
		rc.CatchupAttempt = r.CatchupAttempt
		if r.Catchup != nil {
			rc.Catchup = cloneCatchup(r.Catchup)
		}
		if r.Rollback != nil {
			rb := &RollbackPlan{
				Reason:    r.Rollback.Reason,
				CreatedAt: r.Rollback.CreatedAt,
				Entries:   make(map[string]*RollbackEntry, len(r.Rollback.Entries)),
			}
			for k, e := range r.Rollback.Entries {
				ec := *e
				rb.Entries[k] = &ec
			}
			rc.Rollback = rb
		}
		c.Releases[id] = &rc
	}
	for n, ns := range s.NodeStates {
		nsc := *ns
		c.NodeStates[n] = &nsc
	}
	for _, ev := range s.Events {
		evc := *ev
		c.Events = append(c.Events, &evc)
	}
	return c
}

// emit 在状态上追加一条事件并返回它，事件序号全局单调。
func (s *State) emit(typ string, at time.Time) *Event {
	s.EventSeq++
	ev := &Event{Seq: s.EventSeq, Type: typ, At: at}
	s.Events = append(s.Events, ev)
	return ev
}

// cloneCatchup 返回补跑计划的深拷贝。
func cloneCatchup(p *CatchupPlan) *CatchupPlan {
	cp := &CatchupPlan{
		Attempt:     p.Attempt,
		CreatedAt:   p.CreatedAt,
		State:       p.State,
		AbortReason: p.AbortReason,
		AbortedAt:   p.AbortedAt,
		CompletedAt: p.CompletedAt,
		Waves:       append([]int(nil), p.Waves...),
		Entries:     make(map[string]*CatchupEntry, len(p.Entries)),
	}
	for k, e := range p.Entries {
		ec := *e
		cp.Entries[k] = &ec
	}
	return cp
}
