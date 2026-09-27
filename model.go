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
const RollbackReasonHealthThreshold = "health_threshold"

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
	// ThresholdMetAt 是当前波成功门槛首次达标的时间（观察窗口起点）；
	// 零值表示当前波尚未达标。
	ThresholdMetAt time.Time `json:"threshold_met_at,omitempty"`
	// Samples 以事件号为键存放已接受的健康样本（只增不改）。
	Samples map[string]*HealthSampleRecord `json:"samples,omitempty"`
	// Rollback 是健康阈值触发时一次性生成的回滚计划，未触发为 nil。
	Rollback *RollbackPlan `json:"rollback,omitempty"`

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
