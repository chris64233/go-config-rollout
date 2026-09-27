package configrollout

import "time"

// ReleaseState 是发布的生命周期状态。
//
// 合法迁移：
//
//	Active --pause/失败阈值/超时--> Paused
//	Paused --resume--------------> Active
//	Active --健康失败阈值---------> RollingBack（自动生成一次回滚计划）
//	RollingBack --全部节点恢复---> RolledBack
//	Active | Paused | RollingBack --cancel--> Cancelled
//	Active ----------------------> Completed（最后一波通过观察窗口）
//
// Cancelled / Completed / RolledBack 为终态。RollingBack 是单向的：
// 进入后不可暂停、不可恢复、不会再开放新波次，只能走向终态，
// 因此自动回滚与人工取消/恢复竞争时状态序列保持单调。
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
	// EventWaveObserving 在当前波次成功门槛达成、进入健康观察窗口时生成。
	EventWaveObserving = "wave.observing"
	// EventRollbackStarted 在健康失败阈值触发、回滚计划生成时产生，每次发布至多一条。
	EventRollbackStarted = "release.rollback_started"
	// EventNodeRollback 是回滚通知：针对回滚计划中每个节点恰好一条，
	// 携带该节点应恢复到的发布前配置摘要。
	EventNodeRollback = "node.rollback"
	// EventNodeRolledBack 是节点确认已恢复的回执事件。
	EventNodeRolledBack = "node.rolled_back"
	// EventReleaseRolledBack 是发布回滚完成的终态通知，每次发布至多一条。
	EventReleaseRolledBack = "release.rolled_back"
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
	// Health 是健康观测策略，同样在创建时冻结；零值表示不启用健康观测
	// （波次达标后立即推进，与旧行为一致）。
	Health HealthPolicy `json:"health"`
}

// HealthPolicy 描述健康观测与自动回滚策略，在创建发布时冻结。
type HealthPolicy struct {
	// ObserveWindow 是波次成功门槛达成后必须完整通过的观察窗口；
	// 窗口未走完不得开放下一波。<= 0 表示不启用观测。
	ObserveWindow time.Duration `json:"observe_window"`
	// MinSamples 是做出失败判定所需的最少健康样本数；样本不足时不判定失败。
	MinSamples int `json:"min_samples"`
	// FailureThreshold 位于 (0,1]：观察窗口内不健康样本占比达到该值时
	// 触发自动回滚。
	FailureThreshold float64 `json:"failure_threshold"`
}

// HealthSample 是一条节点健康样本。EventID 全局唯一：相同事件重放幂等，
// 同号异内容返回冲突。样本按（发布, 配置摘要）归属，不能跨发布或跨配置使用；
// 允许乱序到达，评估时按采样时间筛选。
type HealthSample struct {
	EventID   string    `json:"event_id"`
	NodeID    string    `json:"node_id"`
	Digest    string    `json:"digest"`
	Healthy   bool      `json:"healthy"`
	SampledAt time.Time `json:"sampled_at"`
}

// RollbackItemState 是回滚计划中单节点的进度。
type RollbackItemState string

const (
	// RollbackNotified：回滚通知已生成（恰好一次），等待节点确认。
	RollbackNotified RollbackItemState = "notified"
	// RollbackDone：节点已确认恢复到发布前配置。
	RollbackDone RollbackItemState = "done"
)

// RollbackItem 是回滚计划中单节点的前后版本与进度。
type RollbackItem struct {
	NodeID string `json:"node_id"`
	Wave   int    `json:"wave"`
	// FromDigest 是本次发布应用的配置摘要。
	FromDigest string `json:"from_digest"`
	// ToDigest 是该节点发布前已应用的配置摘要（空串表示发布前无配置）。
	ToDigest string            `json:"to_digest"`
	State    RollbackItemState `json:"state"`
	DoneAt   time.Time         `json:"done_at,omitempty"`
}

// RollbackPlan 是健康失败阈值触发时一次性生成的回滚计划：
// 将已应用节点恢复到各自发布前的配置摘要。
type RollbackPlan struct {
	ReleaseID string          `json:"release_id"`
	StartedAt time.Time       `json:"started_at"`
	Items     []*RollbackItem `json:"items"`
}

// NodeLease 是节点在某次发布中的波次归属与结果。
type NodeLease struct {
	Wave   int        `json:"wave"`
	Result NodeResult `json:"result"`
	// PreDigest 是节点在本次发布成功应用之前已应用的配置摘要
	// （空串表示此前无配置），在节点首次成功时冻结，作为回滚目标。
	PreDigest string `json:"pre_digest,omitempty"`
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

	State        ReleaseState `json:"state"`
	PauseReason  PauseReason  `json:"pause_reason"`
	CurrentWave  int          `json:"current_wave"`
	WaveOpenedAt time.Time    `json:"wave_opened_at"`

	// ObservingSince 非零表示当前波次已达成成功门槛、正处于健康观察窗口内；
	// 窗口完整通过后才开放下一波。
	ObservingSince time.Time `json:"observing_since,omitempty"`
	// Samples 是按 EventID 去重的健康样本，只归属本发布与本配置摘要。
	Samples map[string]*HealthSample `json:"samples,omitempty"`
	// Rollback 是健康失败阈值触发时一次性生成的回滚计划。
	Rollback *RollbackPlan `json:"rollback,omitempty"`

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
		rc.Samples = make(map[string]*HealthSample, len(r.Samples))
		for id, sm := range r.Samples {
			smc := *sm
			rc.Samples[id] = &smc
		}
		if r.Rollback != nil {
			rb := *r.Rollback
			rb.Items = make([]*RollbackItem, len(r.Rollback.Items))
			for i, it := range r.Rollback.Items {
				itc := *it
				rb.Items[i] = &itc
			}
			rc.Rollback = &rb
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
