package configrollout

import "time"

// ReleaseState 是发布的生命周期状态。
//
// 合法迁移：
//
//	Active --pause/失败阈值/超时--> Paused
//	Paused --resume--------------> Active
//	Active | Paused --cancel----> Cancelled
//	Active ----------------------> Completed（最后一波达标）
//
// Cancelled / Completed 为终态。
type ReleaseState string

const (
	StateActive    ReleaseState = "active"
	StatePaused    ReleaseState = "paused"
	StateCancelled ReleaseState = "cancelled"
	StateCompleted ReleaseState = "completed"
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
