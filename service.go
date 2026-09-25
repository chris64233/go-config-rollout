package configrollout

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"math"
	"time"
)

// Service 是波次化配置发布服务，线程安全；所有操作通过 Store 串行持久化，
// 进程重启后可从持久化状态继续。
type Service struct {
	store Store
	clock Clock
}

// NewService 创建发布服务。store 通常为 NewFileStore（持久化）或
// NewMemoryStore（测试）。clock 传 nil 使用系统时钟。
func NewService(store Store, clock Clock) *Service {
	if clock == nil {
		clock = systemClock{}
	}
	return &Service{store: store, clock: clock}
}

// ---------- 配置登记 ----------

// RegisterConfig 登记一个配置包。digest 必须是 sha256(content) 的十六进制
// 摘要；相同摘要重复登记且内容一致时幂等成功，内容不一致返回 ErrConfigExists。
func (s *Service) RegisterConfig(digest string, content []byte) (*Config, error) {
	if digest != digestOf(content) {
		return nil, ErrInvalidDigest
	}
	now := s.clock.Now()
	err := s.store.mutate(func(st *State) error {
		if existing, ok := st.Configs[digest]; ok {
			if string(existing.Content) != string(content) {
				return ErrConfigExists
			}
			return nil
		}
		st.Configs[digest] = &Config{Digest: digest, Content: append([]byte(nil), content...), CreatedAt: now}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return &Config{Digest: digest, Content: append([]byte(nil), content...), CreatedAt: now}, nil
}

func digestOf(content []byte) string {
	sum := sha256.Sum256(content)
	return hex.EncodeToString(sum[:])
}

// ---------- 发布创建 ----------

// CreateReleaseInput 为创建发布的参数。
type CreateReleaseInput struct {
	// ID 由调用方指定，必须全局唯一；空串会返回错误。
	ID     string
	Digest string
	// Targets 在创建时被冻结并按顺序切波；同一节点出现多次返回 ErrDuplicateNode。
	Targets  []string
	WaveSize int
	Policy   WavePolicy
}

// CreateRelease 冻结目标集合、切成有序波次并原子开放第 0 波。
func (s *Service) CreateRelease(in CreateReleaseInput) (*Release, error) {
	if in.ID == "" {
		return nil, ErrInvalidReleaseID
	}
	if in.Digest == "" {
		return nil, ErrConfigNotFound
	}
	if in.WaveSize <= 0 {
		return nil, ErrInvalidWaveSize
	}
	if len(in.Targets) == 0 {
		return nil, ErrEmptyTargets
	}
	if in.Policy.MinSuccessRatio <= 0 || in.Policy.MinSuccessRatio > 1 {
		return nil, ErrInvalidPolicy
	}
	if in.Policy.MaxFailures < 0 {
		return nil, ErrInvalidPolicy
	}
	if in.Policy.WaveTimeout <= 0 {
		return nil, ErrInvalidPolicy
	}

	// 冻结：复制一份并校验全局唯一（保持调用方给定的顺序）。
	targets := append([]string(nil), in.Targets...)
	seen := make(map[string]struct{}, len(targets))
	for _, n := range targets {
		if _, dup := seen[n]; dup {
			return nil, ErrDuplicateNode
		}
		seen[n] = struct{}{}
	}
	policy := in.Policy

	var out *Release
	err := s.store.mutate(func(st *State) error {
		if _, ok := st.Configs[in.Digest]; !ok {
			return ErrConfigNotFound
		}
		if _, exists := st.Releases[in.ID]; exists {
			return ErrReleaseExists
		}

		now := s.clock.Now()
		st.NextReleaseSeq++
		r := &Release{
			ID:        in.ID,
			Seq:       st.NextReleaseSeq,
			Digest:    in.Digest,
			Policy:    policy,
			State:     StateActive,
			CreatedAt: now,
			Nodes:     make(map[string]*NodeLease, len(targets)),
		}

		// 有序切波；每个节点恰好落入一个波次。
		for i, n := range targets {
			w := i / in.WaveSize
			for len(r.Waves) <= w {
				r.Waves = append(r.Waves, nil)
			}
			r.Waves[w] = append(r.Waves[w], n)
			r.Nodes[n] = &NodeLease{Wave: w, Result: ResultPending}
		}
		r.RequiredSuccess = make([]int, len(r.Waves))
		for i, w := range r.Waves {
			r.RequiredSuccess[i] = requiredSuccess(policy.MinSuccessRatio, len(w))
		}

		// 原子开放第 0 波：节点进入 in_progress 并记录开波时间。
		r.CurrentWave = 0
		r.WaveOpenedAt = now
		for _, n := range r.Waves[0] {
			r.Nodes[n].Result = ResultInProgress
		}
		r.Version = 1
		st.Releases[r.ID] = r

		ev := st.emit(EventReleaseCreated, now)
		ev.ReleaseID, ev.Digest = r.ID, r.Digest
		open := st.emit(EventWaveOpened, now)
		open.ReleaseID, open.Wave = r.ID, 0

		out = cloneRelease(r)
		return nil
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

func requiredSuccess(ratio float64, size int) int {
	return int(math.Ceil(ratio * float64(size)))
}

// ---------- 回执 ----------

// Receipt 是节点上报的应用结果。
type Receipt struct {
	ReleaseID string
	NodeID    string
	// Digest 为节点实际应用的配置摘要；与发布摘要不一致时回执被拒绝。
	Digest  string
	Success bool
}

// ReceiptOutcome 描述回执处理结果。
type ReceiptOutcome struct {
	// Ignored 为 true 表示该节点已有终态结果，重复/冲突回执被幂等忽略。
	Ignored bool
	// State 为处理后发布状态。
	State ReleaseState
	// CurrentWave 为处理后的当前波次。
	CurrentWave int
	// WaveAdvanced 表示本次（未被忽略的）成功回执触发了波次推进/发布完成。
	WaveAdvanced bool
}

// AckReceipt 处理一条节点回执。
//
// 幂等键为（发布, 节点, 配置摘要）：节点首个终态结果生效，之后重复、乱序、
// 迟到的回执一律忽略，不重复计数。旧波次的迟到回执不能推进当前流程
// （ErrWaveNotOpen）；发布不处于 Active（暂停/取消/完成）时回执被拒绝。
func (s *Service) AckReceipt(rcpt Receipt) (*ReceiptOutcome, error) {
	var out ReceiptOutcome
	err := s.store.mutate(func(st *State) error {
		r, ok := st.Releases[rcpt.ReleaseID]
		if !ok {
			return ErrReleaseNotFound
		}
		lease, ok := r.Nodes[rcpt.NodeID]
		if !ok {
			return ErrNodeNotInRelease
		}
		if rcpt.Digest != r.Digest {
			return ErrDigestMismatch
		}

		// 暂停可能由超时触发：先做超时判定，使状态序列保持单调。
		s.evaluateTimeoutLocked(st, r)

		// 幂等：节点已有终态结果（包括旧波次节点的重复回执）直接忽略。
		if lease.Result == ResultSucceeded || lease.Result == ResultFailed {
			out = ReceiptOutcome{Ignored: true, State: r.State, CurrentWave: r.CurrentWave}
			return nil
		}
		// 旧发布的迟到回执：节点已经应用了更新发布的配置。
		// 该回执不计数、不推进任何流程，也不覆盖节点当前已应用版本。
		if ns := st.NodeStates[rcpt.NodeID]; ns != nil && ns.AppliedReleaseSeq > r.Seq {
			return ErrStaleReceipt
		}
		switch r.State {
		case StateCancelled, StateCompleted:
			return ErrTerminal
		case StatePaused:
			return ErrNotActive
		}
		// 只有当前开放波次中、尚未终态的节点可以确认结果。
		if lease.Wave != r.CurrentWave {
			return ErrWaveNotOpen
		}

		now := s.clock.Now()
		r.Version++
		if !rcpt.Success {
			lease.Result = ResultFailed
			ev := st.emit(EventNodeFailed, now)
			ev.ReleaseID, ev.Wave, ev.NodeID, ev.Digest = r.ID, lease.Wave, rcpt.NodeID, r.Digest
			out = ReceiptOutcome{State: r.State, CurrentWave: r.CurrentWave}

			// 失败数超过阈值立即暂停（暂停优先于其它迁移）。
			if waveCounts(r, r.CurrentWave).Failed > r.Policy.MaxFailures {
				pauseLocked(st, r, PauseFailureThreshold, now)
				out.State = r.State
			}
			return nil
		}

		lease.Result = ResultSucceeded
		ev := st.emit(EventNodeSucceeded, now)
		ev.ReleaseID, ev.Wave, ev.NodeID, ev.Digest = r.ID, lease.Wave, rcpt.NodeID, r.Digest

		// 推进节点“当前已应用版本”：只允许单调向前，旧发布的成功回执
		// 可以计入旧发布自身的审计，但不能覆盖节点较新的已应用版本。
		ns := st.NodeStates[rcpt.NodeID]
		if ns == nil {
			ns = &NodeState{NodeID: rcpt.NodeID}
			st.NodeStates[rcpt.NodeID] = ns
		}
		if r.Seq >= ns.AppliedReleaseSeq {
			st.AppliedSeq++
			ns.AppliedSeq = st.AppliedSeq
			ns.AppliedReleaseSeq = r.Seq
			ns.AppliedReleaseID = r.ID
			ns.AppliedDigest = r.Digest
			ns.AppliedAt = now
		}

		counts := waveCounts(r, r.CurrentWave)
		if counts.Succeeded >= r.RequiredSuccess[r.CurrentWave] {
			// 达标：原子开放下一波，或完成整个发布。
			if r.CurrentWave == len(r.Waves)-1 {
				r.State = StateCompleted
				done := st.emit(EventReleaseCompleted, now)
				done.ReleaseID = r.ID
			} else {
				r.CurrentWave++
				r.WaveOpenedAt = now
				for _, n := range r.Waves[r.CurrentWave] {
					r.Nodes[n].Result = ResultInProgress
				}
				open := st.emit(EventWaveOpened, now)
				open.ReleaseID, open.Wave = r.ID, r.CurrentWave
			}
			out.WaveAdvanced = true
		}
		out.State = r.State
		out.CurrentWave = r.CurrentWave
		return nil
	})
	if err != nil {
		return nil, err
	}
	return &out, nil
}

// ---------- 暂停 / 恢复 / 取消 ----------

// Pause 手动暂停一个进行中的发布。仅 Active -> Paused 合法。
func (s *Service) Pause(releaseID string) error {
	return s.store.mutate(func(st *State) error {
		r, ok := st.Releases[releaseID]
		if !ok {
			return ErrReleaseNotFound
		}
		switch r.State {
		case StateActive:
			pauseLocked(st, r, PauseManual, s.clock.Now())
			return nil
		case StatePaused:
			return ErrAlreadyPaused
		default:
			return ErrTerminal
		}
	})
}

// Resume 恢复被暂停的发布，给予当前波次全新的超时窗口。仅 Paused -> Active 合法。
func (s *Service) Resume(releaseID string) error {
	return s.store.mutate(func(st *State) error {
		r, ok := st.Releases[releaseID]
		if !ok {
			return ErrReleaseNotFound
		}
		if r.State != StatePaused {
			if r.State == StateActive {
				return ErrNotPaused
			}
			return ErrTerminal
		}
		now := s.clock.Now()
		r.State = StateActive
		r.PauseReason = ""
		r.WaveOpenedAt = now
		r.Version++
		ev := st.emit(EventReleaseResumed, now)
		ev.ReleaseID = r.ID
		return nil
	})
}

// Cancel 取消发布。Active/Paused -> Cancelled 是唯一会生成补偿通知的迁移：
// 对取消时刻已经成功的每个节点恰好生成一条 node.compensation 事件；
// 尚未开始的节点之后无法再取得配置。终态发布返回 ErrTerminal。
func (s *Service) Cancel(releaseID string) error {
	return s.store.mutate(func(st *State) error {
		r, ok := st.Releases[releaseID]
		if !ok {
			return ErrReleaseNotFound
		}
		switch r.State {
		case StateCancelled, StateCompleted:
			return ErrTerminal
		}
		now := s.clock.Now()
		r.State = StateCancelled
		r.PauseReason = ""
		r.Version++

		cancel := st.emit(EventReleaseCancelled, now)
		cancel.ReleaseID = r.ID

		// 只对当前已成功的节点补偿；节点在本次发布中只出现一次，
		// 且本迁移只发生一次，所以补偿通知每个成功节点恰好一条。
		for w, wave := range r.Waves {
			for _, n := range wave {
				if r.Nodes[n].Result == ResultSucceeded {
					ev := st.emit(EventNodeCompensation, now)
					ev.ReleaseID, ev.Wave, ev.NodeID, ev.Digest = r.ID, w, n, r.Digest
				}
			}
		}
		return nil
	})
}

// pauseLocked 执行 Active -> Paused 迁移并追加事件。
func pauseLocked(st *State, r *Release, reason PauseReason, at time.Time) {
	r.State = StatePaused
	r.PauseReason = reason
	r.Version++
	ev := st.emit(EventReleasePaused, at)
	ev.ReleaseID = r.ID
	ev.Reason = string(reason)
}

// evaluateTimeoutLocked 检查当前波次是否超时；超时则暂停。
// 在每个会观察/推进发布的事务开始时调用。
func (s *Service) evaluateTimeoutLocked(st *State, r *Release) {
	if r.State != StateActive {
		return
	}
	if s.clock.Now().Sub(r.WaveOpenedAt) <= r.Policy.WaveTimeout {
		return
	}
	counts := waveCounts(r, r.CurrentWave)
	if counts.Succeeded >= r.RequiredSuccess[r.CurrentWave] {
		return // 已达标，等待下一条成功回执推进即可，不算超时
	}
	pauseLocked(st, r, PauseTimeout, s.clock.Now())
}

// TickTimeout 主动驱动超时检查（后台定时器可周期性调用），返回是否发生了暂停。
func (s *Service) TickTimeout(releaseID string) (paused bool, err error) {
	err = s.store.mutate(func(st *State) error {
		r, ok := st.Releases[releaseID]
		if !ok {
			return ErrReleaseNotFound
		}
		before := r.State
		s.evaluateTimeoutLocked(st, r)
		paused = before == StateActive && r.State == StatePaused
		return nil
	})
	return paused, err
}

// ---------- 配置下发门禁 ----------

// GetConfigForNode 返回节点在某次发布中应当应用的配置内容。
// 只有发布处于 Active 且节点属于当前开放波次时才能取得配置；
// 取消后（以及其它非开放情形）未成功的节点不能再取得配置。
func (s *Service) GetConfigForNode(releaseID, nodeID string) (*Config, error) {
	var out *Config
	err := s.store.read(func(st *State) error {
		r, ok := st.Releases[releaseID]
		if !ok {
			return ErrReleaseNotFound
		}
		lease, ok := r.Nodes[nodeID]
		if !ok {
			return ErrNodeNotInRelease
		}
		if r.State != StateActive || lease.Wave != r.CurrentWave {
			// 取消/完成后的任何未成功节点，以及未来波次、旧波次节点。
			return ErrConfigNotAvailable
		}
		cfg := st.Configs[r.Digest]
		if cfg == nil {
			return ErrConfigNotFound
		}
		out = &Config{Digest: cfg.Digest, Content: append([]byte(nil), cfg.Content...), CreatedAt: cfg.CreatedAt}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

// ---------- 查询 ----------

// WaveStatus 是单个波次的进度。
type WaveStatus struct {
	Index      int  `json:"index"`
	Total      int  `json:"total"`
	Required   int  `json:"required"`
	Succeeded  int  `json:"succeeded"`
	Failed     int  `json:"failed"`
	InProgress int  `json:"in_progress"`
	Pending    int  `json:"pending"`
	Open       bool `json:"open"`
}

// Progress 是发布进度快照。
type Progress struct {
	ReleaseID      string       `json:"release_id"`
	Digest         string       `json:"digest"`
	State          ReleaseState `json:"state"`
	PauseReason    PauseReason  `json:"pause_reason,omitempty"`
	CurrentWave    int          `json:"current_wave"`
	Waves          []WaveStatus `json:"waves"`
	TotalNodes     int          `json:"total_nodes"`
	TotalSucceeded int          `json:"total_succeeded"`
	TotalFailed    int          `json:"total_failed"`
	WaveOpenedAt   time.Time    `json:"wave_opened_at"`
}

// GetProgress 返回发布进度；查询时会顺带完成超时暂停判定，使超时状态及时落盘。
func (s *Service) GetProgress(releaseID string) (*Progress, error) {
	var out *Progress
	err := s.store.mutate(func(st *State) error {
		r, ok := st.Releases[releaseID]
		if !ok {
			return ErrReleaseNotFound
		}
		s.evaluateTimeoutLocked(st, r)
		out = progressOf(r)
		return nil
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

func progressOf(r *Release) *Progress {
	p := &Progress{
		ReleaseID:    r.ID,
		Digest:       r.Digest,
		State:        r.State,
		PauseReason:  r.PauseReason,
		CurrentWave:  r.CurrentWave,
		WaveOpenedAt: r.WaveOpenedAt,
		Waves:        make([]WaveStatus, len(r.Waves)),
	}
	for i := range r.Waves {
		c := waveCounts(r, i)
		p.Waves[i] = WaveStatus{
			Index: i, Total: len(r.Waves[i]), Required: r.RequiredSuccess[i],
			Succeeded: c.Succeeded, Failed: c.Failed,
			InProgress: c.InProgress, Pending: c.Pending,
			Open: r.State == StateActive && i == r.CurrentWave,
		}
		p.TotalNodes += p.Waves[i].Total
		p.TotalSucceeded += c.Succeeded
		p.TotalFailed += c.Failed
	}
	return p
}

type counts struct{ Succeeded, Failed, InProgress, Pending int }

func waveCounts(r *Release, w int) counts {
	var c counts
	for _, n := range r.Waves[w] {
		switch r.Nodes[n].Result {
		case ResultSucceeded:
			c.Succeeded++
		case ResultFailed:
			c.Failed++
		case ResultInProgress:
			c.InProgress++
		case ResultPending:
			c.Pending++
		}
	}
	return c
}

// NodeAppliedVersion 返回节点当前已应用的版本（审计用）。
func (s *Service) NodeAppliedVersion(nodeID string) (*NodeState, error) {
	var out *NodeState
	err := s.store.read(func(st *State) error {
		ns, ok := st.NodeStates[nodeID]
		if !ok {
			return ErrNodeNeverApplied
		}
		cp := *ns
		out = &cp
		return nil
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

// ---------- outbox ----------

// PendingEvents 返回所有尚未标记投递的 outbox 事件，按全局序号升序。
func (s *Service) PendingEvents() ([]Event, error) {
	var out []Event
	err := s.store.read(func(st *State) error {
		for _, ev := range st.Events {
			if !ev.Delivered {
				out = append(out, *ev)
			}
		}
		return nil
	})
	return out, err
}

// MarkDelivered 将一条 outbox 事件标记为已投递（仅前进，不删除事件，审计保留）。
func (s *Service) MarkDelivered(seq int64) error {
	return s.store.mutate(func(st *State) error {
		for _, ev := range st.Events {
			if ev.Seq == seq {
				ev.Delivered = true
				return nil
			}
			if ev.Seq > seq {
				break
			}
		}
		return ErrEventNotFound
	})
}

// EventSink 投递一条 outbox 事件；返回 nil 表示投递成功。
// 要求 sink 自身按事件类型/键幂等，以在“投递成功、标记前崩溃”时支持安全重试。
type EventSink func(Event) error

// DispatchOutbox 按序号投递所有未投递事件，每条成功后立即标记；
// sink 返回错误时停止并返回该错误，下次调用从断点继续。
func (s *Service) DispatchOutbox(ctx context.Context, sink EventSink) (int, error) {
	sent := 0
	for {
		if err := ctx.Err(); err != nil {
			return sent, err
		}
		var ev *Event
		if err := s.store.read(func(st *State) error {
			for _, e := range st.Events {
				if !e.Delivered {
					cp := *e
					ev = &cp
					return nil
				}
			}
			return nil
		}); err != nil {
			return sent, err
		}
		if ev == nil {
			return sent, nil
		}
		if err := sink(*ev); err != nil {
			return sent, err
		}
		if err := s.MarkDelivered(ev.Seq); err != nil {
			return sent, err
		}
		sent++
	}
}

// ---------- 其它 ----------

// cloneRelease 返回对外暴露的发布深拷贝，避免调用方修改内部状态。
func cloneRelease(r *Release) *Release {
	cp := *r
	cp.Waves = make([][]string, len(r.Waves))
	for i, w := range r.Waves {
		cp.Waves[i] = append([]string(nil), w...)
	}
	cp.Nodes = make(map[string]*NodeLease, len(r.Nodes))
	for n, l := range r.Nodes {
		lc := *l
		cp.Nodes[n] = &lc
	}
	cp.RequiredSuccess = append([]int(nil), r.RequiredSuccess...)
	return &cp
}
