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
	// 健康策略随发布一并冻结：要么完整关闭（全零值），要么完整合法。
	hp := in.Policy.Health
	if hp.ObserveWindow < 0 {
		return nil, ErrInvalidPolicy
	}
	if hp.ObserveWindow == 0 {
		if hp.MinSamples != 0 || hp.FailureThreshold != 0 {
			return nil, ErrInvalidPolicy
		}
	} else if hp.MinSamples < 1 || hp.FailureThreshold <= 0 || hp.FailureThreshold > 1 {
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
			Samples:   make(map[string]*HealthSample),
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

		// 暂停可能由超时触发、回滚可能由健康阈值触发：先做惰性评估，
		// 使状态序列保持单调。
		s.evaluateLocked(st, r)

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
		case StateCancelled, StateCompleted, StateRolledBack:
			return ErrTerminal
		case StatePaused:
			return ErrNotActive
		case StateRollingBack:
			// 回滚期间不再接受应用回执：迟到的成功回执不得覆盖回滚结果。
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
			// 冻结回滚目标：节点在本次发布成功前的已应用摘要。
			lease.PreDigest = ns.AppliedDigest
			st.AppliedSeq++
			ns.AppliedSeq = st.AppliedSeq
			ns.AppliedReleaseSeq = r.Seq
			ns.AppliedReleaseID = r.ID
			ns.AppliedDigest = r.Digest
			ns.AppliedAt = now
		}

		counts := waveCounts(r, r.CurrentWave)
		if counts.Succeeded >= r.RequiredSuccess[r.CurrentWave] {
			switch {
			case r.Policy.Health.ObserveWindow > 0:
				// 启用健康观测：波次进入观察状态，只有完整通过观察窗口
				// 才允许开放下一波（由 evaluateHealthLocked 推进）。
				// 已在观察中的波次不重置窗口、不重复发事件。
				if r.ObservingSince.IsZero() {
					r.ObservingSince = now
					obs := st.emit(EventWaveObserving, now)
					obs.ReleaseID, obs.Wave = r.ID, r.CurrentWave
				}
			default:
				// 未启用观测：达标即原子推进/完成。
				advanceWaveLocked(st, r, now)
				out.WaveAdvanced = true
			}
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
		case StateRollingBack:
			return ErrRollbackInProgress
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
			switch r.State {
			case StateActive:
				return ErrNotPaused
			case StateRollingBack:
				return ErrRollbackInProgress
			default:
				return ErrTerminal
			}
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
// RollingBack -> Cancelled 也是合法迁移，但不再生成补偿通知：
// 回滚通知（node.rollback）已覆盖全部成功节点，且每类通知只写出一次。
func (s *Service) Cancel(releaseID string) error {
	return s.store.mutate(func(st *State) error {
		r, ok := st.Releases[releaseID]
		if !ok {
			return ErrReleaseNotFound
		}
		switch r.State {
		case StateCancelled, StateCompleted, StateRolledBack:
			return ErrTerminal
		}
		now := s.clock.Now()
		rollingBack := r.State == StateRollingBack
		r.State = StateCancelled
		r.PauseReason = ""
		r.ObservingSince = time.Time{}
		r.Version++

		cancel := st.emit(EventReleaseCancelled, now)
		cancel.ReleaseID = r.ID

		if rollingBack {
			// 回滚计划已覆盖所有成功节点，补偿通知不再重复生成。
			return nil
		}
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

// evaluateLocked 在每个会观察/推进发布的事务开始时调用：先做超时判定，
// 再做健康观测评估（可能推进波次或触发自动回滚），保证状态序列单调。
func (s *Service) evaluateLocked(st *State, r *Release) {
	s.evaluateTimeoutLocked(st, r)
	s.evaluateHealthLocked(st, r)
}

// evaluateTimeoutLocked 检查当前波次是否超时；超时则暂停。
func (s *Service) evaluateTimeoutLocked(st *State, r *Release) {
	if r.State != StateActive {
		return
	}
	if s.clock.Now().Sub(r.WaveOpenedAt) <= r.Policy.WaveTimeout {
		return
	}
	counts := waveCounts(r, r.CurrentWave)
	if counts.Succeeded >= r.RequiredSuccess[r.CurrentWave] {
		return // 已达标（含观察窗口内），等待推进即可，不算超时
	}
	pauseLocked(st, r, PauseTimeout, s.clock.Now())
}

// evaluateHealthLocked 评估当前波次的健康观察窗口：
//   - 样本数达到 MinSamples 且不健康占比达到 FailureThreshold：触发一次自动回滚；
//   - 观察窗口完整通过且未触发失败：原子开放下一波，或完成整个发布。
//
// 样本不足时不做失败判定（窗口走完即放行）。只在 Active 且处于观察状态时执行。
func (s *Service) evaluateHealthLocked(st *State, r *Release) {
	if r.State != StateActive || r.ObservingSince.IsZero() {
		return
	}
	now := s.clock.Now()
	h := healthEval(r, r.CurrentWave)
	if h.Samples >= r.Policy.Health.MinSamples && h.UnhealthyRatio >= r.Policy.Health.FailureThreshold {
		startRollbackLocked(st, r, now)
		return
	}
	if now.Sub(r.ObservingSince) >= r.Policy.Health.ObserveWindow {
		r.ObservingSince = time.Time{}
		r.Version++
		advanceWaveLocked(st, r, now)
	}
}

// healthEvalResult 是当前波次健康证据的汇总（阈值计算的输入）。
type healthEvalResult struct {
	Samples        int
	Unhealthy      int
	UnhealthyRatio float64
}

// healthEval 汇总指定波次的健康样本：只统计本波次节点、且采样时间不早于
// 本波开放时间的样本（乱序到达的样本按采样时间归位，跨波/跨发布的样本不参与）。
func healthEval(r *Release, wave int) healthEvalResult {
	inWave := make(map[string]bool, len(r.Waves[wave]))
	for _, n := range r.Waves[wave] {
		inWave[n] = true
	}
	var res healthEvalResult
	for _, sm := range r.Samples {
		if !inWave[sm.NodeID] || sm.SampledAt.Before(r.WaveOpenedAt) {
			continue
		}
		res.Samples++
		if !sm.Healthy {
			res.Unhealthy++
		}
	}
	if res.Samples > 0 {
		res.UnhealthyRatio = float64(res.Unhealthy) / float64(res.Samples)
	}
	return res
}

// advanceWaveLocked 原子开放下一波，或在末波达标后完成整个发布。
func advanceWaveLocked(st *State, r *Release, now time.Time) {
	if r.CurrentWave == len(r.Waves)-1 {
		r.State = StateCompleted
		r.Version++
		done := st.emit(EventReleaseCompleted, now)
		done.ReleaseID = r.ID
		return
	}
	r.CurrentWave++
	r.WaveOpenedAt = now
	for _, n := range r.Waves[r.CurrentWave] {
		r.Nodes[n].Result = ResultInProgress
	}
	open := st.emit(EventWaveOpened, now)
	open.ReleaseID, open.Wave = r.ID, r.CurrentWave
}

// startRollbackLocked 执行 Active -> RollingBack：一次性生成回滚计划，
// 对每个已成功节点恰好生成一条 node.rollback 通知（携带其发布前配置摘要）。
// 该迁移只发生一次，因此回滚计划与回滚通知都不会重复。
func startRollbackLocked(st *State, r *Release, now time.Time) {
	r.State = StateRollingBack
	r.ObservingSince = time.Time{}
	r.Version++

	plan := &RollbackPlan{ReleaseID: r.ID, StartedAt: now}
	started := st.emit(EventRollbackStarted, now)
	started.ReleaseID = r.ID

	for w, wave := range r.Waves {
		for _, n := range wave {
			if r.Nodes[n].Result != ResultSucceeded {
				continue
			}
			item := &RollbackItem{
				NodeID: n, Wave: w,
				FromDigest: r.Digest, ToDigest: r.Nodes[n].PreDigest,
				State: RollbackNotified,
			}
			plan.Items = append(plan.Items, item)
			ev := st.emit(EventNodeRollback, now)
			ev.ReleaseID, ev.Wave, ev.NodeID, ev.Digest = r.ID, w, n, item.ToDigest
		}
	}
	r.Rollback = plan
	if len(plan.Items) == 0 {
		// 没有已应用节点：回滚立即完成。
		completeRollbackLocked(st, r, now)
	}
}

// completeRollbackLocked 执行 RollingBack -> RolledBack 终态迁移，
// 发布终态通知（release.rolled_back）只在此处写出一次。
func completeRollbackLocked(st *State, r *Release, now time.Time) {
	r.State = StateRolledBack
	r.Version++
	done := st.emit(EventReleaseRolledBack, now)
	done.ReleaseID = r.ID
}

// TickTimeout 主动驱动超时与健康观测评估（后台定时器可周期性调用），
// 返回是否发生了暂停。
func (s *Service) TickTimeout(releaseID string) (paused bool, err error) {
	err = s.store.mutate(func(st *State) error {
		r, ok := st.Releases[releaseID]
		if !ok {
			return ErrReleaseNotFound
		}
		before := r.State
		s.evaluateLocked(st, r)
		paused = before == StateActive && r.State == StatePaused
		return nil
	})
	return paused, err
}

// ---------- 健康样本与回滚 ----------

// RecordSample 记录一条节点健康样本，并在样本落定后评估当前波次的健康阈值。
//
// 样本以 EventID 为幂等键：相同事件号、相同内容的重放幂等成功；同号异内容
// 返回 ErrSampleConflict。样本必须归属该发布（节点在发布内）且摘要与发布
// 配置一致（ErrDigestMismatch），因此不能跨发布或跨配置使用；乱序到达的样本
// 按采样时间参与评估。发布已终态或正在回滚时样本被拒绝。
func (s *Service) RecordSample(releaseID string, sm HealthSample) error {
	if sm.EventID == "" {
		return ErrInvalidSample
	}
	return s.store.mutate(func(st *State) error {
		r, ok := st.Releases[releaseID]
		if !ok {
			return ErrReleaseNotFound
		}
		if _, ok := r.Nodes[sm.NodeID]; !ok {
			return ErrNodeNotInRelease
		}
		if sm.Digest != r.Digest {
			return ErrDigestMismatch
		}
		if existing, ok := r.Samples[sm.EventID]; ok {
			if existing.NodeID == sm.NodeID && existing.Digest == sm.Digest &&
				existing.Healthy == sm.Healthy && existing.SampledAt.Equal(sm.SampledAt) {
				return nil // 相同事件重放：幂等
			}
			return ErrSampleConflict
		}
		switch r.State {
		case StateCancelled, StateCompleted, StateRolledBack:
			return ErrTerminal
		case StateRollingBack:
			return ErrNotActive
		}
		cp := sm
		r.Samples[sm.EventID] = &cp
		s.evaluateHealthLocked(st, r)
		return nil
	})
}

// AckRollback 处理节点的回滚确认：节点已恢复到回滚计划指定的发布前配置摘要。
// restoredDigest 必须与计划中的目标摘要一致（ErrDigestMismatch）；重复确认幂等。
// 全部计划项确认后，发布进入 RolledBack 终态（终态通知只写出一次）。
func (s *Service) AckRollback(releaseID, nodeID, restoredDigest string) error {
	return s.store.mutate(func(st *State) error {
		r, ok := st.Releases[releaseID]
		if !ok {
			return ErrReleaseNotFound
		}
		switch r.State {
		case StateCancelled, StateCompleted, StateRolledBack:
			return ErrTerminal
		case StateRollingBack:
		default:
			return ErrNoRollback
		}
		var item *RollbackItem
		for _, it := range r.Rollback.Items {
			if it.NodeID == nodeID {
				item = it
				break
			}
		}
		if item == nil {
			return ErrNodeNotInRelease
		}
		if item.State == RollbackDone {
			return nil // 重复确认：幂等
		}
		if restoredDigest != item.ToDigest {
			return ErrDigestMismatch
		}

		now := s.clock.Now()
		item.State = RollbackDone
		item.DoneAt = now
		r.Version++
		ev := st.emit(EventNodeRolledBack, now)
		ev.ReleaseID, ev.Wave, ev.NodeID, ev.Digest = r.ID, item.Wave, nodeID, item.ToDigest

		// 恢复节点“当前已应用版本”到发布前摘要；若节点已被更新的发布接管，
		// 则保持单调、不覆盖较新版本（计划项仍记为完成）。
		if ns := st.NodeStates[nodeID]; ns != nil && ns.AppliedReleaseSeq <= r.Seq {
			st.AppliedSeq++
			ns.AppliedSeq = st.AppliedSeq
			ns.AppliedReleaseSeq = r.Seq
			ns.AppliedReleaseID = r.ID
			ns.AppliedDigest = item.ToDigest
			ns.AppliedAt = now
		}

		for _, it := range r.Rollback.Items {
			if it.State != RollbackDone {
				return nil
			}
		}
		completeRollbackLocked(st, r, now)
		return nil
	})
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

// HealthStatus 是当前波次的健康证据与阈值计算快照。
type HealthStatus struct {
	Enabled        bool      `json:"enabled"`
	Observing      bool      `json:"observing"`
	ObservingSince time.Time `json:"observing_since,omitempty"`
	WindowEndsAt   time.Time `json:"window_ends_at,omitempty"`
	// 冻结的阈值参数。
	MinSamples int     `json:"min_samples"`
	Threshold  float64 `json:"failure_threshold"`
	// 阈值计算的输入：窗口内样本数、不健康数与占比。
	Samples        int     `json:"samples"`
	Unhealthy      int     `json:"unhealthy"`
	UnhealthyRatio float64 `json:"unhealthy_ratio"`
	// Verdict：insufficient_samples（样本不足，不做失败判定）/
	// passing（未达失败阈值）/ failed（达到失败阈值，触发回滚）。
	Verdict string `json:"verdict"`
}

// RollbackItemStatus 是回滚计划中单节点的前后版本与进度。
type RollbackItemStatus struct {
	NodeID     string            `json:"node_id"`
	Wave       int               `json:"wave"`
	FromDigest string            `json:"from_digest"`
	ToDigest   string            `json:"to_digest"`
	State      RollbackItemState `json:"state"`
}

// RollbackStatus 是回滚计划的进度快照。
type RollbackStatus struct {
	StartedAt time.Time            `json:"started_at"`
	Total     int                  `json:"total"`
	Done      int                  `json:"done"`
	Complete  bool                 `json:"complete"`
	Items     []RollbackItemStatus `json:"items"`
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
	// Health 在启用健康观测时非空，展示健康证据与阈值计算。
	Health *HealthStatus `json:"health,omitempty"`
	// Rollback 在回滚计划生成后非空，展示每节点前后版本与回滚进度。
	Rollback *RollbackStatus `json:"rollback,omitempty"`
}

// GetProgress 返回发布进度；查询时会顺带完成超时暂停与健康观测评估，
// 使超时/回滚状态及时落盘。
func (s *Service) GetProgress(releaseID string) (*Progress, error) {
	var out *Progress
	err := s.store.mutate(func(st *State) error {
		r, ok := st.Releases[releaseID]
		if !ok {
			return ErrReleaseNotFound
		}
		s.evaluateLocked(st, r)
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
	if r.Policy.Health.ObserveWindow > 0 {
		h := healthEval(r, r.CurrentWave)
		hs := &HealthStatus{
			Enabled:        true,
			Observing:      !r.ObservingSince.IsZero(),
			ObservingSince: r.ObservingSince,
			MinSamples:     r.Policy.Health.MinSamples,
			Threshold:      r.Policy.Health.FailureThreshold,
			Samples:        h.Samples,
			Unhealthy:      h.Unhealthy,
			UnhealthyRatio: h.UnhealthyRatio,
		}
		if hs.Observing {
			hs.WindowEndsAt = r.ObservingSince.Add(r.Policy.Health.ObserveWindow)
		}
		switch {
		case h.Samples < r.Policy.Health.MinSamples:
			hs.Verdict = "insufficient_samples"
		case h.UnhealthyRatio >= r.Policy.Health.FailureThreshold:
			hs.Verdict = "failed"
		default:
			hs.Verdict = "passing"
		}
		p.Health = hs
	}
	if r.Rollback != nil {
		rs := &RollbackStatus{StartedAt: r.Rollback.StartedAt, Total: len(r.Rollback.Items)}
		for _, it := range r.Rollback.Items {
			if it.State == RollbackDone {
				rs.Done++
			}
			rs.Items = append(rs.Items, RollbackItemStatus{
				NodeID: it.NodeID, Wave: it.Wave,
				FromDigest: it.FromDigest, ToDigest: it.ToDigest,
				State: it.State,
			})
		}
		rs.Complete = rs.Done == rs.Total
		p.Rollback = rs
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
	cp.Samples = make(map[string]*HealthSample, len(r.Samples))
	for id, sm := range r.Samples {
		smc := *sm
		cp.Samples[id] = &smc
	}
	if r.Rollback != nil {
		rb := *r.Rollback
		rb.Items = make([]*RollbackItem, len(r.Rollback.Items))
		for i, it := range r.Rollback.Items {
			itc := *it
			rb.Items[i] = &itc
		}
		cp.Rollback = &rb
	}
	return &cp
}
