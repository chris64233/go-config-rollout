package configrollout

import (
	"sort"
	"time"
)

// ---------- 健康样本上报 ----------

// HealthSample 是节点上报的一条健康样本。
type HealthSample struct {
	// EventID 是全局唯一事件号，作为幂等键：相同事件号相同内容的重放
	// 幂等忽略；相同事件号不同内容返回 ErrSampleConflict。
	EventID string
	// ReleaseID / NodeID / Digest 标识样本归属；样本不能跨发布
	// （发布不存在/节点不属于发布）或跨配置（摘要与发布摘要不一致）使用。
	ReleaseID string
	NodeID    string
	Digest    string
	// Healthy 为 false 表示失败样本；节点存在任一失败样本即判定为不健康。
	Healthy bool
	// SampledAt 是采样时间（允许乱序到达，不参与窗口计算，仅作证据留存）。
	SampledAt time.Time
}

// HealthOutcome 描述一条健康样本的处理结果。
type HealthOutcome struct {
	// Duplicate 为 true 表示相同事件号相同内容的重放，被幂等忽略。
	Duplicate bool
	// State / CurrentWave 为处理后的发布状态与当前波次。
	State       ReleaseState
	CurrentWave int
	// RollbackStarted 表示本条样本触发了回滚计划生成。
	RollbackStarted bool
	// WaveAdvanced 表示本条样本使观察期波次通过并推进。
	WaveAdvanced bool
}

// ReportHealth 上报一条健康样本。样本只在发布 Active/Paused 时被接受；
// 正在回滚返回 ErrRollbackInProgress，终态返回 ErrTerminal。
// 样本落库后对 Active 发布做一次健康评估：不健康节点数超过冻结的
// MaxUnhealthy 即生成一次性回滚计划；观察窗口届满且样本数达标则推进波次。
func (s *Service) ReportHealth(sample HealthSample) (*HealthOutcome, error) {
	if sample.EventID == "" || sample.SampledAt.IsZero() {
		return nil, ErrInvalidSample
	}
	var out HealthOutcome
	err := s.store.mutate(func(st *State) error {
		r, ok := st.Releases[sample.ReleaseID]
		if !ok {
			return ErrReleaseNotFound
		}
		if _, ok := r.Nodes[sample.NodeID]; !ok {
			return ErrNodeNotInRelease
		}
		if sample.Digest != r.Digest {
			return ErrDigestMismatch
		}
		if !r.HealthEnabled {
			return ErrHealthNotEnabled
		}
		// 幂等键为事件号：同号同内容重放幂等，同号异内容冲突。
		// 该检查先于隔离拦截：隔离依据样本的重放仍须幂等忽略，
		// 不得因节点已隔离而改变语义（更不能改写既有决定）。
		if prev, ok := r.Samples[sample.EventID]; ok {
			if prev.NodeID == sample.NodeID && prev.Digest == sample.Digest &&
				prev.Healthy == sample.Healthy && prev.SampledAt.Equal(sample.SampledAt) {
				out = HealthOutcome{Duplicate: true, State: r.State, CurrentWave: r.CurrentWave}
				return nil
			}
			return ErrSampleConflict
		}
		// 已隔离节点不能继续接收本次发布的新操作：新事件号的样本一律拒绝，
		// 迟到证据不会重新改写已经形成的隔离/回滚决定。
		if _, q := r.Quarantined[sample.NodeID]; q {
			return ErrNodeQuarantined
		}
		switch r.State {
		case StateActive, StatePaused:
			// 接受样本；Paused 期间只记录证据，恢复时再评估。
		case StateRollingBack:
			return ErrRollbackInProgress
		default:
			return ErrTerminal
		}
		now := s.clock.Now()
		r.Samples[sample.EventID] = &HealthSampleRecord{
			EventID:    sample.EventID,
			NodeID:     sample.NodeID,
			Digest:     sample.Digest,
			Healthy:    sample.Healthy,
			SampledAt:  sample.SampledAt,
			ReceivedAt: now,
		}
		r.Version++

		waveBefore, stateBefore := r.CurrentWave, r.State
		s.evaluateHealthLocked(st, r)
		out = HealthOutcome{
			State:           r.State,
			CurrentWave:     r.CurrentWave,
			RollbackStarted: stateBefore != StateRollingBack && r.State == StateRollingBack,
			WaveAdvanced:    r.CurrentWave != waveBefore || (stateBefore == StateActive && r.State == StateCompleted),
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return &out, nil
}

// ---------- 健康评估与自动回滚 ----------

// evaluateHealthLocked 对 Active 且启用健康观察的发布做评估：
//  1. 当前波已成功且未隔离节点中，含失败样本的节点数超过 MaxUnhealthy ->
//     生成一次性回滚计划（Active -> RollingBack），不再开新波次；
//  2. 隔离不能掩盖大面积故障：累计隔离节点数超过冻结的 MaxQuarantined，
//     或当前波健康覆盖率低于 MinHealthyCoverage -> 同样整体回滚；
//  3. 否则，观察窗口届满且每个未隔离的已成功节点样本数达到 MinSamples ->
//     原子开放下一波（末波则完成发布）。
//
// 回滚判定先于窗口推进判定，二者在同一事务内互斥，保证不会一边回滚
// 一边开新波次。
func (s *Service) evaluateHealthLocked(st *State, r *Release) {
	if r.State != StateActive || !r.HealthEnabled {
		return
	}
	if r.ThresholdMetAt.IsZero() {
		if waveCounts(r, r.CurrentWave).Succeeded < r.RequiredSuccess[r.CurrentWave] {
			return // 成功门槛尚未达标，还未进入观察期
		}
		r.ThresholdMetAt = s.clock.Now()
		r.Version++
	}
	now := s.clock.Now()

	// 阈值计算：当前波已成功、且未被隔离的节点中，存在失败样本的节点数。
	// 已隔离节点不再计入后续健康门槛（既不算不健康，也不要求样本达标）。
	unhealthy := 0
	waveSucceeded := 0
	covered := 0 // 未隔离的已成功节点数（健康覆盖率分子）
	for _, n := range r.Waves[r.CurrentWave] {
		if r.Nodes[n].Result != ResultSucceeded {
			continue
		}
		waveSucceeded++
		if r.Quarantined != nil {
			if _, q := r.Quarantined[n]; q {
				continue
			}
		}
		covered++
		if _, bad := nodeSampleCounts(r, n); bad > 0 {
			unhealthy++
		}
	}
	if unhealthy > r.Health.MaxUnhealthy {
		startRollbackLocked(st, r, RollbackReasonHealthThreshold, now)
		return
	}
	// 隔离上限：累计隔离数与当前波健康覆盖率任一越界即整体回滚。
	if r.QuarantineEnabled {
		if len(r.Quarantined) > r.Quarantine.MaxQuarantined {
			startRollbackLocked(st, r, RollbackReasonQuarantineLimit, now)
			return
		}
		if waveSucceeded > 0 {
			coverage := float64(covered) / float64(waveSucceeded)
			if coverage < r.Quarantine.MinHealthyCoverage {
				startRollbackLocked(st, r, RollbackReasonQuarantineLimit, now)
				return
			}
		}
	}

	// 观察窗口未届满：继续观察。
	if now.Sub(r.ThresholdMetAt) < r.Health.ObserveWindow {
		return
	}
	// 窗口届满但样本数不足：继续等待样本（不算超时，可人工暂停/取消）。
	// 已隔离节点不再要求样本达标。
	for _, n := range r.Waves[r.CurrentWave] {
		if r.Nodes[n].Result != ResultSucceeded {
			continue
		}
		if r.Quarantined != nil {
			if _, q := r.Quarantined[n]; q {
				continue
			}
		}
		if total, _ := nodeSampleCounts(r, n); total < r.Health.MinSamples {
			return
		}
	}
	advanceWaveLocked(st, r, now)
	r.Version++
}

// nodeSampleCounts 统计节点在本次发布中的样本数与失败样本数。
func nodeSampleCounts(r *Release, node string) (total, unhealthy int) {
	for _, smp := range r.Samples {
		if smp.NodeID != node {
			continue
		}
		total++
		if !smp.Healthy {
			unhealthy++
		}
	}
	return total, unhealthy
}

// startRollbackLocked 生成一次性回滚计划：对当前已成功应用配置的每个节点
// （含已隔离节点——整体回滚意味着隔离也不能豁免）恰好写出一条
// node.rollback 通知（目标为各自发布前的配置摘要），发布进入 RollingBack。
// 没有需要恢复的节点时直接落到终态 RolledBack。
func startRollbackLocked(st *State, r *Release, reason string, now time.Time) {
	r.State = StateRollingBack
	r.PauseReason = ""
	r.Version++

	plan := &RollbackPlan{
		Reason:    reason,
		CreatedAt: now,
		Entries:   make(map[string]*RollbackEntry),
	}
	started := st.emit(EventRollbackStarted, now)
	started.ReleaseID = r.ID
	started.Reason = reason

	for w, wave := range r.Waves {
		for _, n := range wave {
			lease := r.Nodes[n]
			if lease.Result != ResultSucceeded {
				continue
			}
			plan.Entries[n] = &RollbackEntry{
				NodeID:     n,
				Wave:       w,
				FromDigest: r.Digest,
				ToDigest:   lease.PrevDigest,
				Notified:   true,
			}
			ev := st.emit(EventNodeRollback, now)
			ev.ReleaseID, ev.Wave, ev.NodeID, ev.Digest = r.ID, w, n, lease.PrevDigest
		}
	}
	r.Rollback = plan
	// 整体回滚后，隔离节点的旧补跑不得继续执行：随回滚一并作废。
	abortCatchupLocked(st, r, CatchupAbortRolledBack, now)
	if len(plan.Entries) == 0 {
		completeRollbackLocked(st, r, now)
	}
}

// completeRollbackLocked 执行 RollingBack -> RolledBack 迁移并写出
// 恰好一次的发布终态通知。
func completeRollbackLocked(st *State, r *Release, now time.Time) {
	r.State = StateRolledBack
	r.Version++
	done := st.emit(EventReleaseRolledBack, now)
	done.ReleaseID = r.ID
}

// ---------- 回滚确认 ----------

// RollbackOutcome 描述一次回滚确认的结果。
type RollbackOutcome struct {
	// Ignored 为 true 表示该节点此前已确认恢复，本次为幂等重放。
	Ignored bool
	State   ReleaseState
	// Restored / Total 是回滚计划的整体进度。
	Restored int
	Total    int
}

// ConfirmRollback 节点确认已恢复到回滚计划指定的发布前配置摘要。
// digest 必须等于计划中的目标摘要（ErrDigestMismatch）；重复确认幂等。
// 全部节点恢复后发布进入终态 RolledBack，并写出恰好一次的终态通知。
// 节点已应用版本只允许单调推进：若节点已被更新的发布推进，恢复结果
// 不会覆盖较新版本。
func (s *Service) ConfirmRollback(releaseID, nodeID, digest string) (*RollbackOutcome, error) {
	var out RollbackOutcome
	err := s.store.mutate(func(st *State) error {
		r, ok := st.Releases[releaseID]
		if !ok {
			return ErrReleaseNotFound
		}
		if r.Rollback == nil {
			return ErrNoRollback
		}
		entry, ok := r.Rollback.Entries[nodeID]
		if !ok {
			return ErrNodeNotInRelease
		}
		if entry.ToDigest != digest {
			return ErrDigestMismatch
		}
		if entry.Restored {
			out = RollbackOutcome{Ignored: true, State: r.State}
			out.Restored, out.Total = rollbackProgress(r.Rollback)
			return nil
		}
		if r.State != StateRollingBack {
			// 计划存在且该节点未恢复，但发布已不在回滚中（不会发生，
			// 防御性返回）。
			return ErrTerminal
		}

		now := s.clock.Now()
		entry.Restored = true
		entry.RestoredAt = now
		r.Version++
		ev := st.emit(EventNodeRestored, now)
		ev.ReleaseID, ev.Wave, ev.NodeID, ev.Digest = r.ID, entry.Wave, nodeID, digest

		// 推进节点已应用版本到回滚目标（单调：不覆盖更新发布的应用结果）。
		ns := st.NodeStates[nodeID]
		if ns == nil {
			ns = &NodeState{NodeID: nodeID}
			st.NodeStates[nodeID] = ns
		}
		if r.Seq >= ns.AppliedReleaseSeq {
			st.AppliedSeq++
			ns.AppliedSeq = st.AppliedSeq
			ns.AppliedReleaseSeq = r.Seq
			ns.AppliedReleaseID = r.ID
			ns.AppliedDigest = entry.ToDigest
			ns.AppliedAt = now
		}

		if restored, total := rollbackProgress(r.Rollback); restored == total {
			completeRollbackLocked(st, r, now)
		}
		out = RollbackOutcome{State: r.State}
		out.Restored, out.Total = rollbackProgress(r.Rollback)
		return nil
	})
	if err != nil {
		return nil, err
	}
	return &out, nil
}

func rollbackProgress(plan *RollbackPlan) (restored, total int) {
	total = len(plan.Entries)
	for _, e := range plan.Entries {
		if e.Restored {
			restored++
		}
	}
	return restored, total
}

// ---------- 健康查询 ----------

// NodeHealthView 是单个节点的健康证据与版本视图。
type NodeHealthView struct {
	NodeID string     `json:"node_id"`
	Wave   int        `json:"wave"`
	Result NodeResult `json:"result"`
	Health NodeHealth `json:"health"`

	Samples          int `json:"samples"`
	UnhealthySamples int `json:"unhealthy_samples"`

	// Quarantined 标记节点已被隔离；Basis 为隔离依据的样本事件号。
	Quarantined bool     `json:"quarantined"`
	Basis       []string `json:"basis,omitempty"`

	// PrevDigest 是发布前已应用的配置摘要（空串表示此前无版本）；
	// CurrentDigest 是节点当前已应用版本（跨发布的最新值）。
	PrevDigest    string `json:"prev_digest,omitempty"`
	CurrentDigest string `json:"current_digest,omitempty"`
}

// RollbackEntryView 是回滚计划中单个节点的恢复进度。
type RollbackEntryView struct {
	NodeID     string    `json:"node_id"`
	Wave       int       `json:"wave"`
	FromDigest string    `json:"from_digest"`
	ToDigest   string    `json:"to_digest"`
	Restored   bool      `json:"restored"`
	RestoredAt time.Time `json:"restored_at,omitempty"`
}

// RollbackView 是回滚计划与进度视图。
type RollbackView struct {
	Reason    string              `json:"reason"`
	CreatedAt time.Time           `json:"created_at"`
	Total     int                 `json:"total"`
	Restored  int                 `json:"restored"`
	Entries   []RollbackEntryView `json:"entries"`
}

// HealthView 是发布健康状态的完整查询视图：冻结的策略、观察窗口、
// 阈值计算、每个节点的健康证据与前后版本、回滚进度。
type HealthView struct {
	ReleaseID string       `json:"release_id"`
	State     ReleaseState `json:"state"`
	Enabled   bool         `json:"enabled"`
	Policy    HealthPolicy `json:"policy"`

	CurrentWave    int       `json:"current_wave"`
	ThresholdMetAt time.Time `json:"threshold_met_at,omitempty"`
	// WindowEndsAt 是观察窗口届满时间（ThresholdMetAt + ObserveWindow）。
	WindowEndsAt time.Time `json:"window_ends_at,omitempty"`
	// Observing 表示当前波已达标、正在观察窗口内。
	Observing bool `json:"observing"`

	// 阈值计算：当前波已成功节点中的不健康节点数 vs 冻结的阈值。
	UnhealthyNodes    int  `json:"unhealthy_nodes"`
	MaxUnhealthy      int  `json:"max_unhealthy"`
	ThresholdBreached bool `json:"threshold_breached"`

	SamplesRecorded int              `json:"samples_recorded"`
	Nodes           []NodeHealthView `json:"nodes"`
	Rollback        *RollbackView    `json:"rollback,omitempty"`

	// Quarantine 展示冻结的隔离策略、隔离决定与覆盖率计算；未启用为 nil。
	Quarantine *QuarantineView `json:"quarantine,omitempty"`
	// Catchup 展示隔离节点的补跑进度；尚未创建计划为 nil。
	Catchup *CatchupView `json:"catchup,omitempty"`
}

// GetHealth 返回发布的健康视图；查询时顺带完成惰性评估（超时/观察窗口），
// 使窗口届满的波次及时推进或触发回滚。
func (s *Service) GetHealth(releaseID string) (*HealthView, error) {
	var out *HealthView
	err := s.store.mutate(func(st *State) error {
		r, ok := st.Releases[releaseID]
		if !ok {
			return ErrReleaseNotFound
		}
		s.evaluateLocked(st, r)
		out = healthViewOf(st, r, s.clock.Now())
		return nil
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

func healthViewOf(st *State, r *Release, now time.Time) *HealthView {
	v := &HealthView{
		ReleaseID:       r.ID,
		State:           r.State,
		Enabled:         r.HealthEnabled,
		Policy:          r.Health,
		CurrentWave:     r.CurrentWave,
		ThresholdMetAt:  r.ThresholdMetAt,
		SamplesRecorded: len(r.Samples),
		MaxUnhealthy:    r.Health.MaxUnhealthy,
	}
	if r.HealthEnabled && !r.ThresholdMetAt.IsZero() {
		v.WindowEndsAt = r.ThresholdMetAt.Add(r.Health.ObserveWindow)
		v.Observing = r.State == StateActive
	}

	// 节点视图按（波次, 节点 ID）排序，输出确定。
	nodes := make([]string, 0, len(r.Nodes))
	for n := range r.Nodes {
		nodes = append(nodes, n)
	}
	sort.Slice(nodes, func(i, j int) bool {
		wi, wj := r.Nodes[nodes[i]].Wave, r.Nodes[nodes[j]].Wave
		if wi != wj {
			return wi < wj
		}
		return nodes[i] < nodes[j]
	})
	for _, n := range nodes {
		lease := r.Nodes[n]
		total, bad := nodeSampleCounts(r, n)
		nv := NodeHealthView{
			NodeID:           n,
			Wave:             lease.Wave,
			Result:           lease.Result,
			Samples:          total,
			UnhealthySamples: bad,
			PrevDigest:       lease.PrevDigest,
		}
		qrec, isQ := r.Quarantined[n]
		if isQ {
			nv.Quarantined = true
			nv.Basis = append([]string(nil), qrec.Basis...)
		}
		switch {
		case isQ:
			// 已隔离：展示隔离依据，不再参与任何健康门槛。
			nv.Health = HealthQuarantined
		case lease.Result != ResultSucceeded:
			nv.Health = HealthUnknown
		case bad > 0:
			nv.Health = HealthUnhealthy
		case r.HealthEnabled && total >= r.Health.MinSamples:
			nv.Health = HealthHealthy
		default:
			nv.Health = HealthObserving
		}
		if ns := st.NodeStates[n]; ns != nil {
			nv.CurrentDigest = ns.AppliedDigest
		}
		if !isQ && lease.Wave == r.CurrentWave && lease.Result == ResultSucceeded && nv.Health == HealthUnhealthy {
			v.UnhealthyNodes++
		}
		v.Nodes = append(v.Nodes, nv)
	}
	v.ThresholdBreached = r.HealthEnabled && v.UnhealthyNodes > r.Health.MaxUnhealthy

	if r.QuarantineEnabled {
		v.Quarantine = quarantineViewOf(r)
	}
	if r.Catchup != nil {
		v.Catchup = catchupViewOf(st, r, now)
	}

	if r.Rollback != nil {
		restored, total := rollbackProgress(r.Rollback)
		rv := &RollbackView{
			Reason:    r.Rollback.Reason,
			CreatedAt: r.Rollback.CreatedAt,
			Total:     total,
			Restored:  restored,
		}
		keys := make([]string, 0, len(r.Rollback.Entries))
		for k := range r.Rollback.Entries {
			keys = append(keys, k)
		}
		sort.Slice(keys, func(i, j int) bool {
			wi, wj := r.Rollback.Entries[keys[i]].Wave, r.Rollback.Entries[keys[j]].Wave
			if wi != wj {
				return wi < wj
			}
			return keys[i] < keys[j]
		})
		for _, k := range keys {
			e := r.Rollback.Entries[k]
			rv.Entries = append(rv.Entries, RollbackEntryView{
				NodeID:     e.NodeID,
				Wave:       e.Wave,
				FromDigest: e.FromDigest,
				ToDigest:   e.ToDigest,
				Restored:   e.Restored,
				RestoredAt: e.RestoredAt,
			})
		}
		v.Rollback = rv
	}
	return v
}
