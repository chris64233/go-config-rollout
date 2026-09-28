package configrollout

import (
	"fmt"
	"sort"
	"time"
)

// 本文件实现第三轮能力：健康观察期的节点隔离（quarantine）与修复后的
// 有序补跑（catch-up）。
//
// 所有状态变更仍通过单个存储事务串行提交，隔离决定、整体回滚、补跑领取
// 并发发生时状态只能向前推进：
//   - 隔离记录一旦形成即冻结，迟到/重复样本只追加证据，不改写决定；
//   - 补跑计划携带单调递增的 fencing token，旧领取者的迟到上报被拒绝；
//   - 整体回滚/取消在同一事务内作废旧补跑，补跑成功不写发布终态通知。

// ---------- 隔离 ----------

// QuarantineInput 是操作员把节点标记为隔离的请求。隔离必须在健康观察期、
// 针对当前波次中已成功应用配置的节点作出，并携带一组健康样本作为依据。
type QuarantineInput struct {
	ReleaseID string
	NodeID    string
	// EvidenceEventIDs 是操作员作出隔离决定所依据的健康样本事件号；
	// 每条都必须是已落库、且属于该（发布, 节点, 配置摘要）的样本。
	EvidenceEventIDs []string
	Reason           string
}

// QuarantineOutcome 描述一次隔离操作的结果。
type QuarantineOutcome struct {
	// AlreadyQuarantined 为 true 表示隔离决定此前已形成，本次幂等忽略；
	// 已冻结的依据/波次/摘要不会被改写。
	AlreadyQuarantined bool
	State              ReleaseState
	// RollbackStarted 表示本次隔离使冻结的隔离闸门（MaxNodes 或
	// MinHealthyCoverage）失守，从而触发现有整体回滚。
	RollbackStarted bool
}

// QuarantineNode 在健康观察期把节点标记为隔离。
//
// 隔离决定绑定（发布, 节点, 配置摘要, 当前波次, 依据样本快照），落定后：
//   - 该节点不计入后续健康门槛（不健康计数、样本充足性、健康覆盖率）；
//   - 该节点不能再取得本次发布的配置（GetConfigForNode 拒绝）；
//   - 迟到/重复的健康样本只追加证据，绝不改写隔离决定。
//
// 隔离不能掩盖大面积故障：决策后若已隔离节点数超过冻结的 MaxNodes，或
// 未隔离已成功节点的健康覆盖率低于 MinHealthyCoverage，在同一事务内
// 触发现有的整体回滚。
func (s *Service) QuarantineNode(in QuarantineInput) (*QuarantineOutcome, error) {
	if len(in.EvidenceEventIDs) == 0 {
		return nil, ErrQuarantineNoEvidence
	}
	var out QuarantineOutcome
	err := s.store.mutate(func(st *State) error {
		r, ok := st.Releases[in.ReleaseID]
		if !ok {
			return ErrReleaseNotFound
		}
		lease, ok := r.Nodes[in.NodeID]
		if !ok {
			return ErrNodeNotInRelease
		}
		if !r.HealthEnabled || r.Health.Quarantine == (QuarantinePolicy{}) {
			return ErrQuarantineNotEnabled
		}

		// 先惰性结算（超时暂停 / 健康回滚），状态只向前推进。
		s.evaluateLocked(st, r)

		// 幂等：隔离决定已形成则原样返回，任何迟到输入都不改写它。
		if _, already := r.Quarantines[in.NodeID]; already {
			out = QuarantineOutcome{AlreadyQuarantined: true, State: r.State}
			return nil
		}

		switch r.State {
		case StateActive:
		case StatePaused:
			return ErrNotActive
		case StateRollingBack:
			return ErrRollbackInProgress
		default:
			return ErrTerminal
		}
		// 隔离只在当前波次的健康观察期、针对已成功节点作出。
		if lease.Wave != r.CurrentWave || lease.Result != ResultSucceeded {
			return ErrWaveNotOpen
		}

		// 校验依据样本：去重后每条事件号都必须存在，且属于该（发布, 节点, 摘要）。
		evidence := make([]string, 0, len(in.EvidenceEventIDs))
		seen := make(map[string]struct{}, len(in.EvidenceEventIDs))
		for _, id := range in.EvidenceEventIDs {
			if _, dup := seen[id]; dup {
				continue
			}
			seen[id] = struct{}{}
			smp, ok := r.Samples[id]
			if !ok || smp.NodeID != in.NodeID || smp.Digest != r.Digest {
				return ErrQuarantineEvidenceMismatch
			}
			evidence = append(evidence, id)
		}
		sort.Strings(evidence)

		now := s.clock.Now()
		total, bad := nodeSampleCounts(r, in.NodeID)
		rec := &QuarantineRecord{
			NodeID:           in.NodeID,
			Wave:             lease.Wave,
			Digest:           r.Digest,
			Reason:           in.Reason,
			QuarantinedAt:    now,
			EvidenceEventIDs: evidence,
			Samples:          total,
			UnhealthySamples: bad,
			PrevDigest:       lease.PrevDigest,
		}
		if ns := st.NodeStates[in.NodeID]; ns != nil {
			rec.CurrentDigest = ns.AppliedDigest
		}
		if r.Quarantines == nil {
			r.Quarantines = make(map[string]*QuarantineRecord)
		}
		r.Quarantines[in.NodeID] = rec
		r.Version++

		ev := st.emit(EventNodeQuarantined, now)
		ev.ReleaseID, ev.Wave, ev.NodeID, ev.Digest = r.ID, lease.Wave, in.NodeID, r.Digest
		if in.Reason != "" {
			ev.Reason = in.Reason
		}

		// 隔离后立即结算健康闸门：MaxNodes / 最低健康覆盖率任一失守即整体回滚。
		stateBefore := r.State
		s.evaluateHealthLocked(st, r)
		out = QuarantineOutcome{
			State:           r.State,
			RollbackStarted: stateBefore == StateActive && r.State == StateRollingBack,
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return &out, nil
}

// isQuarantinedLocked 返回节点在该发布中是否已被隔离。
func isQuarantinedLocked(r *Release, node string) bool {
	_, ok := r.Quarantines[node]
	return ok
}

// ---------- 补跑计划 ----------

// CreateCatchupPlan 为已隔离节点创建补跑计划。
//
// 计划在创建那一刻冻结：起点 FromDigest 为节点当前配置（补跑必须从节点
// 当前配置开始），目标为原发布目标摘要，路径按原发布的波次顺序排列。
// 每个隔离节点至多有一个未作废的补跑计划（ErrCatchupExists）；因基线
// 漂移作废、修复基线后可重新创建。因整体回滚/取消（发布终态）或被更新
// 发布接管而作废的计划不可在旧发布上重建（ErrTerminal / ErrCatchupAborted）。
func (s *Service) CreateCatchupPlan(releaseID, nodeID string) (*CatchupPlan, error) {
	var out *CatchupPlan
	err := s.store.mutate(func(st *State) error {
		r, ok := st.Releases[releaseID]
		if !ok {
			return ErrReleaseNotFound
		}
		lease, ok := r.Nodes[nodeID]
		if !ok {
			return ErrNodeNotInRelease
		}
		if _, ok := r.Quarantines[nodeID]; !ok {
			return ErrNodeNotQuarantined
		}
		switch r.State {
		case StateActive, StatePaused, StateCompleted:
		case StateRollingBack:
			return ErrRollbackInProgress
		default:
			return ErrTerminal
		}
		if newerReleaseSupersedesLocked(st, r, nodeID) {
			return ErrCatchupAborted
		}
		for _, p := range r.CatchupPlans {
			if p.NodeID == nodeID && p.State != CatchupAborted {
				return ErrCatchupExists
			}
		}

		ns := st.NodeStates[nodeID]
		from := ""
		if ns != nil {
			from = ns.AppliedDigest
		}
		now := s.clock.Now()
		st.CatchupSeq++
		plan := &CatchupPlan{
			ID:           fmt.Sprintf("catchup-%d", st.CatchupSeq),
			ReleaseID:    releaseID,
			NodeID:       nodeID,
			CreatedAt:    now,
			FromDigest:   from,
			TargetDigest: r.Digest,
			ReleaseSeq:   r.Seq,
			OriginalWave: lease.Wave,
			Path:         catchupPath(from, r.Digest),
			State:        CatchupPending,
		}
		if r.CatchupPlans == nil {
			r.CatchupPlans = make(map[string]*CatchupPlan)
		}
		r.CatchupPlans[plan.ID] = plan
		r.Version++

		ev := st.emit(EventCatchupCreated, now)
		ev.ReleaseID, ev.NodeID, ev.PlanID, ev.Digest = releaseID, nodeID, plan.ID, r.Digest
		out = cloneCatchupPlan(plan)
		return nil
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

// catchupPath 返回按原发布波次顺序的恢复路径。本发布对每个节点只有一个
// 目标摘要，因此路径为 [起点, 目标]（起点已等于目标时退化为单元素）。
func catchupPath(from, target string) []string {
	if from == target {
		return []string{target}
	}
	return []string{from, target}
}

// ClaimCatchup 领取补跑任务。领取成功返回单调递增的 fencing token：
// 上报成功/失败时必须原样携带；每次领取换发新 token，旧领取者的迟到
// 上报以 ErrCatchupFencing 拒绝，不能覆盖后来的状态。
//
// 领取时重新校验可执行性，任一条件不满足则把计划置为 aborted 并返回
// ErrCatchupAborted：整体发布已回滚/已取消、节点被更新发布取代、
// 节点基线已漂移（当前配置既非计划起点也非目标）。
func (s *Service) ClaimCatchup(planID, claimant string) (token int64, plan *CatchupPlan, err error) {
	var aborted bool
	err = s.store.mutate(func(st *State) error {
		p, r, perr := loadCatchupPlanLocked(st, planID)
		if perr != nil {
			return perr
		}
		switch p.State {
		case CatchupAborted:
			return ErrCatchupAborted
		case CatchupSucceeded:
			return ErrCatchupNotClaimable
		case CatchupRunning:
			return ErrCatchupNotClaimable // 尚未报告失败前不可重复领取
		}

		// 失效条件在领取时重新判定并作废，状态只向前推进。
		// 注意：作废必须在本事务内提交（返回 nil），错误在事务外转换，
		// 否则 mutate 会连同作废一起回滚。
		if reason := catchupInvalidReasonLocked(st, r, p); reason != "" {
			abortCatchupLocked(st, r, p, reason, s.clock.Now())
			aborted = true
			plan = cloneCatchupPlan(p)
			return nil
		}

		now := s.clock.Now()
		p.Fencing++
		p.State = CatchupRunning
		p.ClaimedBy = claimant
		p.ClaimedAt = now
		r.Version++

		ev := st.emit(EventCatchupClaimed, now)
		ev.ReleaseID, ev.NodeID, ev.PlanID = r.ID, p.NodeID, p.ID
		token = p.Fencing
		plan = cloneCatchupPlan(p)
		return nil
	})
	if err != nil {
		return 0, nil, err
	}
	if aborted {
		return 0, plan, ErrCatchupAborted
	}
	return token, plan, nil
}

// CatchupReport 是补跑执行结果上报。
type CatchupReport struct {
	PlanID string
	NodeID string
	// Token 是领取时拿到的 fencing token；旧领取者的迟到上报会被拒绝。
	Token int64
	// Success 为 true 时 Digest 必须等于计划目标摘要。
	Success bool
	Digest  string
}

// CatchupReportOutcome 描述一次补跑上报的结果。
type CatchupReportOutcome struct {
	// Ignored 为 true 表示计划此前已成功，同 token 重放被幂等忽略。
	Ignored bool
	State   CatchupState
}

// ReportCatchup 上报补跑执行结果。
//
// 成功把节点已应用版本单调推进到原发布目标摘要，但不改变发布状态、
// 不重复写出任何发布终态通知（release.completed / release.rolledback）。
// 失败仅把任务退回 failed（计划保留，可重新领取重试）。领取之后发布
// 若已回滚/取消、节点被取代或基线漂移，迟到成功不得落地，计划作废。
func (s *Service) ReportCatchup(rep CatchupReport) (*CatchupReportOutcome, error) {
	var out CatchupReportOutcome
	var aborted bool
	err := s.store.mutate(func(st *State) error {
		p, r, perr := loadCatchupPlanLocked(st, rep.PlanID)
		if perr != nil {
			return perr
		}
		if p.NodeID != rep.NodeID {
			return ErrNodeNotInRelease
		}
		switch p.State {
		case CatchupAborted:
			return ErrCatchupAborted
		case CatchupSucceeded:
			// 同一位领取者的迟到/重复成功：幂等忽略，不再写事件或版本；
			// 更旧领取者的重放以 fencing 拒绝。
			if rep.Token != p.Fencing {
				return ErrCatchupFencing
			}
			out = CatchupReportOutcome{Ignored: true, State: p.State}
			return nil
		case CatchupPending, CatchupFailed:
			return ErrCatchupNotClaimable
		}
		if rep.Token != p.Fencing {
			// 旧领取者的迟到上报：不能覆盖后来的领取/作废。
			return ErrCatchupFencing
		}

		now := s.clock.Now()
		if !rep.Success {
			p.State = CatchupFailed
			r.Version++
			ev := st.emit(EventCatchupFailed, now)
			ev.ReleaseID, ev.NodeID, ev.PlanID = r.ID, p.NodeID, p.ID
			out = CatchupReportOutcome{State: p.State}
			return nil
		}
		if rep.Digest != p.TargetDigest {
			return ErrDigestMismatch
		}
		// 领取与上报之间发布可能已回滚/取消，或节点基线被外部改动：
		// 迟到成功不得覆盖这些后来发生的状态。作废在本事务内提交，
		// 错误在事务外返回（否则 mutate 会把作废一并回滚）。
		if reason := catchupInvalidReasonLocked(st, r, p); reason != "" {
			abortCatchupLocked(st, r, p, reason, now)
			aborted = true
			return nil
		}

		p.State = CatchupSucceeded
		p.SucceededAt = now
		r.Version++

		// 节点已应用版本单调推进到目标摘要（不覆盖更新发布的应用结果）。
		ns := st.NodeStates[p.NodeID]
		if ns == nil {
			ns = &NodeState{NodeID: p.NodeID}
			st.NodeStates[p.NodeID] = ns
		}
		if r.Seq >= ns.AppliedReleaseSeq {
			st.AppliedSeq++
			ns.AppliedSeq = st.AppliedSeq
			ns.AppliedReleaseSeq = r.Seq
			ns.AppliedReleaseID = r.ID
			ns.AppliedDigest = p.TargetDigest
			ns.AppliedAt = now
		}

		ev := st.emit(EventCatchupSucceeded, now)
		ev.ReleaseID, ev.NodeID, ev.PlanID, ev.Digest = r.ID, p.NodeID, p.ID, p.TargetDigest
		// 注意：此处绝不写出发布终态通知。
		out = CatchupReportOutcome{State: p.State}
		return nil
	})
	if err != nil {
		return nil, err
	}
	if aborted {
		return nil, ErrCatchupAborted
	}
	return &out, nil
}

// GetCatchupConfig 返回补跑任务应应用的目标配置内容。只有计划处于
// Running、且 token 与当前领取者匹配时才能取得；计划作废或 token 过期
// （旧领取者的迟到请求）一律拒绝，保证配置不会下发给已失效的补跑。
func (s *Service) GetCatchupConfig(planID string, token int64) (*Config, error) {
	var out *Config
	err := s.store.read(func(st *State) error {
		p, _, perr := loadCatchupPlanLocked(st, planID)
		if perr != nil {
			return perr
		}
		if p.State == CatchupAborted {
			return ErrCatchupAborted
		}
		if p.State != CatchupRunning || token != p.Fencing {
			return ErrCatchupFencing
		}
		cfg := st.Configs[p.TargetDigest]
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

// GetCatchupPlan 返回补跑计划的最新快照（含进度与作废原因）。
func (s *Service) GetCatchupPlan(planID string) (*CatchupPlan, error) {
	var out *CatchupPlan
	err := s.store.read(func(st *State) error {
		p, _, perr := loadCatchupPlanLocked(st, planID)
		if perr != nil {
			return perr
		}
		out = cloneCatchupPlan(p)
		return nil
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

// loadCatchupPlanLocked 按计划 ID 取出计划与其所属发布。
func loadCatchupPlanLocked(st *State, planID string) (*CatchupPlan, *Release, error) {
	if planID == "" {
		return nil, nil, ErrCatchupNotFound
	}
	for _, r := range st.Releases {
		if p, ok := r.CatchupPlans[planID]; ok {
			return p, r, nil
		}
	}
	return nil, nil, ErrCatchupNotFound
}

// catchupInvalidReasonLocked 返回补跑当前不可执行的作废原因；空串表示可执行。
func catchupInvalidReasonLocked(st *State, r *Release, p *CatchupPlan) string {
	switch r.State {
	case StateRollingBack, StateRolledBack:
		return CatchupAbortReleaseRolledBack
	case StateCancelled:
		return CatchupAbortReleaseCancelled
	}
	if newerReleaseSupersedesLocked(st, r, p.NodeID) {
		return CatchupAbortSuperseded
	}
	ns := st.NodeStates[p.NodeID]
	current := ""
	if ns != nil {
		current = ns.AppliedDigest
	}
	// 节点基线漂移：当前配置既不是计划起点，也不是目标摘要
	//（停在目标上视为可重复应用，不算漂移）。
	if current != p.FromDigest && current != p.TargetDigest {
		return CatchupAbortBaselineChanged
	}
	return ""
}

// newerReleaseSupersedesLocked 判断是否存在一个更新的、仍有效（未取消/
// 未回滚）且把该节点冻结为目标的发布 —— 它使旧补跑失去执行资格。
func newerReleaseSupersedesLocked(st *State, r *Release, node string) bool {
	for _, other := range st.Releases {
		if other.Seq <= r.Seq {
			continue
		}
		if _, ok := other.Nodes[node]; !ok {
			continue
		}
		if other.State == StateCancelled || other.State == StateRolledBack {
			continue
		}
		return true
	}
	return false
}

// abortCatchupLocked 把补跑计划置为终态 aborted 并写出恰好一条作废事件。
func abortCatchupLocked(st *State, r *Release, p *CatchupPlan, reason string, now time.Time) {
	p.State = CatchupAborted
	p.AbortReason = reason
	p.AbortedAt = now
	r.Version++
	ev := st.emit(EventCatchupAborted, now)
	ev.ReleaseID, ev.NodeID, ev.PlanID, ev.Reason = r.ID, p.NodeID, p.ID, reason
}

// abortActiveCatchupsLocked 作废发布下所有未终态的补跑计划（整体回滚/
// 取消时在同一事务内调用），保证旧补跑不会在发布终态后继续执行。
func abortActiveCatchupsLocked(st *State, r *Release, reason string, now time.Time) {
	if r.CatchupPlans == nil {
		return
	}
	// 确定顺序输出，事件序列稳定。
	ids := make([]string, 0, len(r.CatchupPlans))
	for id := range r.CatchupPlans {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	for _, id := range ids {
		p := r.CatchupPlans[id]
		if p.State == CatchupAborted || p.State == CatchupSucceeded {
			continue
		}
		abortCatchupLocked(st, r, p, reason, now)
	}
}

func cloneCatchupPlan(p *CatchupPlan) *CatchupPlan {
	cp := *p
	cp.Path = append([]string(nil), p.Path...)
	return &cp
}

// catchupViewsOf 构建发布下全部补跑计划的视图（按计划 ID 排序），并为
// 每个计划标注查询时刻节点的实际配置与无法继续的原因。
func catchupViewsOf(st *State, r *Release) []CatchupPlanView {
	if len(r.CatchupPlans) == 0 {
		return nil
	}
	ids := make([]string, 0, len(r.CatchupPlans))
	for id := range r.CatchupPlans {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	out := make([]CatchupPlanView, 0, len(ids))
	for _, id := range ids {
		p := r.CatchupPlans[id]
		cv := CatchupPlanView{
			PlanID:       p.ID,
			NodeID:       p.NodeID,
			State:        p.State,
			CreatedAt:    p.CreatedAt,
			FromDigest:   p.FromDigest,
			TargetDigest: p.TargetDigest,
			OriginalWave: p.OriginalWave,
			Path:         append([]string(nil), p.Path...),
			Fencing:      p.Fencing,
			ClaimedBy:    p.ClaimedBy,
			ClaimedAt:    p.ClaimedAt,
			SucceededAt:  p.SucceededAt,
			AbortReason:  p.AbortReason,
			AbortedAt:    p.AbortedAt,
		}
		if ns := st.NodeStates[p.NodeID]; ns != nil {
			cv.NodeCurrentDigest = ns.AppliedDigest
		}
		switch p.State {
		case CatchupAborted:
			cv.BlockedReason = blockedReasonText(p.AbortReason)
		case CatchupSucceeded:
			// 已完成；无阻塞原因。
		default:
			if reason := catchupInvalidReasonLocked(st, r, p); reason != "" {
				cv.BlockedReason = blockedReasonText(reason)
			}
		}
		out = append(out, cv)
	}
	return out
}

func blockedReasonText(reason string) string {
	switch reason {
	case CatchupAbortReleaseRolledBack:
		return "release rolled back; catch-up aborted"
	case CatchupAbortReleaseCancelled:
		return "release cancelled; catch-up aborted"
	case CatchupAbortSuperseded:
		return "node superseded by a newer release; catch-up aborted"
	case CatchupAbortBaselineChanged:
		return "node baseline config changed; catch-up aborted"
	default:
		return reason
	}
}
