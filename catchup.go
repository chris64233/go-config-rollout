package configrollout

import (
	"sort"
	"time"
)

// ---------- 补跑计划 ----------
//
// 隔离节点的问题处理完成后，操作员可为其创建补跑计划。补跑从节点创建
// 计划时的实际配置开始，严格按原发布的波次顺序恢复到原发布目标摘要。
// 计划是无锁化的租约模型：领取带 epoch，完成确认必须匹配当前 epoch，
// 因此旧领取者的迟到成功永远无法覆盖后来的回滚/重新领取。

// CatchupSkip 描述创建计划时被跳过的隔离节点及原因。
type CatchupSkip struct {
	NodeID string `json:"node_id"`
	Wave   int    `json:"wave"`
	// Reason：superseded（节点已被更新的发布推进，本发布补跑不再适用）。
	Reason        string `json:"reason"`
	CurrentDigest string `json:"current_digest,omitempty"`
}

const (
	catchupSkipSuperseded = "superseded"
)

// CreateCatchupPlan 为当前所有「已隔离且尚未补跑成功」的节点创建补跑计划。
//
// 计划创建时冻结每个节点的起始配置（节点当前实际摘要与应用序号基线）；
// 已经处于目标摘要的节点直接记为已修复并跳过，已被更新发布推进的节点
// 跳过。整体发布已回滚/取消时不能创建（ErrTerminal / ErrRollbackInProgress）；
// 已存在未结束的计划时返回 ErrCatchupActive（用 RebuildCatchupPlan 重建）。
func (s *Service) CreateCatchupPlan(releaseID string) (*CatchupPlan, []CatchupSkip, error) {
	return s.createCatchup(releaseID, false)
}

// RebuildCatchupPlan 作废当前未结束的计划（写出 catchup.aborted，
// reason=plan_replaced）并以节点最新基线创建新计划。已补跑成功的节点
// 不再纳入；这是补跑失败后「修复问题、有序重跑」的入口。
func (s *Service) RebuildCatchupPlan(releaseID string) (*CatchupPlan, []CatchupSkip, error) {
	return s.createCatchup(releaseID, true)
}

func (s *Service) createCatchup(releaseID string, replace bool) (*CatchupPlan, []CatchupSkip, error) {
	var out *CatchupPlan
	var skipped []CatchupSkip
	var retErr error
	err := s.store.mutate(func(st *State) error {
		r, ok := st.Releases[releaseID]
		if !ok {
			return ErrReleaseNotFound
		}
		if !r.QuarantineEnabled || len(r.Quarantined) == 0 {
			return ErrNoCatchup
		}
		// 创建前先惰性结算：若已应回滚/暂停，先让状态向前到位。
		s.evaluateLocked(st, r)
		switch r.State {
		case StateActive, StatePaused, StateCompleted:
		case StateRollingBack:
			return ErrRollbackInProgress
		default:
			return ErrTerminal
		}

		if r.Catchup != nil {
			active := r.Catchup.State == CatchupPending || r.Catchup.State == CatchupClaimed
			if active && !replace {
				return ErrCatchupActive
			}
		}

		now := s.clock.Now()
		plan := &CatchupPlan{
			CreatedAt: now,
			Entries:   make(map[string]*CatchupEntry),
			State:     CatchupPending,
		}

		// 按（波次, 节点）有序遍历冻结目标，保证 Waves 与输出确定。
		nodeIDs := sortedQuarantinedNodes(r)
		waveSet := map[int]struct{}{}
		for _, n := range nodeIDs {
			q := r.Quarantined[n]
			if q.Remediated {
				continue
			}
			ns := st.NodeStates[n]
			current := ""
			var baselineSeq int64
			var currentRelease string
			if ns != nil {
				current = ns.AppliedDigest
				baselineSeq = ns.AppliedSeq
				currentRelease = ns.AppliedReleaseID
			}
			if ns != nil && ns.AppliedReleaseSeq > r.Seq {
				// 节点已被更新的发布推进，本发布的补跑不再适用。
				skipped = append(skipped, CatchupSkip{
					NodeID: n, Wave: q.Wave, Reason: catchupSkipSuperseded, CurrentDigest: current,
				})
				continue
			}
			// 其余隔离节点全部纳入补跑：即使起始摘要恰好等于目标摘要
			// （节点隔离时仍停留在发布摘要上），问题修复后仍需重新施加
			// 并复验确认 —— 补跑从节点当前配置开始，恢复到目标摘要。
			plan.Entries[n] = &CatchupEntry{
				NodeID:         n,
				Wave:           q.Wave,
				StartDigest:    current, // 从节点当前配置开始（可能为空串）
				StartReleaseID: currentRelease,
				TargetDigest:   r.Digest,
				State:          CatchupPending,
				BaselineSeq:    baselineSeq,
			}
			waveSet[q.Wave] = struct{}{}
		}

		// 重建路径下先把旧计划作废；即使新计划没有可补跑项，旧计划的
		// 作废也必须提交（状态只向前）。
		if replace && r.Catchup != nil {
			active := r.Catchup.State == CatchupPending || r.Catchup.State == CatchupClaimed
			if active {
				abortCatchupLocked(st, r, CatchupAbortReplaced, now)
			}
		}

		if len(plan.Entries) == 0 {
			// 没有可补跑项：不落新计划（旧计划若被作废则保持 aborted）。
			retErr = ErrNoCatchupWork
			return nil
		}

		r.CatchupAttempt++
		plan.Attempt = r.CatchupAttempt
		for w := range waveSet {
			plan.Waves = append(plan.Waves, w)
		}
		sort.Ints(plan.Waves)
		r.Catchup = plan
		r.Version++

		ev := st.emit(EventCatchupCreated, now)
		ev.ReleaseID, ev.Attempt = r.ID, plan.Attempt
		out = cloneCatchup(plan)
		return nil
	})
	if err != nil {
		return nil, nil, err
	}
	if retErr != nil {
		return nil, skipped, retErr
	}
	return out, skipped, nil
}

func sortedQuarantinedNodes(r *Release) []string {
	ids := make([]string, 0, len(r.Quarantined))
	for n := range r.Quarantined {
		ids = append(ids, n)
	}
	sort.Slice(ids, func(i, j int) bool {
		qi, qj := r.Quarantined[ids[i]], r.Quarantined[ids[j]]
		if qi.Wave != qj.Wave {
			return qi.Wave < qj.Wave
		}
		return ids[i] < ids[j]
	})
	return ids
}

// abortCatchupLocked 将未结束的补跑计划作废（状态只向前推进），并写出
// 恰好一条 catchup.aborted。已结束的计划不受影响。调用方持事务。
func abortCatchupLocked(st *State, r *Release, reason string, now time.Time) {
	if r.Catchup == nil {
		return
	}
	if r.Catchup.State != CatchupPending && r.Catchup.State != CatchupClaimed {
		return
	}
	r.Catchup.State = CatchupAborted
	r.Catchup.AbortReason = reason
	r.Catchup.AbortedAt = now
	for _, e := range r.Catchup.Entries {
		if e.State == CatchupPending || e.State == CatchupClaimed {
			e.State = CatchupAborted
			e.FinishedAt = now
		}
	}
	r.Version++
	ev := st.emit(EventCatchupAborted, now)
	ev.ReleaseID, ev.Attempt, ev.Reason = r.ID, r.Catchup.Attempt, reason
}

// maybeAbortCatchupLocked 在发布进入终态（回滚/取消）时作废补跑计划：
// 整体回滚/取消后，旧补跑不得继续执行。
func maybeAbortCatchupForStateLocked(st *State, r *Release, now time.Time) {
	switch r.State {
	case StateRollingBack:
		abortCatchupLocked(st, r, CatchupAbortRolledBack, now)
	case StateCancelled:
		abortCatchupLocked(st, r, CatchupAbortRolledBack, now)
	}
}

// abortSupersededCatchupLocked 在某节点被 current 发布（成功回执或补跑
// 成功）推进已应用版本时，作废其它发布中包含该节点的活跃补跑计划：
// 节点已被（新）发布推进，旧补跑的基线失效，不得再执行。
func abortSupersededCatchupLocked(st *State, current *Release, nodeID string, now time.Time) {
	for id, rel := range st.Releases {
		if id == current.ID || rel.Catchup == nil {
			continue
		}
		if rel.Catchup.State != CatchupPending && rel.Catchup.State != CatchupClaimed {
			continue
		}
		if _, ok := rel.Catchup.Entries[nodeID]; !ok {
			continue
		}
		abortCatchupLocked(st, rel, CatchupAbortSuperseded, now)
	}
}

// ---------- 领取与回报 ----------

// CatchupLease 是一次补跑领取的租约。
type CatchupLease struct {
	Attempt int64  `json:"attempt"`
	Epoch   int64  `json:"epoch"`
	NodeID  string `json:"node_id"`
	Wave    int    `json:"wave"`
	// FromDigest 是节点当前（补跑起始）配置摘要；ToDigest 是目标摘要。
	FromDigest string    `json:"from_digest"`
	ToDigest   string    `json:"to_digest"`
	ClaimedBy  string    `json:"claimed_by"`
	LeaseUntil time.Time `json:"lease_until"`
}

// ClaimCatchup 领取下一个可执行的补跑项。严格按原发布波次顺序：前序波次
// 全部成功后本波才可领取；同波内任一项失败则形成屏障（ErrCatchupBlocked），
// 需 RebuildCatchupPlan 后才能继续。
//
// 租约：leaseTTL 后未回报的项可被重新领取，每次领取 epoch 加一；
// 完成回报必须携带领取时的 epoch，旧领取者的迟到成功一律拒绝。
func (s *Service) ClaimCatchup(releaseID, workerID string, leaseTTL time.Duration) (*CatchupLease, error) {
	if workerID == "" || leaseTTL <= 0 {
		return nil, ErrInvalidPolicy
	}
	var out *CatchupLease
	var retErr error
	err := s.store.mutate(func(st *State) error {
		r, ok := st.Releases[releaseID]
		if !ok {
			return ErrReleaseNotFound
		}
		s.evaluateLocked(st, r)
		plan := r.Catchup
		if plan == nil {
			return ErrNoCatchup
		}
		now0 := s.clock.Now()
		maybeAbortCatchupForStateLocked(st, r, now0)
		if plan.State != CatchupPending && plan.State != CatchupClaimed {
			return ErrCatchupNotActive
		}
		switch r.State {
		case StateRollingBack:
			// 上面的惰性作废已提交；这里返回的错误仅用于告知调用方。
			retErr = ErrRollbackInProgress
			return nil
		case StateCancelled, StateRolledBack:
			retErr = ErrTerminal
			return nil
		}
		now := now0

		// 严格波次屏障，按波次升序考察：
		//  - 波内存在失败项 -> ErrCatchupBlocked（不得跳过失败节点）；
		//  - 波内存在可领取项（pending 或租约过期）-> 领取其中节点 ID 最小者；
		//  - 波内尚有未成功项但都持有效租约（进行中）-> 当前无工作可领，
		//    绝不跳到后序波次；
		//  - 波内全部成功 -> 继续考察下一波。
		var candidate *CatchupEntry
		var blocked bool
	loop:
		for _, w := range plan.Waves {
			var pick string
			unfinished := false
			for _, e := range plan.Entries {
				if e.Wave != w {
					continue
				}
				switch {
				case e.State == CatchupSucceeded:
					continue
				case e.State == CatchupFailed:
					blocked = true
					break loop
				case e.State == CatchupPending:
					unfinished = true
				case e.State == CatchupClaimed && !e.LeaseUntil.After(now):
					unfinished = true // 租约过期，可重新领取
				default:
					unfinished = true // 有效租约，进行中
					continue
				}
				if pick == "" || e.NodeID < pick {
					pick = e.NodeID
				}
			}
			switch {
			case pick != "":
				candidate = plan.Entries[pick]
				break loop
			case unfinished:
				retErr = ErrNoCatchupWork // 前序波次仍在进行，后序波次等待
				return nil
			}
		}
		if blocked {
			retErr = ErrCatchupBlocked
			return nil
		}
		if candidate == nil {
			retErr = ErrNoCatchupWork
			return nil
		}

		// 领取前再次校验节点基线：基线已变化（被新发布推进/人工改动）
		// 时，旧补跑不得执行 —— 先在本事务内提交计划作废，再返回错误。
		ns := st.NodeStates[candidate.NodeID]
		var seq int64
		if ns != nil {
			seq = ns.AppliedSeq
		}
		if seq != candidate.BaselineSeq {
			abortCatchupLocked(st, r, CatchupAbortSuperseded, now)
			retErr = ErrBaselineChanged
			return nil
		}

		candidate.ClaimEpoch++
		candidate.State = CatchupClaimed
		candidate.ClaimedBy = workerID
		candidate.ClaimedAt = now
		candidate.LeaseUntil = now.Add(leaseTTL)
		plan.State = CatchupClaimed
		r.Version++

		ev := st.emit(EventNodeCatchup, now)
		ev.ReleaseID, ev.Wave, ev.NodeID = r.ID, candidate.Wave, candidate.NodeID
		ev.Digest, ev.ToDigest = candidate.StartDigest, candidate.TargetDigest
		ev.Epoch, ev.Attempt = candidate.ClaimEpoch, plan.Attempt

		out = &CatchupLease{
			Attempt: plan.Attempt, Epoch: candidate.ClaimEpoch,
			NodeID: candidate.NodeID, Wave: candidate.Wave,
			FromDigest: candidate.StartDigest, ToDigest: candidate.TargetDigest,
			ClaimedBy: workerID, LeaseUntil: candidate.LeaseUntil,
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	if retErr != nil {
		return nil, retErr
	}
	return out, nil
}

// CatchupResult 是领取者对补跑项的回报。
type CatchupResult struct {
	ReleaseID string
	NodeID    string
	// Epoch 必须等于领取租约上的 epoch；不匹配说明租约已失效
	// （过期被他人重新领取），回报被拒绝（ErrClaimLost）。
	Epoch int64
	// Digest 为节点实际应用后的配置摘要；成功时必须等于目标摘要。
	Digest  string
	Success bool
}

// CatchupOutcome 描述一次补跑回报的结果。
type CatchupOutcome struct {
	Ignored bool
	State   CatchupState
	// Succeeded / Total 是当前计划的整体成功进度。
	Succeeded int
	Total     int
}

// CompleteCatchup 提交补跑结果。成功后节点已应用版本单调推进到目标摘要，
// 隔离决定标记 Remediated；所有项成功后计划进入 done 并写出恰好一条
// catchup.completed —— 它不是发布终态通知，release.completed 不会被
// 补跑重复写出。失败使该项形成波次屏障，需修复后 RebuildCatchupPlan。
func (s *Service) CompleteCatchup(res CatchupResult) (*CatchupOutcome, error) {
	var out CatchupOutcome
	var retErr error
	err := s.store.mutate(func(st *State) error {
		r, ok := st.Releases[res.ReleaseID]
		if !ok {
			return ErrReleaseNotFound
		}
		plan := r.Catchup
		if plan == nil {
			return ErrNoCatchup
		}
		entry, ok := plan.Entries[res.NodeID]
		if !ok {
			return ErrCatchupEntryMissing
		}
		// 已成功：幂等忽略，绝不重复推进/重复发通知。
		if entry.State == CatchupSucceeded {
			out = CatchupOutcome{Ignored: true, State: plan.State}
			out.Succeeded, out.Total = catchupCounters(plan)
			return nil
		}
		s.evaluateLocked(st, r)
		now0 := s.clock.Now()
		maybeAbortCatchupForStateLocked(st, r, now0)
		// 计划在本事务内刚被作废：作废必须提交，再以错误告知调用方。
		if plan.State == CatchupAborted {
			retErr = ErrCatchupNotActive
			return nil
		}
		if entry.State == CatchupFailed {
			// 失败项需先重建计划才能再回报。
			retErr = ErrCatchupNotActive
			return nil
		}
		if entry.State != CatchupClaimed {
			retErr = ErrCatchupNotClaimed
			return nil
		}
		if entry.ClaimEpoch != res.Epoch {
			// 旧领取者的迟到成功：租约已过期并被重新领取，不能覆盖。
			retErr = ErrClaimLost
			return nil
		}
		now := now0

		if !res.Success {
			entry.State = CatchupFailed
			entry.FinishedAt = now
			r.Version++
			ev := st.emit(EventNodeCatchupFailed, now)
			ev.ReleaseID, ev.Wave, ev.NodeID = r.ID, entry.Wave, res.NodeID
			ev.Digest, ev.Epoch, ev.Attempt = res.Digest, entry.ClaimEpoch, plan.Attempt
			out.State = plan.State
			out.Succeeded, out.Total = catchupCounters(plan)
			return nil
		}
		if res.Digest != entry.TargetDigest {
			retErr = ErrDigestMismatch
			return nil
		}
		// 成功回报到达前发布已整体回滚：上面的 maybeAbort 已把计划作废；
		// 此处再校验一次节点基线，旧补跑成功不得覆盖后来的变化。
		ns := st.NodeStates[res.NodeID]
		var seq int64
		if ns != nil {
			seq = ns.AppliedSeq
		}
		if seq != entry.BaselineSeq {
			abortCatchupLocked(st, r, CatchupAbortSuperseded, now)
			retErr = ErrBaselineChanged
			return nil
		}

		entry.State = CatchupSucceeded
		entry.FinishedAt = now
		r.Version++
		ev := st.emit(EventNodeCatchupSucceeded, now)
		ev.ReleaseID, ev.Wave, ev.NodeID = r.ID, entry.Wave, res.NodeID
		ev.Digest, ev.Epoch, ev.Attempt = res.Digest, entry.ClaimEpoch, plan.Attempt

		// 单调推进节点已应用版本（基线校验保证没有更新的发布抢先）。
		if ns == nil {
			ns = &NodeState{NodeID: res.NodeID}
			st.NodeStates[res.NodeID] = ns
		}
		if r.Seq >= ns.AppliedReleaseSeq {
			st.AppliedSeq++
			ns.AppliedSeq = st.AppliedSeq
			ns.AppliedReleaseSeq = r.Seq
			ns.AppliedReleaseID = r.ID
			ns.AppliedDigest = res.Digest
			ns.AppliedAt = now
			// 本补跑把节点推进到本发布目标：其它发布针对该节点的活跃
			// 补跑基线同样失效，一并作废。
			abortSupersededCatchupLocked(st, r, res.NodeID, now)
		}
		if q := r.Quarantined[res.NodeID]; q != nil {
			q.Remediated = true
		}

		succeeded, total := catchupCounters(plan)
		if succeeded == total {
			plan.State = CatchupDone
			plan.CompletedAt = now
			done := st.emit(EventCatchupCompleted, now)
			done.ReleaseID, done.Attempt = r.ID, plan.Attempt
		}
		out = CatchupOutcome{State: plan.State, Succeeded: succeeded, Total: total}
		return nil
	})
	if err != nil {
		return nil, err
	}
	if retErr != nil {
		return nil, retErr
	}
	return &out, nil
}

func catchupCounters(plan *CatchupPlan) (succeeded, total int) {
	total = len(plan.Entries)
	for _, e := range plan.Entries {
		if e.State == CatchupSucceeded {
			succeeded++
		}
	}
	return succeeded, total
}

// GetCatchupConfig 返回补跑目标配置内容；只有该节点存在有效领取租约
// （未过期）时才能取得，避免未领取者提前拉取。
func (s *Service) GetCatchupConfig(releaseID, nodeID string) (*Config, error) {
	var out *Config
	err := s.store.read(func(st *State) error {
		r, ok := st.Releases[releaseID]
		if !ok {
			return ErrReleaseNotFound
		}
		plan := r.Catchup
		if plan == nil {
			return ErrNoCatchup
		}
		entry, ok := plan.Entries[nodeID]
		if !ok {
			return ErrCatchupEntryMissing
		}
		now := s.clock.Now()
		if entry.State != CatchupClaimed || !entry.LeaseUntil.After(now) {
			return ErrConfigNotAvailable
		}
		cfg := st.Configs[entry.TargetDigest]
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

// ---------- 补跑查询 ----------

// CatchupEntryView 是单个补跑项的视图（含无法继续的原因）。
type CatchupEntryView struct {
	NodeID       string       `json:"node_id"`
	Wave         int          `json:"wave"`
	State        CatchupState `json:"state"`
	StartDigest  string       `json:"start_digest,omitempty"`
	TargetDigest string       `json:"target_digest"`
	// CurrentDigest 是节点此刻的实际配置摘要；BaselineChanged 为 true
	// 表示它已偏离创建计划时的基线，旧补跑不得执行。
	CurrentDigest   string    `json:"current_digest,omitempty"`
	BaselineChanged bool      `json:"baseline_changed"`
	ClaimedBy       string    `json:"claimed_by,omitempty"`
	LeaseUntil      time.Time `json:"lease_until,omitempty"`
	Epoch           int64     `json:"epoch"`
	FinishedAt      time.Time `json:"finished_at,omitempty"`
	// BlockedReason 给出该项当前无法继续的原因（屏障/租约/基线/计划作废）。
	BlockedReason string `json:"blocked_reason,omitempty"`
}

// CatchupView 是补跑计划与进度视图。
type CatchupView struct {
	Attempt   int64        `json:"attempt"`
	State     CatchupState `json:"state"`
	CreatedAt time.Time    `json:"created_at"`
	Succeeded int          `json:"succeeded"`
	Failed    int          `json:"failed"`
	Total     int          `json:"total"`
	// AbortReason 是计划无法继续的整体原因（release_rolled_back /
	// release_superseded / plan_replaced）。
	AbortReason string             `json:"abort_reason,omitempty"`
	AbortedAt   time.Time          `json:"aborted_at,omitempty"`
	CompletedAt time.Time          `json:"completed_at,omitempty"`
	Entries     []CatchupEntryView `json:"entries"`
}

// GetCatchup 返回补跑计划视图；查询时顺带惰性结算（回滚触发时及时作废
// 旧计划），但不改变可领取结果。
func (s *Service) GetCatchup(releaseID string) (*CatchupView, error) {
	var out *CatchupView
	err := s.store.mutate(func(st *State) error {
		r, ok := st.Releases[releaseID]
		if !ok {
			return ErrReleaseNotFound
		}
		s.evaluateLocked(st, r)
		maybeAbortCatchupForStateLocked(st, r, s.clock.Now())
		if r.Catchup == nil {
			return ErrNoCatchup
		}
		out = catchupViewOf(st, r, s.clock.Now())
		return nil
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

func catchupViewOf(st *State, r *Release, now time.Time) *CatchupView {
	plan := r.Catchup
	v := &CatchupView{
		Attempt: plan.Attempt, State: plan.State, CreatedAt: plan.CreatedAt,
		AbortReason: plan.AbortReason, AbortedAt: plan.AbortedAt, CompletedAt: plan.CompletedAt,
		Total: len(plan.Entries),
	}

	// 波次屏障预计算：每波是否全部成功、是否存在失败项。
	waveAllSucceeded := map[int]bool{}
	waveHasFailure := map[int]bool{}
	for _, w := range plan.Waves {
		all, failed := true, false
		for _, e := range plan.Entries {
			if e.Wave != w {
				continue
			}
			if e.State != CatchupSucceeded {
				all = false
			}
			if e.State == CatchupFailed {
				failed = true
			}
		}
		waveAllSucceeded[w], waveHasFailure[w] = all, failed
	}

	ids := make([]string, 0, len(plan.Entries))
	for id := range plan.Entries {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool {
		ei, ej := plan.Entries[ids[i]], plan.Entries[ids[j]]
		if ei.Wave != ej.Wave {
			return ei.Wave < ej.Wave
		}
		return ids[i] < ids[j]
	})
	for _, id := range ids {
		e := plan.Entries[id]
		ev := CatchupEntryView{
			NodeID: e.NodeID, Wave: e.Wave, State: e.State,
			StartDigest: e.StartDigest, TargetDigest: e.TargetDigest,
			ClaimedBy: e.ClaimedBy, LeaseUntil: e.LeaseUntil, Epoch: e.ClaimEpoch,
			FinishedAt: e.FinishedAt,
		}
		if ns := st.NodeStates[id]; ns != nil {
			ev.CurrentDigest = ns.AppliedDigest
			ev.BaselineChanged = ns.AppliedSeq != e.BaselineSeq
		} else if e.BaselineSeq != 0 {
			ev.BaselineChanged = true
		}
		switch {
		case plan.State == CatchupAborted:
			ev.BlockedReason = "plan_aborted:" + plan.AbortReason
		case e.State == CatchupFailed:
			ev.BlockedReason = "failed"
		case ev.BaselineChanged && (e.State == CatchupPending || e.State == CatchupClaimed):
			ev.BlockedReason = "baseline_changed"
		case e.State == CatchupClaimed && !e.LeaseUntil.After(now):
			ev.BlockedReason = "lease_expired_reclaimable"
		}
		// 波次屏障：前序波次未全部成功。
		if ev.BlockedReason == "" && (e.State == CatchupPending ||
			(e.State == CatchupClaimed && !e.LeaseUntil.After(now))) {
			for _, w := range plan.Waves {
				if w >= e.Wave {
					break
				}
				if !waveAllSucceeded[w] {
					ev.BlockedReason = "earlier_wave_unfinished"
					break
				}
			}
		}
		if e.State == CatchupSucceeded {
			v.Succeeded++
		}
		if e.State == CatchupFailed {
			v.Failed++
		}
		v.Entries = append(v.Entries, ev)
	}
	return v
}
