package configrollout

import (
	"sort"
)

// ---------- 节点隔离 ----------

// QuarantineInput 是操作员依据健康样本把节点标记为隔离的请求。
type QuarantineInput struct {
	ReleaseID string
	NodeID    string
	// Basis 是隔离依据：一组已落库健康样本的事件号，必须全部存在且
	// 属于该节点；空列表返回 ErrQuarantineBasisMissing。
	Basis []string
	// Reason 是操作员备注的隔离原因（可选，仅作证据留存）。
	Reason string
}

// QuarantineOutcome 描述一次隔离操作的结果。
type QuarantineOutcome struct {
	// Already 为 true 表示节点此前已被隔离，重复操作幂等返回。
	Already bool
	State   ReleaseState
	// RollbackStarted 表示本次隔离使累计隔离数/健康覆盖率越过冻结限制，
	// 在同一事务内触发了整体回滚。
	RollbackStarted bool
}

// QuarantineNode 在健康观察期间隔离一个已成功节点。
//
// 隔离决定绑定发布、节点、配置摘要（恒等于发布摘要）与当前波次，一旦
// 写出即不可变：之后迟到或重复到达的健康样本不能改写它，节点也不再
// 计入后续健康门槛、不能继续接收本次发布的新操作。隔离不能掩盖大面积
// 故障：累计隔离数超过 MaxQuarantined 或当前波健康覆盖率低于
// MinHealthyCoverage 时，在同一事务内触发现有的整体回滚。
func (s *Service) QuarantineNode(in QuarantineInput) (*QuarantineOutcome, error) {
	basis := dedupSorted(in.Basis)
	if len(basis) == 0 {
		return nil, ErrQuarantineBasisMissing
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
		if !r.QuarantineEnabled {
			return ErrQuarantineNotEnabled
		}

		// 惰性评估：若超时/健康阈值已应触发暂停或回滚，先结算，状态只向前。
		s.evaluateLocked(st, r)

		// 已隔离：幂等返回，绝不改写既有决定。
		if _, q := r.Quarantined[in.NodeID]; q {
			out = QuarantineOutcome{Already: true, State: r.State}
			return nil
		}

		switch r.State {
		case StateActive, StatePaused:
			// 允许隔离；Paused 期间隔离证据照常落库。
		case StateRollingBack:
			return ErrRollbackInProgress
		default:
			return ErrTerminal
		}
		// 只能隔离当前观察波中已成功应用配置的节点：未来波次尚未应用，
		// 旧波次节点已随波次推进离开观察期。
		if lease.Wave != r.CurrentWave || lease.Result != ResultSucceeded {
			return ErrQuarantineNodeNotSucceeded
		}
		// 依据校验：每条样本事件号必须存在且属于该节点（样本天然属于
		// 本发布与发布摘要，无需重复校验）。
		for _, id := range basis {
			smp, ok := r.Samples[id]
			if !ok || smp.NodeID != in.NodeID {
				return ErrQuarantineBasisUnknown
			}
		}

		now := s.clock.Now()
		r.Quarantined[in.NodeID] = &QuarantineRecord{
			NodeID:        in.NodeID,
			Wave:          lease.Wave,
			Digest:        r.Digest,
			Basis:         append([]string(nil), basis...),
			Reason:        in.Reason,
			QuarantinedAt: now,
		}
		r.Version++
		ev := st.emit(EventNodeQuarantined, now)
		ev.ReleaseID, ev.Wave, ev.NodeID, ev.Digest = r.ID, lease.Wave, in.NodeID, r.Digest

		stateBefore := r.State
		// Active 时立即结算隔离限制；Paused 期间只记录，恢复时统一结算
		// （与健康样本的暂停语义一致）。
		if r.State == StateActive {
			s.evaluateHealthLocked(st, r)
		}
		out = QuarantineOutcome{
			State:           r.State,
			RollbackStarted: stateBefore != StateRollingBack && r.State == StateRollingBack,
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return &out, nil
}

// dedupSorted 返回去重并排序后的副本，输出确定，便于视图与测试。
func dedupSorted(in []string) []string {
	if len(in) == 0 {
		return nil
	}
	set := make(map[string]struct{}, len(in))
	out := make([]string, 0, len(in))
	for _, s := range in {
		if s == "" {
			continue
		}
		if _, dup := set[s]; dup {
			continue
		}
		set[s] = struct{}{}
		out = append(out, s)
	}
	sort.Strings(out)
	return out
}

// ---------- 隔离查询 ----------

// QuarantineView 是隔离策略与隔离决定的查询视图。
type QuarantineView struct {
	Enabled bool             `json:"enabled"`
	Policy  QuarantinePolicy `json:"policy"`

	// TotalQuarantined 是累计隔离节点数（跨波次，单调只增）。
	TotalQuarantined int `json:"total_quarantined"`
	// CurrentWave 覆盖率计算所用的快照。
	WaveSucceeded int `json:"wave_succeeded"`
	WaveCovered   int `json:"wave_covered"`
	// Coverage 为未隔离的已成功节点 / 当前波全部已成功节点；
	// 尚无成功节点时为 1。
	Coverage float64 `json:"coverage"`
	// LimitBreached 表示按冻结限制此刻应当整体回滚（实际回滚状态见
	// HealthView.State；暂停期可能尚未结算）。
	LimitBreached bool `json:"limit_breached"`

	Records []QuarantineRecord `json:"records"`
}

func quarantineViewOf(r *Release) *QuarantineView {
	v := &QuarantineView{
		Enabled:          r.QuarantineEnabled,
		Policy:           r.Quarantine,
		TotalQuarantined: len(r.Quarantined),
		Coverage:         1,
	}
	waveSucceeded, covered := 0, 0
	for _, n := range r.Waves[r.CurrentWave] {
		if r.Nodes[n].Result != ResultSucceeded {
			continue
		}
		waveSucceeded++
		if _, q := r.Quarantined[n]; q {
			continue
		}
		covered++
	}
	v.WaveSucceeded, v.WaveCovered = waveSucceeded, covered
	if waveSucceeded > 0 {
		v.Coverage = float64(covered) / float64(waveSucceeded)
	}
	if r.QuarantineEnabled {
		v.LimitBreached = len(r.Quarantined) > r.Quarantine.MaxQuarantined ||
			(waveSucceeded > 0 && v.Coverage < r.Quarantine.MinHealthyCoverage)
	}

	keys := make([]string, 0, len(r.Quarantined))
	for k := range r.Quarantined {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool {
		qi, qj := r.Quarantined[keys[i]], r.Quarantined[keys[j]]
		if qi.Wave != qj.Wave {
			return qi.Wave < qj.Wave
		}
		if !qi.QuarantinedAt.Equal(qj.QuarantinedAt) {
			return qi.QuarantinedAt.Before(qj.QuarantinedAt)
		}
		return keys[i] < keys[j]
	})
	for _, k := range keys {
		q := r.Quarantined[k]
		v.Records = append(v.Records, QuarantineRecord{
			NodeID:        q.NodeID,
			Wave:          q.Wave,
			Digest:        q.Digest,
			Basis:         append([]string(nil), q.Basis...),
			Reason:        q.Reason,
			QuarantinedAt: q.QuarantinedAt,
		})
	}
	return v
}
