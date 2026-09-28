package configrollout

import (
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"
)

// ---------- 辅助 ----------

func testQuarantinePolicy() HealthPolicy {
	return HealthPolicy{
		ObserveWindow: 30 * time.Minute,
		MinSamples:    1,
		MaxUnhealthy:  1,
		Quarantine: QuarantinePolicy{
			MaxNodes:           1,
			MinHealthyCoverage: 0.5,
		},
	}
}

func mustQuarantine(t *testing.T, s *Service, releaseID, nodeID string, evidence []string) *QuarantineOutcome {
	t.Helper()
	o, err := s.QuarantineNode(QuarantineInput{
		ReleaseID: releaseID, NodeID: nodeID,
		EvidenceEventIDs: evidence, Reason: QuarantineReasonManual,
	})
	if err != nil {
		t.Fatalf("quarantine %s: %v", nodeID, err)
	}
	return o
}

// quarantineSetup 建立 d1 发布到 [a,b]（第 0 波），两节点均成功，
// 并各上报一条健康样本，返回启用隔离策略的服务。
func quarantineSetup(t *testing.T, hp HealthPolicy) (*Service, *offsetClock, string, string) {
	t.Helper()
	s, clk := newTestService(t)
	d := mustRegister(t, s, "v1")
	mustCreateWithHealth(t, s, "rel", d, []string{"a", "b"}, 2, testPolicy(), hp)
	mustAck(t, s, "rel", "a", d, true)
	mustAck(t, s, "rel", "b", d, true)
	mustReport(t, s, clk, "e-a1", "rel", "a", d, true)
	mustReport(t, s, clk, "e-b1", "rel", "b", d, true)
	return s, clk, "rel", d
}

// ---------- 隔离策略冻结与校验 ----------

func TestQuarantinePolicyFrozenAndValidated(t *testing.T) {
	s, _ := newTestService(t)
	d := mustRegister(t, s, "v1")
	base := HealthPolicy{ObserveWindow: time.Minute, MinSamples: 1, MaxUnhealthy: 0}

	// 隔离闸门非法：必须与健康观察一并启用，MaxNodes>=1，覆盖率在 (0,1]。
	for _, q := range []QuarantinePolicy{
		{MaxNodes: 0, MinHealthyCoverage: 0.5},
		{MaxNodes: 1, MinHealthyCoverage: 0},
		{MaxNodes: 1, MinHealthyCoverage: 1.1},
		{MaxNodes: -1, MinHealthyCoverage: 0.5},
	} {
		hp := base
		hp.Quarantine = q
		_, err := s.CreateRelease(CreateReleaseInput{
			ID: fmt.Sprintf("bad-%v", q), Digest: d, Targets: []string{"n"},
			WaveSize: 1, Policy: testPolicy(), Health: hp,
		})
		if !errors.Is(err, ErrInvalidPolicy) {
			t.Fatalf("quarantine policy %+v: %v", q, err)
		}
	}

	// 只给隔离闸门、不给健康观察策略：同样非法。
	_, err := s.CreateRelease(CreateReleaseInput{
		ID: "no-health", Digest: d, Targets: []string{"n"}, WaveSize: 1,
		Policy: testPolicy(),
		Health: HealthPolicy{Quarantine: QuarantinePolicy{MaxNodes: 1, MinHealthyCoverage: 0.5}},
	})
	if !errors.Is(err, ErrInvalidPolicy) {
		t.Fatalf("quarantine without health policy: %v", err)
	}

	hp := testQuarantinePolicy()
	r := mustCreateWithHealth(t, s, "rel", d, []string{"a", "b"}, 2, testPolicy(), hp)
	if r.Health.Quarantine != hp.Quarantine {
		t.Fatalf("quarantine policy not frozen: %+v", r.Health.Quarantine)
	}

	// 未配置隔离闸门的发布不允许隔离。
	plain := mustCreateWithHealth(t, s, "rel-plain", d, []string{"z"}, 1, testPolicy(), testHealthPolicy())
	_, err = s.QuarantineNode(QuarantineInput{ReleaseID: plain.ID, NodeID: "z", EvidenceEventIDs: []string{"x"}})
	if !errors.Is(err, ErrQuarantineNotEnabled) {
		t.Fatalf("quarantine without policy: %v", err)
	}
}

// ---------- 隔离决定：绑定、依据冻结、拒绝新操作 ----------

func TestQuarantineNodeFreezesDecision(t *testing.T) {
	s, clk, rel, d := quarantineSetup(t, testQuarantinePolicy())

	// 依据校验：空依据 / 不存在事件号 / 属于别的节点，全部拒绝。
	if _, err := s.QuarantineNode(QuarantineInput{ReleaseID: rel, NodeID: "a"}); !errors.Is(err, ErrQuarantineNoEvidence) {
		t.Fatalf("no evidence: %v", err)
	}
	if _, err := s.QuarantineNode(QuarantineInput{ReleaseID: rel, NodeID: "a", EvidenceEventIDs: []string{"nope"}}); !errors.Is(err, ErrQuarantineEvidenceMismatch) {
		t.Fatalf("unknown evidence: %v", err)
	}
	if _, err := s.QuarantineNode(QuarantineInput{ReleaseID: rel, NodeID: "a", EvidenceEventIDs: []string{"e-b1"}}); !errors.Is(err, ErrQuarantineEvidenceMismatch) {
		t.Fatalf("other node evidence: %v", err)
	}
	if _, err := s.QuarantineNode(QuarantineInput{ReleaseID: "rel-x", NodeID: "a", EvidenceEventIDs: []string{"e-a1"}}); !errors.Is(err, ErrReleaseNotFound) {
		t.Fatalf("unknown release: %v", err)
	}
	if _, err := s.QuarantineNode(QuarantineInput{ReleaseID: rel, NodeID: "zzz", EvidenceEventIDs: []string{"e-a1"}}); !errors.Is(err, ErrNodeNotInRelease) {
		t.Fatalf("unknown node: %v", err)
	}

	// 正式隔离 a（依据可含重复事件号，去重后冻结）。
	o := mustQuarantine(t, s, rel, "a", []string{"e-a1", "e-a1"})
	if o.AlreadyQuarantined || o.State != StateActive {
		t.Fatalf("outcome = %+v", o)
	}

	hv, _ := s.GetHealth(rel)
	if hv.QuarantinedNodes != 1 || !hv.QuarantineEnabled {
		t.Fatalf("health view = %+v", hv)
	}
	var na NodeHealthView
	for _, n := range hv.Nodes {
		if n.NodeID == "a" {
			na = n
		}
	}
	if !na.Quarantined || na.Health != HealthQuarantined || na.Quarantine == nil {
		t.Fatalf("node a view = %+v", na)
	}
	if q := na.Quarantine; q.Wave != 0 || q.Digest != d || len(q.EvidenceEventIDs) != 1 ||
		q.EvidenceEventIDs[0] != "e-a1" || q.Samples != 1 {
		t.Fatalf("quarantine view = %+v", q)
	}
	if na.CurrentDigest != d {
		t.Fatalf("actual digest = %s", na.CurrentDigest)
	}

	// 被隔离节点不能再取得本次发布的配置。
	if _, err := s.GetConfigForNode(rel, "a"); !errors.Is(err, ErrConfigNotAvailable) {
		t.Fatalf("quarantined node got config: %v", err)
	}

	// 重复隔离幂等：已冻结的依据不被改写（即使传入不同事件号）。
	o2 := mustQuarantine(t, s, rel, "a", []string{"e-b1"})
	if !o2.AlreadyQuarantined {
		t.Fatalf("re-quarantine should be idempotent: %+v", o2)
	}
	hv, _ = s.GetHealth(rel)
	for _, n := range hv.Nodes {
		if n.NodeID == "a" && (n.Quarantine == nil || len(n.Quarantine.EvidenceEventIDs) != 1) {
			t.Fatalf("decision rewritten: %+v", n.Quarantine)
		}
	}

	// 迟到样本（含失败样本）只追加证据，不改写隔离决定，也不因其被隔离
	// 而触发回滚（不计入健康门槛）。
	mustReport(t, s, clk, "e-a-late1", rel, "a", d, false)
	mustReport(t, s, clk, "e-a-late2", rel, "a", d, true)
	hv, _ = s.GetHealth(rel)
	if hv.State != StateActive || hv.SamplesRecorded != 4 || hv.UnhealthyNodes != 0 {
		t.Fatalf("late samples changed decision: state=%s samples=%d unhealthy=%d", hv.State, hv.SamplesRecorded, hv.UnhealthyNodes)
	}
	for _, n := range hv.Nodes {
		if n.NodeID == "a" {
			if n.Quarantine.Samples != 1 { // 隔离时刻快照仍为 1
				t.Fatalf("frozen evidence snapshot changed: %+v", n.Quarantine)
			}
		}
	}
}

// TestQuarantineExcludedFromGates 被隔离节点不计入样本充足性：同伴健康
// 且窗口届满即可推进；末波隔离不阻止发布完成。
func TestQuarantineExcludedFromGates(t *testing.T) {
	s, clk, rel, d := quarantineSetup(t, testQuarantinePolicy())
	mustQuarantine(t, s, rel, "a", []string{"e-a1"})

	// 即使 a 从此不上报更多样本，b 样本达标且窗口届满即推进/完成。
	clk.advance(31 * time.Minute)
	p, err := s.GetProgress(rel)
	if err != nil {
		t.Fatal(err)
	}
	// 两节点的末波：a 隔离、b 健康，观察通过后发布完成。
	if p.State != StateCompleted {
		t.Fatalf("state = %s, want completed", p.State)
	}

	// 健康覆盖率视图：已隔离节点不计入分母，b 健康 -> 100%。
	hv, _ := s.GetHealth(rel)
	if hv.CoverageNumerator != 1 || hv.CoverageDenominator != 1 || hv.HealthyCoverage != 1.0 {
		t.Fatalf("coverage = %d/%d (%f)", hv.CoverageNumerator, hv.CoverageDenominator, hv.HealthyCoverage)
	}

	// 发布完成事件恰好一条；被隔离节点没有额外终态通知。
	evs, _ := s.PendingEvents()
	if n := countEvents(evs, EventReleaseCompleted); n != 1 {
		t.Fatalf("completed events = %d", n)
	}
	_ = d
}

// TestQuarantineStateGuards 隔离只在当前波次观察期、针对已成功节点。
func TestQuarantineStateGuards(t *testing.T) {
	s, clk, rel, d := quarantineSetup(t, testQuarantinePolicy())

	// 暂停期间不可隔离。
	if err := s.Pause(rel); err != nil {
		t.Fatal(err)
	}
	if _, err := s.QuarantineNode(QuarantineInput{ReleaseID: rel, NodeID: "a", EvidenceEventIDs: []string{"e-a1"}}); !errors.Is(err, ErrNotActive) {
		t.Fatalf("quarantine while paused: %v", err)
	}
	if err := s.Resume(rel); err != nil {
		t.Fatal(err)
	}

	// 推进过波次后，旧波次节点不能隔离；不存在“未来波节点已成功”情形，
	// 这里用失败节点验证 Result 守卫：新建发布让 c 失败。
	other := mustCreateWithHealth(t, s, "rel2", d, []string{"c"}, 1,
		WavePolicy{MinSuccessRatio: 1, MaxFailures: 5, WaveTimeout: time.Hour}, testQuarantinePolicy())
	mustAck(t, s, other.ID, "c", d, false)
	if _, err := s.QuarantineNode(QuarantineInput{ReleaseID: other.ID, NodeID: "c", EvidenceEventIDs: []string{"e-a1"}}); !errors.Is(err, ErrWaveNotOpen) {
		t.Fatalf("quarantine failed/non-succeeded node: %v", err)
	}
	_ = clk
}

// ---------- 隔离闸门：不能掩盖大面积故障 ----------

// TestQuarantineMaxNodesTriggersRollback 已隔离数超过冻结的 MaxNodes
// 立即触发现有整体回滚，被隔离节点同样进入回滚计划恢复到发布前摘要。
func TestQuarantineMaxNodesTriggersRollback(t *testing.T) {
	s, clk := newTestService(t)
	d0 := mustRegister(t, s, "v0")
	d1 := mustRegister(t, s, "v1")
	mustCreate(t, s, "rel0", d0, []string{"a", "b"}, 2, testPolicy())
	mustAck(t, s, "rel0", "a", d0, true)
	mustAck(t, s, "rel0", "b", d0, true)

	hp := testQuarantinePolicy() // MaxNodes=1
	hp.MaxUnhealthy = 5
	mustCreateWithHealth(t, s, "rel", d1, []string{"a", "b", "c"}, 1, testPolicy(), hp)
	mustAck(t, s, "rel", "a", d1, true)
	mustReport(t, s, clk, "ea", "rel", "a", d1, true)

	// 隔离 a：第 0 波（单节点波）仅剩 a，隔离数=1，尚未超 MaxNodes。
	mustQuarantine(t, s, "rel", "a", []string{"ea"})

	// 窗口届满，第 1 波开放后 b 成功并隔离 -> 隔离数 2 > MaxNodes 1 -> 整体回滚。
	clk.advance(31 * time.Minute)
	p, _ := s.GetProgress("rel") // 惰性结算推进到第 1 波
	if p.CurrentWave != 1 {
		t.Fatalf("wave = %d, want 1", p.CurrentWave)
	}
	mustAck(t, s, "rel", "b", d1, true)
	mustReport(t, s, clk, "eb", "rel", "b", d1, true)
	o := mustQuarantine(t, s, "rel", "b", []string{"eb"})
	if !o.RollbackStarted || o.State != StateRollingBack {
		t.Fatalf("outcome = %+v", o)
	}

	// 回滚计划覆盖所有已成功节点（含被隔离的 a、b），恢复到 d0。
	hv, _ := s.GetHealth("rel")
	if hv.Rollback == nil || hv.Rollback.Total != 2 {
		t.Fatalf("rollback = %+v", hv.Rollback)
	}
	for _, e := range hv.Rollback.Entries {
		if e.ToDigest != d0 || e.FromDigest != d1 {
			t.Fatalf("entry = %+v", e)
		}
	}
	// 回滚后不能再隔离。
	if _, err := s.QuarantineNode(QuarantineInput{ReleaseID: "rel", NodeID: "c", EvidenceEventIDs: []string{"ea"}}); !errors.Is(err, ErrRollbackInProgress) {
		t.Fatalf("quarantine during rollback: %v", err)
	}
}

// TestQuarantineCoverageTriggersRollback 把健康节点隔离、留下不健康同伴
// 导致健康覆盖率低于冻结下限时，仍触发现有整体回滚。
func TestQuarantineCoverageTriggersRollback(t *testing.T) {
	s, clk, rel, d := quarantineSetup(t, testQuarantinePolicy()) // 覆盖率下限 0.5

	// b 出现失败样本：MaxUnhealthy=1，尚不因绝对数量回滚。
	mustReport(t, s, clk, "e-b-bad", rel, "b", d, false)
	p, _ := s.GetProgress(rel)
	if p.State != StateActive {
		t.Fatalf("state before quarantine = %s", p.State)
	}

	// 隔离健康节点 a（依据 a 自己的样本）：未隔离且已有结论的节点只剩
	// 不健康的 b，覆盖率 0 < 0.5 -> 整体回滚。
	o := mustQuarantine(t, s, rel, "a", []string{"e-a1"})
	if !o.RollbackStarted || o.State != StateRollingBack {
		t.Fatalf("outcome = %+v", o)
	}
	hv, _ := s.GetHealth(rel)
	if hv.CoverageNumerator != 0 || hv.CoverageDenominator != 1 {
		t.Fatalf("coverage = %d/%d", hv.CoverageNumerator, hv.CoverageDenominator)
	}
	evs, _ := s.PendingEvents()
	if n := countEvents(evs, EventRollbackStarted); n != 1 {
		t.Fatalf("rollback started = %d", n)
	}
}

// TestRollbackDecisionNotRewrittenByLateSamples 回滚决定形成后，迟到的
// 健康样本不能撤销回滚、不改写隔离决定。
func TestRollbackDecisionNotRewrittenByLateSamples(t *testing.T) {
	s, clk, rel, d := quarantineSetup(t, testQuarantinePolicy())
	// b 两条失败样本：unhealthy=1，不超过 MaxUnhealthy=1；隔离 a 后
	// 覆盖率 0 < 0.5 触发回滚。
	mustReport(t, s, clk, "e-b-bad", rel, "b", d, false)
	mustQuarantine(t, s, rel, "a", []string{"e-a1"})
	if p, _ := s.GetProgress(rel); p.State != StateRollingBack {
		t.Fatalf("state = %s", p.State)
	}

	// 迟到的健康样本（即使让 b 看起来恢复）不改变任何已形成的决定。
	if _, err := s.ReportHealth(HealthSample{EventID: "e-late", ReleaseID: rel, NodeID: "b", Digest: d, Healthy: true, SampledAt: clk.Now()}); !errors.Is(err, ErrRollbackInProgress) {
		t.Fatalf("sample during rollback: %v", err)
	}
	p, _ := s.GetProgress(rel)
	if p.State != StateRollingBack {
		t.Fatalf("rollback rewritten to %s", p.State)
	}
}

// ---------- 补跑：创建 / 领取 / 上报 ----------

// repairNodeBaseline 模拟运维把被隔离节点修复回指定基线（带外操作）。
func repairNodeBaseline(t *testing.T, s *Service, node, digest string, releaseSeq int64) {
	t.Helper()
	if err := s.store.mutate(func(st *State) error {
		ns := st.NodeStates[node]
		if ns == nil {
			ns = &NodeState{NodeID: node}
			st.NodeStates[node] = ns
		}
		ns.AppliedDigest = digest
		ns.AppliedReleaseSeq = releaseSeq
		st.AppliedSeq++
		ns.AppliedSeq = st.AppliedSeq
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func TestCatchupHappyPath(t *testing.T) {
	s, clk, rel, d := quarantineSetup(t, testQuarantinePolicy())
	mustQuarantine(t, s, rel, "a", []string{"e-a1"})
	clk.advance(31 * time.Minute)
	if p, _ := s.GetProgress(rel); p.State != StateCompleted {
		t.Fatalf("state = %s", p.State)
	}

	// 运维修复 a：回退到发布前基线（空串，表示此前无版本）。
	repairNodeBaseline(t, s, "a", "", 0)

	plan, err := s.CreateCatchupPlan(rel, "a")
	if err != nil {
		t.Fatalf("create catchup: %v", err)
	}
	if plan.State != CatchupPending || plan.FromDigest != "" || plan.TargetDigest != d ||
		plan.OriginalWave != 0 || len(plan.Path) != 2 || plan.Path[0] != "" || plan.Path[1] != d {
		t.Fatalf("plan = %+v", plan)
	}
	// 重复创建未作废计划被拒。
	if _, err := s.CreateCatchupPlan(rel, "a"); !errors.Is(err, ErrCatchupExists) {
		t.Fatalf("duplicate plan: %v", err)
	}
	// 未隔离节点不能创建补跑。
	if _, err := s.CreateCatchupPlan(rel, "b"); !errors.Is(err, ErrNodeNotQuarantined) {
		t.Fatalf("catchup for non-quarantined: %v", err)
	}

	// 未领取不能上报；领取拿到 fencing token。
	if _, err := s.ReportCatchup(CatchupReport{PlanID: plan.ID, NodeID: "a", Token: 1, Success: true, Digest: d}); !errors.Is(err, ErrCatchupNotClaimable) {
		t.Fatalf("report before claim: %v", err)
	}
	token, claimed, err := s.ClaimCatchup(plan.ID, "worker-1")
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	if token != 1 || claimed.State != CatchupRunning || claimed.ClaimedBy != "worker-1" {
		t.Fatalf("claim = token %d %+v", token, claimed)
	}
	// 运行中不可重复领取。
	if _, _, err := s.ClaimCatchup(plan.ID, "worker-2"); !errors.Is(err, ErrCatchupNotClaimable) {
		t.Fatalf("re-claim while running: %v", err)
	}
	// 只有持有效 token 才能取得补跑配置。
	if _, err := s.GetCatchupConfig(plan.ID, token+1); !errors.Is(err, ErrCatchupFencing) {
		t.Fatalf("stale token got config: %v", err)
	}
	cfg, err := s.GetCatchupConfig(plan.ID, token)
	if err != nil || cfg.Digest != d {
		t.Fatalf("catchup config = %+v %v", cfg, err)
	}
	// 错误摘要的成功上报被拒。
	if _, err := s.ReportCatchup(CatchupReport{plan.ID, "a", token, true, "deadbeef"}); !errors.Is(err, ErrDigestMismatch) {
		t.Fatalf("wrong digest success: %v", err)
	}
	// 节点不匹配被拒。
	if _, err := s.ReportCatchup(CatchupReport{plan.ID, "b", token, true, d}); !errors.Is(err, ErrNodeNotInRelease) {
		t.Fatalf("wrong node: %v", err)
	}

	// 成功：节点版本单调推进到目标；补跑不写发布终态通知。
	completedBefore := countEventsOf(t, s, EventReleaseCompleted)
	ro, err := s.ReportCatchup(CatchupReport{plan.ID, "a", token, true, d})
	if err != nil || ro.State != CatchupSucceeded {
		t.Fatalf("report success: %+v %v", ro, err)
	}
	ns, _ := s.NodeAppliedVersion("a")
	if ns.AppliedDigest != d {
		t.Fatalf("node digest = %s", ns.AppliedDigest)
	}
	if n := countEventsOf(t, s, EventReleaseCompleted); n != completedBefore {
		t.Fatalf("catchup emitted a release terminal event: %d -> %d", completedBefore, n)
	}
	// 重复成功幂等；终态计划不可再领取。
	ro2, err := s.ReportCatchup(CatchupReport{plan.ID, "a", token, true, d})
	if err != nil || !ro2.Ignored {
		t.Fatalf("replay success: %+v %v", ro2, err)
	}
	if _, _, err := s.ClaimCatchup(plan.ID, "w"); !errors.Is(err, ErrCatchupNotClaimable) {
		t.Fatalf("claim succeeded plan: %v", err)
	}

	// 视图：补跑成功、节点实际配置为目标摘要、无阻塞原因。
	hv, _ := s.GetHealth(rel)
	if len(hv.Catchups) != 1 {
		t.Fatalf("catchups = %+v", hv.Catchups)
	}
	cv := hv.Catchups[0]
	if cv.State != CatchupSucceeded || cv.NodeCurrentDigest != d || cv.BlockedReason != "" {
		t.Fatalf("catchup view = %+v", cv)
	}
}

func countEventsOf(t *testing.T, s *Service, typ string) int {
	t.Helper()
	evs, err := s.PendingEvents()
	if err != nil {
		t.Fatal(err)
	}
	return countEvents(evs, typ)
}

// TestCatchupFencingFailAndReclaim 失败可重试；每次领取换发新 token，
// 旧领取者的迟到成功被 fencing 拒绝，状态只能向前。
func TestCatchupFencingFailAndReclaim(t *testing.T) {
	s, clk, rel, d := quarantineSetup(t, testQuarantinePolicy())
	mustQuarantine(t, s, rel, "a", []string{"e-a1"})
	clk.advance(31 * time.Minute)
	repairNodeBaseline(t, s, "a", "", 0)
	plan, _ := s.CreateCatchupPlan(rel, "a")

	t1, _, err := s.ClaimCatchup(plan.ID, "w1")
	if err != nil || t1 != 1 {
		t.Fatalf("claim 1: token=%d err=%v", t1, err)
	}
	// w1 上报失败 -> 计划退回 failed，可被重新领取。
	ro, err := s.ReportCatchup(CatchupReport{plan.ID, "a", t1, false, ""})
	if err != nil || ro.State != CatchupFailed {
		t.Fatalf("failure report: %+v %v", ro, err)
	}
	t2, _, err := s.ClaimCatchup(plan.ID, "w2")
	if err != nil || t2 != 2 {
		t.Fatalf("claim 2: token=%d err=%v", t2, err)
	}
	// w1 的迟到成功：token 过期，拒绝，且不改变计划状态。
	if _, err := s.ReportCatchup(CatchupReport{plan.ID, "a", t1, true, d}); !errors.Is(err, ErrCatchupFencing) {
		t.Fatalf("stale claimant success: %v", err)
	}
	got, _ := s.GetCatchupPlan(plan.ID)
	if got.State != CatchupRunning || got.Fencing != 2 {
		t.Fatalf("plan after stale report = %+v", got)
	}
	// w2 正常成功。
	if _, err := s.ReportCatchup(CatchupReport{plan.ID, "a", t2, true, d}); err != nil {
		t.Fatalf("current claimant success: %v", err)
	}
	got, _ = s.GetCatchupPlan(plan.ID)
	if got.State != CatchupSucceeded {
		t.Fatalf("state = %s", got.State)
	}
	if n := countEventsOf(t, s, EventCatchupSucceeded); n != 1 {
		t.Fatalf("catchup succeeded events = %d, want 1", n)
	}
}

// ---------- 补跑失效条件 ----------

// TestCatchupAbortOnRollback 领取后发布整体回滚：同事务作废补跑，
// 旧领取者的迟到成功不能落地。
func TestCatchupAbortOnRollback(t *testing.T) {
	s, clk := newTestService(t)
	d := mustRegister(t, s, "v1")
	hp := HealthPolicy{
		ObserveWindow: time.Hour, MinSamples: 1, MaxUnhealthy: 0,
		Quarantine: QuarantinePolicy{MaxNodes: 2, MinHealthyCoverage: 0.01},
	}
	mustCreateWithHealth(t, s, "rel", d, []string{"a", "c"}, 1,
		WavePolicy{MinSuccessRatio: 1, MaxFailures: 5, WaveTimeout: 24 * time.Hour}, hp)
	mustAck(t, s, "rel", "a", d, true)
	mustReport(t, s, clk, "ea", "rel", "a", d, true)
	mustQuarantine(t, s, "rel", "a", []string{"ea"})

	plan, _ := s.CreateCatchupPlan("rel", "a")
	token, _, err := s.ClaimCatchup(plan.ID, "w")
	if err != nil {
		t.Fatal(err)
	}

	// 窗口届满后第 1 波开放，c 成功后上报失败样本 -> MaxUnhealthy=0
	// -> 整体回滚，同一事务把 a 的补跑作废。
	clk.advance(61 * time.Minute)
	if p, _ := s.GetProgress("rel"); p.CurrentWave != 1 {
		t.Fatalf("wave = %d, want 1", p.CurrentWave)
	}
	mustAck(t, s, "rel", "c", d, true)
	o := mustReport(t, s, clk, "ec-bad", "rel", "c", d, false)
	if !o.RollbackStarted {
		t.Fatalf("rollback not started: %+v", o)
	}
	got, _ := s.GetCatchupPlan(plan.ID)
	if got.State != CatchupAborted || got.AbortReason != CatchupAbortReleaseRolledBack {
		t.Fatalf("plan = %+v", got)
	}
	// 旧领取者的迟到成功：计划已作废，拒绝且不写版本。
	if _, err := s.ReportCatchup(CatchupReport{plan.ID, "a", token, true, d}); !errors.Is(err, ErrCatchupAborted) {
		t.Fatalf("late success after rollback: %v", err)
	}
	if _, err := s.GetCatchupConfig(plan.ID, token); !errors.Is(err, ErrCatchupAborted) {
		t.Fatalf("config after rollback: %v", err)
	}
	if n := countEventsOf(t, s, EventCatchupSucceeded); n != 0 {
		t.Fatalf("catchup succeeded after rollback: %d", n)
	}
}

// TestCatchupAbortOnCancel 取消发布在同一事务作废补跑。
func TestCatchupAbortOnCancel(t *testing.T) {
	s, _, rel, _ := quarantineSetup(t, testQuarantinePolicy())
	mustQuarantine(t, s, rel, "a", []string{"e-a1"})
	plan, _ := s.CreateCatchupPlan(rel, "a")
	if _, _, err := s.ClaimCatchup(plan.ID, "w"); err != nil {
		t.Fatal(err)
	}
	if err := s.Cancel(rel); err != nil {
		t.Fatal(err)
	}
	got, _ := s.GetCatchupPlan(plan.ID)
	if got.State != CatchupAborted || got.AbortReason != CatchupAbortReleaseCancelled {
		t.Fatalf("plan = %+v", got)
	}
}

// TestCatchupAbortOnSuperseded 新发布把同节点纳入目标时，旧补跑在新发布
// 创建事务内立即作废；领取时也再次拦截。
func TestCatchupAbortOnSuperseded(t *testing.T) {
	s, clk, rel, d1 := quarantineSetup(t, testQuarantinePolicy())
	mustQuarantine(t, s, rel, "a", []string{"e-a1"})
	plan, _ := s.CreateCatchupPlan(rel, "a")

	d2 := mustRegister(t, s, "v2")
	mustCreate(t, s, "rel2", d2, []string{"a"}, 1, testPolicy())

	got, _ := s.GetCatchupPlan(plan.ID)
	if got.State != CatchupAborted || got.AbortReason != CatchupAbortSuperseded {
		t.Fatalf("plan = %+v", got)
	}
	if _, _, err := s.ClaimCatchup(plan.ID, "w"); !errors.Is(err, ErrCatchupAborted) {
		t.Fatalf("claim superseded plan: %v", err)
	}
	// 节点仍在旧发布隔离记录中，但已被更新发布接管：旧发布上不允许
	// 再创建补跑（只能在新发布流程中处理该节点）。
	if _, err := s.CreateCatchupPlan(rel, "a"); !errors.Is(err, ErrCatchupAborted) {
		t.Fatalf("recreate on superseded release: %v", err)
	}
	_ = clk
	_ = d1
}

// TestCatchupAbortOnBaselineChanged 领取时节点基线既非起点也非目标：
// 计划作废，补跑不得在漂移后的基线上执行。
func TestCatchupAbortOnBaselineChanged(t *testing.T) {
	s, clk, rel, d := quarantineSetup(t, testQuarantinePolicy())
	dx := mustRegister(t, s, "vx")
	mustQuarantine(t, s, rel, "a", []string{"e-a1"})
	plan, _ := s.CreateCatchupPlan(rel, "a") // from = d

	// 带外漂移：节点当前配置变成无关摘要 dx，但没有更新的发布存在。
	repairNodeBaseline(t, s, "a", dx, plan.ReleaseSeq)
	if _, _, err := s.ClaimCatchup(plan.ID, "w"); !errors.Is(err, ErrCatchupAborted) {
		t.Fatalf("claim with drifted baseline: %v", err)
	}
	got, _ := s.GetCatchupPlan(plan.ID)
	if got.State != CatchupAborted || got.AbortReason != CatchupAbortBaselineChanged {
		t.Fatalf("plan = %+v", got)
	}
	hv, _ := s.GetHealth(rel)
	var cv *CatchupPlanView
	for i := range hv.Catchups {
		if hv.Catchups[i].PlanID == plan.ID {
			cv = &hv.Catchups[i]
		}
	}
	if cv == nil || cv.NodeCurrentDigest != dx || cv.BlockedReason == "" {
		t.Fatalf("catchup view = %+v", cv)
	}
	_ = clk
	_ = d
}

// ---------- 并发：状态只能向前 ----------

// TestCatchupClaimReportRace 并发领取/上报（含失败重试），最终补跑恰好
// 成功一次，fencing 与事件序列单调。
func TestCatchupClaimReportRace(t *testing.T) {
	s, clk, rel, d := quarantineSetup(t, testQuarantinePolicy())
	mustQuarantine(t, s, rel, "a", []string{"e-a1"})
	clk.advance(31 * time.Minute)
	repairNodeBaseline(t, s, "a", "", 0)
	plan, _ := s.CreateCatchupPlan(rel, "a")

	var wg sync.WaitGroup
	for i := 0; i < 30; i++ {
		wg.Add(2)
		go func(i int) {
			defer wg.Done()
			tok, _, err := s.ClaimCatchup(plan.ID, fmt.Sprintf("w%d", i))
			if err != nil {
				return
			}
			// 一半概率报失败（使计划可被下一个领取者接手），一半报成功。
			ok := i%2 == 0
			_, _ = s.ReportCatchup(CatchupReport{plan.ID, "a", tok, ok, d})
		}(i)
		go func(i int) {
			defer wg.Done()
			// 旧 token 的迟到上报永远不应成功覆盖。
			_, _ = s.ReportCatchup(CatchupReport{plan.ID, "a", int64(i), true, d})
		}(i)
	}
	wg.Wait()

	got, _ := s.GetCatchupPlan(plan.ID)
	switch got.State {
	case CatchupSucceeded:
	case CatchupFailed:
		tok, _, err := s.ClaimCatchup(plan.ID, "finisher")
		if err != nil {
			t.Fatalf("finisher claim: %v", err)
		}
		if _, err := s.ReportCatchup(CatchupReport{plan.ID, "a", tok, true, d}); err != nil {
			t.Fatalf("finisher: %v", err)
		}
	case CatchupRunning:
		// 最后一个领取者尚未上报：以其当前 token 报成功收尾。
		if _, err := s.ReportCatchup(CatchupReport{plan.ID, "a", got.Fencing, true, d}); err != nil {
			t.Fatalf("running finisher: %v", err)
		}
	default:
		t.Fatalf("unexpected plan state %s", got.State)
	}
	got, _ = s.GetCatchupPlan(plan.ID)
	if got.State != CatchupSucceeded {
		t.Fatalf("final state = %s", got.State)
	}
	if n := countEventsOf(t, s, EventCatchupSucceeded); n != 1 {
		t.Fatalf("catchup succeeded events = %d, want exactly 1", n)
	}
	ns, _ := s.NodeAppliedVersion("a")
	if ns.AppliedDigest != d {
		t.Fatalf("node digest = %s", ns.AppliedDigest)
	}
}

// TestQuarantineRollbackCancelRace 隔离、失败样本（可能回滚）、取消并发：
// 状态只向前推进，事件序列合法且回滚/取消互斥。
func TestQuarantineRollbackCancelRace(t *testing.T) {
	s, clk, rel, d := quarantineSetup(t, testQuarantinePolicy())

	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(3)
		go func(i int) {
			defer wg.Done()
			_, _ = s.QuarantineNode(QuarantineInput{
				ReleaseID: rel, NodeID: "a",
				EvidenceEventIDs: []string{"e-a1"},
			})
		}(i)
		go func(i int) {
			defer wg.Done()
			_, _ = s.ReportHealth(HealthSample{
				EventID: fmt.Sprintf("bad-%d", i), ReleaseID: rel, NodeID: "b",
				Digest: d, Healthy: false, SampledAt: clk.Now(),
			})
		}(i)
		go func() { defer wg.Done(); _ = s.Cancel(rel) }()
	}
	wg.Wait()

	p, _ := s.GetProgress(rel)
	evs, _ := s.PendingEvents()
	assertLegalLifecycle(t, evs)
	rollbacks := countEvents(evs, EventRollbackStarted)
	cancels := countEvents(evs, EventReleaseCancelled)
	if rollbacks > 1 || cancels > 1 || (rollbacks == 1 && cancels == 1) {
		t.Fatalf("rollbacks=%d cancels=%d state=%s", rollbacks, cancels, p.State)
	}
	if n := countEvents(evs, EventNodeQuarantined); n > 1 {
		t.Fatalf("quarantine events = %d", n)
	}
}

// TestCatchupAlreadyAtTarget 节点当前配置已等于目标摘要时，路径退化为
// 单元素，补跑仍可领取/上报（停在目标上不算基线漂移）。
func TestCatchupAlreadyAtTarget(t *testing.T) {
	s, clk, rel, d := quarantineSetup(t, testQuarantinePolicy())
	mustQuarantine(t, s, rel, "a", []string{"e-a1"})
	// 不做带外修复：a 当前仍是目标摘要 d。
	plan, err := s.CreateCatchupPlan(rel, "a")
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	if plan.FromDigest != d || len(plan.Path) != 1 || plan.Path[0] != d {
		t.Fatalf("plan = %+v", plan)
	}
	tok, _, err := s.ClaimCatchup(plan.ID, "w")
	if err != nil {
		t.Fatalf("claim at-target: %v", err)
	}
	if _, err := s.ReportCatchup(CatchupReport{plan.ID, "a", tok, true, d}); err != nil {
		t.Fatalf("report at-target: %v", err)
	}
	_ = clk
}

// TestCatchupCreationGuards 创建补跑与查询的各类拒绝条件。
func TestCatchupCreationGuards(t *testing.T) {
	s, _, rel, d := quarantineSetup(t, testQuarantinePolicy())
	mustQuarantine(t, s, rel, "a", []string{"e-a1"})

	if _, err := s.CreateCatchupPlan("nope", "a"); !errors.Is(err, ErrReleaseNotFound) {
		t.Fatalf("unknown release: %v", err)
	}
	if _, err := s.CreateCatchupPlan(rel, "zzz"); !errors.Is(err, ErrNodeNotInRelease) {
		t.Fatalf("unknown node: %v", err)
	}
	if _, _, err := s.ClaimCatchup("nope-plan", "w"); !errors.Is(err, ErrCatchupNotFound) {
		t.Fatalf("claim unknown plan: %v", err)
	}
	if _, err := s.GetCatchupPlan("nope-plan"); !errors.Is(err, ErrCatchupNotFound) {
		t.Fatalf("get unknown plan: %v", err)
	}
	if _, err := s.GetCatchupConfig("nope-plan", 1); !errors.Is(err, ErrCatchupNotFound) {
		t.Fatalf("config unknown plan: %v", err)
	}

	// 发布取消后不能创建补跑。
	if err := s.Cancel(rel); err != nil {
		t.Fatal(err)
	}
	if _, err := s.CreateCatchupPlan(rel, "a"); !errors.Is(err, ErrTerminal) {
		t.Fatalf("create catchup after cancel: %v", err)
	}
	_ = d
}

// ---------- 重启持久化 ----------

// TestQuarantineAndCatchupSurviveRestart 重启后隔离依据与补跑进度可恢复。
func TestQuarantineAndCatchupSurviveRestart(t *testing.T) {
	path := mustTempFile(t)
	clk := &offsetClock{t: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)}
	svc := NewService(mustFileStore(t, path), clk)
	d := mustRegister(t, svc, "v1")
	mustCreateWithHealth(t, svc, "rel", d, []string{"a", "b"}, 2, testPolicy(), testQuarantinePolicy())
	mustAck(t, svc, "rel", "a", d, true)
	mustAck(t, svc, "rel", "b", d, true)
	mustReport(t, svc, clk, "ea", "rel", "a", d, true)
	mustReport(t, svc, clk, "eb", "rel", "b", d, true)
	mustQuarantine(t, svc, "rel", "a", []string{"ea"})
	plan, _ := svc.CreateCatchupPlan("rel", "a")

	svc2 := NewService(mustFileStore(t, path), clk)
	hv, err := svc2.GetHealth("rel")
	if err != nil {
		t.Fatal(err)
	}
	if hv.QuarantinedNodes != 1 || len(hv.Catchups) != 1 {
		t.Fatalf("view after restart = %+v", hv)
	}
	var a NodeHealthView
	for _, n := range hv.Nodes {
		if n.NodeID == "a" {
			a = n
		}
	}
	if !a.Quarantined || len(a.Quarantine.EvidenceEventIDs) != 1 {
		t.Fatalf("quarantine lost after restart: %+v", a)
	}
	// 重启后继续领取/上报，fencing 从 1 重新起算（此前未领取过）。
	tok, _, err := svc2.ClaimCatchup(plan.ID, "w")
	if err != nil || tok != 1 {
		t.Fatalf("claim after restart: token=%d err=%v", tok, err)
	}
	if _, err := svc2.ReportCatchup(CatchupReport{plan.ID, "a", tok, true, d}); err != nil {
		t.Fatalf("report after restart: %v", err)
	}
	evs, _ := svc2.PendingEvents()
	if n := countEvents(evs, EventNodeQuarantined); n != 1 {
		t.Fatalf("quarantine events after restart = %d", n)
	}
	if n := countEvents(evs, EventCatchupSucceeded); n != 1 {
		t.Fatalf("catchup success events = %d", n)
	}
}
