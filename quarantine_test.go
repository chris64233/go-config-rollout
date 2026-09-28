package configrollout

import (
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"
)

// ---------- 测试辅助 ----------

func testQuarantinePolicy() QuarantinePolicy {
	return QuarantinePolicy{MaxQuarantined: 2, MinHealthyCoverage: 0.5}
}

func mustCreateWithQuarantine(t *testing.T, s *Service, id, digest string, nodes []string, size int, hp HealthPolicy, qp QuarantinePolicy) *Release {
	t.Helper()
	r, err := s.CreateRelease(CreateReleaseInput{
		ID: id, Digest: digest, Targets: nodes, WaveSize: size,
		Policy: testPolicy(), Health: hp, Quarantine: qp,
	})
	if err != nil {
		t.Fatalf("create release: %v", err)
	}
	return r
}

func mustQuarantine(t *testing.T, s *Service, releaseID, nodeID string, basis []string) *QuarantineOutcome {
	t.Helper()
	o, err := s.QuarantineNode(QuarantineInput{ReleaseID: releaseID, NodeID: nodeID, Basis: basis})
	if err != nil {
		t.Fatalf("quarantine %s: %v", nodeID, err)
	}
	return o
}

// 让发布的前 n 个节点在指定波次成功并各上报一条健康样本，返回样本事件号。
func succeedAndSample(t *testing.T, s *Service, clk *offsetClock, releaseID, digest string, nodes []string) []string {
	t.Helper()
	ids := make([]string, 0, len(nodes))
	for i, n := range nodes {
		mustAck(t, s, releaseID, n, digest, true)
		id := fmt.Sprintf("e-%s-%d", n, i)
		mustReport(t, s, clk, id, releaseID, n, digest, true)
		ids = append(ids, id)
	}
	return ids
}

// ---------- 策略冻结与校验 ----------

func TestQuarantinePolicyFrozenAndValidated(t *testing.T) {
	s, _ := newTestService(t)
	d := mustRegister(t, s, "v1")

	// 隔离策略必须随健康观察启用。
	if _, err := s.CreateRelease(CreateReleaseInput{
		ID: "x", Digest: d, Targets: []string{"n"}, WaveSize: 1,
		Policy:     testPolicy(),
		Quarantine: QuarantinePolicy{MaxQuarantined: 1, MinHealthyCoverage: 0.5},
	}); !errors.Is(err, ErrInvalidPolicy) {
		t.Fatalf("quarantine without health: %v", err)
	}
	// 非法字段值。
	for _, bad := range []QuarantinePolicy{
		{MaxQuarantined: -1, MinHealthyCoverage: 0.5},
		{MaxQuarantined: 1, MinHealthyCoverage: 0},
		{MaxQuarantined: 1, MinHealthyCoverage: 1.5},
	} {
		if _, err := s.CreateRelease(CreateReleaseInput{
			ID: "y", Digest: d, Targets: []string{"n"}, WaveSize: 1,
			Policy: testPolicy(), Health: testHealthPolicy(), Quarantine: bad,
		}); !errors.Is(err, ErrInvalidPolicy) {
			t.Fatalf("bad quarantine policy %+v: %v", bad, err)
		}
	}

	qp := testQuarantinePolicy()
	r := mustCreateWithQuarantine(t, s, "rel", d, []string{"a"}, 1, testHealthPolicy(), qp)
	if !r.QuarantineEnabled || r.Quarantine != qp {
		t.Fatalf("quarantine policy not frozen: %+v", r.Quarantine)
	}
	// 只启用健康、不启用隔离：隔离操作被拒。
	mustCreateWithHealth(t, s, "rel-h", d, []string{"z"}, 1, testPolicy(), testHealthPolicy())
	_, err := s.QuarantineNode(QuarantineInput{ReleaseID: "rel-h", NodeID: "z", Basis: []string{"e"}})
	if !errors.Is(err, ErrQuarantineNotEnabled) {
		t.Fatalf("quarantine on disabled release: %v", err)
	}
}

// ---------- 隔离决定与依据 ----------

func TestQuarantineNodeDecisionAndBasis(t *testing.T) {
	s, clk := newTestService(t)
	d := mustRegister(t, s, "v1")
	hp := HealthPolicy{ObserveWindow: time.Hour, MinSamples: 1, MaxUnhealthy: 5}
	mustCreateWithQuarantine(t, s, "rel", d, []string{"a", "b"}, 2, hp, testQuarantinePolicy())
	mustAck(t, s, "rel", "a", d, true)
	mustAck(t, s, "rel", "b", d, true)
	mustReport(t, s, clk, "e-a", "rel", "a", d, false) // a 有失败样本

	// 依据校验。
	if _, err := s.QuarantineNode(QuarantineInput{ReleaseID: "rel", NodeID: "a"}); !errors.Is(err, ErrQuarantineBasisMissing) {
		t.Fatalf("empty basis: %v", err)
	}
	if _, err := s.QuarantineNode(QuarantineInput{ReleaseID: "rel", NodeID: "a", Basis: []string{"nope"}}); !errors.Is(err, ErrQuarantineBasisUnknown) {
		t.Fatalf("unknown basis: %v", err)
	}
	// 他人节点的样本不能作为依据（先给 b 一个样本）。
	mustReport(t, s, clk, "e-b", "rel", "b", d, true)
	if _, err := s.QuarantineNode(QuarantineInput{ReleaseID: "rel", NodeID: "a", Basis: []string{"e-b"}}); !errors.Is(err, ErrQuarantineBasisUnknown) {
		t.Fatalf("other node basis: %v", err)
	}
	// 发布/节点不存在。
	if _, err := s.QuarantineNode(QuarantineInput{ReleaseID: "nope", NodeID: "a", Basis: []string{"e-a"}}); !errors.Is(err, ErrReleaseNotFound) {
		t.Fatalf("unknown release: %v", err)
	}
	if _, err := s.QuarantineNode(QuarantineInput{ReleaseID: "rel", NodeID: "zzz", Basis: []string{"e-a"}}); !errors.Is(err, ErrNodeNotInRelease) {
		t.Fatalf("unknown node: %v", err)
	}

	// 正常隔离：绑定发布/节点/摘要/波次。
	o := mustQuarantine(t, s, "rel", "a", []string{"e-a"})
	if o.Already || o.State != StateActive {
		t.Fatalf("outcome = %+v", o)
	}
	hv, _ := s.GetHealth("rel")
	if len(hv.Quarantine.Records) != 1 {
		t.Fatalf("records = %+v", hv.Quarantine.Records)
	}
	rec := hv.Quarantine.Records[0]
	if rec.NodeID != "a" || rec.Wave != 0 || rec.Digest != d {
		t.Fatalf("record = %+v", rec)
	}
	if len(rec.Basis) != 1 || rec.Basis[0] != "e-a" {
		t.Fatalf("basis = %v", rec.Basis)
	}
	// 节点健康判定变为 quarantined。
	byNode := map[string]NodeHealthView{}
	for _, n := range hv.Nodes {
		byNode[n.NodeID] = n
	}
	if !byNode["a"].Quarantined || byNode["a"].Health != HealthQuarantined {
		t.Fatalf("node a = %+v", byNode["a"])
	}

	// 重复隔离幂等，不改写决定。
	o2 := mustQuarantine(t, s, "rel", "a", []string{"e-a"})
	if !o2.Already {
		t.Fatal("re-quarantine should be idempotent Already")
	}
	evs, _ := s.PendingEvents()
	if n := countEvents(evs, EventNodeQuarantined); n != 1 {
		t.Fatalf("quarantine events = %d, want 1", n)
	}
}

// 只能隔离当前观察波中已成功的节点。
func TestQuarantineOnlySucceededCurrentWave(t *testing.T) {
	s, clk := newTestService(t)
	d := mustRegister(t, s, "v1")
	hp := HealthPolicy{ObserveWindow: time.Hour, MinSamples: 1, MaxUnhealthy: 5}
	// 每波 1 个，共 3 个；先让 a 成功并推进过观察，b 成为当前波。
	mustCreateWithQuarantine(t, s, "rel", d, []string{"a", "b", "c"}, 1, hp, testQuarantinePolicy())
	mustAck(t, s, "rel", "a", d, true)
	mustReport(t, s, clk, "e-a", "rel", "a", d, true)
	clk.advance(time.Hour + time.Minute)
	p, _ := s.GetProgress("rel")
	if p.CurrentWave != 1 {
		t.Fatalf("current wave = %d, want 1", p.CurrentWave)
	}
	// 旧波次节点 a 已离开观察期，不能隔离。
	if _, err := s.QuarantineNode(QuarantineInput{ReleaseID: "rel", NodeID: "a", Basis: []string{"e-a"}}); !errors.Is(err, ErrQuarantineNodeNotSucceeded) {
		t.Fatalf("quarantine past wave node: %v", err)
	}
	// 未来波次 c 尚未成功。
	if _, err := s.QuarantineNode(QuarantineInput{ReleaseID: "rel", NodeID: "c", Basis: []string{"e-a"}}); !errors.Is(err, ErrQuarantineNodeNotSucceeded) {
		t.Fatalf("quarantine future node: %v", err)
	}
}

// 隔离后节点不接收新操作，迟到/重复样本不改写决定。
func TestQuarantinedNodeReceivesNoNewActions(t *testing.T) {
	s, clk := newTestService(t)
	d := mustRegister(t, s, "v1")
	hp := HealthPolicy{ObserveWindow: time.Hour, MinSamples: 1, MaxUnhealthy: 5}
	mustCreateWithQuarantine(t, s, "rel", d, []string{"a", "b"}, 2, hp, testQuarantinePolicy())
	mustAck(t, s, "rel", "a", d, true)
	mustAck(t, s, "rel", "b", d, true)
	mustReport(t, s, clk, "e-a", "rel", "a", d, false)
	mustQuarantine(t, s, "rel", "a", []string{"e-a"})

	// 新事件号样本被拒。
	if _, err := s.ReportHealth(HealthSample{EventID: "e-a-late", ReleaseID: "rel", NodeID: "a", Digest: d, Healthy: true, SampledAt: clk.Now()}); !errors.Is(err, ErrNodeQuarantined) {
		t.Fatalf("new sample after quarantine: %v", err)
	}
	// 依据样本重放仍幂等，不新增、不改写。
	o := mustReport(t, s, clk, "e-a", "rel", "a", d, false)
	if !o.Duplicate {
		t.Fatal("basis replay not idempotent")
	}
	hv, _ := s.GetHealth("rel")
	if hv.SamplesRecorded != 1 {
		t.Fatalf("samples = %d, want 1", hv.SamplesRecorded)
	}
	// 不能再拉取配置。
	if _, err := s.GetConfigForNode("rel", "a"); !errors.Is(err, ErrNodeQuarantined) {
		t.Fatalf("get config after quarantine: %v", err)
	}
}

// 隔离节点不计入健康门槛：失败节点被隔离后移出不健康统计，其余节点可
// 正常通过观察。
func TestQuarantinedNodeExcludedFromHealthGate(t *testing.T) {
	s, clk := newTestService(t)
	d := mustRegister(t, s, "v1")
	// 窗口 30m、每节点 1 样本、允许 1 个不健康；4 节点一波。
	hp := HealthPolicy{ObserveWindow: 30 * time.Minute, MinSamples: 1, MaxUnhealthy: 1}
	qp := QuarantinePolicy{MaxQuarantined: 2, MinHealthyCoverage: 0.5}
	mustCreateWithQuarantine(t, s, "rel", d, []string{"a", "b", "c", "d"}, 4, hp, qp)
	for _, n := range []string{"a", "b", "c", "d"} {
		mustAck(t, s, "rel", n, d, true)
	}
	// a 失败样本：不健康数 1，恰好容忍。
	mustReport(t, s, clk, "e-a", "rel", "a", d, false)
	// 隔离 a 后它移出不健康统计：此后 b 的失败样本只计 1 个（若不排除
	// a，a+b=2 > MaxUnhealthy=1 已回滚）。
	mustQuarantine(t, s, "rel", "a", []string{"e-a"})
	mustReport(t, s, clk, "e-b", "rel", "b", d, false)
	p, _ := s.GetProgress("rel")
	if p.State != StateActive {
		t.Fatalf("state = %s, want active", p.State)
	}
	mustReport(t, s, clk, "e-c", "rel", "c", d, true)
	mustReport(t, s, clk, "e-d", "rel", "d", d, true)

	// 覆盖率 3/4 = 0.75（a 已隔离）>= 0.5，未超限；窗口届满即可完成，
	// 无需为隔离节点 a 补样本。
	hv, _ := s.GetHealth("rel")
	if hv.UnhealthyNodes != 1 {
		t.Fatalf("unhealthy nodes = %d, want 1 (b only, a quarantined)", hv.UnhealthyNodes)
	}
	if hv.Quarantine.Coverage != 0.75 {
		t.Fatalf("coverage = %v, want 0.75", hv.Quarantine.Coverage)
	}
	clk.advance(31 * time.Minute)
	p, err := s.GetProgress("rel")
	if err != nil {
		t.Fatal(err)
	}
	if p.State != StateCompleted {
		t.Fatalf("state = %s, want completed", p.State)
	}
}

// 隔离数量超过冻结上限 -> 整体回滚。
func TestQuarantineMaxCountTriggersRollback(t *testing.T) {
	s, clk := newTestService(t)
	d := mustRegister(t, s, "v1")
	hp := HealthPolicy{ObserveWindow: time.Hour, MinSamples: 1, MaxUnhealthy: 5}
	qp := QuarantinePolicy{MaxQuarantined: 1, MinHealthyCoverage: 0.1}
	mustCreateWithQuarantine(t, s, "rel", d, []string{"a", "b", "c"}, 3, hp, qp)
	for _, n := range []string{"a", "b", "c"} {
		mustAck(t, s, "rel", n, d, true)
		mustReport(t, s, clk, "e-"+n, "rel", n, d, false)
	}
	mustQuarantine(t, s, "rel", "a", []string{"e-a"})
	// 第二个隔离使累计数 2 > MaxQuarantined 1 -> 回滚。
	o := mustQuarantine(t, s, "rel", "b", []string{"e-b"})
	if !o.RollbackStarted || o.State != StateRollingBack {
		t.Fatalf("outcome = %+v", o)
	}
	hv, _ := s.GetHealth("rel")
	if hv.Rollback == nil || hv.Rollback.Reason != RollbackReasonQuarantineLimit {
		t.Fatalf("rollback = %+v", hv.Rollback)
	}
	// 整体回滚不豁免隔离节点：a、b 都在回滚计划中。
	if hv.Rollback.Total != 3 {
		t.Fatalf("rollback total = %d, want 3 (all succeeded nodes)", hv.Rollback.Total)
	}
	evs, _ := s.PendingEvents()
	if n := countEvents(evs, EventRollbackStarted); n != 1 {
		t.Fatalf("rollback.started = %d", n)
	}
}

// 健康覆盖率低于冻结下限 -> 整体回滚。
func TestQuarantineCoverageTriggersRollback(t *testing.T) {
	s, clk := newTestService(t)
	d := mustRegister(t, s, "v1")
	hp := HealthPolicy{ObserveWindow: time.Hour, MinSamples: 1, MaxUnhealthy: 5}
	qp := QuarantinePolicy{MaxQuarantined: 10, MinHealthyCoverage: 0.6}
	mustCreateWithQuarantine(t, s, "rel", d, []string{"a", "b", "c"}, 3, hp, qp)
	for _, n := range []string{"a", "b", "c"} {
		mustAck(t, s, "rel", n, d, true)
		mustReport(t, s, clk, "e-"+n, "rel", n, d, false)
	}
	// 隔离 2 个 -> 覆盖率 1/3 ≈ 0.33 < 0.6 -> 回滚。
	mustQuarantine(t, s, "rel", "a", []string{"e-a"})
	o := mustQuarantine(t, s, "rel", "b", []string{"e-b"})
	if !o.RollbackStarted || o.State != StateRollingBack {
		t.Fatalf("outcome = %+v", o)
	}
	hv, _ := s.GetHealth("rel")
	if hv.Quarantine.Coverage >= 0.6 || !hv.Quarantine.LimitBreached {
		t.Fatalf("coverage view = %+v", hv.Quarantine)
	}
}

// 暂停期隔离只记录，恢复时按冻结限制结算（可能立即回滚）。
func TestQuarantineWhilePausedSettlesOnResume(t *testing.T) {
	s, clk := newTestService(t)
	d := mustRegister(t, s, "v1")
	hp := HealthPolicy{ObserveWindow: time.Hour, MinSamples: 1, MaxUnhealthy: 5}
	qp := QuarantinePolicy{MaxQuarantined: 0, MinHealthyCoverage: 0.1}
	mustCreateWithQuarantine(t, s, "rel", d, []string{"a", "b"}, 2, hp, qp)
	mustAck(t, s, "rel", "a", d, true)
	mustAck(t, s, "rel", "b", d, true)
	mustReport(t, s, clk, "e-a", "rel", "a", d, false)
	if err := s.Pause("rel"); err != nil {
		t.Fatal(err)
	}
	// 暂停期间隔离成功记录，但不结算（不回滚）。
	o := mustQuarantine(t, s, "rel", "a", []string{"e-a"})
	if o.RollbackStarted || o.State != StatePaused {
		t.Fatalf("outcome = %+v", o)
	}
	if err := s.Resume("rel"); err != nil {
		t.Fatal(err)
	}
	p, _ := s.GetProgress("rel")
	if p.State != StateRollingBack {
		t.Fatalf("state = %s, want rolling_back", p.State)
	}
}

// 隔离/回滚/样本并发：最终只有一条单调状态序列，回滚与隔离限制只结算一次。
func TestQuarantineConcurrentMonotonic(t *testing.T) {
	s, clk := newTestService(t)
	d := mustRegister(t, s, "v1")
	hp := HealthPolicy{ObserveWindow: time.Hour, MinSamples: 1, MaxUnhealthy: 5}
	qp := QuarantinePolicy{MaxQuarantined: 1, MinHealthyCoverage: 0.1}
	mustCreateWithQuarantine(t, s, "rel", d, []string{"a", "b", "c"}, 3, hp, qp)
	for _, n := range []string{"a", "b", "c"} {
		mustAck(t, s, "rel", n, d, true)
		mustReport(t, s, clk, "e-"+n, "rel", n, d, false)
	}

	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(3)
		go func() {
			defer wg.Done()
			_, _ = s.QuarantineNode(QuarantineInput{ReleaseID: "rel", NodeID: "a", Basis: []string{"e-a"}})
		}()
		go func() {
			defer wg.Done()
			_, _ = s.QuarantineNode(QuarantineInput{ReleaseID: "rel", NodeID: "b", Basis: []string{"e-b"}})
		}()
		go func(i int) {
			defer wg.Done()
			_, _ = s.ReportHealth(HealthSample{EventID: fmt.Sprintf("late-%d", i), ReleaseID: "rel", NodeID: "c", Digest: d, Healthy: true, SampledAt: clk.Now()})
		}(i)
	}
	wg.Wait()

	p, _ := s.GetProgress("rel")
	evs, _ := s.PendingEvents()
	assertLegalLifecycle(t, evs)
	// MaxQuarantined=1：隔离第二个必触发回滚；回滚至多一次。
	if n := countEvents(evs, EventRollbackStarted); n > 1 {
		t.Fatalf("rollback started = %d", n)
	}
	if p.State != StateRollingBack {
		t.Fatalf("state = %s, want rolling_back", p.State)
	}
}
