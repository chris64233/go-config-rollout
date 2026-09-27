package configrollout

import (
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"
)

// ---------- 健康测试辅助 ----------

func testHealthPolicy() HealthPolicy {
	return HealthPolicy{ObserveWindow: 30 * time.Minute, MinSamples: 1, MaxUnhealthy: 0}
}

func mustCreateWithHealth(t *testing.T, s *Service, id, digest string, nodes []string, size int, policy WavePolicy, hp HealthPolicy) *Release {
	t.Helper()
	r, err := s.CreateRelease(CreateReleaseInput{ID: id, Digest: digest, Targets: nodes, WaveSize: size, Policy: policy, Health: hp})
	if err != nil {
		t.Fatalf("create release: %v", err)
	}
	return r
}

func sampleAt(clk *offsetClock) time.Time { return clk.Now() }

func mustReport(t *testing.T, s *Service, clk *offsetClock, eventID, releaseID, nodeID, digest string, healthy bool) *HealthOutcome {
	t.Helper()
	o, err := s.ReportHealth(HealthSample{
		EventID: eventID, ReleaseID: releaseID, NodeID: nodeID,
		Digest: digest, Healthy: healthy, SampledAt: sampleAt(clk),
	})
	if err != nil {
		t.Fatalf("report health %s: %v", eventID, err)
	}
	return o
}

func countEvents(evs []Event, typ string) int {
	var n int
	for _, ev := range evs {
		if ev.Type == typ {
			n++
		}
	}
	return n
}

// ---------- 健康策略冻结与校验 ----------

func TestHealthPolicyFrozenAndValidated(t *testing.T) {
	s, _ := newTestService(t)
	d := mustRegister(t, s, "v1")

	// 非法健康策略：启用（任一字段非零）但字段不合法。
	for _, bad := range []HealthPolicy{
		{ObserveWindow: 0, MinSamples: 1, MaxUnhealthy: 0},
		{ObserveWindow: time.Minute, MinSamples: 0, MaxUnhealthy: 0},
		{ObserveWindow: time.Minute, MinSamples: 1, MaxUnhealthy: -1},
		{ObserveWindow: -time.Minute},
	} {
		_, err := s.CreateRelease(CreateReleaseInput{
			ID: "x", Digest: d, Targets: []string{"n"}, WaveSize: 1,
			Policy: testPolicy(), Health: bad,
		})
		if !errors.Is(err, ErrInvalidPolicy) {
			t.Fatalf("bad health policy %+v: %v", bad, err)
		}
	}

	hp := testHealthPolicy()
	r := mustCreateWithHealth(t, s, "rel", d, []string{"a", "b"}, 1, testPolicy(), hp)
	if !r.HealthEnabled || r.Health != hp {
		t.Fatalf("health policy not frozen: %+v", r.Health)
	}
	// 零值健康策略 = 不启用，保持旧行为（达标立即推进）。
	r2 := mustCreate(t, s, "rel-plain", d, []string{"z"}, 1, testPolicy())
	if r2.HealthEnabled {
		t.Fatal("zero health policy should be disabled")
	}

	// 未启用健康观察的发布不接受样本。
	_, err := s.ReportHealth(HealthSample{
		EventID: "e1", ReleaseID: "rel-plain", NodeID: "z", Digest: d,
		Healthy: true, SampledAt: time.Now(),
	})
	if !errors.Is(err, ErrHealthNotEnabled) {
		t.Fatalf("sample on disabled release: %v", err)
	}
}

// ---------- 观察窗口门禁 ----------

func TestObservationWindowGatesWaveAdvance(t *testing.T) {
	s, clk := newTestService(t)
	d := mustRegister(t, s, "v1")
	hp := HealthPolicy{ObserveWindow: 30 * time.Minute, MinSamples: 2, MaxUnhealthy: 0}
	mustCreateWithHealth(t, s, "rel", d, []string{"a", "b", "c", "d"}, 2, testPolicy(), hp)

	// 第 0 波两个节点成功：进入观察期但不推进。
	o := mustAck(t, s, "rel", "a", d, true)
	o = mustAck(t, s, "rel", "b", d, true)
	if o.WaveAdvanced || o.CurrentWave != 0 {
		t.Fatalf("wave advanced during observation: %+v", o)
	}
	p, _ := s.GetProgress("rel")
	if p.CurrentWave != 0 || p.State != StateActive {
		t.Fatalf("progress = %+v", p)
	}

	// 窗口届满但样本不足：仍不推进。
	clk.advance(31 * time.Minute)
	if paused, _ := s.TickTimeout("rel"); paused {
		t.Fatal("observing wave must not be timed out")
	}
	p, _ = s.GetProgress("rel")
	if p.CurrentWave != 0 {
		t.Fatalf("advanced without enough samples: %+v", p)
	}

	// 样本乱序到达（先 b 后 a，各 2 条）后窗口与样本同时满足 -> 推进。
	mustReport(t, s, clk, "e-b1", "rel", "b", d, true)
	mustReport(t, s, clk, "e-a1", "rel", "a", d, true)
	mustReport(t, s, clk, "e-b2", "rel", "b", d, true)
	o2 := mustReport(t, s, clk, "e-a2", "rel", "a", d, true)
	if !o2.WaveAdvanced || o2.CurrentWave != 1 {
		t.Fatalf("outcome = %+v", o2)
	}
	p, _ = s.GetProgress("rel")
	if p.CurrentWave != 1 || !p.Waves[1].Open {
		t.Fatalf("wave 1 not opened: %+v", p)
	}
}

func TestLastWaveObservationBeforeComplete(t *testing.T) {
	s, clk := newTestService(t)
	d := mustRegister(t, s, "v1")
	hp := HealthPolicy{ObserveWindow: time.Hour, MinSamples: 1, MaxUnhealthy: 0}
	mustCreateWithHealth(t, s, "rel", d, []string{"a"}, 1, testPolicy(), hp)

	mustAck(t, s, "rel", "a", d, true)
	// 末波也要通过完整观察窗口才能完成。
	p, _ := s.GetProgress("rel")
	if p.State != StateActive {
		t.Fatalf("completed before observation passed: %s", p.State)
	}
	mustReport(t, s, clk, "e1", "rel", "a", d, true)
	clk.advance(time.Hour + time.Minute)
	p, _ = s.GetProgress("rel")
	if p.State != StateCompleted {
		t.Fatalf("state = %s, want completed", p.State)
	}
}

// ---------- 样本幂等与冲突 ----------

func TestHealthSampleIdempotencyAndConflict(t *testing.T) {
	s, clk := newTestService(t)
	d := mustRegister(t, s, "v1")
	d2 := mustRegister(t, s, "v2")
	hp := testHealthPolicy()
	mustCreateWithHealth(t, s, "rel", d, []string{"a", "b"}, 2, WavePolicy{MinSuccessRatio: 1, MaxFailures: 5, WaveTimeout: time.Hour}, hp)
	mustAck(t, s, "rel", "a", d, true)

	// 基本校验。
	if _, err := s.ReportHealth(HealthSample{ReleaseID: "rel", NodeID: "a", Digest: d, Healthy: true, SampledAt: sampleAt(clk)}); !errors.Is(err, ErrInvalidSample) {
		t.Fatalf("empty event id: %v", err)
	}
	if _, err := s.ReportHealth(HealthSample{EventID: "e", ReleaseID: "rel", NodeID: "a", Digest: d, Healthy: true}); !errors.Is(err, ErrInvalidSample) {
		t.Fatalf("zero sampled-at: %v", err)
	}
	if _, err := s.ReportHealth(HealthSample{EventID: "e", ReleaseID: "nope", NodeID: "a", Digest: d, Healthy: true, SampledAt: sampleAt(clk)}); !errors.Is(err, ErrReleaseNotFound) {
		t.Fatalf("unknown release: %v", err)
	}
	// 跨发布：节点不属于该发布。
	if _, err := s.ReportHealth(HealthSample{EventID: "e", ReleaseID: "rel", NodeID: "zzz", Digest: d, Healthy: true, SampledAt: sampleAt(clk)}); !errors.Is(err, ErrNodeNotInRelease) {
		t.Fatalf("node not in release: %v", err)
	}
	// 跨配置：样本摘要与发布摘要不一致。
	if _, err := s.ReportHealth(HealthSample{EventID: "e", ReleaseID: "rel", NodeID: "a", Digest: d2, Healthy: true, SampledAt: sampleAt(clk)}); !errors.Is(err, ErrDigestMismatch) {
		t.Fatalf("cross-config sample: %v", err)
	}

	// 正常上报 + 相同事件号相同内容重放：幂等。
	first := mustReport(t, s, clk, "e-1", "rel", "a", d, true)
	if first.Duplicate {
		t.Fatal("first report marked duplicate")
	}
	replay := mustReport(t, s, clk, "e-1", "rel", "a", d, true)
	if !replay.Duplicate {
		t.Fatal("replay should be idempotent duplicate")
	}
	hv, err := s.GetHealth("rel")
	if err != nil {
		t.Fatal(err)
	}
	if hv.SamplesRecorded != 1 {
		t.Fatalf("samples recorded = %d, want 1", hv.SamplesRecorded)
	}

	// 相同事件号不同内容：冲突。
	if _, err := s.ReportHealth(HealthSample{EventID: "e-1", ReleaseID: "rel", NodeID: "a", Digest: d, Healthy: false, SampledAt: sampleAt(clk)}); !errors.Is(err, ErrSampleConflict) {
		t.Fatalf("same id different content: %v", err)
	}
	if _, err := s.ReportHealth(HealthSample{EventID: "e-1", ReleaseID: "rel", NodeID: "b", Digest: d, Healthy: true, SampledAt: sampleAt(clk)}); !errors.Is(err, ErrSampleConflict) {
		t.Fatalf("same id different node: %v", err)
	}

	// 乱序到达：采样时间早于已收样本也接受（按事件号去重，不看时间序）。
	clk.advance(time.Hour)
	late := HealthSample{EventID: "e-0", ReleaseID: "rel", NodeID: "a", Digest: d, Healthy: true, SampledAt: sampleAt(clk).Add(-2 * time.Hour)}
	if _, err := s.ReportHealth(late); err != nil {
		t.Fatalf("out-of-order sample: %v", err)
	}
}

// ---------- 健康阈值触发自动回滚 ----------

// 先让 a、b 在 rel-0 应用 v0，再在 rel-1 应用 v1 并触发健康回滚，
// 验证回滚计划把节点恢复到各自发布前的 v0。
func TestHealthThresholdTriggersRollback(t *testing.T) {
	s, clk := newTestService(t)
	d0 := mustRegister(t, s, "v0")
	d1 := mustRegister(t, s, "v1")
	mustCreate(t, s, "rel-0", d0, []string{"a", "b"}, 2, testPolicy())
	mustAck(t, s, "rel-0", "a", d0, true)
	mustAck(t, s, "rel-0", "b", d0, true)

	hp := HealthPolicy{ObserveWindow: 30 * time.Minute, MinSamples: 1, MaxUnhealthy: 0}
	mustCreateWithHealth(t, s, "rel-1", d1, []string{"a", "b", "c", "d"}, 2, testPolicy(), hp)
	mustAck(t, s, "rel-1", "a", d1, true)
	mustAck(t, s, "rel-1", "b", d1, true)

	// a 上报失败样本 -> 超过 MaxUnhealthy=0 -> 一次性生成回滚计划。
	o := mustReport(t, s, clk, "e-bad", "rel-1", "a", d1, false)
	if !o.RollbackStarted || o.State != StateRollingBack {
		t.Fatalf("outcome = %+v", o)
	}
	p, _ := s.GetProgress("rel-1")
	if p.State != StateRollingBack || p.CurrentWave != 0 {
		t.Fatalf("progress = %+v", p)
	}

	// 回滚计划：a、b 各一条，目标为发布前的 v0。
	hv, _ := s.GetHealth("rel-1")
	if hv.Rollback == nil || hv.Rollback.Total != 2 || hv.Rollback.Restored != 0 {
		t.Fatalf("rollback view = %+v", hv.Rollback)
	}
	for _, e := range hv.Rollback.Entries {
		if e.FromDigest != d1 || e.ToDigest != d0 {
			t.Fatalf("entry = %+v, want %s -> %s", e, d1, d0)
		}
	}

	// 回滚期间：回执、样本、暂停、恢复、取消全部被拒，不会一边回滚一边开新波次。
	if _, err := s.AckReceipt(Receipt{"rel-1", "c", d1, true}); !errors.Is(err, ErrRollbackInProgress) {
		t.Fatalf("receipt during rollback: %v", err)
	}
	if _, err := s.ReportHealth(HealthSample{EventID: "e-x", ReleaseID: "rel-1", NodeID: "b", Digest: d1, Healthy: true, SampledAt: sampleAt(clk)}); !errors.Is(err, ErrRollbackInProgress) {
		t.Fatalf("sample during rollback: %v", err)
	}
	if err := s.Pause("rel-1"); !errors.Is(err, ErrRollbackInProgress) {
		t.Fatalf("pause during rollback: %v", err)
	}
	if err := s.Resume("rel-1"); !errors.Is(err, ErrRollbackInProgress) {
		t.Fatalf("resume during rollback: %v", err)
	}
	if err := s.Cancel("rel-1"); !errors.Is(err, ErrRollbackInProgress) {
		t.Fatalf("cancel during rollback: %v", err)
	}

	// 确认恢复：错误摘要被拒；逐节点确认后回到 v0。
	if _, err := s.ConfirmRollback("rel-1", "a", d1); !errors.Is(err, ErrDigestMismatch) {
		t.Fatalf("wrong restore digest: %v", err)
	}
	if _, err := s.ConfirmRollback("rel-1", "zzz", d0); !errors.Is(err, ErrNodeNotInRelease) {
		t.Fatalf("unknown node: %v", err)
	}
	ro, err := s.ConfirmRollback("rel-1", "a", d0)
	if err != nil || ro.Restored != 1 || ro.Total != 2 || ro.State != StateRollingBack {
		t.Fatalf("confirm a: %+v %v", ro, err)
	}
	// 重复确认幂等。
	ro, err = s.ConfirmRollback("rel-1", "a", d0)
	if err != nil || !ro.Ignored {
		t.Fatalf("re-confirm: %+v %v", ro, err)
	}
	ro, err = s.ConfirmRollback("rel-1", "b", d0)
	if err != nil || ro.State != StateRolledBack {
		t.Fatalf("confirm b: %+v %v", ro, err)
	}

	// 节点已应用版本恢复到发布前的 v0。
	for _, n := range []string{"a", "b"} {
		ns, err := s.NodeAppliedVersion(n)
		if err != nil || ns.AppliedDigest != d0 {
			t.Fatalf("node %s applied = %+v %v", n, ns, err)
		}
	}

	// 事件：回滚通知与终态通知分别恰好一次；从未开放第 1 波。
	evs, _ := s.PendingEvents()
	var rel1Evs []Event
	for _, ev := range evs {
		if ev.ReleaseID == "rel-1" {
			rel1Evs = append(rel1Evs, ev)
		}
	}
	assertLegalLifecycle(t, rel1Evs)
	if n := countEvents(rel1Evs, EventRollbackStarted); n != 1 {
		t.Fatalf("rollback.started = %d", n)
	}
	if n := countEvents(rel1Evs, EventNodeRollback); n != 2 {
		t.Fatalf("node.rollback = %d", n)
	}
	if n := countEvents(rel1Evs, EventNodeRestored); n != 2 {
		t.Fatalf("node.restored = %d", n)
	}
	if n := countEvents(rel1Evs, EventReleaseRolledBack); n != 1 {
		t.Fatalf("release.rolledback = %d", n)
	}
	for _, ev := range rel1Evs {
		if ev.Type == EventWaveOpened && ev.Wave != 0 {
			t.Fatal("wave opened while rolling back")
		}
	}

	// 终态后：回执/样本/取消均拒绝。
	if _, err := s.AckReceipt(Receipt{"rel-1", "c", d1, true}); !errors.Is(err, ErrTerminal) {
		t.Fatalf("receipt after rolled back: %v", err)
	}
	if _, err := s.ReportHealth(HealthSample{EventID: "e-y", ReleaseID: "rel-1", NodeID: "b", Digest: d1, Healthy: true, SampledAt: sampleAt(clk)}); !errors.Is(err, ErrTerminal) {
		t.Fatalf("sample after rolled back: %v", err)
	}
	if err := s.Cancel("rel-1"); !errors.Is(err, ErrTerminal) {
		t.Fatalf("cancel after rolled back: %v", err)
	}
}

// TestLateSuccessReceiptDoesNotOverrideRollback 回滚完成后，节点的迟到
// 成功回执不得覆盖已恢复的版本。
func TestLateSuccessReceiptDoesNotOverrideRollback(t *testing.T) {
	s, clk := newTestService(t)
	d0 := mustRegister(t, s, "v0")
	d1 := mustRegister(t, s, "v1")
	mustCreate(t, s, "rel-0", d0, []string{"a"}, 1, testPolicy())
	mustAck(t, s, "rel-0", "a", d0, true)

	hp := testHealthPolicy()
	mustCreateWithHealth(t, s, "rel-1", d1, []string{"a"}, 1, testPolicy(), hp)
	mustAck(t, s, "rel-1", "a", d1, true)
	mustReport(t, s, clk, "e-bad", "rel-1", "a", d1, false)
	if _, err := s.ConfirmRollback("rel-1", "a", d0); err != nil {
		t.Fatal(err)
	}
	ns, _ := s.NodeAppliedVersion("a")
	if ns.AppliedDigest != d0 {
		t.Fatalf("applied = %+v", ns)
	}

	// 迟到的成功回执（同一发布同一摘要重放）：幂等忽略，不覆盖回滚结果。
	o, err := s.AckReceipt(Receipt{"rel-1", "a", d1, true})
	if err != nil || !o.Ignored {
		t.Fatalf("late success receipt: %+v %v", o, err)
	}
	ns2, _ := s.NodeAppliedVersion("a")
	if ns2.AppliedDigest != d0 || ns2.AppliedSeq != ns.AppliedSeq {
		t.Fatalf("rollback result overridden: %+v", ns2)
	}
}

// TestRollbackVsCancelResumeRace 自动回滚、人工取消、恢复并发竞争时，
// 最终只留下一条单调状态序列：回滚与取消互斥，回滚期间不开新波次。
func TestRollbackVsCancelResumeRace(t *testing.T) {
	s, clk := newTestService(t)
	d := mustRegister(t, s, "v1")
	hp := HealthPolicy{ObserveWindow: time.Hour, MinSamples: 1, MaxUnhealthy: 0}
	policy := WavePolicy{MinSuccessRatio: 1, MaxFailures: 5, WaveTimeout: 24 * time.Hour}
	mustCreateWithHealth(t, s, "rel", d, []string{"a", "b", "c", "d"}, 2, policy, hp)
	mustAck(t, s, "rel", "a", d, true)
	mustAck(t, s, "rel", "b", d, true) // 第 0 波达标，进入观察期
	if err := s.Pause("rel"); err != nil {
		t.Fatal(err)
	}

	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(3)
		go func(i int) {
			defer wg.Done()
			_, _ = s.ReportHealth(HealthSample{
				EventID: fmt.Sprintf("e-bad-%d", i), ReleaseID: "rel", NodeID: "a",
				Digest: d, Healthy: false, SampledAt: sampleAt(clk),
			})
		}(i)
		go func() { defer wg.Done(); _ = s.Cancel("rel") }()
		go func() { defer wg.Done(); _ = s.Resume("rel") }()
	}
	wg.Wait()

	p, err := s.GetProgress("rel")
	if err != nil {
		t.Fatal(err)
	}
	evs, _ := s.PendingEvents()
	assertLegalLifecycle(t, evs)

	rollbackStarted := countEvents(evs, EventRollbackStarted)
	cancelled := countEvents(evs, EventReleaseCancelled)
	if rollbackStarted > 1 || cancelled > 1 {
		t.Fatalf("rollback=%d cancel=%d", rollbackStarted, cancelled)
	}
	// 回滚与取消互斥：只能有一个发生。
	if rollbackStarted == 1 && cancelled == 1 {
		t.Fatal("rollback and cancel both happened")
	}
	switch p.State {
	case StateRollingBack:
		// 回滚竞争获胜：完成回滚，验证不开新波次、通知唯一。
		if cancelled != 0 {
			t.Fatal("cancel event emitted during rollback")
		}
		hv, _ := s.GetHealth("rel")
		for _, e := range hv.Rollback.Entries {
			if _, err := s.ConfirmRollback("rel", e.NodeID, e.ToDigest); err != nil {
				t.Fatalf("confirm %s: %v", e.NodeID, err)
			}
		}
		p, _ = s.GetProgress("rel")
		if p.State != StateRolledBack {
			t.Fatalf("state = %s", p.State)
		}
		evs, _ = s.PendingEvents()
		assertLegalLifecycle(t, evs)
		if n := countEvents(evs, EventReleaseRolledBack); n != 1 {
			t.Fatalf("rolledback events = %d", n)
		}
		if n := countEvents(evs, EventNodeRollback); n != 2 {
			t.Fatalf("node.rollback = %d, want 2", n)
		}
	case StateCancelled:
		if rollbackStarted != 0 {
			t.Fatal("rollback started after cancel")
		}
	case StatePaused, StateActive:
		// 样本在暂停期间被记录但尚未评估（所有 resume 都先于样本到达），
		// 也属于合法结果；事件序列仍须单调合法。
	default:
		t.Fatalf("unexpected final state %s", p.State)
	}
}

// TestRollbackNotificationsSurviveRestart 回滚通知与终态通知在重启后
// 仍然只写出一次。
func TestRollbackNotificationsSurviveRestart(t *testing.T) {
	path := mustTempFile(t)
	clk := &offsetClock{t: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)}
	svc := NewService(mustFileStore(t, path), clk)
	d := mustRegister(t, svc, "v1")
	hp := testHealthPolicy()
	mustCreateWithHealth(t, svc, "rel", d, []string{"a", "b"}, 2, testPolicy(), hp)
	mustAck(t, svc, "rel", "a", d, true)
	mustAck(t, svc, "rel", "b", d, true)
	mustReport(t, svc, clk, "e-bad", "rel", "a", d, false)

	// 模拟重启：回滚计划与通知已落盘。
	svc2 := NewService(mustFileStore(t, path), clk)
	p, _ := svc2.GetProgress("rel")
	if p.State != StateRollingBack {
		t.Fatalf("state = %s", p.State)
	}
	// 重启后重复触发路径全部无效，不会重复生成通知。
	if err := svc2.Cancel("rel"); !errors.Is(err, ErrRollbackInProgress) {
		t.Fatalf("cancel during rollback: %v", err)
	}
	if _, err := svc2.ConfirmRollback("rel", "a", ""); err != nil {
		t.Fatal(err)
	}
	if _, err := svc2.ConfirmRollback("rel", "b", ""); err != nil {
		t.Fatal(err)
	}
	// 再次重启：终态与事件仍然唯一。
	svc3 := NewService(mustFileStore(t, path), clk)
	p, _ = svc3.GetProgress("rel")
	if p.State != StateRolledBack {
		t.Fatalf("state = %s", p.State)
	}
	evs, _ := svc3.PendingEvents()
	assertLegalLifecycle(t, evs)
	if n := countEvents(evs, EventRollbackStarted); n != 1 {
		t.Fatalf("rollback.started = %d", n)
	}
	if n := countEvents(evs, EventNodeRollback); n != 2 {
		t.Fatalf("node.rollback = %d", n)
	}
	if n := countEvents(evs, EventReleaseRolledBack); n != 1 {
		t.Fatalf("release.rolledback = %d", n)
	}
}

// TestGetHealthView 查询视图展示健康证据、阈值计算、前后版本与回滚进度。
func TestGetHealthView(t *testing.T) {
	s, clk := newTestService(t)
	d0 := mustRegister(t, s, "v0")
	d1 := mustRegister(t, s, "v1")
	mustCreate(t, s, "rel-0", d0, []string{"a"}, 1, testPolicy())
	mustAck(t, s, "rel-0", "a", d0, true)

	hp := HealthPolicy{ObserveWindow: time.Hour, MinSamples: 2, MaxUnhealthy: 0}
	mustCreateWithHealth(t, s, "rel-1", d1, []string{"a", "b"}, 2, testPolicy(), hp)
	mustAck(t, s, "rel-1", "a", d1, true)
	mustAck(t, s, "rel-1", "b", d1, true)
	mustReport(t, s, clk, "e1", "rel-1", "a", d1, true)
	mustReport(t, s, clk, "e2", "rel-1", "a", d1, true)
	mustReport(t, s, clk, "e3", "rel-1", "b", d1, false)

	hv, err := s.GetHealth("rel-1")
	if err != nil {
		t.Fatal(err)
	}
	// 阈值计算：b 不健康 > MaxUnhealthy=0 -> 已触发回滚。
	if !hv.ThresholdBreached || hv.UnhealthyNodes != 1 || hv.MaxUnhealthy != 0 {
		t.Fatalf("threshold view = %+v", hv)
	}
	if hv.State != StateRollingBack || hv.Rollback == nil {
		t.Fatalf("view = %+v", hv)
	}
	if !hv.Observing && hv.State == StateActive {
		t.Fatal("should be observing")
	}
	if hv.WindowEndsAt.IsZero() {
		t.Fatal("window end not exposed")
	}
	// 每个节点的前后版本与健康证据。
	byNode := map[string]NodeHealthView{}
	for _, n := range hv.Nodes {
		byNode[n.NodeID] = n
	}
	na := byNode["a"]
	if na.Health != HealthHealthy || na.Samples != 2 || na.PrevDigest != d0 || na.CurrentDigest != d1 {
		t.Fatalf("node a view = %+v", na)
	}
	nb := byNode["b"]
	if nb.Health != HealthUnhealthy || nb.UnhealthySamples != 1 || nb.PrevDigest != "" || nb.CurrentDigest != d1 {
		t.Fatalf("node b view = %+v", nb)
	}
	// 回滚进度：确认 a 后进度前进。
	if _, err := s.ConfirmRollback("rel-1", "a", d0); err != nil {
		t.Fatal(err)
	}
	hv2, _ := s.GetHealth("rel-1")
	if hv2.Rollback.Restored != 1 || hv2.Rollback.Total != 2 {
		t.Fatalf("rollback progress = %+v", hv2.Rollback)
	}
	// a 的当前版本已恢复为 v0，b 仍是 v1。
	byNode = map[string]NodeHealthView{}
	for _, n := range hv2.Nodes {
		byNode[n.NodeID] = n
	}
	if byNode["a"].CurrentDigest != d0 || byNode["b"].CurrentDigest != d1 {
		t.Fatalf("post-rollback versions: a=%s b=%s", byNode["a"].CurrentDigest, byNode["b"].CurrentDigest)
	}
}

// TestHealthSamplesAcceptedWhilePaused 暂停期间样本只记录不评估，
// 恢复时一次性结算（可能立即回滚）。
func TestHealthSamplesAcceptedWhilePaused(t *testing.T) {
	s, clk := newTestService(t)
	d := mustRegister(t, s, "v1")
	hp := testHealthPolicy()
	mustCreateWithHealth(t, s, "rel", d, []string{"a"}, 1, testPolicy(), hp)
	mustAck(t, s, "rel", "a", d, true)
	if err := s.Pause("rel"); err != nil {
		t.Fatal(err)
	}
	// 暂停期间接受样本但不触发评估。
	o := mustReport(t, s, clk, "e-bad", "rel", "a", d, false)
	if o.State != StatePaused || o.RollbackStarted {
		t.Fatalf("outcome = %+v", o)
	}
	// 恢复时结算：立即进入回滚。
	if err := s.Resume("rel"); err != nil {
		t.Fatal(err)
	}
	p, _ := s.GetProgress("rel")
	if p.State != StateRollingBack {
		t.Fatalf("state = %s", p.State)
	}
}

func mustTempFile(t *testing.T) string {
	t.Helper()
	return t.TempDir() + "/state.json"
}
