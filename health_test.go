package configrollout

import (
	"errors"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

// ---------- 健康观测测试辅助 ----------

func healthPolicy() WavePolicy {
	return WavePolicy{
		MinSuccessRatio: 1,
		MaxFailures:     5,
		WaveTimeout:     time.Hour,
		Health: HealthPolicy{
			ObserveWindow:    10 * time.Minute,
			MinSamples:       2,
			FailureThreshold: 0.5,
		},
	}
}

func mustSample(t *testing.T, s *Service, releaseID, eventID, nodeID, digest string, healthy bool, at time.Time) {
	t.Helper()
	if err := s.RecordSample(releaseID, HealthSample{
		EventID: eventID, NodeID: nodeID, Digest: digest, Healthy: healthy, SampledAt: at,
	}); err != nil {
		t.Fatalf("record sample %s: %v", eventID, err)
	}
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

// ---------- 健康策略校验 ----------

func TestHealthPolicyValidation(t *testing.T) {
	s, _ := newTestService(t)
	d := mustRegister(t, s, "v1")

	for _, bad := range []WavePolicy{
		// 观察窗口为负。
		{MinSuccessRatio: 1, MaxFailures: 0, WaveTimeout: time.Hour, Health: HealthPolicy{ObserveWindow: -time.Second}},
		// 未启用观测（窗口为 0）却设置了其它健康参数。
		{MinSuccessRatio: 1, MaxFailures: 0, WaveTimeout: time.Hour, Health: HealthPolicy{MinSamples: 1}},
		{MinSuccessRatio: 1, MaxFailures: 0, WaveTimeout: time.Hour, Health: HealthPolicy{FailureThreshold: 0.5}},
		// 启用观测但参数非法。
		{MinSuccessRatio: 1, MaxFailures: 0, WaveTimeout: time.Hour, Health: HealthPolicy{ObserveWindow: time.Minute, MinSamples: 0, FailureThreshold: 0.5}},
		{MinSuccessRatio: 1, MaxFailures: 0, WaveTimeout: time.Hour, Health: HealthPolicy{ObserveWindow: time.Minute, MinSamples: 1, FailureThreshold: 0}},
		{MinSuccessRatio: 1, MaxFailures: 0, WaveTimeout: time.Hour, Health: HealthPolicy{ObserveWindow: time.Minute, MinSamples: 1, FailureThreshold: 1.1}},
	} {
		_, err := s.CreateRelease(CreateReleaseInput{ID: "x", Digest: d, Targets: []string{"n"}, WaveSize: 1, Policy: bad})
		if !errors.Is(err, ErrInvalidPolicy) {
			t.Fatalf("bad health policy %+v: %v", bad.Health, err)
		}
	}
}

// ---------- 观察窗口门禁 ----------

func TestObservationWindowGatesWaveAdvance(t *testing.T) {
	s, clk := newTestService(t)
	d := mustRegister(t, s, "v1")
	mustCreate(t, s, "rel", d, []string{"a", "b", "c", "d"}, 2, healthPolicy())

	// 第 0 波全部成功：进入观察状态，但不开放下一波。
	o := mustAck(t, s, "rel", "a", d, true)
	if o.WaveAdvanced {
		t.Fatal("wave must not advance before observation window passes")
	}
	o = mustAck(t, s, "rel", "b", d, true)
	if o.WaveAdvanced || o.CurrentWave != 0 {
		t.Fatalf("outcome = %+v", o)
	}
	p, _ := s.GetProgress("rel")
	if p.State != StateActive || p.CurrentWave != 0 {
		t.Fatalf("progress = %+v", p)
	}
	if p.Health == nil || !p.Health.Observing {
		t.Fatalf("health status = %+v", p.Health)
	}
	// 下一波节点仍取不到配置。
	if _, err := s.GetConfigForNode("rel", "c"); !errors.Is(err, ErrConfigNotAvailable) {
		t.Fatalf("next wave config during observation: %v", err)
	}

	// 窗口未走完：不推进。
	clk.advance(9 * time.Minute)
	if _, err := s.TickTimeout("rel"); err != nil {
		t.Fatal(err)
	}
	p, _ = s.GetProgress("rel")
	if p.CurrentWave != 0 {
		t.Fatalf("advanced before window passed: %+v", p)
	}

	// 窗口完整通过：开放下一波。
	clk.advance(2 * time.Minute)
	if _, err := s.TickTimeout("rel"); err != nil {
		t.Fatal(err)
	}
	p, _ = s.GetProgress("rel")
	if p.CurrentWave != 1 || p.State != StateActive {
		t.Fatalf("progress after window = %+v", p)
	}
	if p.Health.Observing {
		t.Fatalf("still observing after advance: %+v", p.Health)
	}

	// 事件流：恰好一条 wave.observing，两条 wave.opened。
	evs, _ := s.PendingEvents()
	if n := countEvents(evs, EventWaveObserving); n != 1 {
		t.Fatalf("wave.observing events = %d, want 1", n)
	}
	if n := countEvents(evs, EventWaveOpened); n != 2 {
		t.Fatalf("wave.opened events = %d, want 2", n)
	}
	assertLegalLifecycle(t, evs)
}

func TestObservationWindowCompletesRelease(t *testing.T) {
	s, clk := newTestService(t)
	d := mustRegister(t, s, "v1")
	mustCreate(t, s, "rel", d, []string{"a"}, 1, healthPolicy())
	mustAck(t, s, "rel", "a", d, true)

	p, _ := s.GetProgress("rel")
	if p.State != StateActive {
		t.Fatalf("completed before observation window: %s", p.State)
	}
	clk.advance(11 * time.Minute)
	if _, err := s.TickTimeout("rel"); err != nil {
		t.Fatal(err)
	}
	p, _ = s.GetProgress("rel")
	if p.State != StateCompleted {
		t.Fatalf("state = %s, want completed", p.State)
	}
}

// ---------- 健康样本：幂等、冲突、归属 ----------

func TestRecordSampleValidation(t *testing.T) {
	s, _ := newTestService(t)
	d := mustRegister(t, s, "v1")
	d2 := mustRegister(t, s, "v2")
	mustCreate(t, s, "rel", d, []string{"a", "b"}, 2, healthPolicy())
	now := time.Date(2026, 1, 1, 0, 1, 0, 0, time.UTC)

	if err := s.RecordSample("rel", HealthSample{NodeID: "a", Digest: d, Healthy: true, SampledAt: now}); !errors.Is(err, ErrInvalidSample) {
		t.Fatalf("empty event id: %v", err)
	}
	if err := s.RecordSample("nope", HealthSample{EventID: "e1", NodeID: "a", Digest: d, Healthy: true, SampledAt: now}); !errors.Is(err, ErrReleaseNotFound) {
		t.Fatalf("missing release: %v", err)
	}
	if err := s.RecordSample("rel", HealthSample{EventID: "e1", NodeID: "zzz", Digest: d, Healthy: true, SampledAt: now}); !errors.Is(err, ErrNodeNotInRelease) {
		t.Fatalf("missing node: %v", err)
	}
	// 跨配置使用：样本摘要与发布配置不一致。
	if err := s.RecordSample("rel", HealthSample{EventID: "e1", NodeID: "a", Digest: d2, Healthy: true, SampledAt: now}); !errors.Is(err, ErrDigestMismatch) {
		t.Fatalf("cross-config sample: %v", err)
	}
}

func TestSampleIdempotencyAndConflict(t *testing.T) {
	s, _ := newTestService(t)
	d := mustRegister(t, s, "v1")
	mustCreate(t, s, "rel", d, []string{"a", "b"}, 2, healthPolicy())
	mustCreate(t, s, "rel-2", d, []string{"x"}, 1, healthPolicy())
	now := time.Date(2026, 1, 1, 0, 1, 0, 0, time.UTC)
	sm := HealthSample{EventID: "e1", NodeID: "a", Digest: d, Healthy: true, SampledAt: now}

	// 相同事件重放：幂等。
	mustSample(t, s, "rel", "e1", "a", d, true, now)
	if err := s.RecordSample("rel", sm); err != nil {
		t.Fatalf("replay: %v", err)
	}
	p, _ := s.GetProgress("rel")
	if p.Health.Samples != 1 {
		t.Fatalf("samples after replay = %d, want 1", p.Health.Samples)
	}

	// 同号异内容：冲突。
	conflict := sm
	conflict.Healthy = false
	if err := s.RecordSample("rel", conflict); !errors.Is(err, ErrSampleConflict) {
		t.Fatalf("same id different content: %v", err)
	}
	conflict = sm
	conflict.SampledAt = now.Add(time.Second)
	if err := s.RecordSample("rel", conflict); !errors.Is(err, ErrSampleConflict) {
		t.Fatalf("same id different time: %v", err)
	}

	// 相同事件号不能跨发布混用：另一个发布有独立的样本空间。
	mustSample(t, s, "rel-2", "e1", "x", d, false, now)
	p2, _ := s.GetProgress("rel-2")
	if p2.Health.Samples != 1 || p2.Health.Unhealthy != 1 {
		t.Fatalf("rel-2 samples = %+v", p2.Health)
	}
	p, _ = s.GetProgress("rel")
	if p.Health.Samples != 1 || p.Health.Unhealthy != 0 {
		t.Fatalf("rel samples polluted: %+v", p.Health)
	}
}

func TestOutOfOrderSamplesCountedBySampleTime(t *testing.T) {
	s, clk := newTestService(t)
	d := mustRegister(t, s, "v1")
	mustCreate(t, s, "rel", d, []string{"a", "b"}, 2, healthPolicy())
	waveOpened := clk.Now()

	// 波次开放前的样本（属于上一轮观测/环境）不参与本次评估。
	mustSample(t, s, "rel", "old", "a", d, false, waveOpened.Add(-time.Minute))

	mustAck(t, s, "rel", "a", d, true)
	mustAck(t, s, "rel", "b", d, true) // 进入观察状态

	// 乱序到达：采样时间在窗口内的迟到样本仍然计入。
	clk.advance(5 * time.Minute)
	mustSample(t, s, "rel", "s2", "b", d, false, waveOpened.Add(2*time.Minute))
	mustSample(t, s, "rel", "s1", "a", d, true, waveOpened.Add(1*time.Minute))

	p, _ := s.GetProgress("rel")
	if p.Health.Samples != 2 || p.Health.Unhealthy != 1 {
		t.Fatalf("health evidence = %+v", p.Health)
	}
	if p.Health.UnhealthyRatio != 0.5 {
		t.Fatalf("ratio = %v", p.Health.UnhealthyRatio)
	}
}

// ---------- 样本不足不判定失败 ----------

func TestInsufficientSamplesDoesNotTriggerRollback(t *testing.T) {
	s, clk := newTestService(t)
	d := mustRegister(t, s, "v1")
	policy := healthPolicy()
	policy.Health.MinSamples = 3
	mustCreate(t, s, "rel", d, []string{"a", "b", "c"}, 2, policy)
	mustAck(t, s, "rel", "a", d, true)
	mustAck(t, s, "rel", "b", d, true)

	// 只有 1 条（不健康）样本，不足 MinSamples：不判定失败。
	mustSample(t, s, "rel", "s1", "a", d, false, clk.Now())
	p, _ := s.GetProgress("rel")
	if p.Health.Verdict != "insufficient_samples" {
		t.Fatalf("verdict = %s", p.Health.Verdict)
	}
	if p.State != StateActive {
		t.Fatalf("state = %s", p.State)
	}

	// 窗口走完即放行，开放下一波。
	clk.advance(11 * time.Minute)
	if _, err := s.TickTimeout("rel"); err != nil {
		t.Fatal(err)
	}
	p, _ = s.GetProgress("rel")
	if p.CurrentWave != 1 || p.State != StateActive {
		t.Fatalf("progress = %+v", p)
	}
}

// ---------- 自动回滚 ----------

// setupRollback 构造：v1 已在 a、b 上应用（rel-1 完成），rel-2 把 a、b 推进到 v2
// 并进入观察状态，随后不健康样本触发自动回滚。返回 v1/v2 摘要。
func setupRollback(t *testing.T, s *Service) (d1, d2 string) {
	t.Helper()
	d1 = mustRegister(t, s, "v1")
	d2 = mustRegister(t, s, "v2")
	// rel-1：a、b 应用 v1（不启用健康观测，直接完成）。
	mustCreate(t, s, "rel-1", d1, []string{"a", "b"}, 2, testPolicy())
	mustAck(t, s, "rel-1", "a", d1, true)
	mustAck(t, s, "rel-1", "b", d1, true)
	// rel-2：a、b、c 目标 v2，第 0 波 a、b 成功后进入观察。
	mustCreate(t, s, "rel-2", d2, []string{"a", "b", "c"}, 2, healthPolicy())
	mustAck(t, s, "rel-2", "a", d2, true)
	mustAck(t, s, "rel-2", "b", d2, true)
	// 2 条不健康样本（达到 MinSamples，占比 1.0 >= 0.5）触发回滚。
	mustSample(t, s, "rel-2", "s1", "a", d2, false, s.clock.Now())
	mustSample(t, s, "rel-2", "s2", "b", d2, false, s.clock.Now())
	return d1, d2
}

func TestHealthFailureTriggersRollbackPlanOnce(t *testing.T) {
	s, _ := newTestService(t)
	d1, d2 := setupRollback(t, s)

	p, _ := s.GetProgress("rel-2")
	if p.State != StateRollingBack {
		t.Fatalf("state = %s, want rolling_back", p.State)
	}
	// 查询展示阈值计算证据。
	if p.Health.Verdict != "failed" || p.Health.Samples != 2 || p.Health.Unhealthy != 2 {
		t.Fatalf("health = %+v", p.Health)
	}
	if p.Health.Threshold != 0.5 || p.Health.MinSamples != 2 {
		t.Fatalf("threshold params = %+v", p.Health)
	}
	// 回滚计划：每个已应用节点的前后版本。
	if p.Rollback == nil || p.Rollback.Total != 2 || p.Rollback.Done != 0 {
		t.Fatalf("rollback = %+v", p.Rollback)
	}
	for _, it := range p.Rollback.Items {
		if it.FromDigest != d2 || it.ToDigest != d1 {
			t.Fatalf("item %s from/to = %s/%s", it.NodeID, it.FromDigest, it.ToDigest)
		}
		if it.State != RollbackNotified {
			t.Fatalf("item %s state = %s", it.NodeID, it.State)
		}
	}

	// 回滚通知每个节点恰好一条，rollback_started 恰好一条。
	evs, _ := s.PendingEvents()
	if n := countEvents(evs, EventRollbackStarted); n != 1 {
		t.Fatalf("rollback_started = %d, want 1", n)
	}
	notify := map[string]int{}
	for _, ev := range evs {
		if ev.Type == EventNodeRollback {
			notify[ev.NodeID]++
			if ev.Digest != d1 {
				t.Fatalf("rollback notification digest = %s, want pre-release %s", ev.Digest, d1)
			}
		}
	}
	if len(notify) != 2 || notify["a"] != 1 || notify["b"] != 1 {
		t.Fatalf("rollback notifications = %v", notify)
	}

	// 回滚期间不会再开放新波次：c 取不到配置，回执被拒绝。
	if _, err := s.GetConfigForNode("rel-2", "c"); !errors.Is(err, ErrConfigNotAvailable) {
		t.Fatalf("config during rollback: %v", err)
	}
	if _, err := s.AckReceipt(Receipt{"rel-2", "c", d2, true}); !errors.Is(err, ErrNotActive) {
		t.Fatalf("receipt during rollback: %v", err)
	}
	// 回滚期间不能暂停/恢复。
	if err := s.Pause("rel-2"); !errors.Is(err, ErrRollbackInProgress) {
		t.Fatalf("pause during rollback: %v", err)
	}
	if err := s.Resume("rel-2"); !errors.Is(err, ErrRollbackInProgress) {
		t.Fatalf("resume during rollback: %v", err)
	}
	assertLegalLifecycle(t, evs)
}

func TestRollbackAckRestoresPreReleaseVersion(t *testing.T) {
	s, _ := newTestService(t)
	d1, d2 := setupRollback(t, s)

	// 目标摘要不符：拒绝。
	if err := s.AckRollback("rel-2", "a", d2); !errors.Is(err, ErrDigestMismatch) {
		t.Fatalf("wrong restore digest: %v", err)
	}
	// 不在计划中的节点：拒绝。
	if err := s.AckRollback("rel-2", "c", d1); !errors.Is(err, ErrNodeNotInRelease) {
		t.Fatalf("node not in plan: %v", err)
	}

	if err := s.AckRollback("rel-2", "a", d1); err != nil {
		t.Fatalf("ack a: %v", err)
	}
	// 重复确认幂等。
	if err := s.AckRollback("rel-2", "a", d1); err != nil {
		t.Fatalf("duplicate ack a: %v", err)
	}
	// 节点已应用版本恢复到发布前摘要。
	ns, err := s.NodeAppliedVersion("a")
	if err != nil || ns.AppliedDigest != d1 {
		t.Fatalf("node a version = %+v, %v", ns, err)
	}
	p, _ := s.GetProgress("rel-2")
	if p.Rollback.Done != 1 || p.Rollback.Complete {
		t.Fatalf("rollback progress = %+v", p.Rollback)
	}
	if p.State != StateRollingBack {
		t.Fatalf("state = %s", p.State)
	}

	// 全部确认后进入 RolledBack 终态，终态通知恰好一条。
	if err := s.AckRollback("rel-2", "b", d1); err != nil {
		t.Fatalf("ack b: %v", err)
	}
	p, _ = s.GetProgress("rel-2")
	if p.State != StateRolledBack || !p.Rollback.Complete {
		t.Fatalf("final = %s %+v", p.State, p.Rollback)
	}
	evs, _ := s.PendingEvents()
	if n := countEvents(evs, EventReleaseRolledBack); n != 1 {
		t.Fatalf("rolled_back events = %d, want 1", n)
	}
	if n := countEvents(evs, EventNodeRolledBack); n != 2 {
		t.Fatalf("node rolled back events = %d, want 2", n)
	}
	assertLegalLifecycle(t, evs)
}

func TestLateSuccessReceiptDoesNotOverrideRollback(t *testing.T) {
	s, _ := newTestService(t)
	d1, d2 := setupRollback(t, s)

	if err := s.AckRollback("rel-2", "a", d1); err != nil {
		t.Fatal(err)
	}
	if err := s.AckRollback("rel-2", "b", d1); err != nil {
		t.Fatal(err)
	}
	// 发布已 RolledBack：迟到成功回执不得覆盖回滚结果。
	// a 已有终态结果：幂等忽略；c 从未回执：终态拒绝。
	o, err := s.AckReceipt(Receipt{"rel-2", "a", d2, true})
	if err != nil || !o.Ignored {
		t.Fatalf("late receipt for a: %+v, %v", o, err)
	}
	if _, err := s.AckReceipt(Receipt{"rel-2", "c", d2, true}); !errors.Is(err, ErrTerminal) {
		t.Fatalf("late receipt for c: %v", err)
	}
	ns, _ := s.NodeAppliedVersion("a")
	if ns.AppliedDigest != d1 {
		t.Fatalf("rollback result overridden: %+v", ns)
	}
	p, _ := s.GetProgress("rel-2")
	if p.State != StateRolledBack || p.Rollback.Done != 2 {
		t.Fatalf("state mutated by late receipt: %+v", p)
	}
}

func TestCancelDuringRollingBack(t *testing.T) {
	s, _ := newTestService(t)
	setupRollback(t, s)

	if err := s.Cancel("rel-2"); err != nil {
		t.Fatalf("cancel during rollback: %v", err)
	}
	p, _ := s.GetProgress("rel-2")
	if p.State != StateCancelled {
		t.Fatalf("state = %s", p.State)
	}
	evs, _ := s.PendingEvents()
	// 回滚通知已覆盖全部成功节点：取消不再生成补偿通知。
	if n := countEvents(evs, EventNodeCompensation); n != 0 {
		t.Fatalf("compensation events during rollback cancel = %d, want 0", n)
	}
	if n := countEvents(evs, EventNodeRollback); n != 2 {
		t.Fatalf("rollback notifications = %d, want 2", n)
	}
	assertLegalLifecycle(t, evs)
}

// TestConcurrentRollbackCancelResume 自动回滚与人工取消/恢复/回执并发竞争时，
// 最终只形成一条单调状态序列，回滚通知与终态通知都不重复。
func TestConcurrentRollbackCancelResume(t *testing.T) {
	s, _ := newTestService(t)
	d1, d2 := setupRollback(t, s)

	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(5)
		go func() { defer wg.Done(); _ = s.Cancel("rel-2") }()
		go func() { defer wg.Done(); _ = s.Resume("rel-2") }()
		go func() { defer wg.Done(); _ = s.Pause("rel-2") }()
		go func() { defer wg.Done(); _ = s.AckRollback("rel-2", "a", d1) }()
		go func() { defer wg.Done(); _ = s.AckRollback("rel-2", "b", d1) }()
	}
	wg.Wait()

	p, err := s.GetProgress("rel-2")
	if err != nil {
		t.Fatal(err)
	}
	// 终态只能是 RolledBack（回滚完成）或 Cancelled（取消抢先）。
	if p.State != StateRolledBack && p.State != StateCancelled {
		t.Fatalf("final state = %s", p.State)
	}
	evs, _ := s.PendingEvents()
	assertLegalLifecycle(t, evs)
	if n := countEvents(evs, EventRollbackStarted); n != 1 {
		t.Fatalf("rollback_started = %d, want 1", n)
	}
	if n := countEvents(evs, EventNodeRollback); n != 2 {
		t.Fatalf("rollback notifications = %d, want 2", n)
	}
	if n := countEvents(evs, EventReleaseRolledBack); n > 1 {
		t.Fatalf("rolled_back events = %d", n)
	}
	if n := countEvents(evs, EventReleaseCancelled); n > 1 {
		t.Fatalf("cancel events = %d", n)
	}
	// 回滚期间不会出现新的开波事件（rel-2 只有第 0 波开放过一次）。
	var opens int
	for _, ev := range evs {
		if ev.Type == EventWaveOpened && ev.ReleaseID == "rel-2" {
			opens++
		}
	}
	if opens != 1 {
		t.Fatalf("rel-2 wave.opened = %d, want 1 (no new wave during rollback)", opens)
	}
	_ = d2
}

// ---------- 重启恢复 ----------

func TestRollbackSurvivesRestart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.json")
	clk := &offsetClock{t: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)}
	svc := NewService(mustFileStore(t, path), clk)
	d1, _ := setupRollback(t, svc)

	if err := svc.AckRollback("rel-2", "a", d1); err != nil {
		t.Fatal(err)
	}

	// 进程重启：回滚计划、逐项进度、样本证据全部恢复。
	svc2 := NewService(mustFileStore(t, path), clk)
	p, err := svc2.GetProgress("rel-2")
	if err != nil {
		t.Fatal(err)
	}
	if p.State != StateRollingBack || p.Rollback.Total != 2 || p.Rollback.Done != 1 {
		t.Fatalf("reloaded rollback = %+v %+v", p, p.Rollback)
	}
	if p.Health.Samples != 2 || p.Health.Unhealthy != 2 {
		t.Fatalf("reloaded health = %+v", p.Health)
	}

	// 重启后继续完成回滚；通知不重复。
	if err := svc2.AckRollback("rel-2", "b", d1); err != nil {
		t.Fatal(err)
	}
	p, _ = svc2.GetProgress("rel-2")
	if p.State != StateRolledBack {
		t.Fatalf("state = %s", p.State)
	}
	evs, _ := svc2.PendingEvents()
	if n := countEvents(evs, EventNodeRollback); n != 2 {
		t.Fatalf("rollback notifications after restart = %d, want 2", n)
	}
	if n := countEvents(evs, EventReleaseRolledBack); n != 1 {
		t.Fatalf("rolled_back events after restart = %d, want 1", n)
	}
	assertLegalLifecycle(t, evs)
}

// ---------- 查询展示 ----------

func TestProgressShowsHealthEvidenceAndRollback(t *testing.T) {
	s, _ := newTestService(t)
	d := mustRegister(t, s, "v1")
	policy := healthPolicy()
	policy.Health.MinSamples = 3
	mustCreate(t, s, "rel", d, []string{"a", "b"}, 2, policy)
	mustAck(t, s, "rel", "a", d, true)
	mustAck(t, s, "rel", "b", d, true)

	// 观察中、样本不足。
	p, _ := s.GetProgress("rel")
	if p.Health == nil || !p.Health.Enabled || !p.Health.Observing {
		t.Fatalf("health = %+v", p.Health)
	}
	if p.Health.Verdict != "insufficient_samples" || p.Health.WindowEndsAt.IsZero() {
		t.Fatalf("health = %+v", p.Health)
	}

	// 样本到达但未达失败阈值（1/3 不健康 < 0.5）。
	mustSample(t, s, "rel", "s1", "a", d, true, s.clock.Now())
	mustSample(t, s, "rel", "s2", "b", d, false, s.clock.Now())
	mustSample(t, s, "rel", "s3", "a", d, true, s.clock.Now())
	p, _ = s.GetProgress("rel")
	if p.Health.Verdict != "passing" || p.Health.Samples != 3 || p.Health.Unhealthy != 1 {
		t.Fatalf("health = %+v", p.Health)
	}
	if p.Health.UnhealthyRatio != 1.0/3.0 || p.Health.Threshold != 0.5 {
		t.Fatalf("threshold calc = %+v", p.Health)
	}
	if p.Rollback != nil {
		t.Fatalf("rollback should be nil before trigger: %+v", p.Rollback)
	}
}
