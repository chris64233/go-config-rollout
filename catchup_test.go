package configrollout

import (
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"
)

// ---------- 补跑测试辅助 ----------

// setupMultiWaveQuarantine 构造一个 2 波（每波 2 节点）发布，在观察期隔离
// 第 0 波的 a 与第 1 波的 c，并让发布正常走到 Completed。
// 返回 (service, clock, digest, releaseID)。
func setupMultiWaveQuarantine(t *testing.T, qp QuarantinePolicy) (*Service, *offsetClock, string, string) {
	t.Helper()
	s, clk := newTestService(t)
	d := mustRegister(t, s, "v1")
	hp := HealthPolicy{ObserveWindow: 30 * time.Minute, MinSamples: 1, MaxUnhealthy: 5}
	mustCreateWithQuarantine(t, s, "rel", d, []string{"a", "b", "c", "d"}, 2, hp, qp)

	// 第 0 波：a、b 成功；a 上报失败样本后隔离；b 健康，窗口届满推进。
	mustAck(t, s, "rel", "a", d, true)
	mustAck(t, s, "rel", "b", d, true)
	mustReport(t, s, clk, "e-a", "rel", "a", d, false)
	mustReport(t, s, clk, "e-b", "rel", "b", d, true)
	mustQuarantine(t, s, "rel", "a", []string{"e-a"})
	clk.advance(31 * time.Minute)
	p, _ := s.GetProgress("rel")
	if p.CurrentWave != 1 {
		t.Fatalf("setup: current wave = %d, want 1", p.CurrentWave)
	}

	// 第 1 波：c、d 成功；c 隔离；窗口届满发布完成。
	mustAck(t, s, "rel", "c", d, true)
	mustAck(t, s, "rel", "d", d, true)
	mustReport(t, s, clk, "e-c", "rel", "c", d, false)
	mustReport(t, s, clk, "e-d", "rel", "d", d, true)
	mustQuarantine(t, s, "rel", "c", []string{"e-c"})
	clk.advance(31 * time.Minute)
	p, _ = s.GetProgress("rel")
	if p.State != StateCompleted {
		t.Fatalf("setup: state = %s, want completed", p.State)
	}
	return s, clk, d, "rel"
}

func mustCreateCatchup(t *testing.T, s *Service, releaseID string) *CatchupPlan {
	t.Helper()
	plan, skipped, err := s.CreateCatchupPlan(releaseID)
	if err != nil {
		t.Fatalf("create catchup: %v", err)
	}
	if len(skipped) != 0 {
		t.Fatalf("unexpected skips: %+v", skipped)
	}
	return plan
}

func mustClaim(t *testing.T, s *Service, releaseID, worker string, ttl time.Duration) *CatchupLease {
	t.Helper()
	l, err := s.ClaimCatchup(releaseID, worker, ttl)
	if err != nil {
		t.Fatalf("claim catchup: %v", err)
	}
	return l
}

func mustComplete(t *testing.T, s *Service, res CatchupResult) *CatchupOutcome {
	t.Helper()
	o, err := s.CompleteCatchup(res)
	if err != nil {
		t.Fatalf("complete catchup: %v", err)
	}
	return o
}

// ---------- 创建计划：冻结起点、按波次排序 ----------

func TestCreateCatchupPlanFrozenAndOrdered(t *testing.T) {
	s, _, d, rel := setupMultiWaveQuarantine(t, testQuarantinePolicy())

	if _, _, err := s.CreateCatchupPlan("nope"); !errors.Is(err, ErrReleaseNotFound) {
		t.Fatalf("unknown release: %v", err)
	}
	plan := mustCreateCatchup(t, s, rel)
	if plan.Attempt != 1 || plan.State != CatchupPending {
		t.Fatalf("plan = %+v", plan)
	}
	// 波次升序。
	if len(plan.Waves) != 2 || plan.Waves[0] != 0 || plan.Waves[1] != 1 {
		t.Fatalf("waves = %v", plan.Waves)
	}
	// 只有隔离节点 a、c 入计划；从节点当前配置开始，目标为发布摘要。
	if len(plan.Entries) != 2 {
		t.Fatalf("entries = %d, want 2", len(plan.Entries))
	}
	ea, ec := plan.Entries["a"], plan.Entries["c"]
	if ea == nil || ec == nil || ea.Wave != 0 || ec.Wave != 1 {
		t.Fatalf("entries = %+v %+v", ea, ec)
	}
	if ea.TargetDigest != d || ec.TargetDigest != d {
		t.Fatalf("target digest wrong: %+v %+v", ea, ec)
	}
	if ea.StartDigest != d || ec.StartDigest != d {
		t.Fatalf("start digest should be captured current digest: %+v %+v", ea, ec)
	}
	if ea.BaselineSeq == 0 {
		t.Fatal("baseline seq not captured")
	}

	// 重复创建被拒。
	if _, _, err := s.CreateCatchupPlan(rel); !errors.Is(err, ErrCatchupActive) {
		t.Fatalf("double create: %v", err)
	}
	// 无隔离的发布不能创建。
	s2, _ := newTestService(t)
	d2 := mustRegister(t, s2, "v1")
	mustCreate(t, s2, "r2", d2, []string{"x"}, 1, testPolicy())
	if _, _, err := s2.CreateCatchupPlan("r2"); !errors.Is(err, ErrNoCatchup) {
		t.Fatalf("create without quarantine: %v", err)
	}

	evs, _ := s.PendingEvents()
	if n := countEvents(evs, EventCatchupCreated); n != 1 {
		t.Fatalf("catchup.created = %d, want 1", n)
	}
}

// ---------- 波次顺序领取与并发 ----------

func TestClaimCatchupWaveOrderAndConcurrency(t *testing.T) {
	s, _, d, rel := setupMultiWaveQuarantine(t, testQuarantinePolicy())
	mustCreateCatchup(t, s, rel)
	ttl := time.Minute

	// 首个领取的是第 0 波的 a。
	l1 := mustClaim(t, s, rel, "w1", ttl)
	if l1.NodeID != "a" || l1.Wave != 0 || l1.FromDigest != d || l1.ToDigest != d || l1.Epoch != 1 {
		t.Fatalf("lease1 = %+v", l1)
	}
	// a 租约有效期间，不能跳到第 1 波领取 c。
	if _, err := s.ClaimCatchup(rel, "w2", ttl); !errors.Is(err, ErrNoCatchupWork) {
		t.Fatalf("claim across in-flight wave: %v", err)
	}
	// 未领取者不能取配置；持有效租约可以。
	if _, err := s.GetCatchupConfig(rel, "c"); !errors.Is(err, ErrConfigNotAvailable) {
		t.Fatalf("get config without lease: %v", err)
	}
	cfg, err := s.GetCatchupConfig(rel, "a")
	if err != nil || cfg.Digest != d {
		t.Fatalf("get catchup config: %+v %v", cfg, err)
	}

	// a 成功后第 1 波开放给 c。
	o := mustComplete(t, s, CatchupResult{ReleaseID: rel, NodeID: "a", Epoch: l1.Epoch, Digest: d, Success: true})
	if o.Succeeded != 1 || o.Total != 2 {
		t.Fatalf("outcome = %+v", o)
	}
	l2 := mustClaim(t, s, rel, "w2", ttl)
	if l2.NodeID != "c" || l2.Wave != 1 || l2.Epoch != 1 {
		t.Fatalf("lease2 = %+v", l2)
	}

	// 并发领取同一节点只有一个成功（其它为 ErrNoCatchupWork）。
	// 场景：新计划含单节点，50 个 worker 并发。
	s3, _, d3, rel3 := func() (*Service, *offsetClock, string, string) {
		ss, cc, dd, rr := setupSingleQuarantine(t)
		return ss, cc, dd, rr
	}()
	_ = d3
	mustCreateCatchup(t, s3, rel3)
	var wg sync.WaitGroup
	var winners, nowork int64
	var mu sync.Mutex
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_, err := s3.ClaimCatchup(rel3, fmt.Sprintf("worker-%d", i), time.Minute)
			mu.Lock()
			defer mu.Unlock()
			switch {
			case err == nil:
				winners++
			case errors.Is(err, ErrNoCatchupWork):
				nowork++
			default:
				t.Errorf("unexpected claim error: %v", err)
			}
		}(i)
	}
	wg.Wait()
	if winners != 1 || winners+nowork != 50 {
		t.Fatalf("winners=%d nowork=%d", winners, nowork)
	}
}

// setupSingleQuarantine 构造单波单隔离节点且发布已完成的场景。
func setupSingleQuarantine(t *testing.T) (*Service, *offsetClock, string, string) {
	t.Helper()
	s, clk := newTestService(t)
	d := mustRegister(t, s, "v1")
	hp := HealthPolicy{ObserveWindow: 30 * time.Minute, MinSamples: 1, MaxUnhealthy: 5}
	mustCreateWithQuarantine(t, s, "rel", d, []string{"a", "b"}, 2, hp, testQuarantinePolicy())
	mustAck(t, s, "rel", "a", d, true)
	mustAck(t, s, "rel", "b", d, true)
	mustReport(t, s, clk, "e-a", "rel", "a", d, false)
	mustReport(t, s, clk, "e-b", "rel", "b", d, true)
	mustQuarantine(t, s, "rel", "a", []string{"e-a"})
	clk.advance(31 * time.Minute)
	p, _ := s.GetProgress("rel")
	if p.State != StateCompleted {
		t.Fatalf("setup: state = %s", p.State)
	}
	return s, clk, d, "rel"
}

// ---------- 租约过期重领与旧领取者迟到回报 ----------

func TestCatchupLeaseExpiryAndStaleEpoch(t *testing.T) {
	s, clk, d, rel := setupSingleQuarantine(t)
	mustCreateCatchup(t, s, rel)

	l1 := mustClaim(t, s, rel, "old-worker", time.Minute)
	// 租约未过期：同一 worker 重领只拿到 ErrNoCatchupWork。
	if _, err := s.ClaimCatchup(rel, "new-worker", time.Minute); !errors.Is(err, ErrNoCatchupWork) {
		t.Fatalf("reclaim within lease: %v", err)
	}
	// 旧领取者用旧 epoch 回报成功前，租约过期被新领取者拿走（epoch+1）。
	clk.advance(time.Minute + time.Second)
	l2 := mustClaim(t, s, rel, "new-worker", time.Minute)
	if l2.Epoch != l1.Epoch+1 {
		t.Fatalf("epoch = %d, want %d", l2.Epoch, l1.Epoch+1)
	}
	// 旧领取者迟到成功：epoch 不匹配，拒绝，不能覆盖新领取。
	if _, err := s.CompleteCatchup(CatchupResult{ReleaseID: rel, NodeID: "a", Epoch: l1.Epoch, Digest: d, Success: true}); !errors.Is(err, ErrClaimLost) {
		t.Fatalf("stale epoch success: %v", err)
	}
	// 新领取者用新 epoch 成功。
	o := mustComplete(t, s, CatchupResult{ReleaseID: rel, NodeID: "a", Epoch: l2.Epoch, Digest: d, Success: true})
	if o.State != CatchupDone || o.Succeeded != 1 {
		t.Fatalf("outcome = %+v", o)
	}
	// 重复成功幂等，不重复推进、不重复发通知。
	o2 := mustComplete(t, s, CatchupResult{ReleaseID: rel, NodeID: "a", Epoch: l2.Epoch, Digest: d, Success: true})
	if !o2.Ignored {
		t.Fatal("duplicate completion not ignored")
	}
	evs, _ := s.PendingEvents()
	if n := countEvents(evs, EventCatchupCompleted); n != 1 {
		t.Fatalf("catchup.completed = %d, want 1", n)
	}
	if n := countEvents(evs, EventNodeCatchupSucceeded); n != 1 {
		t.Fatalf("node.catchup_succeeded = %d, want 1", n)
	}
	if n := countEvents(evs, EventReleaseCompleted); n != 1 {
		t.Fatalf("release.completed = %d, catchup must not duplicate it", n)
	}
}

// 成功摘要必须等于目标摘要；未领取/错误节点回报被拒。
func TestCatchupCompleteValidation(t *testing.T) {
	s, _, d, rel := setupSingleQuarantine(t)
	mustCreateCatchup(t, s, rel)

	// 未领取不能回报。
	if _, err := s.CompleteCatchup(CatchupResult{ReleaseID: rel, NodeID: "a", Epoch: 1, Digest: d, Success: true}); !errors.Is(err, ErrCatchupNotClaimed) {
		t.Fatalf("complete without claim: %v", err)
	}
	l := mustClaim(t, s, rel, "w", time.Minute)
	// 错误摘要。
	if _, err := s.CompleteCatchup(CatchupResult{ReleaseID: rel, NodeID: "a", Epoch: l.Epoch, Digest: "deadbeef", Success: true}); !errors.Is(err, ErrDigestMismatch) {
		t.Fatalf("wrong digest: %v", err)
	}
	// 计划中不存在的节点。
	if _, err := s.CompleteCatchup(CatchupResult{ReleaseID: rel, NodeID: "zzz", Epoch: l.Epoch, Digest: d, Success: true}); !errors.Is(err, ErrCatchupEntryMissing) {
		t.Fatalf("unknown entry: %v", err)
	}
}

// ---------- 失败屏障与重建 ----------

func TestCatchupFailureBarrierAndRebuild(t *testing.T) {
	s, _, d, rel := setupMultiWaveQuarantine(t, testQuarantinePolicy())
	plan := mustCreateCatchup(t, s, rel)

	// a 失败 -> 第 1 波 c 被屏障挡住。
	la := mustClaim(t, s, rel, "w", time.Minute)
	if _, err := s.CompleteCatchup(CatchupResult{ReleaseID: rel, NodeID: "a", Epoch: la.Epoch, Digest: d, Success: false}); err != nil {
		t.Fatalf("report failure: %v", err)
	}
	if _, err := s.ClaimCatchup(rel, "w", time.Minute); !errors.Is(err, ErrCatchupBlocked) {
		t.Fatalf("claim past failure: %v", err)
	}
	// 重建：旧计划作废（plan_replaced），新计划排除已成功节点、保留失败项。
	plan2, _, err := s.RebuildCatchupPlan(rel)
	if err != nil {
		t.Fatalf("rebuild: %v", err)
	}
	if plan2.Attempt != plan.Attempt+1 {
		t.Fatalf("attempt = %d, want %d", plan2.Attempt, plan.Attempt+1)
	}
	if _, ok := plan2.Entries["a"]; !ok {
		t.Fatal("failed node a should be in rebuilt plan")
	}
	if _, ok := plan2.Entries["c"]; !ok {
		t.Fatal("pending node c should be in rebuilt plan")
	}
	cv, _ := s.GetCatchup(rel)
	if cv.State != CatchupPending || cv.AbortReason != "" {
		t.Fatalf("current view = %+v", cv)
	}
	evs, _ := s.PendingEvents()
	if n := countEvents(evs, EventCatchupAborted); n != 1 {
		t.Fatalf("catchup.aborted = %d, want 1", n)
	}
	if n := countEvents(evs, EventCatchupCreated); n != 2 {
		t.Fatalf("catchup.created = %d, want 2", n)
	}

	// 重建后有序跑完。
	la2 := mustClaim(t, s, rel, "w", time.Minute)
	if la2.NodeID != "a" {
		t.Fatalf("rebuilt first claim = %s, want a", la2.NodeID)
	}
	mustComplete(t, s, CatchupResult{ReleaseID: rel, NodeID: "a", Epoch: la2.Epoch, Digest: d, Success: true})
	lc := mustClaim(t, s, rel, "w", time.Minute)
	if lc.NodeID != "c" {
		t.Fatalf("second claim = %s, want c", lc.NodeID)
	}
	o := mustComplete(t, s, CatchupResult{ReleaseID: rel, NodeID: "c", Epoch: lc.Epoch, Digest: d, Success: true})
	if o.State != CatchupDone {
		t.Fatalf("outcome = %+v", o)
	}
	// 终态计划不可再领取/回报。
	if _, err := s.ClaimCatchup(rel, "w", time.Minute); !errors.Is(err, ErrCatchupNotActive) {
		t.Fatalf("claim done plan: %v", err)
	}
	// 隔离节点已标记修复；再次重建没有可补跑项。
	if _, _, err := s.RebuildCatchupPlan(rel); !errors.Is(err, ErrNoCatchupWork) {
		t.Fatalf("rebuild after all remediated: %v", err)
	}
}

// 已成功节点在重建时排除（只重跑未完成项）。
func TestCatchupRebuildExcludesSucceeded(t *testing.T) {
	s, _, d, rel := setupMultiWaveQuarantine(t, testQuarantinePolicy())
	mustCreateCatchup(t, s, rel)
	la := mustClaim(t, s, rel, "w", time.Minute)
	mustComplete(t, s, CatchupResult{ReleaseID: rel, NodeID: "a", Epoch: la.Epoch, Digest: d, Success: true})
	lc := mustClaim(t, s, rel, "w", time.Minute)
	if _, err := s.CompleteCatchup(CatchupResult{ReleaseID: rel, NodeID: "c", Epoch: lc.Epoch, Digest: d, Success: false}); err != nil {
		t.Fatal(err)
	}
	plan2, _, err := s.RebuildCatchupPlan(rel)
	if err != nil {
		t.Fatal(err)
	}
	if len(plan2.Entries) != 1 {
		t.Fatalf("rebuilt entries = %d, want 1", len(plan2.Entries))
	}
	if _, ok := plan2.Entries["c"]; !ok {
		t.Fatalf("rebuilt should only contain failed c: %+v", plan2.Entries)
	}
}

// ---------- 整体回滚作废补跑，旧领取者迟到成功不能覆盖 ----------

func TestCatchupAbortedByRollback(t *testing.T) {
	s, clk := newTestService(t)
	d := mustRegister(t, s, "v1")
	// MaxUnhealthy=0：隔离 a 后，b 的失败样本立即触发整体回滚。
	hp := HealthPolicy{ObserveWindow: time.Hour, MinSamples: 1, MaxUnhealthy: 0}
	qp := QuarantinePolicy{MaxQuarantined: 4, MinHealthyCoverage: 0.1}
	mustCreateWithQuarantine(t, s, "rel", d, []string{"a", "b"}, 2, hp, qp)
	mustAck(t, s, "rel", "a", d, true)
	mustAck(t, s, "rel", "b", d, true)
	// a 以健康样本为依据隔离（操作员决定），隔离后 a 移出健康统计。
	mustReport(t, s, clk, "e-a", "rel", "a", d, true)
	mustQuarantine(t, s, "rel", "a", []string{"e-a"})
	mustCreateCatchup(t, s, "rel")
	lease := mustClaim(t, s, "rel", "w", time.Hour)

	// b 失败样本 -> 不健康数 1 > 0 -> 整体回滚，补跑同事务作废。
	o := mustReport(t, s, clk, "e-b-bad", "rel", "b", d, false)
	if !o.RollbackStarted || o.State != StateRollingBack {
		t.Fatalf("health outcome = %+v", o)
	}
	cv, _ := s.GetCatchup("rel")
	if cv.State != CatchupAborted || cv.AbortReason != CatchupAbortRolledBack {
		t.Fatalf("catchup view = %+v", cv)
	}
	// 旧领取者迟到成功：计划已作废，拒绝，节点版本不被覆盖。
	before, _ := s.NodeAppliedVersion("a")
	if _, err := s.CompleteCatchup(CatchupResult{ReleaseID: "rel", NodeID: "a", Epoch: lease.Epoch, Digest: d, Success: true}); !errors.Is(err, ErrCatchupNotActive) {
		t.Fatalf("late success after rollback: %v", err)
	}
	after, _ := s.NodeAppliedVersion("a")
	if after.AppliedSeq != before.AppliedSeq {
		t.Fatalf("node version changed: before=%d after=%d", before.AppliedSeq, after.AppliedSeq)
	}
	// 计划已随回滚作废：领取返回“计划不再活跃”；发布本身也在回滚中。
	if _, err := s.ClaimCatchup("rel", "w", time.Minute); !errors.Is(err, ErrCatchupNotActive) {
		t.Fatalf("claim after rollback: %v", err)
	}
	evs, _ := s.PendingEvents()
	if n := countEvents(evs, EventCatchupCompleted); n != 0 {
		t.Fatalf("catchup.completed = %d, want 0", n)
	}
	if n := countEvents(evs, EventCatchupAborted); n != 1 {
		t.Fatalf("catchup.aborted = %d, want 1", n)
	}
	// 回滚计划包含隔离节点 a（整体回滚不豁免隔离）。
	hv, _ := s.GetHealth("rel")
	if hv.Rollback == nil || hv.Rollback.Total != 2 {
		t.Fatalf("rollback view = %+v", hv.Rollback)
	}
}

// 取消发布同样作废补跑（发布在观察期、仍 Active 时取消）。
func TestCatchupAbortedByCancel(t *testing.T) {
	s, clk := newTestService(t)
	d := mustRegister(t, s, "v1")
	hp := HealthPolicy{ObserveWindow: time.Hour, MinSamples: 1, MaxUnhealthy: 5}
	mustCreateWithQuarantine(t, s, "rel", d, []string{"a", "b"}, 2, hp, testQuarantinePolicy())
	mustAck(t, s, "rel", "a", d, true)
	mustAck(t, s, "rel", "b", d, true)
	mustReport(t, s, clk, "e-a", "rel", "a", d, false)
	mustReport(t, s, clk, "e-b", "rel", "b", d, true)
	mustQuarantine(t, s, "rel", "a", []string{"e-a"})
	p, _ := s.GetProgress("rel")
	if p.State != StateActive {
		t.Fatalf("setup state = %s, want active (observation window open)", p.State)
	}
	mustCreateCatchup(t, s, "rel")
	mustClaim(t, s, "rel", "w", time.Hour)
	if err := s.Cancel("rel"); err != nil {
		t.Fatalf("cancel: %v", err)
	}
	cv, _ := s.GetCatchup("rel")
	if cv.State != CatchupAborted {
		t.Fatalf("view = %+v", cv)
	}
}

// ---------- 被新发布取代 ----------

func TestCatchupAbortedByNewerRelease(t *testing.T) {
	s, _, _, rel := setupSingleQuarantine(t)
	mustCreateCatchup(t, s, rel)
	mustClaim(t, s, rel, "w", time.Hour)

	// 新发布把节点 a 推进到新配置：旧发布补跑基线失效。
	d2 := mustRegister(t, s, "v2")
	mustCreate(t, s, "rel-2", d2, []string{"a"}, 1, testPolicy())
	mustAck(t, s, "rel-2", "a", d2, true)

	cv, _ := s.GetCatchup(rel)
	if cv.State != CatchupAborted || cv.AbortReason != CatchupAbortSuperseded {
		t.Fatalf("view = %+v", cv)
	}
	if _, err := s.ClaimCatchup(rel, "w", time.Minute); !errors.Is(err, ErrCatchupNotActive) {
		t.Fatalf("claim superseded plan: %v", err)
	}
	// 新发布场景：创建计划时节点已被更新发布推进 -> 跳过该节点，无可补跑项。
	// （隔离记录仍在旧发布，但节点已不属于旧补跑范围。）
}

// 节点基线在计划创建后发生带外变化：领取即作废并拒绝执行。
func TestCatchupBaselineDriftRejectsClaim(t *testing.T) {
	s, _, _, rel := setupSingleQuarantine(t)
	plan := mustCreateCatchup(t, s, rel)
	base := plan.Entries["a"].BaselineSeq

	// 模拟带外基线变化（不产生更新发布；例如直接维护节点版本）。
	if err := s.store.mutate(func(st *State) error {
		st.AppliedSeq++
		ns := st.NodeStates["a"]
		ns.AppliedSeq = st.AppliedSeq
		ns.AppliedDigest = "fixed-out-of-band"
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if base == 0 {
		t.Fatal("baseline not captured")
	}
	if _, err := s.ClaimCatchup(rel, "w", time.Minute); !errors.Is(err, ErrBaselineChanged) {
		t.Fatalf("claim with drifted baseline: %v", err)
	}
	cv, _ := s.GetCatchup(rel)
	if cv.State != CatchupAborted || cv.AbortReason != CatchupAbortSuperseded {
		t.Fatalf("view = %+v", cv)
	}
}

// ---------- 查询视图 ----------

func TestGetCatchupView(t *testing.T) {
	s, _, d, rel := setupMultiWaveQuarantine(t, testQuarantinePolicy())
	// 无计划时查询。
	if _, err := s.GetCatchup(rel); !errors.Is(err, ErrNoCatchup) {
		t.Fatalf("view without plan: %v", err)
	}
	mustCreateCatchup(t, s, rel)

	cv, _ := s.GetCatchup(rel)
	if cv.Total != 2 || cv.State != CatchupPending || cv.Attempt != 1 {
		t.Fatalf("view = %+v", cv)
	}
	byNode := map[string]CatchupEntryView{}
	for _, e := range cv.Entries {
		byNode[e.NodeID] = e
	}
	ca, cc := byNode["a"], byNode["c"]
	if ca.Wave != 0 || cc.Wave != 1 {
		t.Fatalf("entries order: %+v %+v", ca, cc)
	}
	if ca.StartDigest != d || ca.TargetDigest != d || ca.CurrentDigest != d {
		t.Fatalf("a digests = %+v", ca)
	}
	// c 在前序波次未完成前被屏障挡住，视图给出原因。
	if cc.BlockedReason != "earlier_wave_unfinished" {
		t.Fatalf("c blocked reason = %q", cc.BlockedReason)
	}
	// a 可领取，无阻塞原因。
	if ca.BlockedReason != "" {
		t.Fatalf("a blocked reason = %q", ca.BlockedReason)
	}

	// 领取 a 后：c 仍被屏障；a 显示领取者与租约。
	la := mustClaim(t, s, rel, "w1", time.Minute)
	cv, _ = s.GetCatchup(rel)
	byNode = map[string]CatchupEntryView{}
	for _, e := range cv.Entries {
		byNode[e.NodeID] = e
	}
	if byNode["a"].ClaimedBy != "w1" || byNode["a"].Epoch != la.Epoch {
		t.Fatalf("a claimed view = %+v", byNode["a"])
	}

	// a 成功后：c 的屏障解除；进度更新。
	mustComplete(t, s, CatchupResult{ReleaseID: rel, NodeID: "a", Epoch: la.Epoch, Digest: d, Success: true})
	cv, _ = s.GetCatchup(rel)
	byNode = map[string]CatchupEntryView{}
	for _, e := range cv.Entries {
		byNode[e.NodeID] = e
	}
	if byNode["c"].BlockedReason != "" || byNode["a"].State != CatchupSucceeded {
		t.Fatalf("after a success: a=%+v c=%+v", byNode["a"], byNode["c"])
	}
	if cv.Succeeded != 1 {
		t.Fatalf("succeeded = %d", cv.Succeeded)
	}
}

// ---------- 重启恢复 ----------

func TestCatchupStateSurvivesRestart(t *testing.T) {
	path := mustTempFile(t)
	clk := &offsetClock{t: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)}
	svc := NewService(mustFileStore(t, path), clk)
	d := mustRegister(t, svc, "v1")
	hp := HealthPolicy{ObserveWindow: 30 * time.Minute, MinSamples: 1, MaxUnhealthy: 5}
	mustCreateWithQuarantine(t, svc, "rel", d, []string{"a", "b"}, 2, hp, testQuarantinePolicy())
	mustAck(t, svc, "rel", "a", d, true)
	mustAck(t, svc, "rel", "b", d, true)
	mustReport(t, svc, clk, "e-a", "rel", "a", d, false)
	mustReport(t, svc, clk, "e-b", "rel", "b", d, true)
	mustQuarantine(t, svc, "rel", "a", []string{"e-a"})
	clk.advance(31 * time.Minute)
	_, _ = svc.GetProgress("rel")
	plan, _, err := svc.CreateCatchupPlan("rel")
	if err != nil {
		t.Fatal(err)
	}
	lease := mustClaim(t, svc, "rel", "w", time.Hour)

	// 重启：计划、attempt、epoch、租约全部恢复。
	svc2 := NewService(mustFileStore(t, path), clk)
	cv, err := svc2.GetCatchup("rel")
	if err != nil || cv.Attempt != plan.Attempt {
		t.Fatalf("catchup after restart: %+v %v", cv, err)
	}
	// 旧 epoch 回报仍被正确校验；正确 epoch 成功。
	if _, err := svc2.CompleteCatchup(CatchupResult{ReleaseID: "rel", NodeID: "a", Epoch: lease.Epoch - 1, Digest: d, Success: true}); !errors.Is(err, ErrClaimLost) {
		t.Fatalf("stale epoch after restart: %v", err)
	}
	if _, err := svc2.CompleteCatchup(CatchupResult{ReleaseID: "rel", NodeID: "a", Epoch: lease.Epoch, Digest: d, Success: true}); err != nil {
		t.Fatalf("valid completion after restart: %v", err)
	}
	cv, _ = svc2.GetCatchup("rel")
	if cv.State != CatchupDone {
		t.Fatalf("state after restart completion: %+v", cv)
	}
	// 再重启：终态通知仍只一条，重复完成幂等。
	svc3 := NewService(mustFileStore(t, path), clk)
	if _, err := svc3.CompleteCatchup(CatchupResult{ReleaseID: "rel", NodeID: "a", Epoch: lease.Epoch, Digest: d, Success: true}); err != nil {
		t.Fatalf("idempotent after second restart: %v", err)
	}
	evs, _ := svc3.PendingEvents()
	if n := countEvents(evs, EventCatchupCompleted); n != 1 {
		t.Fatalf("catchup.completed = %d, want 1", n)
	}
}

// ---------- 补充边界 ----------

// 回滚中/取消后不能创建补跑计划。
func TestCreateCatchupRejectsTerminalOrRollback(t *testing.T) {
	s, clk := newTestService(t)
	d := mustRegister(t, s, "v1")
	hp := HealthPolicy{ObserveWindow: time.Hour, MinSamples: 1, MaxUnhealthy: 0}
	qp := QuarantinePolicy{MaxQuarantined: 4, MinHealthyCoverage: 0.1}
	mustCreateWithQuarantine(t, s, "rel", d, []string{"a", "b"}, 2, hp, qp)
	mustAck(t, s, "rel", "a", d, true)
	mustAck(t, s, "rel", "b", d, true)
	mustReport(t, s, clk, "e-a", "rel", "a", d, true)
	mustQuarantine(t, s, "rel", "a", []string{"e-a"})
	// b 失败样本 -> MaxUnhealthy=0 -> 回滚。
	mustReport(t, s, clk, "e-bad", "rel", "b", d, false)
	if _, _, err := s.CreateCatchupPlan("rel"); !errors.Is(err, ErrRollbackInProgress) {
		t.Fatalf("create during rollback: %v", err)
	}

	// 另一个发布：2 节点波、隔离其中 1 个（覆盖率 0.5 不回滚），Active 时取消。
	mustCreateWithQuarantine(t, s, "rel-2", d, []string{"c", "e"}, 2,
		HealthPolicy{ObserveWindow: time.Hour, MinSamples: 1, MaxUnhealthy: 5}, qp)
	mustAck(t, s, "rel-2", "c", d, true)
	mustAck(t, s, "rel-2", "e", d, true)
	mustReport(t, s, clk, "e-c", "rel-2", "c", d, false)
	mustReport(t, s, clk, "e-e", "rel-2", "e", d, true)
	mustQuarantine(t, s, "rel-2", "c", []string{"e-c"})
	if p, _ := s.GetProgress("rel-2"); p.State != StateActive {
		t.Fatalf("rel-2 state = %s, want active", p.State)
	}
	if err := s.Cancel("rel-2"); err != nil {
		t.Fatal(err)
	}
	if _, _, err := s.CreateCatchupPlan("rel-2"); !errors.Is(err, ErrTerminal) {
		t.Fatalf("create after cancel: %v", err)
	}
}

// 创建计划时节点已被更新发布推进 -> 跳过；全部被跳过时无可补跑项。
func TestCreateCatchupSkipsSupersededNodes(t *testing.T) {
	s, _, _, rel := setupSingleQuarantine(t)
	// 新发布 rel-2 把 a 推进到 v2。
	d2 := mustRegister(t, s, "v2")
	mustCreate(t, s, "rel-2", d2, []string{"a"}, 1, testPolicy())
	mustAck(t, s, "rel-2", "a", d2, true)

	_, skipped, err := s.CreateCatchupPlan(rel)
	if !errors.Is(err, ErrNoCatchupWork) {
		t.Fatalf("want ErrNoCatchupWork, got %v (skipped=%+v)", err, skipped)
	}
	if len(skipped) != 1 || skipped[0].Reason != catchupSkipSuperseded {
		t.Fatalf("skips = %+v", skipped)
	}
}

// GetCatchupConfig 的门禁：无计划/非计划节点/无租约拒绝。
func TestGetCatchupConfigGating(t *testing.T) {
	s, _, d, rel := setupMultiWaveQuarantine(t, testQuarantinePolicy())
	if _, err := s.GetCatchupConfig(rel, "a"); !errors.Is(err, ErrNoCatchup) {
		t.Fatalf("config without plan: %v", err)
	}
	mustCreateCatchup(t, s, rel)
	if _, err := s.GetCatchupConfig(rel, "zzz"); !errors.Is(err, ErrCatchupEntryMissing) {
		t.Fatalf("config unknown node: %v", err)
	}
	if _, err := s.GetCatchupConfig(rel, "a"); !errors.Is(err, ErrConfigNotAvailable) {
		t.Fatalf("config without lease: %v", err)
	}
	l := mustClaim(t, s, rel, "w", time.Minute)
	cfg, err := s.GetCatchupConfig(rel, "a")
	if err != nil || cfg.Digest != d {
		t.Fatalf("config with lease: %+v %v", cfg, err)
	}
	_ = l
}

// GetHealth 内嵌隔离与补跑视图。
func TestGetHealthEmbedsQuarantineAndCatchup(t *testing.T) {
	s, _, d, rel := setupMultiWaveQuarantine(t, testQuarantinePolicy())
	hv, _ := s.GetHealth(rel)
	if hv.Quarantine == nil {
		t.Fatal("quarantine view missing")
	}
	if hv.Quarantine.TotalQuarantined != 2 {
		t.Fatalf("quarantined = %d, want 2", hv.Quarantine.TotalQuarantined)
	}
	if hv.Catchup != nil {
		t.Fatal("catchup should be nil before creation")
	}
	mustCreateCatchup(t, s, rel)
	hv, _ = s.GetHealth(rel)
	if hv.Catchup == nil || hv.Catchup.Total != 2 {
		t.Fatalf("embedded catchup = %+v", hv.Catchup)
	}
	// 隔离节点的实际配置（发布摘要）与节点版本一致。
	for _, n := range hv.Nodes {
		if n.Quarantined && n.CurrentDigest != d {
			t.Fatalf("quarantined node %s current = %s", n.NodeID, n.CurrentDigest)
		}
	}
}

// 领取后、回报前节点基线发生变化：旧领取者的迟到成功不得覆盖，
// 计划在该事务内作废（ErrBaselineChanged），作废状态被持久化。
func TestCatchupBaselineDriftDuringLeaseRejectsCompletion(t *testing.T) {
	s, _, d, rel := setupSingleQuarantine(t)
	plan := mustCreateCatchup(t, s, rel)
	lease := mustClaim(t, s, rel, "w", time.Hour)

	// 领取之后节点被带外推进到其它摘要。
	if err := s.store.mutate(func(st *State) error {
		st.AppliedSeq++
		ns := st.NodeStates["a"]
		ns.AppliedSeq = st.AppliedSeq
		ns.AppliedDigest = "fixed-out-of-band"
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.CompleteCatchup(CatchupResult{ReleaseID: rel, NodeID: "a", Epoch: lease.Epoch, Digest: d, Success: true}); !errors.Is(err, ErrBaselineChanged) {
		t.Fatalf("completion after drift: %v", err)
	}
	cv, _ := s.GetCatchup(rel)
	if cv.State != CatchupAborted || cv.AbortReason != CatchupAbortSuperseded {
		t.Fatalf("view = %+v", cv)
	}
	// 再次查询/回报，计划保持作废（不会重复写 aborted，也不会复活）。
	if _, err := s.CompleteCatchup(CatchupResult{ReleaseID: rel, NodeID: "a", Epoch: lease.Epoch, Digest: d, Success: true}); !errors.Is(err, ErrCatchupNotActive) {
		t.Fatalf("completion on aborted: %v", err)
	}
	evs, _ := s.PendingEvents()
	if n := countEvents(evs, EventCatchupAborted); n != 1 {
		t.Fatalf("aborted events = %d, want 1", n)
	}
	_ = plan
}

// 已失败的补跑项在重建之前不能再回报（形成屏障，状态只向前）。
func TestCatchupFailedEntryRejectsCompletionUntilRebuild(t *testing.T) {
	s, _, d, rel := setupMultiWaveQuarantine(t, testQuarantinePolicy())
	mustCreateCatchup(t, s, rel)
	la := mustClaim(t, s, rel, "w", time.Minute)
	if _, err := s.CompleteCatchup(CatchupResult{ReleaseID: rel, NodeID: "a", Epoch: la.Epoch, Digest: d, Success: false}); err != nil {
		t.Fatal(err)
	}
	// 对失败项再回报成功：被拒，不能复活。
	if _, err := s.CompleteCatchup(CatchupResult{ReleaseID: rel, NodeID: "a", Epoch: la.Epoch, Digest: d, Success: true}); !errors.Is(err, ErrCatchupNotActive) {
		t.Fatalf("complete failed entry: %v", err)
	}
	// 重建后可领取并成功。
	_, _, err := s.RebuildCatchupPlan(rel)
	if err != nil {
		t.Fatal(err)
	}
	la2 := mustClaim(t, s, rel, "w", time.Minute)
	mustComplete(t, s, CatchupResult{ReleaseID: rel, NodeID: "a", Epoch: la2.Epoch, Digest: d, Success: true})
}

// 隔离依据去重排序；暂停期隔离后视图显示越界但发布仍暂停（恢复时结算）。
func TestQuarantineBasisDedupAndPausedBreachView(t *testing.T) {
	s, clk := newTestService(t)
	d := mustRegister(t, s, "v1")
	hp := HealthPolicy{ObserveWindow: time.Hour, MinSamples: 1, MaxUnhealthy: 5}
	qp := QuarantinePolicy{MaxQuarantined: 1, MinHealthyCoverage: 0.9}
	mustCreateWithQuarantine(t, s, "rel", d, []string{"a", "b", "c"}, 3, hp, qp)
	for _, n := range []string{"a", "b", "c"} {
		mustAck(t, s, "rel", n, d, true)
	}
	mustReport(t, s, clk, "e-a-1", "rel", "a", d, false)
	mustReport(t, s, clk, "e-a-2", "rel", "a", d, false)
	if err := s.Pause("rel"); err != nil {
		t.Fatal(err)
	}
	// 依据乱序 + 重复传入；落库的 basis 去重排序。
	o, err := s.QuarantineNode(QuarantineInput{
		ReleaseID: "rel", NodeID: "a",
		Basis: []string{"e-a-2", "e-a-1", "e-a-2", ""},
	})
	if err != nil || o.State != StatePaused {
		t.Fatalf("quarantine paused: %+v %v", o, err)
	}
	hv, _ := s.GetHealth("rel")
	got := hv.Quarantine.Records[0].Basis
	if len(got) != 2 || got[0] != "e-a-1" || got[1] != "e-a-2" {
		t.Fatalf("basis = %v", got)
	}
	// 暂停期不结算，发布仍 paused；视图标记按冻结限制已越界。
	if hv.State != StatePaused || !hv.Quarantine.LimitBreached {
		t.Fatalf("state=%s breached=%v", hv.State, hv.Quarantine.LimitBreached)
	}
}

// 重建并发：旧领取者持有租约期间计划被重建（plan_replaced），其迟到成功
// 不能写入新计划，也不能重复写出终态通知；新计划可正常领取跑完。
func TestCatchupOldClaimRejectedAfterRebuild(t *testing.T) {
	s, _, d, rel := setupSingleQuarantine(t)
	mustCreateCatchup(t, s, rel)
	oldLease := mustClaim(t, s, rel, "w-old", time.Hour)

	// 旧计划仍活跃时重建：旧计划 aborted（plan_replaced），新计划 pending。
	if _, _, err := s.RebuildCatchupPlan(rel); err != nil {
		t.Fatalf("rebuild: %v", err)
	}
	cv, _ := s.GetCatchup(rel)
	if cv.State != CatchupPending {
		t.Fatalf("new plan state = %s, want pending", cv.State)
	}

	// 旧领取者的迟到成功：新计划中的项尚未领取，回报被拒且不污染新计划。
	if _, err := s.CompleteCatchup(CatchupResult{
		ReleaseID: rel, NodeID: "a", Epoch: oldLease.Epoch, Digest: d, Success: true,
	}); !errors.Is(err, ErrCatchupNotClaimed) {
		t.Fatalf("old claim success after rebuild: %v", err)
	}
	cv, _ = s.GetCatchup(rel)
	if cv.State != CatchupPending || cv.Entries[0].State != CatchupPending {
		t.Fatalf("new plan corrupted by stale completion: %+v", cv)
	}
	evs, _ := s.PendingEvents()
	if n := countEvents(evs, EventCatchupAborted); n != 1 {
		t.Fatalf("catchup.aborted = %d, want 1", n)
	}
	if n := countEvents(evs, EventNodeCatchupSucceeded); n != 0 {
		t.Fatalf("stale success wrote node.catchup_succeeded: %d", n)
	}

	// 新计划从 epoch 1 重新领取，正常跑完并只写一次终态通知。
	newLease := mustClaim(t, s, rel, "w-new", time.Hour)
	if newLease.Attempt != 2 || newLease.Epoch != 1 {
		t.Fatalf("new lease = %+v, want attempt 2 epoch 1", newLease)
	}
	o := mustComplete(t, s, CatchupResult{
		ReleaseID: rel, NodeID: "a", Epoch: newLease.Epoch, Digest: d, Success: true,
	})
	if o.State != CatchupDone {
		t.Fatalf("outcome = %+v", o)
	}
	evs, _ = s.PendingEvents()
	if n := countEvents(evs, EventCatchupCompleted); n != 1 {
		t.Fatalf("catchup.completed = %d, want 1", n)
	}
}

// 发布走到终态 RolledBack 后，持有租约的旧补跑领取者的迟到成功仍被拒绝，
// 计划保持作废，节点版本不被覆盖，终态通知不被重复写出。
func TestCatchupLateCompletionAfterRolledBackTerminal(t *testing.T) {
	s, clk := newTestService(t)
	d := mustRegister(t, s, "v1")
	// MaxUnhealthy=0：隔离 a 后，b 的失败样本立即整体回滚。
	hp := HealthPolicy{ObserveWindow: time.Hour, MinSamples: 1, MaxUnhealthy: 0}
	qp := QuarantinePolicy{MaxQuarantined: 4, MinHealthyCoverage: 0.1}
	mustCreateWithQuarantine(t, s, "rel", d, []string{"a", "b"}, 2, hp, qp)
	mustAck(t, s, "rel", "a", d, true)
	mustAck(t, s, "rel", "b", d, true)
	mustReport(t, s, clk, "e-a", "rel", "a", d, true)
	mustQuarantine(t, s, "rel", "a", []string{"e-a"})
	mustCreateCatchup(t, s, "rel")
	lease := mustClaim(t, s, "rel", "w", time.Hour)

	// b 失败样本触发整体回滚（补跑同事务作废），随后两个节点确认恢复，
	// 发布落到终态 RolledBack。
	mustReport(t, s, clk, "e-bad", "rel", "b", d, false)
	if _, err := s.ConfirmRollback("rel", "a", ""); err != nil {
		t.Fatalf("confirm rollback a: %v", err)
	}
	if _, err := s.ConfirmRollback("rel", "b", ""); err != nil {
		t.Fatalf("confirm rollback b: %v", err)
	}
	p, _ := s.GetProgress("rel")
	if p.State != StateRolledBack {
		t.Fatalf("state = %s, want rolled_back", p.State)
	}

	// 旧领取者迟到成功：计划已作废，拒绝；节点版本不被覆盖。
	before, _ := s.NodeAppliedVersion("a")
	if _, err := s.CompleteCatchup(CatchupResult{
		ReleaseID: "rel", NodeID: "a", Epoch: lease.Epoch, Digest: d, Success: true,
	}); !errors.Is(err, ErrCatchupNotActive) {
		t.Fatalf("late success after rolled_back: %v", err)
	}
	after, _ := s.NodeAppliedVersion("a")
	if after.AppliedSeq != before.AppliedSeq || after.AppliedDigest != before.AppliedDigest {
		t.Fatalf("node version overwritten: before=%+v after=%+v", before, after)
	}
	if _, err := s.ClaimCatchup("rel", "w", time.Hour); !errors.Is(err, ErrCatchupNotActive) {
		t.Fatalf("claim on rolled-back release: %v", err)
	}
	evs, _ := s.PendingEvents()
	if n := countEvents(evs, EventCatchupAborted); n != 1 {
		t.Fatalf("catchup.aborted = %d, want 1", n)
	}
	if n := countEvents(evs, EventCatchupCompleted); n != 0 {
		t.Fatalf("catchup.completed = %d, want 0", n)
	}
	if n := countEvents(evs, EventReleaseRolledBack); n != 1 {
		t.Fatalf("release.rolledback = %d, want 1", n)
	}
}
