package configrollout

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

// ---------- 测试辅助 ----------

func testConfig(content string) (digest string, data []byte) {
	data = []byte(content)
	d := digestOf(data)
	return d, data
}

func newTestService(t *testing.T) (*Service, *offsetClock) {
	t.Helper()
	clk := &offsetClock{t: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)}
	return NewService(NewMemoryStore(), clk), clk
}

func mustRegister(t *testing.T, s *Service, content string) string {
	t.Helper()
	d, data := testConfig(content)
	if _, err := s.RegisterConfig(d, data); err != nil {
		t.Fatalf("register config: %v", err)
	}
	return d
}

func mustCreate(t *testing.T, s *Service, id, digest string, nodes []string, size int, policy WavePolicy) *Release {
	t.Helper()
	r, err := s.CreateRelease(CreateReleaseInput{ID: id, Digest: digest, Targets: nodes, WaveSize: size, Policy: policy})
	if err != nil {
		t.Fatalf("create release: %v", err)
	}
	return r
}

func testPolicy() WavePolicy {
	return WavePolicy{MinSuccessRatio: 1, MaxFailures: 0, WaveTimeout: time.Hour}
}

func mustAck(t *testing.T, s *Service, releaseID, nodeID, digest string, success bool) *ReceiptOutcome {
	t.Helper()
	o, err := s.AckReceipt(Receipt{ReleaseID: releaseID, NodeID: nodeID, Digest: digest, Success: success})
	if err != nil {
		t.Fatalf("ack receipt %s: %v", nodeID, err)
	}
	return o
}

// ---------- 配置登记 ----------

func TestRegisterConfig(t *testing.T) {
	s, _ := newTestService(t)
	d, data := testConfig("v1")

	if _, err := s.RegisterConfig("deadbeef", data); !errors.Is(err, ErrInvalidDigest) {
		t.Fatalf("want ErrInvalidDigest, got %v", err)
	}

	cfg, err := s.RegisterConfig(d, data)
	if err != nil {
		t.Fatalf("register: %v", err)
	}
	if cfg.Digest != d {
		t.Fatalf("digest mismatch")
	}

	// 相同摘要 + 相同内容：幂等。
	if _, err := s.RegisterConfig(d, append([]byte(nil), data...)); err != nil {
		t.Fatalf("idempotent register: %v", err)
	}
	// 相同摘要 + 不同内容：冲突（摘要本身由 sha256 保证，这里直接构造不可能的冲突，
	// 改为验证非法摘要路径已覆盖，冲突分支通过直接写库验证）。
	stErr := s.store.mutate(func(st *State) error {
		st.Configs[d].Content = []byte("tampered")
		return nil
	})
	if stErr != nil {
		t.Fatal(stErr)
	}
	if _, err := s.RegisterConfig(d, data); !errors.Is(err, ErrConfigExists) {
		t.Fatalf("want ErrConfigExists, got %v", err)
	}
}

// ---------- 创建发布：冻结与分波 ----------

func TestCreateReleaseFreezesTargetsAndSplitsWaves(t *testing.T) {
	s, _ := newTestService(t)
	d := mustRegister(t, s, "v1")
	nodes := []string{"n1", "n2", "n3", "n4", "n5"}

	r := mustCreate(t, s, "rel-1", d, nodes, 2, testPolicy())

	want := [][]string{{"n1", "n2"}, {"n3", "n4"}, {"n5"}}
	if len(r.Waves) != len(want) {
		t.Fatalf("waves = %v, want %v", r.Waves, want)
	}
	for i := range want {
		if len(r.Waves[i]) != len(want[i]) {
			t.Fatalf("wave %d = %v, want %v", i, r.Waves[i], want[i])
		}
		for j := range want[i] {
			if r.Waves[i][j] != want[i][j] {
				t.Fatalf("wave %d = %v, want %v", i, r.Waves[i], want[i])
			}
		}
	}
	// 每个节点恰好出现一次。
	counts := map[string]int{}
	for _, w := range r.Waves {
		for _, n := range w {
			counts[n]++
		}
	}
	for _, n := range nodes {
		if counts[n] != 1 {
			t.Fatalf("node %s appears %d times", n, counts[n])
		}
	}
	// 第 0 波已开放，其余为 pending。
	for _, n := range want[0] {
		if r.Nodes[n].Result != ResultInProgress {
			t.Fatalf("wave0 node %s = %s", n, r.Nodes[n].Result)
		}
	}
	for _, w := range want[1:] {
		for _, n := range w {
			if r.Nodes[n].Result != ResultPending {
				t.Fatalf("future node %s = %s", n, r.Nodes[n].Result)
			}
		}
	}
	if r.RequiredSuccess[0] != 2 || r.RequiredSuccess[2] != 1 {
		t.Fatalf("required = %v", r.RequiredSuccess)
	}

	// 冻结：调用后修改入参切片不影响发布。
	nodes[0] = "MUTATED"
	p, err := s.GetProgress("rel-1")
	if err != nil {
		t.Fatal(err)
	}
	if p.Waves[0].Total != 2 {
		t.Fatalf("release not frozen: %+v", p.Waves[0])
	}
}

func TestCreateReleaseValidation(t *testing.T) {
	s, _ := newTestService(t)
	d := mustRegister(t, s, "v1")

	_, err := s.CreateRelease(CreateReleaseInput{ID: "", Digest: d, Targets: []string{"n"}, WaveSize: 1, Policy: testPolicy()})
	if !errors.Is(err, ErrInvalidReleaseID) {
		t.Fatalf("empty id: %v", err)
	}
	_, err = s.CreateRelease(CreateReleaseInput{ID: "x", Digest: "sha256:" + d, Targets: []string{"n"}, WaveSize: 1, Policy: testPolicy()})
	if !errors.Is(err, ErrConfigNotFound) {
		t.Fatalf("unknown digest: %v", err)
	}
	_, err = s.CreateRelease(CreateReleaseInput{ID: "x", Digest: d, Targets: []string{"n"}, WaveSize: 0, Policy: testPolicy()})
	if !errors.Is(err, ErrInvalidWaveSize) {
		t.Fatalf("wave size: %v", err)
	}
	_, err = s.CreateRelease(CreateReleaseInput{ID: "x", Digest: d, Targets: nil, WaveSize: 1, Policy: testPolicy()})
	if !errors.Is(err, ErrEmptyTargets) {
		t.Fatalf("empty targets: %v", err)
	}
	_, err = s.CreateRelease(CreateReleaseInput{ID: "x", Digest: d, Targets: []string{"n", "n"}, WaveSize: 1, Policy: testPolicy()})
	if !errors.Is(err, ErrDuplicateNode) {
		t.Fatalf("dup node: %v", err)
	}
	for _, bad := range []WavePolicy{
		{MinSuccessRatio: 0, MaxFailures: 0, WaveTimeout: time.Second},
		{MinSuccessRatio: 1.1, MaxFailures: 0, WaveTimeout: time.Second},
		{MinSuccessRatio: 1, MaxFailures: -1, WaveTimeout: time.Second},
		{MinSuccessRatio: 1, MaxFailures: 0, WaveTimeout: 0},
	} {
		if _, err := s.CreateRelease(CreateReleaseInput{ID: "x", Digest: d, Targets: []string{"n"}, WaveSize: 1, Policy: bad}); !errors.Is(err, ErrInvalidPolicy) {
			t.Fatalf("bad policy %+v: %v", bad, err)
		}
	}
	mustCreate(t, s, "dup", d, []string{"n"}, 1, testPolicy())
	if _, err := s.CreateRelease(CreateReleaseInput{ID: "dup", Digest: d, Targets: []string{"n"}, WaveSize: 1, Policy: testPolicy()}); !errors.Is(err, ErrReleaseExists) {
		t.Fatalf("dup release: %v", err)
	}
}

// ---------- 波次推进 ----------

func TestWaveProgression(t *testing.T) {
	s, _ := newTestService(t)
	d := mustRegister(t, s, "v1")
	// 2 波、每波 2 节点，门槛 50% -> 每波需要 1 个成功即可推进。
	policy := WavePolicy{MinSuccessRatio: 0.5, MaxFailures: 1, WaveTimeout: time.Hour}
	r := mustCreate(t, s, "rel", d, []string{"a", "b", "c", "d"}, 2, policy)
	if r.RequiredSuccess[0] != 1 {
		t.Fatalf("required = %v", r.RequiredSuccess)
	}

	// 未来波次节点在波次开放前既不能确认也取不到配置。
	if _, err := s.AckReceipt(Receipt{"rel", "c", d, true}); !errors.Is(err, ErrWaveNotOpen) {
		t.Fatalf("future wave ack: %v", err)
	}
	if _, err := s.GetConfigForNode("rel", "c"); !errors.Is(err, ErrConfigNotAvailable) {
		t.Fatalf("future wave config: %v", err)
	}

	// 第 0 波 1 个成功即达标，原子开放第 1 波。
	o := mustAck(t, s, "rel", "a", d, true)
	if !o.WaveAdvanced || o.CurrentWave != 1 || o.State != StateActive {
		t.Fatalf("outcome = %+v", o)
	}

	// 旧波次的迟到成功回执：节点 a 已成功 -> 幂等忽略；未成功的 b 已不在当前波 -> 拒绝。
	o = mustAck(t, s, "rel", "a", d, true)
	if !o.Ignored {
		t.Fatalf("duplicate success should be ignored, got %+v", o)
	}
	if _, err := s.AckReceipt(Receipt{"rel", "b", d, true}); !errors.Is(err, ErrWaveNotOpen) {
		t.Fatalf("late old-wave ack: %v", err)
	}

	// 第 1 波达标 -> 发布完成。
	o = mustAck(t, s, "rel", "c", d, true)
	if !o.WaveAdvanced || o.State != StateCompleted {
		t.Fatalf("outcome = %+v", o)
	}
	if _, err := s.AckReceipt(Receipt{"rel", "d", d, true}); !errors.Is(err, ErrTerminal) {
		t.Fatalf("ack after complete: %v", err)
	}

	p, err := s.GetProgress("rel")
	if err != nil {
		t.Fatal(err)
	}
	if p.State != StateCompleted || p.TotalSucceeded != 2 || p.TotalFailed != 0 {
		t.Fatalf("progress = %+v", p)
	}
	// 节点 d 未成功，整个发布仍按门槛完成。
	if p.Waves[1].Succeeded != 1 || p.Waves[1].InProgress != 1 {
		t.Fatalf("wave1 = %+v", p.Waves[1])
	}
}

func TestReceiptValidationAndIdempotency(t *testing.T) {
	s, _ := newTestService(t)
	d := mustRegister(t, s, "v1")
	d2 := mustRegister(t, s, "v2")
	policy := WavePolicy{MinSuccessRatio: 1, MaxFailures: 5, WaveTimeout: time.Hour}
	mustCreate(t, s, "rel", d, []string{"a", "b", "c"}, 3, policy)

	if _, err := s.AckReceipt(Receipt{"nope", "a", d, true}); !errors.Is(err, ErrReleaseNotFound) {
		t.Fatalf("missing release: %v", err)
	}
	if _, err := s.AckReceipt(Receipt{"rel", "zzz", d, true}); !errors.Is(err, ErrNodeNotInRelease) {
		t.Fatalf("missing node: %v", err)
	}
	if _, err := s.AckReceipt(Receipt{"rel", "a", d2, true}); !errors.Is(err, ErrDigestMismatch) {
		t.Fatalf("digest mismatch: %v", err)
	}

	// 乱序：先报失败再报成功 —— 失败先落定，成功回执被幂等忽略，结果保持 failed。
	mustAck(t, s, "rel", "a", d, false)
	if o := mustAck(t, s, "rel", "a", d, true); !o.Ignored {
		t.Fatalf("success after fail should be ignored: %+v", o)
	}
	// 先报成功再报失败 —— 成功保持。
	mustAck(t, s, "rel", "b", d, true)
	if o := mustAck(t, s, "rel", "b", d, false); !o.Ignored {
		t.Fatalf("fail after success should be ignored: %+v", o)
	}
	p, _ := s.GetProgress("rel")
	if p.Waves[0].Succeeded != 1 || p.Waves[0].Failed != 1 {
		t.Fatalf("counts = %+v", p.Waves[0])
	}

	ns, err := s.NodeAppliedVersion("b")
	if err != nil || ns.AppliedDigest != d {
		t.Fatalf("applied version: %+v %v", ns, err)
	}
}

// ---------- 失败阈值 ----------

func TestFailureThresholdPauses(t *testing.T) {
	s, _ := newTestService(t)
	d := mustRegister(t, s, "v1")
	policy := WavePolicy{MinSuccessRatio: 1, MaxFailures: 1, WaveTimeout: time.Hour}
	mustCreate(t, s, "rel", d, []string{"a", "b", "c"}, 3, policy)

	mustAck(t, s, "rel", "a", d, false) // 1 个失败，未超过阈值（>1 才暂停）
	p, _ := s.GetProgress("rel")
	if p.State != StateActive {
		t.Fatalf("state = %s", p.State)
	}
	mustAck(t, s, "rel", "b", d, false) // 第 2 个失败，超过阈值
	p, _ = s.GetProgress("rel")
	if p.State != StatePaused || p.PauseReason != PauseFailureThreshold {
		t.Fatalf("state = %s reason = %s", p.State, p.PauseReason)
	}

	// 暂停期间任何回执都被拒绝。
	if _, err := s.AckReceipt(Receipt{"rel", "c", d, true}); !errors.Is(err, ErrNotActive) {
		t.Fatalf("ack while paused: %v", err)
	}

	if err := s.Resume("rel"); err != nil {
		t.Fatalf("resume: %v", err)
	}
	p, _ = s.GetProgress("rel")
	if p.State != StateActive || p.PauseReason != "" {
		t.Fatalf("after resume: %+v", p)
	}
}

// ---------- 超时 ----------

func TestWaveTimeout(t *testing.T) {
	s, clk := newTestService(t)
	d := mustRegister(t, s, "v1")
	policy := WavePolicy{MinSuccessRatio: 1, MaxFailures: 0, WaveTimeout: 30 * time.Minute}
	mustCreate(t, s, "rel", d, []string{"a", "b"}, 2, policy)

	// 窗口内未达标不暂停。
	clk.advance(20 * time.Minute)
	paused, err := s.TickTimeout("rel")
	if err != nil || paused {
		t.Fatalf("tick within window: %v %v", paused, err)
	}

	// 超时：TickTimeout / 进度查询 / 回执任一路径都会先暂停。
	clk.advance(11 * time.Minute)
	paused, err = s.TickTimeout("rel")
	if err != nil || !paused {
		t.Fatalf("tick timeout: %v %v", paused, err)
	}
	p, _ := s.GetProgress("rel")
	if p.State != StatePaused || p.PauseReason != PauseTimeout {
		t.Fatalf("state = %s reason = %s", p.State, p.PauseReason)
	}

	// 恢复后给予全新超时窗口。
	if err := s.Resume("rel"); err != nil {
		t.Fatal(err)
	}
	clk.advance(25 * time.Minute)
	paused, _ = s.TickTimeout("rel")
	if paused {
		t.Fatal("resume should reset timeout window")
	}
	clk.advance(6 * time.Minute)
	if _, err := s.AckReceipt(Receipt{"rel", "a", d, true}); !errors.Is(err, ErrNotActive) {
		t.Fatalf("receipt after timeout should be rejected after auto-pause: %v", err)
	}
}

func TestTimeoutDoesNotFireAfterThresholdMet(t *testing.T) {
	s, clk := newTestService(t)
	d := mustRegister(t, s, "v1")
	policy := WavePolicy{MinSuccessRatio: 0.5, MaxFailures: 0, WaveTimeout: time.Minute}
	mustCreate(t, s, "rel", d, []string{"a", "b"}, 2, policy)
	// 达标即完成，完成态不再受超时影响。
	mustAck(t, s, "rel", "a", d, true)
	clk.advance(time.Hour)
	p, _ := s.GetProgress("rel")
	if p.State != StateCompleted {
		t.Fatalf("state = %s, want completed", p.State)
	}
}

// ---------- 暂停 / 恢复 / 取消 的单调状态序列 ----------

func TestPauseResumeCancelTransitions(t *testing.T) {
	s, _ := newTestService(t)
	d := mustRegister(t, s, "v1")
	mustCreate(t, s, "rel", d, []string{"a"}, 1, testPolicy())

	if err := s.Resume("rel"); !errors.Is(err, ErrNotPaused) {
		t.Fatalf("resume active: %v", err)
	}
	if err := s.Pause("rel"); err != nil {
		t.Fatal(err)
	}
	if err := s.Pause("rel"); !errors.Is(err, ErrAlreadyPaused) {
		t.Fatalf("double pause: %v", err)
	}
	if err := s.Resume("rel"); err != nil {
		t.Fatal(err)
	}
	if err := s.Cancel("rel"); err != nil {
		t.Fatal(err)
	}
	for _, op := range []func() error{
		func() error { return s.Pause("rel") },
		func() error { return s.Resume("rel") },
		func() error { return s.Cancel("rel") },
	} {
		if err := op(); !errors.Is(err, ErrTerminal) {
			t.Fatalf("op after cancel: %v", err)
		}
	}

	// 事件流必须是一条合法的单调状态序列。
	evs, _ := s.PendingEvents()
	assertLegalLifecycle(t, evs)
	var cancels int
	for _, ev := range evs {
		if ev.Type == EventReleaseCancelled {
			cancels++
		}
	}
	if cancels != 1 {
		t.Fatalf("cancel events = %d, want 1", cancels)
	}
}

// assertLegalLifecycle 校验事件流重放出的状态迁移全部合法且终态唯一。
func assertLegalLifecycle(t *testing.T, evs []Event) {
	t.Helper()
	state := StateActive
	var prev int64
	for _, ev := range evs {
		if ev.Seq <= prev {
			t.Fatalf("event seq not monotonic: %d after %d", ev.Seq, prev)
		}
		prev = ev.Seq
		switch ev.Type {
		case EventWaveOpened, EventReleaseCreated, EventNodeSucceeded, EventNodeFailed, EventNodeCompensation:
			// 不改变发布主状态。
		case EventReleasePaused:
			if state != StateActive {
				t.Fatalf("illegal pause from %s", state)
			}
			state = StatePaused
		case EventReleaseResumed:
			if state != StatePaused {
				t.Fatalf("illegal resume from %s", state)
			}
			state = StateActive
		case EventReleaseCancelled:
			if state != StateActive && state != StatePaused {
				t.Fatalf("illegal cancel from %s", state)
			}
			state = StateCancelled
		case EventReleaseCompleted:
			if state != StateActive {
				t.Fatalf("illegal complete from %s", state)
			}
			state = StateCompleted
		}
	}
}

// TestConcurrentPauseResumeCancel 并发发起恢复/暂停/取消，最终只能有一条单调序列。
func TestConcurrentPauseResumeCancel(t *testing.T) {
	s, _ := newTestService(t)
	d := mustRegister(t, s, "v1")
	mustCreate(t, s, "rel", d, []string{"a", "b", "c", "d"}, 2, testPolicy())
	// 让两个节点先成功，再取消时应产生恰好 2 条补偿。
	mustAck(t, s, "rel", "a", d, true)
	mustAck(t, s, "rel", "b", d, true) // 推进到第 1 波

	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(3)
		go func() { defer wg.Done(); _ = s.Pause("rel") }()
		go func() { defer wg.Done(); _ = s.Resume("rel") }()
		go func() { defer wg.Done(); _ = s.Cancel("rel") }()
	}
	wg.Wait()

	p, err := s.GetProgress("rel")
	if err != nil {
		t.Fatal(err)
	}
	if p.State != StateCancelled {
		t.Fatalf("final state = %s, want cancelled", p.State)
	}
	evs, _ := s.PendingEvents()
	assertLegalLifecycle(t, evs)

	comp := map[string]int{}
	var cancelEvents int
	for _, ev := range evs {
		switch ev.Type {
		case EventReleaseCancelled:
			cancelEvents++
		case EventNodeCompensation:
			comp[ev.NodeID]++
		}
	}
	if cancelEvents != 1 {
		t.Fatalf("cancel events = %d", cancelEvents)
	}
	if len(comp) != 2 {
		t.Fatalf("compensation nodes = %v, want 2", comp)
	}
	for n, c := range comp {
		if c != 1 {
			t.Fatalf("node %s compensated %d times", n, c)
		}
	}
}

// ---------- 取消语义 ----------

func TestCancelSemantics(t *testing.T) {
	s, _ := newTestService(t)
	d := mustRegister(t, s, "v1")
	// 第 0 波 a,b；第 1 波 c。a 成功、b 失败，然后取消。
	policy := WavePolicy{MinSuccessRatio: 1, MaxFailures: 5, WaveTimeout: time.Hour}
	mustCreate(t, s, "rel", d, []string{"a", "b", "c"}, 2, policy)
	mustAck(t, s, "rel", "a", d, true)
	mustAck(t, s, "rel", "b", d, false)

	if err := s.Cancel("rel"); err != nil {
		t.Fatal(err)
	}

	// 已成功节点保留审计结果。
	p, _ := s.GetProgress("rel")
	if p.Waves[0].Succeeded != 1 || p.Waves[0].Failed != 1 {
		t.Fatalf("audit results lost: %+v", p.Waves[0])
	}
	ns, err := s.NodeAppliedVersion("a")
	if err != nil || ns.AppliedDigest != d {
		t.Fatalf("succeeded node version lost: %+v", ns)
	}

	// 尚未开始（当前波及未来波中未成功）的节点不能再取得配置。
	// b 已有终态结果：迟到回执被幂等忽略（不改变审计结果，也不推进流程）；
	// c 从未回执：直接以终态拒绝。
	for _, n := range []string{"b", "c"} {
		if _, err := s.GetConfigForNode("rel", n); !errors.Is(err, ErrConfigNotAvailable) {
			t.Fatalf("node %s config after cancel: %v", n, err)
		}
	}
	if o, err := s.AckReceipt(Receipt{"rel", "b", d, true}); err != nil || !o.Ignored {
		t.Fatalf("late receipt for terminal node b: %+v, %v", o, err)
	}
	if _, err := s.AckReceipt(Receipt{"rel", "c", d, true}); !errors.Is(err, ErrTerminal) {
		t.Fatalf("node c ack after cancel: %v", err)
	}
	p, _ = s.GetProgress("rel")
	if p.Waves[0].Failed != 1 || p.State != StateCancelled {
		t.Fatalf("state/results mutated by ignored receipt: %+v", p)
	}

	// 补偿事件只针对已成功的 a，恰好一条。
	evs, _ := s.PendingEvents()
	var compNodes []string
	for _, ev := range evs {
		if ev.Type == EventNodeCompensation {
			compNodes = append(compNodes, ev.NodeID)
		}
	}
	if len(compNodes) != 1 || compNodes[0] != "a" {
		t.Fatalf("compensation = %v, want [a]", compNodes)
	}
}

// ---------- 旧发布 / 新版本防护 ----------

func TestOldReleaseReceiptDoesNotOverrideNewerVersion(t *testing.T) {
	s, _ := newTestService(t)
	d1 := mustRegister(t, s, "cfg-v1")
	d2 := mustRegister(t, s, "cfg-v2")
	policy := WavePolicy{MinSuccessRatio: 1, MaxFailures: 5, WaveTimeout: time.Hour}
	// 旧发布 rel-old：节点 x 第 0 波，先暂停挂起（模拟旧发布迟迟未完成）。
	mustCreate(t, s, "rel-old", d1, []string{"x", "y"}, 1, policy)
	if err := s.Pause("rel-old"); err != nil {
		t.Fatal(err)
	}
	// 新发布 rel-new 完成 x 对 v2 的应用。
	mustCreate(t, s, "rel-new", d2, []string{"x", "z"}, 1, policy)
	mustAck(t, s, "rel-new", "x", d2, true)

	ns, _ := s.NodeAppliedVersion("x")
	if ns.AppliedReleaseID != "rel-new" || ns.AppliedDigest != d2 {
		t.Fatalf("applied = %+v", ns)
	}

	// 旧发布恢复后，x 关于旧摘要的迟到回执到达：判定为陈旧回执，拒绝。
	if err := s.Resume("rel-old"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.AckReceipt(Receipt{"rel-old", "x", d1, true}); !errors.Is(err, ErrStaleReceipt) {
		t.Fatalf("stale receipt: %v", err)
	}
	// 节点当前已应用版本未被覆盖。
	ns2, _ := s.NodeAppliedVersion("x")
	if ns2.AppliedSeq != ns.AppliedSeq || ns2.AppliedDigest != d2 {
		t.Fatalf("applied version overridden: %+v", ns2)
	}
	// 旧发布进度未被推进（x 仍 in_progress，发布仍 active 且停在第 0 波）。
	p, _ := s.GetProgress("rel-old")
	if p.Waves[0].Succeeded != 0 || p.CurrentWave != 0 {
		t.Fatalf("old release advanced by stale receipt: %+v", p)
	}
}

// ---------- 进程重启恢复（文件持久化）----------

func TestRestartRecoveryFromFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.json")
	clk := &offsetClock{t: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)}

	svc := NewService(mustFileStore(t, path), clk)
	d := mustRegister(t, svc, "v1")
	policy := WavePolicy{MinSuccessRatio: 1, MaxFailures: 0, WaveTimeout: time.Hour}
	mustCreate(t, svc, "rel", d, []string{"a", "b", "c", "d"}, 2, policy)
	mustAck(t, svc, "rel", "a", d, true)
	mustAck(t, svc, "rel", "b", d, true) // 推进到第 1 波
	// 标记部分 outbox 已投递，验证 delivered 标记同样持久化。
	evs, _ := svc.PendingEvents()
	if len(evs) < 2 {
		t.Fatalf("events = %d", len(evs))
	}
	if err := svc.MarkDelivered(evs[0].Seq); err != nil {
		t.Fatal(err)
	}

	// 模拟进程重启：用同一文件重新打开存储与服务。
	svc2 := NewService(mustFileStore(t, path), clk)
	p, err := svc2.GetProgress("rel")
	if err != nil {
		t.Fatalf("reload: %v", err)
	}
	if p.State != StateActive || p.CurrentWave != 1 || p.TotalSucceeded != 2 {
		t.Fatalf("reloaded progress = %+v", p)
	}
	cfg, err := svc2.GetConfigForNode("rel", "c")
	if err != nil || cfg.Digest != d {
		t.Fatalf("config fetch after restart: %v %v", cfg, err)
	}

	// 继续推进直到完成。
	mustAck(t, svc2, "rel", "c", d, true)
	mustAck(t, svc2, "rel", "d", d, true)
	p, _ = svc2.GetProgress("rel")
	if p.State != StateCompleted {
		t.Fatalf("state = %s", p.State)
	}

	// outbox：第一条保持已投递，其余待投递，序号连续。
	pending, _ := svc2.PendingEvents()
	for _, ev := range pending {
		if ev.Seq == evs[0].Seq {
			t.Fatal("delivered flag did not survive restart")
		}
	}
	allEvs := pending
	if len(allEvs) == 0 {
		t.Fatal("expected pending events after restart")
	}
}

func TestRestartMidCancelCompensationPersisted(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.json")
	clk := &offsetClock{t: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)}
	svc := NewService(mustFileStore(t, path), clk)
	d := mustRegister(t, svc, "v1")
	mustCreate(t, svc, "rel", d, []string{"a", "b"}, 1, testPolicy())
	mustAck(t, svc, "rel", "a", d, true)
	if err := svc.Cancel("rel"); err != nil {
		t.Fatal(err)
	}

	// 重启后取消状态与补偿事件都在；重复取消不会再生成补偿。
	svc2 := NewService(mustFileStore(t, path), clk)
	p, _ := svc2.GetProgress("rel")
	if p.State != StateCancelled {
		t.Fatalf("state = %s", p.State)
	}
	var comp int
	evs, _ := svc2.PendingEvents()
	for _, ev := range evs {
		if ev.Type == EventNodeCompensation {
			comp++
		}
	}
	if comp != 1 {
		t.Fatalf("compensation after restart = %d", comp)
	}
	if err := svc2.Cancel("rel"); !errors.Is(err, ErrTerminal) {
		t.Fatalf("double cancel: %v", err)
	}
	evs2, _ := svc2.PendingEvents()
	comp2 := 0
	for _, ev := range evs2 {
		if ev.Type == EventNodeCompensation {
			comp2++
		}
	}
	if comp2 != 1 {
		t.Fatalf("compensation duplicated: %d", comp2)
	}
}

func mustFileStore(t *testing.T, path string) *MemoryStore {
	t.Helper()
	st, err := NewFileStore(path)
	if err != nil {
		t.Fatalf("open file store: %v", err)
	}
	return st
}

// ---------- outbox 派发 ----------

func TestDispatchOutboxRetryAndOrdering(t *testing.T) {
	s, _ := newTestService(t)
	d := mustRegister(t, s, "v1")
	mustCreate(t, s, "rel", d, []string{"a"}, 1, testPolicy())
	mustAck(t, s, "rel", "a", d, true)

	var mu sync.Mutex
	var got []int64
	failOnce := map[int64]bool{}
	sink := func(ev Event) error {
		mu.Lock()
		defer mu.Unlock()
		if !failOnce[ev.Seq] && len(got) == 2 {
			failOnce[ev.Seq] = true
			return fmt.Errorf("boom")
		}
		got = append(got, ev.Seq)
		return nil
	}

	n, err := s.DispatchOutbox(context.Background(), sink)
	if err == nil {
		t.Fatal("expected sink error to propagate")
	}
	if n != 2 {
		t.Fatalf("sent before failure = %d", n)
	}
	// 从断点继续，最终全部投递；每条事件的 sink 调用满足“至少一次”，
	// 事件序号严格有序（sink 需按键幂等去重）。
	n2, err := s.DispatchOutbox(context.Background(), sink)
	if err != nil {
		t.Fatalf("redispatch: %v", err)
	}
	if n2 == 0 {
		t.Fatal("expected resume to make progress")
	}
	pending, _ := s.PendingEvents()
	if len(pending) != 0 {
		t.Fatalf("pending remains: %d", len(pending))
	}
	mu.Lock()
	defer mu.Unlock()
	for i := 1; i < len(got); i++ {
		if got[i] < got[i-1] {
			t.Fatalf("outbox delivered out of order: %v", got)
		}
	}
}

// TestConcurrentDuplicateReceipts 大量并发重复/乱序回执下，节点结果与事件只计一次。
func TestConcurrentDuplicateReceipts(t *testing.T) {
	s, _ := newTestService(t)
	d := mustRegister(t, s, "v1")
	mustCreate(t, s, "rel", d, []string{"a", "b"}, 1, testPolicy())

	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(2)
		go func() { defer wg.Done(); _, _ = s.AckReceipt(Receipt{"rel", "a", d, true}) }()
		go func() { defer wg.Done(); _, _ = s.AckReceipt(Receipt{"rel", "a", d, false}) }()
	}
	wg.Wait()

	p, _ := s.GetProgress("rel")
	if p.Waves[0].Succeeded+p.Waves[0].Failed != 1 {
		t.Fatalf("node a counted %d times: %+v", p.Waves[0].Succeeded+p.Waves[0].Failed, p.Waves[0])
	}
	evs, _ := s.PendingEvents()
	var terminal int
	for _, ev := range evs {
		if ev.NodeID == "a" && (ev.Type == EventNodeSucceeded || ev.Type == EventNodeFailed) {
			terminal++
		}
	}
	if terminal != 1 {
		t.Fatalf("terminal events for a = %d, want 1", terminal)
	}
}

// ---------- 其它 ----------

func TestGetConfigForNodeGate(t *testing.T) {
	s, _ := newTestService(t)
	d := mustRegister(t, s, "v1")
	mustCreate(t, s, "rel", d, []string{"a", "b"}, 1, testPolicy())

	cfg, err := s.GetConfigForNode("rel", "a")
	if err != nil || string(cfg.Content) != "v1" {
		t.Fatalf("fetch open wave: %v", err)
	}
	if _, err := s.GetConfigForNode("rel", "b"); !errors.Is(err, ErrConfigNotAvailable) {
		t.Fatalf("future wave: %v", err)
	}
	if err := s.Pause("rel"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.GetConfigForNode("rel", "a"); !errors.Is(err, ErrConfigNotAvailable) {
		t.Fatalf("paused wave must not serve config: %v", err)
	}
	if err := s.Resume("rel"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.GetConfigForNode("rel", "a"); err != nil {
		t.Fatalf("resume should reopen gate: %v", err)
	}
}

func TestProgressWavesSorted(t *testing.T) {
	s, _ := newTestService(t)
	d := mustRegister(t, s, "v1")
	policy := WavePolicy{MinSuccessRatio: 0.5, MaxFailures: 1, WaveTimeout: time.Hour}
	mustCreate(t, s, "rel", d, []string{"a", "b", "c", "d", "e"}, 2, policy)

	p, err := s.GetProgress("rel")
	if err != nil {
		t.Fatal(err)
	}
	if len(p.Waves) != 3 || p.TotalNodes != 5 {
		t.Fatalf("progress = %+v", p)
	}
	if !p.Waves[0].Open || p.Waves[1].Open {
		t.Fatalf("open flags = %+v", p.Waves)
	}
}
