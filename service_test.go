package configrollout

import (
	"context"
	"errors"
	"strconv"
	"sync"
	"testing"
	"time"
)

type fakeClock struct {
	mu sync.Mutex
	t  time.Time
}

func newFakeClock() *fakeClock {
	return &fakeClock{t: time.Date(2026, 9, 25, 10, 0, 0, 0, time.UTC)}
}

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}

func (c *fakeClock) advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.t = c.t.Add(d)
}

func newTestService(t *testing.T) (*Service, *fakeClock) {
	t.Helper()
	clk := newFakeClock()
	s := NewService(NewMemoryStore(), clk)
	if err := s.Start(context.Background()); err != nil {
		t.Fatalf("start: %v", err)
	}
	return s, clk
}

func mustRegister(t *testing.T, s *Service, digest, content string) {
	t.Helper()
	if _, err := s.RegisterConfig(context.Background(), digest, []byte(content)); err != nil {
		t.Fatalf("register %s: %v", digest, err)
	}
}

func okReceipt(s *Service, rollout, node, digest string) (*NodeResult, error) {
	return s.RecordReceipt(context.Background(), Receipt{
		RolloutID: rollout,
		NodeID:    node,
		Digest:    digest,
		ReceiptID: "rc-" + node,
		Result:    ResultSucceeded,
	})
}

func failReceipt(s *Service, rollout, node, digest string) (*NodeResult, error) {
	return s.RecordReceipt(context.Background(), Receipt{
		RolloutID: rollout,
		NodeID:    node,
		Digest:    digest,
		Result:    ResultFailed,
	})
}

// ---------- 配置登记 ----------

func TestRegisterConfigIdempotentAndConflict(t *testing.T) {
	s, _ := newTestService(t)
	ctx := context.Background()

	if _, err := s.RegisterConfig(ctx, "", []byte("x")); !errors.Is(err, ErrInvalidDigest) {
		t.Fatalf("empty digest: %v", err)
	}
	c1, err := s.RegisterConfig(ctx, "sha256:aaa", []byte("v1"))
	if err != nil {
		t.Fatalf("register: %v", err)
	}
	// 相同内容重复登记幂等。
	c2, err := s.RegisterConfig(ctx, "sha256:aaa", []byte("v1"))
	if err != nil {
		t.Fatalf("re-register same: %v", err)
	}
	if c1.CreatedAt != c2.CreatedAt {
		t.Fatalf("idempotent register returned different records")
	}
	// 内容不同违反不可变约定。
	if _, err := s.RegisterConfig(ctx, "sha256:aaa", []byte("v2")); !errors.Is(err, ErrConfigConflict) {
		t.Fatalf("conflict expected, got %v", err)
	}
}

// ---------- 创建发布与冻结校验 ----------

func TestCreateRolloutFreezesNodes(t *testing.T) {
	s, _ := newTestService(t)
	ctx := context.Background()
	mustRegister(t, s, "d1", "cfg")

	// 配置必须先登记。
	_, err := s.CreateRollout(ctx, CreateRolloutInput{
		Digest: "missing", Waves: [][]string{{"n1"}},
	})
	if !errors.Is(err, ErrConfigNotFound) {
		t.Fatalf("expect config not found, got %v", err)
	}
	// 节点在整个发布中只能出现一次（即使跨波次）。
	_, err = s.CreateRollout(ctx, CreateRolloutInput{
		Digest: "d1", Waves: [][]string{{"n1", "n2"}, {"n1"}},
	})
	if !errors.Is(err, ErrDuplicateNode) {
		t.Fatalf("expect duplicate node, got %v", err)
	}
	// 空波次 / 空节点。
	_, err = s.CreateRollout(ctx, CreateRolloutInput{Digest: "d1", Waves: [][]string{{}}})
	if !errors.Is(err, ErrEmptyWaves) {
		t.Fatalf("expect empty waves, got %v", err)
	}
	_, err = s.CreateRollout(ctx, CreateRolloutInput{Digest: "d1", Waves: [][]string{{""}}})
	if !errors.Is(err, ErrEmptyNode) {
		t.Fatalf("expect empty node, got %v", err)
	}
	// 门槛超界。
	_, err = s.CreateRollout(ctx, CreateRolloutInput{
		Digest: "d1", Waves: [][]string{{"n1"}}, SuccessThreshold: 2,
	})
	if !errors.Is(err, ErrInvalidThreshold) {
		t.Fatalf("expect invalid threshold, got %v", err)
	}

	r, err := s.CreateRollout(ctx, CreateRolloutInput{
		Digest: "d1",
		Waves:  [][]string{{"n1", "n2"}, {"n3"}},
	})
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	if r.Status != StatusActive || r.CurrentWave != 0 {
		t.Fatalf("new rollout should be active on wave 0, got %s wave %d", r.Status, r.CurrentWave)
	}
	// 冻结后修改入参切片不影响发布状态。
	r.Waves[0][0] = "mutated"
	p, err := s.GetProgress(ctx, r.ID)
	if err != nil {
		t.Fatalf("progress: %v", err)
	}
	if p.Waves[0].Nodes[0] != "n1" {
		t.Fatalf("node set was not frozen: %v", p.Waves[0].Nodes)
	}
}

// ---------- 波次推进 ----------

func TestWavesAdvanceAtomicallyByThreshold(t *testing.T) {
	s, _ := newTestService(t)
	ctx := context.Background()
	mustRegister(t, s, "d1", "cfg")

	r, err := s.CreateRollout(ctx, CreateRolloutInput{
		ID:     "ro",
		Digest: "d1",
		Waves:  [][]string{{"a", "b"}, {"c"}},
	})
	if err != nil {
		t.Fatalf("create: %v", err)
	}

	// 第二波节点在第一波开放期间拿不到配置、也不能回执。
	if _, err := s.FetchConfig(ctx, r.ID, "c"); !errors.Is(err, ErrWaveNotOpen) {
		t.Fatalf("future wave fetch: %v", err)
	}
	if _, err := okReceipt(s, r.ID, "c", "d1"); !errors.Is(err, ErrWaveNotOpen) {
		t.Fatalf("future wave receipt: %v", err)
	}

	if _, err := okReceipt(s, r.ID, "a", "d1"); err != nil {
		t.Fatalf("receipt a: %v", err)
	}
	// 仅 1/2，默认门槛为全部成功，波次不推进。
	p, _ := s.GetProgress(ctx, r.ID)
	if p.CurrentWave != 0 || p.Waves[1].State != WavePending {
		t.Fatalf("wave should not advance at 1/2")
	}
	if _, err := okReceipt(s, r.ID, "b", "d1"); err != nil {
		t.Fatalf("receipt b: %v", err)
	}
	p, _ = s.GetProgress(ctx, r.ID)
	if p.CurrentWave != 1 || p.Waves[0].State != WaveDone || p.Waves[1].State != WaveOpen {
		t.Fatalf("wave 1 should be open, got current=%d states=%s/%s",
			p.CurrentWave, p.Waves[0].State, p.Waves[1].State)
	}

	// 旧波次节点的“迟到但首次”回执不能再推进，也不能记录。
	if _, err := okReceipt(s, r.ID, "a", "d1"); err != nil {
		// a 已有回执 -> 幂等返回，不应报错
		t.Fatalf("duplicate receipt for a should be idempotent, got %v", err)
	}
	// 构造一个旧波次中从未回执的节点需要门槛 < 全量，见另一用例。

	if _, err := okReceipt(s, r.ID, "c", "d1"); err != nil {
		t.Fatalf("receipt c: %v", err)
	}
	p, _ = s.GetProgress(ctx, r.ID)
	if p.Status != StatusSucceeded {
		t.Fatalf("rollout should succeed, got %s", p.Status)
	}
	if _, err := okReceipt(s, r.ID, "c", "d1"); err != nil {
		t.Fatalf("duplicate receipt after success should stay idempotent: %v", err)
	}
}

func TestPartialThresholdLeavesLateOldWaveReceiptRejected(t *testing.T) {
	s, _ := newTestService(t)
	ctx := context.Background()
	mustRegister(t, s, "d1", "cfg")

	r, err := s.CreateRollout(ctx, CreateRolloutInput{
		ID: "ro", Digest: "d1",
		Waves:            [][]string{{"a", "b", "c"}, {"d"}},
		SuccessThreshold: 1,
	})
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	if _, err := okReceipt(s, r.ID, "a", "d1"); err != nil {
		t.Fatalf("receipt a: %v", err)
	}
	p, _ := s.GetProgress(ctx, r.ID)
	if p.CurrentWave != 1 {
		t.Fatalf("threshold 1 should open wave 1, got %d", p.CurrentWave)
	}
	// b、c 属于旧波次，波次已关闭：迟到的首条回执必须拒绝。
	if _, err := okReceipt(s, r.ID, "b", "d1"); !errors.Is(err, ErrWaveNotOpen) {
		t.Fatalf("late old-wave receipt expected ErrWaveNotOpen, got %v", err)
	}
	// 旧波次节点也不能再拉配置。
	if _, err := s.FetchConfig(ctx, r.ID, "c"); !errors.Is(err, ErrWaveNotOpen) {
		t.Fatalf("old wave fetch expected ErrWaveNotOpen, got %v", err)
	}
}

// ---------- 失败阈值与超时 ----------

func TestFailureThresholdAccumulatesAcrossWaves(t *testing.T) {
	s, _ := newTestService(t)
	ctx := context.Background()
	mustRegister(t, s, "d1", "cfg")

	r, err := s.CreateRollout(ctx, CreateRolloutInput{
		ID: "ro", Digest: "d1",
		// 门槛 1：a 成功即可推进；b 的失败计入发布级总数。
		Waves:            [][]string{{"a", "b"}, {"c", "d"}},
		SuccessThreshold: 1,
		MaxFailures:      1,
	})
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	if _, err := failReceipt(s, r.ID, "b", "d1"); err != nil {
		t.Fatalf("fail b: %v", err)
	}
	if _, err := okReceipt(s, r.ID, "a", "d1"); err != nil {
		t.Fatalf("success a: %v", err)
	}
	p, _ := s.GetProgress(ctx, r.ID)
	if p.CurrentWave != 1 || p.TotalFailed != 1 {
		t.Fatalf("wave 2 should be open with 1 accumulated failure, got %+v", p)
	}
	// 第二波再失败一个：发布级失败总数 2 > 1，必须暂停（阈值不随换波重置）。
	if _, err := failReceipt(s, r.ID, "c", "d1"); err != nil {
		t.Fatalf("fail c: %v", err)
	}
	p, _ = s.GetProgress(ctx, r.ID)
	if p.Status != StatusPaused || p.PauseReason != ReasonFailureThreshold {
		t.Fatalf("expect paused by accumulated failures, got %s/%s", p.Status, p.PauseReason)
	}
}

func TestFailureThresholdPauses(t *testing.T) {
	s, _ := newTestService(t)
	ctx := context.Background()
	mustRegister(t, s, "d1", "cfg")

	r, err := s.CreateRollout(ctx, CreateRolloutInput{
		ID: "ro", Digest: "d1", Waves: [][]string{{"a", "b", "c"}},
		MaxFailures: 1, // 容忍 1 个失败，第 2 个失败触发暂停
	})
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	if _, err := failReceipt(s, r.ID, "a", "d1"); err != nil {
		t.Fatalf("fail a: %v", err)
	}
	p, _ := s.GetProgress(ctx, r.ID)
	if p.Status != StatusActive {
		t.Fatalf("one failure within tolerance, status=%s", p.Status)
	}
	if _, err := failReceipt(s, r.ID, "b", "d1"); err != nil {
		t.Fatalf("fail b: %v", err)
	}
	p, _ = s.GetProgress(ctx, r.ID)
	if p.Status != StatusPaused || p.PauseReason != ReasonFailureThreshold {
		t.Fatalf("expect paused by failure threshold, got %s/%s", p.Status, p.PauseReason)
	}
	// 暂停期间回执与拉取都被拒绝。
	if _, err := okReceipt(s, r.ID, "c", "d1"); !errors.Is(err, ErrRolloutPaused) {
		t.Fatalf("receipt while paused: %v", err)
	}
	if _, err := s.FetchConfig(ctx, r.ID, "c"); !errors.Is(err, ErrRolloutPaused) {
		t.Fatalf("fetch while paused: %v", err)
	}
}

func TestWaveTimeoutPauses(t *testing.T) {
	s, clk := newTestService(t)
	ctx := context.Background()
	mustRegister(t, s, "d1", "cfg")

	r, err := s.CreateRollout(ctx, CreateRolloutInput{
		ID: "ro", Digest: "d1", Waves: [][]string{{"a"}},
		WaveTimeout: time.Minute,
	})
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	clk.advance(61 * time.Second)

	// 到点后的回执触发暂停并被拒绝。
	if _, err := okReceipt(s, r.ID, "a", "d1"); !errors.Is(err, ErrRolloutPaused) {
		t.Fatalf("receipt after timeout: %v", err)
	}
	p, _ := s.GetProgress(ctx, r.ID)
	if p.Status != StatusPaused || p.PauseReason != ReasonWaveTimeout {
		t.Fatalf("expect paused by timeout, got %s/%s", p.Status, p.PauseReason)
	}

	// 恢复后重新给一个完整超时窗口。
	if err := s.Resume(ctx, r.ID); err != nil {
		t.Fatalf("resume: %v", err)
	}
	clk.advance(30 * time.Second)
	if _, err := okReceipt(s, r.ID, "a", "d1"); err != nil {
		t.Fatalf("receipt within fresh window should succeed: %v", err)
	}

	// SweepTimeouts 在无人交互时也能暂停超时发布。
	r2, _ := s.CreateRollout(ctx, CreateRolloutInput{
		ID: "ro2", Digest: "d1", Waves: [][]string{{"b"}}, WaveTimeout: time.Minute,
	})
	clk.advance(61 * time.Second)
	n, err := s.SweepTimeouts(ctx)
	if err != nil || n != 1 {
		t.Fatalf("sweep: n=%d err=%v", n, err)
	}
	p2, _ := s.GetProgress(ctx, r2.ID)
	if p2.Status != StatusPaused {
		t.Fatalf("swept rollout should be paused, got %s", p2.Status)
	}
}

// ---------- 回执幂等与陈旧回执 ----------

func TestDuplicateReceiptIsIdempotent(t *testing.T) {
	s, _ := newTestService(t)
	ctx := context.Background()
	mustRegister(t, s, "d1", "cfg")
	r, _ := s.CreateRollout(ctx, CreateRolloutInput{
		ID: "ro", Digest: "d1", Waves: [][]string{{"a", "b"}},
	})

	first, err := okReceipt(s, r.ID, "a", "d1")
	if err != nil {
		t.Fatalf("first: %v", err)
	}
	// 重复、甚至内容相矛盾（failed）的回执都不能覆盖首条审计结果。
	dup, err := failReceipt(s, r.ID, "a", "d1")
	if err != nil {
		t.Fatalf("duplicate should be idempotent, got %v", err)
	}
	if dup.Status != ResultSucceeded || dup.RecordedAt != first.RecordedAt {
		t.Fatalf("duplicate receipt altered audit record: %+v vs %+v", dup, first)
	}
	p, _ := s.GetProgress(ctx, r.ID)
	if p.TotalFailed != 0 || p.TotalSucceeded != 1 {
		t.Fatalf("counters changed by duplicate: %+v", p)
	}
}

func TestDigestMismatchRejected(t *testing.T) {
	s, _ := newTestService(t)
	ctx := context.Background()
	mustRegister(t, s, "d1", "cfg")
	mustRegister(t, s, "d2", "cfg2")
	r, _ := s.CreateRollout(ctx, CreateRolloutInput{
		ID: "ro", Digest: "d1", Waves: [][]string{{"a"}},
	})

	// 节点报上一份配置 d2 的回执：旧配置迟到回执，拒绝且不推进。
	if _, err := okReceipt(s, r.ID, "a", "d2"); !errors.Is(err, ErrDigestMismatch) {
		t.Fatalf("expect digest mismatch, got %v", err)
	}
	p, _ := s.GetProgress(ctx, r.ID)
	if p.TotalSucceeded != 0 {
		t.Fatalf("mismatched receipt was recorded")
	}
}

func TestStaleReceiptFromNewerRolloutRejected(t *testing.T) {
	s, _ := newTestService(t)
	ctx := context.Background()
	mustRegister(t, s, "d1", "cfg")

	// 旧发布 A（seq=1）门槛为全量，n2 先成功。
	a, err := s.CreateRollout(ctx, CreateRolloutInput{
		ID: "A", Digest: "d1", Waves: [][]string{{"n1", "n2"}},
	})
	if err != nil {
		t.Fatalf("create A: %v", err)
	}
	if _, err := okReceipt(s, a.ID, "n2", "d1"); err != nil {
		t.Fatalf("n2 in A: %v", err)
	}
	// 新发布 B（seq=2）中 n1 成功，节点已应用版本推进到 seq2。
	b, err := s.CreateRollout(ctx, CreateRolloutInput{
		ID: "B", Digest: "d1", Waves: [][]string{{"n1"}},
	})
	if err != nil {
		t.Fatalf("create B: %v", err)
	}
	if _, err := okReceipt(s, b.ID, "n1", "d1"); err != nil {
		t.Fatalf("n1 in B: %v", err)
	}
	applied, _ := s.GetApplied(ctx, "n1")
	if applied == nil || applied.Seq != 2 || applied.RolloutID != "B" {
		t.Fatalf("applied ref wrong: %+v", applied)
	}
	// n1 对旧发布 A 的迟到首条回执：不得推进 A，也不得覆盖已应用版本。
	if _, err := okReceipt(s, a.ID, "n1", "d1"); !errors.Is(err, ErrStaleReceipt) {
		t.Fatalf("expect stale receipt, got %v", err)
	}
	p, _ := s.GetProgress(ctx, a.ID)
	if p.TotalSucceeded != 1 || p.CurrentWave != 0 {
		t.Fatalf("stale receipt advanced old rollout: %+v", p)
	}
	applied2, _ := s.GetApplied(ctx, "n1")
	if applied2.Seq != 2 {
		t.Fatalf("stale receipt overwrote applied version: %d", applied2.Seq)
	}
}

func TestNodeNotInRolloutAndInvalidReceipt(t *testing.T) {
	s, _ := newTestService(t)
	mustRegister(t, s, "d1", "cfg")
	r, _ := s.CreateRollout(context.Background(), CreateRolloutInput{
		ID: "ro", Digest: "d1", Waves: [][]string{{"a"}},
	})
	if _, err := s.RecordReceipt(context.Background(), Receipt{
		RolloutID: r.ID, NodeID: "ghost", Digest: "d1", Result: ResultSucceeded,
	}); !errors.Is(err, ErrNodeNotInRollout) {
		t.Fatalf("expect node not in rollout, got %v", err)
	}
	if _, err := s.RecordReceipt(context.Background(), Receipt{
		RolloutID: r.ID, NodeID: "a", Digest: "d1", Result: "bogus",
	}); !errors.Is(err, ErrInvalidResult) {
		t.Fatalf("expect invalid result, got %v", err)
	}
	if _, err := s.GetProgress(context.Background(), "nope"); !errors.Is(err, ErrRolloutNotFound) {
		t.Fatalf("expect rollout not found, got %v", err)
	}
}

// ---------- 暂停 / 恢复 / 取消的状态单调与补偿 ----------

func TestPauseResumeCancelTransitions(t *testing.T) {
	s, _ := newTestService(t)
	ctx := context.Background()
	mustRegister(t, s, "d1", "cfg")
	r, _ := s.CreateRollout(ctx, CreateRolloutInput{
		ID: "ro", Digest: "d1", Waves: [][]string{{"a", "b"}},
	})

	if err := s.Pause(ctx, r.ID); err != nil {
		t.Fatalf("pause: %v", err)
	}
	if err := s.Pause(ctx, r.ID); err != nil {
		t.Fatalf("pause should be idempotent: %v", err)
	}
	if err := s.Resume(ctx, r.ID); err != nil {
		t.Fatalf("resume: %v", err)
	}
	if err := s.Resume(ctx, r.ID); err != nil {
		t.Fatalf("resume active should be no-op: %v", err)
	}
	if _, err := okReceipt(s, r.ID, "a", "d1"); err != nil {
		t.Fatalf("receipt after resume: %v", err)
	}
	if err := s.Cancel(ctx, r.ID); err != nil {
		t.Fatalf("cancel: %v", err)
	}
	// 终态上的操作。
	if err := s.Cancel(ctx, r.ID); err != nil {
		t.Fatalf("cancel should be idempotent: %v", err)
	}
	if err := s.Pause(ctx, r.ID); !errors.Is(err, ErrRolloutCancelled) {
		t.Fatalf("pause after cancel: %v", err)
	}
	if err := s.Resume(ctx, r.ID); !errors.Is(err, ErrRolloutCancelled) {
		t.Fatalf("resume after cancel: %v", err)
	}
	if _, err := okReceipt(s, r.ID, "b", "d1"); !errors.Is(err, ErrRolloutCancelled) {
		t.Fatalf("receipt after cancel: %v", err)
	}

	p, _ := s.GetProgress(ctx, r.ID)
	// 状态序列必须单调可回放。
	if err := validateMonotonicHistory(p.History, StatusCancelled); err != nil {
		t.Fatalf("history not monotonic: %v\n%+v", err, p.History)
	}
	// 已成功节点的审计结果保留。
	if p.TotalSucceeded != 1 {
		t.Fatalf("succeeded audit result lost after cancel")
	}
	res, _ := s.GetNodeResult(ctx, r.ID, "a")
	if res == nil || res.Status != ResultSucceeded {
		t.Fatalf("node audit record missing: %+v", res)
	}
}

func TestCancelBlocksUnstartedNodesAndEmitsCompensationOnce(t *testing.T) {
	s, _ := newTestService(t)
	ctx := context.Background()
	mustRegister(t, s, "d1", "cfg")

	// 门槛 1：a 成功后第二波已开放；b 尚未开始。
	r, _ := s.CreateRollout(ctx, CreateRolloutInput{
		ID: "ro", Digest: "d1",
		Waves:            [][]string{{"a", "b"}, {"c"}},
		SuccessThreshold: 1,
	})
	if _, err := okReceipt(s, r.ID, "a", "d1"); err != nil {
		t.Fatalf("a: %v", err)
	}
	if err := s.Cancel(ctx, r.ID); err != nil {
		t.Fatalf("cancel: %v", err)
	}

	// 取消后：未开始节点（b、c）都拿不到配置。
	for _, n := range []string{"b", "c"} {
		if _, err := s.FetchConfig(ctx, r.ID, n); !errors.Is(err, ErrRolloutCancelled) {
			t.Fatalf("node %s fetch after cancel: %v", n, err)
		}
	}

	// 补偿通知：仅为已成功的 a 生成，且恰好一条。
	pending, err := s.PendingOutbox(ctx)
	if err != nil {
		t.Fatalf("outbox: %v", err)
	}
	var comps []*OutboxMessage
	for _, m := range pending {
		if m.Type == evtNodeCompensation {
			comps = append(comps, m)
		}
	}
	if len(comps) != 1 || comps[0].Key != "compensation:ro:a" {
		t.Fatalf("expect exactly one compensation for a, got %+v", comps)
	}

	// 再次取消不会重复生成补偿。
	if err := s.Cancel(ctx, r.ID); err != nil {
		t.Fatalf("second cancel: %v", err)
	}
	pending, _ = s.PendingOutbox(ctx)
	comps = comps[:0]
	for _, m := range pending {
		if m.Type == evtNodeCompensation {
			comps = append(comps, m)
		}
	}
	if len(comps) != 1 {
		t.Fatalf("compensation duplicated, got %d", len(comps))
	}

	// 标记投递后从待发列表消失，但记录仍可审计。
	if err := s.MarkOutboxPublished(ctx, []string{comps[0].Key}); err != nil {
		t.Fatalf("mark published: %v", err)
	}
	pending, _ = s.PendingOutbox(ctx)
	for _, m := range pending {
		if m.Type == evtNodeCompensation {
			t.Fatalf("compensation still pending after publish")
		}
	}
}

func TestCannotCancelSucceeded(t *testing.T) {
	s, _ := newTestService(t)
	ctx := context.Background()
	mustRegister(t, s, "d1", "cfg")
	r, _ := s.CreateRollout(ctx, CreateRolloutInput{
		ID: "ro", Digest: "d1", Waves: [][]string{{"a"}},
	})
	if _, err := okReceipt(s, r.ID, "a", "d1"); err != nil {
		t.Fatalf("a: %v", err)
	}
	if err := s.Cancel(ctx, r.ID); !errors.Is(err, ErrAlreadyFinished) {
		t.Fatalf("cancel succeeded rollout: %v", err)
	}
}

// validateMonotonicHistory 回放状态序列：相邻事件首尾相接且迁移合法。
func validateMonotonicHistory(h []StatusEvent, want Status) error {
	allowed := map[Status]map[Status]bool{
		"":              {StatusActive: true},
		StatusActive:    {StatusPaused: true, StatusCancelled: true, StatusSucceeded: true},
		StatusPaused:    {StatusActive: true, StatusCancelled: true},
		StatusCancelled: {},
		StatusSucceeded: {},
	}
	var cur Status
	for i, e := range h {
		if e.From != cur {
			return errors.New("event " + strconv.Itoa(i) + ": from " + string(e.From) + " != cur " + string(cur))
		}
		if !allowed[cur][e.To] {
			return errors.New("event " + strconv.Itoa(i) + ": illegal transition " + string(cur) + "->" + string(e.To))
		}
		cur = e.To
	}
	if cur != want {
		return errors.New("final status " + string(cur) + ", want " + string(want))
	}
	return nil
}

// ---------- 并发竞争：暂停/恢复/取消/回执 ----------

func TestConcurrentControlOperations(t *testing.T) {
	s, _ := newTestService(t)
	ctx := context.Background()
	mustRegister(t, s, "d1", "cfg")
	r, _ := s.CreateRollout(ctx, CreateRolloutInput{
		ID: "ro", Digest: "d1",
		Waves:       [][]string{{"n1", "n2", "n3", "n4", "n5", "n6", "n7", "n8"}},
		MaxFailures: 100, // 不因失败自动暂停，纯粹测试人工操作竞争
	})

	var wg sync.WaitGroup
	ops := []func(){
		func() { _ = s.Pause(ctx, r.ID) },
		func() { _ = s.Resume(ctx, r.ID) },
		func() { _ = s.Cancel(ctx, r.ID) },
	}
	for round := 0; round < 20; round++ {
		for _, op := range ops {
			wg.Add(1)
			op := op
			go func() { defer wg.Done(); op() }()
		}
		wg.Add(1)
		node := "n" + string(rune('1'+(round%8)))
		go func() {
			defer wg.Done()
			_, _ = okReceipt(s, r.ID, node, "d1")
		}()
	}
	wg.Wait()

	p, err := s.GetProgress(ctx, r.ID)
	if err != nil {
		t.Fatalf("progress: %v", err)
	}
	if err := validateMonotonicHistory(p.History, p.Status); err != nil {
		t.Fatalf("non-monotonic history under race: %v\n%+v", err, p.History)
	}
	// 每个节点至多一条审计记录。
	seen := map[string]bool{}
	for i := 1; i <= 8; i++ {
		id := "n" + string(rune('0'+i))
		res, err := s.GetNodeResult(ctx, r.ID, id)
		if err != nil {
			t.Fatalf("node result %s: %v", id, err)
		}
		if res != nil {
			if seen[id] {
				t.Fatalf("duplicate audit record for %s", id)
			}
			seen[id] = true
		}
	}
	if p.Status == StatusCancelled {
		// 取消若发生，补偿条数必须恰好等于已成功节点数。
		pending, _ := s.PendingOutbox(ctx)
		compKeys := map[string]bool{}
		for _, m := range pending {
			if m.Type == evtNodeCompensation {
				if compKeys[m.Key] {
					t.Fatalf("duplicate compensation %s", m.Key)
				}
				compKeys[m.Key] = true
			}
		}
		if len(compKeys) != p.TotalSucceeded {
			t.Fatalf("compensation count %d != succeeded %d", len(compKeys), p.TotalSucceeded)
		}
	}
}

// ---------- 进程重启恢复 ----------

func TestRecoveryAcrossRestartWithFileStore(t *testing.T) {
	dir := t.TempDir()
	path := dir + "/state.json"
	ctx := context.Background()

	clk := newFakeClock()
	svc1 := NewService(NewFileStore(path), clk)
	if err := svc1.Start(ctx); err != nil {
		t.Fatalf("start 1: %v", err)
	}
	if _, err := svc1.RegisterConfig(ctx, "d1", []byte("cfg")); err != nil {
		t.Fatalf("register: %v", err)
	}
	r, err := svc1.CreateRollout(ctx, CreateRolloutInput{
		ID: "ro", Digest: "d1", Waves: [][]string{{"a", "b"}, {"c"}},
	})
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	if _, err := okReceipt(svc1, r.ID, "a", "d1"); err != nil {
		t.Fatalf("a: %v", err)
	}
	if err := svc1.Pause(ctx, r.ID); err != nil {
		t.Fatalf("pause: %v", err)
	}

	// 模拟进程重启：新服务从同一文件加载。
	svc2 := NewService(NewFileStore(path), clk)
	if err := svc2.Start(ctx); err != nil {
		t.Fatalf("start 2: %v", err)
	}
	p, err := svc2.GetProgress(ctx, r.ID)
	if err != nil {
		t.Fatalf("progress after restart: %v", err)
	}
	if p.Status != StatusPaused || p.CurrentWave != 0 || p.TotalSucceeded != 1 {
		t.Fatalf("state not recovered: %+v", p)
	}
	// 暂停语义在重启后仍然生效。
	if _, err := okReceipt(svc2, r.ID, "b", "d1"); !errors.Is(err, ErrRolloutPaused) {
		t.Fatalf("paused state not enforced after restart: %v", err)
	}
	if err := svc2.Resume(ctx, r.ID); err != nil {
		t.Fatalf("resume: %v", err)
	}
	if _, err := okReceipt(svc2, r.ID, "b", "d1"); err != nil {
		t.Fatalf("b after resume: %v", err)
	}
	if _, err := okReceipt(svc2, r.ID, "c", "d1"); err != nil {
		t.Fatalf("c wave 2 after restart: %v", err)
	}
	p, _ = svc2.GetProgress(ctx, r.ID)
	if p.Status != StatusSucceeded {
		t.Fatalf("rollout should complete after recovery, got %s", p.Status)
	}

	// outbox 在重启后仍可查询且包含两波开放事件。
	pending, err := svc2.PendingOutbox(ctx)
	if err != nil {
		t.Fatalf("outbox after restart: %v", err)
	}
	waveOpenKeys := map[string]bool{}
	for _, m := range pending {
		if m.Type == evtWaveOpened {
			waveOpenKeys[m.Key] = true
		}
	}
	if !waveOpenKeys["wave-opened:ro:0"] || !waveOpenKeys["wave-opened:ro:1"] {
		t.Fatalf("wave open events lost across restart: %+v", waveOpenKeys)
	}
}

func TestFileStoreAtomicReload(t *testing.T) {
	dir := t.TempDir()
	path := dir + "/state.json"
	store := NewFileStore(path)
	ctx := context.Background()

	d := newData()
	d.Configs["x"] = &Config{Digest: "x", CreatedAt: time.Now()}
	if err := store.Save(ctx, d); err != nil {
		t.Fatalf("save: %v", err)
	}
	got, err := store.Load(ctx)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if got.Configs["x"] == nil || got.Configs["x"].Digest != "x" {
		t.Fatalf("round trip mismatch: %+v", got)
	}
	if _, err := store.Load(ctx); err != nil {
		t.Fatalf("second load: %v", err)
	}
}

// ---------- outbox 与进度细节 ----------

func TestOutboxLifecycle(t *testing.T) {
	s, _ := newTestService(t)
	ctx := context.Background()
	mustRegister(t, s, "d1", "cfg")
	r, _ := s.CreateRollout(ctx, CreateRolloutInput{
		ID: "ro", Digest: "d1", Waves: [][]string{{"a"}},
	})
	pending, _ := s.PendingOutbox(ctx)
	if len(pending) != 1 || pending[0].Type != evtWaveOpened {
		t.Fatalf("expect one wave-opened event, got %+v", pending)
	}
	if _, err := okReceipt(s, r.ID, "a", "d1"); err != nil {
		t.Fatalf("a: %v", err)
	}
	pending, _ = s.PendingOutbox(ctx)
	if len(pending) != 2 || pending[1].Type != evtRolloutSucceeded {
		t.Fatalf("expect succeeded event appended, got %+v", pending)
	}
	keys := []string{pending[0].Key, pending[1].Key}
	if err := s.MarkOutboxPublished(ctx, keys); err != nil {
		t.Fatalf("mark: %v", err)
	}
	left, _ := s.PendingOutbox(ctx)
	if len(left) != 0 {
		t.Fatalf("all should be published, got %d", len(left))
	}
	// 重复标记无副作用、无错误。
	if err := s.MarkOutboxPublished(ctx, keys); err != nil {
		t.Fatalf("idempotent mark: %v", err)
	}

	// 未知 Key 的尝试计数报错；真实 Key 可累计尝试次数。
	if err := s.RecordOutboxAttempt(ctx, "missing-key"); err == nil {
		t.Fatalf("unknown outbox key should error")
	}
	if err := s.RecordOutboxAttempt(ctx, keys[0]); err != nil {
		t.Fatalf("record attempt: %v", err)
	}
}

func TestRunTimeoutSweeper(t *testing.T) {
	s, clk := newTestService(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	mustRegister(t, s, "d1", "cfg")
	if _, err := s.CreateRollout(ctx, CreateRolloutInput{
		ID: "ro", Digest: "d1", Waves: [][]string{{"a"}}, WaveTimeout: time.Minute,
	}); err != nil {
		t.Fatalf("create: %v", err)
	}

	done := make(chan error, 1)
	go func() { done <- s.RunTimeoutSweeper(ctx, 10*time.Millisecond) }()
	clk.advance(2 * time.Minute)
	// 等待 sweeper 跑过若干个 tick 并把发布暂停。
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		p, _ := s.GetProgress(ctx, "ro")
		if p.Status == StatusPaused {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	p, _ := s.GetProgress(ctx, "ro")
	if p.Status != StatusPaused || p.PauseReason != ReasonWaveTimeout {
		t.Fatalf("sweeper did not pause timed-out rollout: %s/%s", p.Status, p.PauseReason)
	}
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("sweeper exit: %v", err)
	}
	if err := s.RunTimeoutSweeper(ctx, 0); err == nil {
		t.Fatalf("non-positive interval should error")
	}
}

func TestProgressCounters(t *testing.T) {
	s, _ := newTestService(t)
	ctx := context.Background()
	mustRegister(t, s, "d1", "cfg")
	r, _ := s.CreateRollout(ctx, CreateRolloutInput{
		ID: "ro", Digest: "d1",
		Waves:            [][]string{{"a", "b"}, {"c", "d"}},
		SuccessThreshold: 1,
		MaxFailures:      10,
	})
	_, _ = failReceipt(s, r.ID, "b", "d1")
	_, _ = okReceipt(s, r.ID, "a", "d1") // 达标后开放第二波
	p, err := s.GetProgress(ctx, r.ID)
	if err != nil {
		t.Fatalf("progress: %v", err)
	}
	if p.TotalSucceeded != 1 || p.TotalFailed != 1 {
		t.Fatalf("totals: %+v", p)
	}
	if p.Waves[0].Succeeded != 1 || p.Waves[0].Failed != 1 || p.Waves[0].Pending != 0 {
		t.Fatalf("wave 0 counts: %+v", p.Waves[0])
	}
	if p.Waves[1].State != WaveOpen || p.Waves[1].Threshold != 1 {
		t.Fatalf("wave 1 state: %+v", p.Waves[1])
	}

	ids, err := s.ListRolloutIDs(ctx)
	if err != nil || len(ids) != 1 || ids[0] != r.ID {
		t.Fatalf("list rollouts: %v %v", ids, err)
	}
}
