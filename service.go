package configrollout

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"
)

// Clock 抽象时间来源，测试中可替换为可控时钟。
type Clock interface {
	Now() time.Time
}

type systemClock struct{}

func (systemClock) Now() time.Time { return time.Now() }

// Service 是配置发布服务。所有方法都可被并发调用：内部以单一互斥锁
// 串行化状态迁移，每次迁移后原子落盘（状态与 outbox 同快照提交）。
type Service struct {
	store Store
	clock Clock

	mu      sync.Mutex
	started bool
	data    *Data
}

// NewService 创建服务。store 为持久化目标；clock 传 nil 时使用系统时钟。
func NewService(store Store, clock Clock) *Service {
	if clock == nil {
		clock = systemClock{}
	}
	return &Service{store: store, clock: clock}
}

// Start 从持久化存储加载状态，使流程在进程重启后继续。重复调用无害。
func (s *Service) Start(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.started {
		return nil
	}
	d, err := s.store.Load(ctx)
	if err != nil {
		return fmt.Errorf("load rollout state: %w", err)
	}
	if d == nil {
		d = newData()
	}
	s.data = d
	s.started = true
	return nil
}

func (s *Service) snapshot() *Data {
	if s.data == nil {
		panic("configrollout: service not started; call Start first")
	}
	return s.data
}

// commit 原子持久化当前快照（状态 + outbox）。
func (s *Service) commit(ctx context.Context) error {
	if err := s.store.Save(ctx, s.data); err != nil {
		return fmt.Errorf("persist rollout state: %w", err)
	}
	return nil
}

// RegisterConfig 登记一份不可变配置。同一摘要重复登记相同内容是幂等的；
// 内容不同则视为冲突。
func (s *Service) RegisterConfig(ctx context.Context, digest string, content []byte) (*Config, error) {
	if digest == "" {
		return nil, ErrInvalidDigest
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	d := s.snapshot()
	now := s.clock.Now()

	if existing, ok := d.Configs[digest]; ok {
		if string(existing.Content) != string(content) {
			return nil, ErrConfigConflict
		}
		out := *existing
		return &out, nil
	}
	cfg := &Config{Digest: digest, Content: append([]byte(nil), content...), CreatedAt: now}
	d.Configs[digest] = cfg
	if err := s.commit(ctx); err != nil {
		return nil, err
	}
	out := *cfg
	return &out, nil
}

// CreateRolloutInput 为创建发布的参数。
type CreateRolloutInput struct {
	// ID 为调用方指定的发布 ID，需全局唯一；为空时自动生成。
	ID string
	// Digest 为已登记配置的摘要。
	Digest string
	// Waves 为冻结的、有顺序的波次，每个波次是节点 ID 列表。
	// 整个发布中一个节点只能出现一次。
	Waves [][]string
	// SuccessThreshold 为每波所需成功数；0 表示该波全部成功。
	SuccessThreshold int
	// MaxFailures 为允许的失败节点总数上限；失败数超过它即暂停（即容忍 MaxFailures 个失败）。
	MaxFailures int
	// WaveTimeout 为单波等待超时；0 表示不超时。
	WaveTimeout time.Duration
}

// CreateRollout 冻结目标节点并创建发布，第一波原子开放。
func (s *Service) CreateRollout(ctx context.Context, in CreateRolloutInput) (*Rollout, error) {
	if in.Digest == "" {
		return nil, ErrInvalidDigest
	}
	if in.WaveTimeout < 0 {
		return nil, ErrInvalidThreshold
	}
	if len(in.Waves) == 0 {
		return nil, ErrEmptyWaves
	}

	seen := make(map[string]struct{})
	wavesCopy := make([][]string, len(in.Waves))
	for wi, wave := range in.Waves {
		if len(wave) == 0 {
			return nil, ErrEmptyWaves
		}
		wavesCopy[wi] = append([]string(nil), wave...)
		for _, n := range wave {
			if n == "" {
				return nil, ErrEmptyNode
			}
			if _, dup := seen[n]; dup {
				return nil, fmt.Errorf("%w: %s", ErrDuplicateNode, n)
			}
			seen[n] = struct{}{}
		}
	}
	if in.SuccessThreshold < 0 || in.SuccessThreshold > len(in.Waves[0]) {
		return nil, fmt.Errorf("%w: threshold %d exceeds first wave size %d",
			ErrInvalidThreshold, in.SuccessThreshold, len(in.Waves[0]))
	}
	for _, w := range in.Waves[1:] {
		if in.SuccessThreshold > len(w) {
			return nil, fmt.Errorf("%w: threshold %d exceeds wave size %d",
				ErrInvalidThreshold, in.SuccessThreshold, len(w))
		}
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	d := s.snapshot()

	if _, ok := d.Configs[in.Digest]; !ok {
		return nil, fmt.Errorf("%w: %s", ErrConfigNotFound, in.Digest)
	}
	id := in.ID
	if id == "" {
		id = fmt.Sprintf("rollout-%d", d.NextRolloutSeq)
	}
	if _, exists := d.Rollouts[id]; exists {
		return nil, fmt.Errorf("rollout id already exists: %s", id)
	}

	now := s.clock.Now()
	r := &Rollout{
		ID:               id,
		Seq:              d.NextRolloutSeq,
		Digest:           in.Digest,
		Waves:            wavesCopy,
		Status:           StatusActive,
		SuccessThreshold: in.SuccessThreshold,
		MaxFailures:      in.MaxFailures,
		WaveTimeout:      in.WaveTimeout,
		CurrentWave:      0,
		WaveOpenedAt:     now,
		Results:          map[string]*NodeResult{},
		CreatedAt:        now,
		UpdatedAt:        now,
	}
	d.NextRolloutSeq++
	d.Rollouts[id] = r
	r.History = append(r.History, StatusEvent{At: now, From: "", To: StatusActive})

	emitWaveOpened(d, r, 0, now)

	if err := s.commit(ctx); err != nil {
		return nil, err
	}
	return cloneRollout(r), nil
}

// FetchConfig 供当前波次的节点拉取配置。只有处于 active 且所属波次恰好是
// 当前开放波次的节点才能拿到配置；取消后未开始（以及已取消发布内全部）
// 节点都会被拒绝。
func (s *Service) FetchConfig(ctx context.Context, rolloutID, nodeID string) (*Config, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	d := s.snapshot()
	now := s.clock.Now()

	r, err := d.requireActiveRollout(rolloutID, nodeID, s, now)
	if err != nil {
		return nil, err
	}
	cfg := d.Configs[r.Digest]
	out := *cfg
	return &out, nil
}

// requireActiveRollout 封装“节点能否在当前波次上行动”的全部校验，
// 并在发现波次超时时顺带把发布暂停落盘。FetchConfig 与 RecordReceipt 共用。
func (d *Data) requireActiveRollout(rolloutID, nodeID string, s *Service, now time.Time) (*Rollout, error) {
	r, ok := d.Rollouts[rolloutID]
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrRolloutNotFound, rolloutID)
	}
	switch r.Status {
	case StatusCancelled:
		return nil, fmt.Errorf("%w: %s", ErrRolloutCancelled, rolloutID)
	case StatusSucceeded:
		return nil, fmt.Errorf("%w: %s", ErrAlreadyFinished, rolloutID)
	case StatusPaused:
		return nil, pausedError(r)
	}
	wave := r.waveOf(nodeID)
	if wave < 0 {
		return nil, fmt.Errorf("%w: node %s", ErrNodeNotInRollout, nodeID)
	}
	if wave != r.CurrentWave {
		return nil, fmt.Errorf("%w: node %s belongs to wave %d, current wave %d",
			ErrWaveNotOpen, nodeID, wave, r.CurrentWave)
	}
	if r.timedOut(now) {
		d.pause(r, ReasonWaveTimeout, now)
		if err := s.store.Save(context.Background(), d); err != nil {
			return nil, fmt.Errorf("persist timeout pause: %w", err)
		}
		return nil, pausedError(r)
	}
	return r, nil
}

func pausedError(r *Rollout) error {
	if r.PauseReason == ReasonWaveTimeout {
		return fmt.Errorf("%w: wave timeout", ErrRolloutPaused)
	}
	if r.PauseReason == ReasonFailureThreshold {
		return fmt.Errorf("%w: failure threshold exceeded", ErrRolloutPaused)
	}
	return ErrRolloutPaused
}

func (r *Rollout) timedOut(now time.Time) bool {
	return r.Status == StatusActive && r.WaveTimeout > 0 &&
		now.Sub(r.WaveOpenedAt) >= r.WaveTimeout
}

// Receipt 是节点上报的应用结果。
type Receipt struct {
	RolloutID string
	NodeID    string
	Digest    string // 节点实际应用的配置摘要
	ReceiptID string // 节点侧回执 ID，用于审计
	Result    ResultStatus
}

// RecordReceipt 处理一条节点回执。回执按（发布、节点、配置摘要）幂等：
// 重复回执返回首次记录且不产生副作用；旧波次、旧发布、摘要不符、节点已
// 应用更新版本的回执一律被拒绝，不会推进流程也不会覆盖更新版本。
func (s *Service) RecordReceipt(ctx context.Context, rc Receipt) (*NodeResult, error) {
	if rc.Result != ResultSucceeded && rc.Result != ResultFailed {
		return nil, ErrInvalidResult
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	d := s.snapshot()
	now := s.clock.Now()

	r, ok := d.Rollouts[rc.RolloutID]
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrRolloutNotFound, rc.RolloutID)
	}
	// 摘要不符属于旧配置/旧发布的串扰回执，任何情况下都不得进入流程。
	if rc.Digest != r.Digest {
		return nil, fmt.Errorf("%w: receipt %s, rollout %s", ErrDigestMismatch, rc.Digest, r.Digest)
	}
	// 幂等优先：该节点在本次发布已有回执时，重复/迟到的同摘要回执
	// 一律返回首条记录，不产生任何副作用，也不再校验波次是否仍开放。
	if existing, ok := r.Results[rc.NodeID]; ok {
		return cloneResult(existing), nil
	}
	// 首条新回执：节点已在更新的发布中应用了更新版本时拒绝，
	// 旧发布不能借此推进流程或覆盖节点较新的已应用版本。
	if last := d.NodeLastApplied[rc.NodeID]; last != nil && last.Seq > r.Seq {
		return nil, fmt.Errorf("%w: node %s already applied rollout %s seq %d",
			ErrStaleReceipt, rc.NodeID, last.RolloutID, last.Seq)
	}

	wave := r.waveOf(rc.NodeID)
	if wave < 0 {
		return nil, fmt.Errorf("%w: node %s", ErrNodeNotInRollout, rc.NodeID)
	}
	// 只有当前波次可以首次回执；旧波次迟到的首条回执、新波次提前回执都拒绝。
	if wave != r.CurrentWave {
		return nil, fmt.Errorf("%w: node %s belongs to wave %d, current wave %d",
			ErrWaveNotOpen, rc.NodeID, wave, r.CurrentWave)
	}
	switch r.Status {
	case StatusCancelled:
		return nil, fmt.Errorf("%w: %s", ErrRolloutCancelled, r.ID)
	case StatusSucceeded:
		return nil, fmt.Errorf("%w: %s", ErrAlreadyFinished, r.ID)
	case StatusPaused:
		return nil, pausedError(r)
	}
	if r.timedOut(now) {
		d.pause(r, ReasonWaveTimeout, now)
		if err := s.store.Save(context.Background(), d); err != nil {
			return nil, fmt.Errorf("persist timeout pause: %w", err)
		}
		return nil, pausedError(r)
	}

	res := &NodeResult{
		NodeID:     rc.NodeID,
		Wave:       wave,
		Digest:     rc.Digest,
		Status:     rc.Result,
		ReceiptID:  rc.ReceiptID,
		RecordedAt: now,
	}
	r.Results[rc.NodeID] = res
	r.UpdatedAt = now

	if rc.Result == ResultSucceeded {
		// 只增不减地推进节点“已应用最新版本”，旧发布不能降级覆盖。
		last := d.NodeLastApplied[rc.NodeID]
		if last == nil || r.Seq >= last.Seq {
			d.NodeLastApplied[rc.NodeID] = &AppliedRef{
				RolloutID: r.ID,
				Seq:       r.Seq,
				Wave:      wave,
				Digest:    r.Digest,
				AppliedAt: now,
			}
		}
	}

	if err := s.afterReceipt(ctx, d, r, now); err != nil {
		return nil, err
	}
	return cloneResult(res), nil
}

// afterReceipt 依据最新回执推进波次；任一暂停/完成分支都会原子落盘。
func (s *Service) afterReceipt(ctx context.Context, d *Data, r *Rollout, now time.Time) error {
	succeeded, _ := r.waveCounts(r.CurrentWave)
	totalFailed := r.totalFailures()

	// 发布内容忍的失败总数超过阈值：立即暂停，成功不再推动波次。
	if r.MaxFailures > 0 && totalFailed > r.MaxFailures {
		d.pause(r, ReasonFailureThreshold, now)
		return s.commit(ctx)
	}

	threshold := r.thresholdFor(r.CurrentWave)
	if succeeded < threshold {
		return s.commit(ctx)
	}

	// 当前波次达标。若是最后一波，发布成功；否则原子开放下一波。
	if r.CurrentWave == len(r.Waves)-1 {
		d.transition(r, StatusActive, StatusSucceeded, "", now)
		emitRolloutEvent(d, r, evtRolloutSucceeded, now, nil)
		return s.commit(ctx)
	}

	next := r.CurrentWave + 1
	r.CurrentWave = next
	r.WaveOpenedAt = now
	r.UpdatedAt = now
	emitWaveOpened(d, r, next, now)
	return s.commit(ctx)
}

// Pause 人工暂停发布，仅 active 可暂停；对已暂停发布调用是幂等空操作。
func (s *Service) Pause(ctx context.Context, rolloutID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	d := s.snapshot()
	r, ok := d.Rollouts[rolloutID]
	if !ok {
		return fmt.Errorf("%w: %s", ErrRolloutNotFound, rolloutID)
	}
	if r.Status == StatusPaused {
		return nil
	}
	if r.Status == StatusCancelled {
		return fmt.Errorf("%w: %s", ErrRolloutCancelled, rolloutID)
	}
	if r.Status == StatusSucceeded {
		return fmt.Errorf("%w: %s", ErrAlreadyFinished, rolloutID)
	}
	now := s.clock.Now()
	d.pause(r, ReasonManual, now)
	return s.commit(ctx)
}

// Resume 恢复被暂停的发布并重新计时；对 active 发布调用是幂等空操作。
func (s *Service) Resume(ctx context.Context, rolloutID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	d := s.snapshot()
	r, ok := d.Rollouts[rolloutID]
	if !ok {
		return fmt.Errorf("%w: %s", ErrRolloutNotFound, rolloutID)
	}
	if r.Status == StatusActive {
		return nil
	}
	if r.Status == StatusCancelled {
		return fmt.Errorf("%w: %s", ErrRolloutCancelled, rolloutID)
	}
	if r.Status == StatusSucceeded {
		return fmt.Errorf("%w: %s", ErrAlreadyFinished, rolloutID)
	}
	now := s.clock.Now()
	d.transition(r, StatusPaused, StatusActive, "", now)
	r.PauseReason = ""
	r.WaveOpenedAt = now // 恢复后重新给予完整的波次超时窗口
	emitRolloutEvent(d, r, evtRolloutResumed, now, nil)
	return s.commit(ctx)
}

// Cancel 取消发布。取消后任何节点都无法再取得配置或上报回执；
// 已成功节点的审计结果原样保留，并为每个已成功节点生成恰好一次补偿通知。
// 对已取消发布重复调用是幂等空操作。
func (s *Service) Cancel(ctx context.Context, rolloutID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	d := s.snapshot()
	r, ok := d.Rollouts[rolloutID]
	if !ok {
		return fmt.Errorf("%w: %s", ErrRolloutNotFound, rolloutID)
	}
	if r.Status == StatusCancelled {
		return nil
	}
	if r.Status == StatusSucceeded {
		return fmt.Errorf("%w: %s", ErrAlreadyFinished, rolloutID)
	}
	now := s.clock.Now()
	from := r.Status
	d.transition(r, from, StatusCancelled, ReasonManual, now)

	// 为所有已经成功应用的节点登记补偿通知；去重键保证补偿只生成一次，
	// 即使取消逻辑因任何原因被再次执行（例如旧版本代码的重复调用）。
	for nodeID, res := range r.Results {
		if res.Status != ResultSucceeded {
			continue
		}
		payload := map[string]any{
			"rollout_id": r.ID,
			"node_id":    nodeID,
			"wave":       res.Wave,
			"digest":     res.Digest,
		}
		raw, _ := json.Marshal(payload)
		d.appendOutbox("compensation:"+r.ID+":"+nodeID, evtNodeCompensation, raw, now)
	}
	emitRolloutEvent(d, r, evtRolloutCancelled, now, map[string]any{"reason": ReasonManual})
	return s.commit(ctx)
}

// pause 把发布切到暂停态并写审计与 outbox（调用方负责落盘）。
func (d *Data) pause(r *Rollout, reason string, now time.Time) {
	d.transition(r, r.Status, StatusPaused, reason, now)
	r.PauseReason = reason
	emitRolloutEvent(d, r, evtRolloutPaused, now, map[string]any{"reason": reason})
}

// transition 追加一条状态迁移事件。所有状态变更都必须经过这里，
// 从而保证暂停/恢复/取消竞争时最终只有一条单调的状态序列。
func (d *Data) transition(r *Rollout, from, to Status, reason string, now time.Time) {
	r.Status = to
	r.UpdatedAt = now
	r.History = append(r.History, StatusEvent{At: now, From: from, To: to, Reason: reason})
}

// SweepTimeouts 检查全部 active 发布的波次超时，超时的暂停并原子落盘。
// 进程重启后应调用一次（Start 不自动触发），运行期可由定时器周期调用。
func (s *Service) SweepTimeouts(ctx context.Context) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	d := s.snapshot()
	now := s.clock.Now()
	paused := 0
	for _, r := range d.Rollouts {
		if r.timedOut(now) {
			d.pause(r, ReasonWaveTimeout, now)
			paused++
		}
	}
	if paused == 0 {
		return 0, nil
	}
	if err := s.commit(ctx); err != nil {
		return 0, err
	}
	return paused, nil
}

// RunTimeoutSweeper 按 interval 周期性执行 SweepTimeouts，直到 ctx 取消。
func (s *Service) RunTimeoutSweeper(ctx context.Context, interval time.Duration) error {
	if interval <= 0 {
		return fmt.Errorf("sweeper interval must be positive")
	}
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-t.C:
			if _, err := s.SweepTimeouts(ctx); err != nil {
				return err
			}
		}
	}
}
