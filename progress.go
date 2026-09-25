package configrollout

import (
	"context"
	"fmt"
	"sort"
	"time"
)

// WaveProgress 是单个波次的进度视图。
type WaveProgress struct {
	Index     int       `json:"index"`
	State     WaveState `json:"state"`
	Nodes     []string  `json:"nodes"`
	Succeeded int       `json:"succeeded"`
	Failed    int       `json:"failed"`
	Pending   int       `json:"pending"`
	Threshold int       `json:"threshold"`
	OpenedAt  time.Time `json:"opened_at,omitempty"`
}

// Progress 是一次发布的完整进度视图。
type Progress struct {
	ID             string         `json:"id"`
	Digest         string         `json:"digest"`
	Status         Status         `json:"status"`
	PauseReason    string         `json:"pause_reason,omitempty"`
	CurrentWave    int            `json:"current_wave"`
	TotalWaves     int            `json:"total_waves"`
	Waves          []WaveProgress `json:"waves"`
	TotalSucceeded int            `json:"total_succeeded"`
	TotalFailed    int            `json:"total_failed"`
	History        []StatusEvent  `json:"history"`
	WaveOpenedAt   time.Time      `json:"wave_opened_at,omitempty"`
	UpdatedAt      time.Time      `json:"updated_at"`
}

// GetProgress 返回发布进度；不存在时返回 ErrRolloutNotFound。
// 该方法只读，不会触发超时暂停等副作用。
func (s *Service) GetProgress(ctx context.Context, rolloutID string) (*Progress, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	d := s.snapshot()
	r, ok := d.Rollouts[rolloutID]
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrRolloutNotFound, rolloutID)
	}
	return buildProgress(r), nil
}

// ListRolloutIDs 按创建顺序（Seq 升序）返回全部发布 ID。
func (s *Service) ListRolloutIDs(ctx context.Context) ([]string, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	d := s.snapshot()

	type entry struct {
		id  string
		seq int64
	}
	entries := make([]entry, 0, len(d.Rollouts))
	for id, r := range d.Rollouts {
		entries = append(entries, entry{id, r.Seq})
	}
	sort.Slice(entries, func(i, j int) bool { return entries[i].seq < entries[j].seq })

	ids := make([]string, len(entries))
	for i, e := range entries {
		ids[i] = e.id
	}
	return ids, nil
}

// GetNodeResult 返回某节点在某发布中的回执审计记录；尚未上报时返回 (nil, nil)。
func (s *Service) GetNodeResult(ctx context.Context, rolloutID, nodeID string) (*NodeResult, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	d := s.snapshot()
	r, ok := d.Rollouts[rolloutID]
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrRolloutNotFound, rolloutID)
	}
	if res := r.Results[nodeID]; res != nil {
		return cloneResult(res), nil
	}
	return nil, nil
}

// GetApplied 返回节点当前记录在案的“已应用最新版本”，无记录时返回 nil。
func (s *Service) GetApplied(ctx context.Context, nodeID string) (*AppliedRef, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	d := s.snapshot()
	if ref := d.NodeLastApplied[nodeID]; ref != nil {
		out := *ref
		return &out, nil
	}
	return nil, nil
}

func buildProgress(r *Rollout) *Progress {
	p := &Progress{
		ID:           r.ID,
		Digest:       r.Digest,
		Status:       r.Status,
		PauseReason:  r.PauseReason,
		CurrentWave:  r.CurrentWave,
		TotalWaves:   len(r.Waves),
		WaveOpenedAt: r.WaveOpenedAt,
		UpdatedAt:    r.UpdatedAt,
		History:      append([]StatusEvent(nil), r.History...),
	}
	for i, nodes := range r.Waves {
		wp := WaveProgress{
			Index:     i,
			Nodes:     append([]string(nil), nodes...),
			Threshold: r.thresholdFor(i),
		}
		state := WavePending
		switch {
		case i < r.CurrentWave:
			state = WaveDone
		case i == r.CurrentWave:
			if r.Status == StatusSucceeded {
				state = WaveDone
			} else if r.Status == StatusCancelled {
				state = WaveInterrupted
			} else {
				state = WaveOpen
				wp.OpenedAt = r.WaveOpenedAt
			}
		default: // i > CurrentWave
			if r.Status == StatusCancelled {
				state = WaveInterrupted
			}
		}
		wp.State = state
		wp.Pending = len(nodes)
		p.Waves = append(p.Waves, wp)
	}
	for _, res := range r.Results {
		if res.Wave < 0 || res.Wave >= len(p.Waves) {
			continue
		}
		switch res.Status {
		case ResultSucceeded:
			p.Waves[res.Wave].Succeeded++
			p.TotalSucceeded++
		case ResultFailed:
			p.Waves[res.Wave].Failed++
			p.TotalFailed++
		}
		p.Waves[res.Wave].Pending--
	}
	return p
}
