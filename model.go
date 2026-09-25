package configrollout

import (
	"encoding/json"
	"time"
)

// Status 是发布的整体状态。状态只能沿允许的方向迁移：
//
//	Active -> Paused -> Active ...
//	Active | Paused -> Cancelled
//	Active -> Succeeded
//
// Cancelled / Succeeded 为终态。
type Status string

const (
	StatusActive    Status = "active"
	StatusPaused    Status = "paused"
	StatusCancelled Status = "cancelled"
	StatusSucceeded Status = "succeeded"
)

// 暂停原因。
const (
	ReasonManual           = "manual"
	ReasonFailureThreshold = "failure_threshold"
	ReasonWaveTimeout      = "wave_timeout"
)

// ResultStatus 是单个节点对某次发布的应用结果。
type ResultStatus string

const (
	ResultSucceeded ResultStatus = "succeeded"
	ResultFailed    ResultStatus = "failed"
)

// WaveState 描述单个波次在进度视图中的状态。
type WaveState string

const (
	WavePending     WaveState = "pending"     // 尚未开放
	WaveOpen        WaveState = "open"        // 当前开放中
	WaveDone        WaveState = "done"        // 已完成
	WaveInterrupted WaveState = "interrupted" // 发布取消时仍未完成的波次
)

// Config 是不可变配置项，以摘要作为唯一标识。
type Config struct {
	Digest    string    `json:"digest"`
	Content   []byte    `json:"content"`
	CreatedAt time.Time `json:"created_at"`
}

// NodeResult 是节点在某次发布中的回执审计记录。
// 一个节点在一次发布中至多有一条记录。
type NodeResult struct {
	NodeID     string       `json:"node_id"`
	Wave       int          `json:"wave"`
	Digest     string       `json:"digest"`
	Status     ResultStatus `json:"status"`
	ReceiptID  string       `json:"receipt_id,omitempty"`
	RecordedAt time.Time    `json:"recorded_at"`
}

// StatusEvent 是发布状态迁移的一条不可变审计记录。
type StatusEvent struct {
	At     time.Time `json:"at"`
	From   Status    `json:"from"`
	To     Status    `json:"to"`
	Reason string    `json:"reason,omitempty"`
}

// AppliedRef 记录某个节点“已应用的最新版本”，用于让旧发布的迟到回执
// 无法覆盖节点上更新的已应用版本。序号随发布创建单调递增。
type AppliedRef struct {
	RolloutID string    `json:"rollout_id"`
	Seq       int64     `json:"seq"`
	Wave      int       `json:"wave"`
	Digest    string    `json:"digest"`
	AppliedAt time.Time `json:"applied_at"`
}

// Rollout 是一次配置发布的完整状态。
type Rollout struct {
	ID     string     `json:"id"`
	Seq    int64      `json:"seq"`
	Digest string     `json:"digest"`
	Waves  [][]string `json:"waves"`
	Status Status     `json:"status"`

	// SuccessThreshold 为每个波次所需的成功节点数；0 表示波次内全部成功。
	SuccessThreshold int `json:"success_threshold"`
	// MaxFailures 为允许的失败节点数；失败数一旦超过该值即暂停。
	MaxFailures int `json:"max_failures"`
	// WaveTimeout 为单个波次的等待超时；0 表示不超时。
	WaveTimeout time.Duration `json:"wave_timeout"`

	CurrentWave  int       `json:"current_wave"`
	WaveOpenedAt time.Time `json:"wave_opened_at"`
	PauseReason  string    `json:"pause_reason,omitempty"`

	// Results 按节点 ID 保存回执，节点在一次发布中只出现一次。
	Results map[string]*NodeResult `json:"results"`
	History []StatusEvent          `json:"history"`

	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
}

// waveOf 返回节点所属波次，找不到返回 -1。
func (r *Rollout) waveOf(nodeID string) int {
	for i, w := range r.Waves {
		for _, n := range w {
			if n == nodeID {
				return i
			}
		}
	}
	return -1
}

// thresholdFor 返回指定波次实际需要的成功数。
func (r *Rollout) thresholdFor(wave int) int {
	if r.SuccessThreshold > 0 {
		return r.SuccessThreshold
	}
	return len(r.Waves[wave])
}

// waveCounts 统计当前波次的成功/失败数。
func (r *Rollout) waveCounts(wave int) (succeeded, failed int) {
	for _, res := range r.Results {
		if res.Wave != wave {
			continue
		}
		switch res.Status {
		case ResultSucceeded:
			succeeded++
		case ResultFailed:
			failed++
		}
	}
	return
}

// totalFailures 统计整次发布中所有波次的失败节点数；
// MaxFailures 是发布级阈值，不因换波而重置。
func (r *Rollout) totalFailures() int {
	failed := 0
	for _, res := range r.Results {
		if res.Status == ResultFailed {
			failed++
		}
	}
	return failed
}

// OutboxMessage 是事务性 outbox 中的一条通知。
type OutboxMessage struct {
	Seq         int64           `json:"seq"`
	Key         string          `json:"key"` // 去重键，同一 Key 全局只生成一次
	Type        string          `json:"type"`
	Payload     json.RawMessage `json:"payload"`
	Attempts    int             `json:"attempts"`
	CreatedAt   time.Time       `json:"created_at"`
	PublishedAt *time.Time      `json:"published_at,omitempty"`
}

// Data 是持久化的完整状态快照。状态与 outbox 位于同一快照中，
// 因而任何一次提交对二者都是原子的。
type Data struct {
	Version         int                       `json:"version"`
	NextRolloutSeq  int64                     `json:"next_rollout_seq"`
	NextMessageSeq  int64                     `json:"next_message_seq"`
	Configs         map[string]*Config        `json:"configs"`
	Rollouts        map[string]*Rollout       `json:"rollouts"`
	NodeLastApplied map[string]*AppliedRef    `json:"node_last_applied"`
	Outbox          []*OutboxMessage          `json:"outbox"`
	OutboxByKey     map[string]*OutboxMessage `json:"outbox_by_key"`
}

func newData() *Data {
	return &Data{
		Version:         1,
		NextRolloutSeq:  1,
		Configs:         map[string]*Config{},
		Rollouts:        map[string]*Rollout{},
		NodeLastApplied: map[string]*AppliedRef{},
		OutboxByKey:     map[string]*OutboxMessage{},
	}
}

// appendOutbox 在快照中追加一条 outbox 消息；Key 已存在时不重复生成。
// 返回消息以及是否为新插入。
func (d *Data) appendOutbox(key, typ string, payload json.RawMessage, now time.Time) (*OutboxMessage, bool) {
	if m, ok := d.OutboxByKey[key]; ok {
		return m, false
	}
	m := &OutboxMessage{
		Seq:       d.NextMessageSeq,
		Key:       key,
		Type:      typ,
		Payload:   payload,
		CreatedAt: now,
	}
	d.NextMessageSeq++
	d.Outbox = append(d.Outbox, m)
	d.OutboxByKey[key] = m
	return m, true
}
