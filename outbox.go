package configrollout

import (
	"context"
	"encoding/json"
	"fmt"
)

// PendingOutbox 返回尚未发布的 outbox 消息，按 Seq 升序。
// 消息与业务状态在同一事务中写入，重启后不会丢失。
func (s *Service) PendingOutbox(ctx context.Context) ([]*OutboxMessage, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	d := s.snapshot()

	out := make([]*OutboxMessage, 0, len(d.Outbox))
	for _, m := range d.Outbox {
		if m.PublishedAt == nil {
			out = append(out, cloneMessage(m))
		}
	}
	return out, nil
}

// MarkOutboxPublished 将一批消息（按 Key）标记为已发布并原子落盘。
// 对同一消息重复标记是幂等的；未知的 Key 会被忽略。投递方至少投递一次，
// 去重由消息 Key 在消费侧或本方法共同保证。
func (s *Service) MarkOutboxPublished(ctx context.Context, keys []string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	d := s.snapshot()
	now := s.clock.Now()
	changed := false
	for _, key := range keys {
		m, ok := d.OutboxByKey[key]
		if !ok || m.PublishedAt != nil {
			continue
		}
		m.PublishedAt = &now
		m.Attempts++
		changed = true
	}
	if !changed {
		return nil
	}
	return s.commit(ctx)
}

// RecordOutboxAttempt 记录一次投递尝试（成功与否都可调用），用于排障统计。
func (s *Service) RecordOutboxAttempt(ctx context.Context, key string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	d := s.snapshot()
	m, ok := d.OutboxByKey[key]
	if !ok {
		return fmt.Errorf("outbox message not found: %s", key)
	}
	m.Attempts++
	return s.commit(ctx)
}

func cloneMessage(m *OutboxMessage) *OutboxMessage {
	out := *m
	out.Payload = append(json.RawMessage(nil), m.Payload...)
	if m.PublishedAt != nil {
		t := *m.PublishedAt
		out.PublishedAt = &t
	}
	return &out
}

func cloneResult(r *NodeResult) *NodeResult {
	out := *r
	return &out
}

func cloneRollout(r *Rollout) *Rollout {
	out := *r
	out.Waves = make([][]string, len(r.Waves))
	for i, w := range r.Waves {
		out.Waves[i] = append([]string(nil), w...)
	}
	out.Results = make(map[string]*NodeResult, len(r.Results))
	for k, v := range r.Results {
		out.Results[k] = cloneResult(v)
	}
	out.History = append([]StatusEvent(nil), r.History...)
	return &out
}
