package configrollout

import (
	"encoding/json"
	"strconv"
	"time"
)

// outbox 事件类型。
const (
	evtWaveOpened       = "wave.opened"
	evtRolloutPaused    = "rollout.paused"
	evtRolloutResumed   = "rollout.resumed"
	evtRolloutCancelled = "rollout.cancelled"
	evtRolloutSucceeded = "rollout.succeeded"
	evtNodeCompensation = "node.compensation"
)

// emitWaveOpened 记录“某波次开放”通知。波次在一次发布中只开放一次，
// 去重键包含波次序号。
func emitWaveOpened(d *Data, r *Rollout, wave int, now time.Time) {
	payload := map[string]any{
		"rollout_id": r.ID,
		"wave":       wave,
		"digest":     r.Digest,
		"nodes":      append([]string(nil), r.Waves[wave]...),
		"threshold":  r.thresholdFor(wave),
		"opened_at":  now,
	}
	raw, _ := json.Marshal(payload)
	d.appendOutbox("wave-opened:"+r.ID+":"+strconv.Itoa(wave), evtWaveOpened, raw, now)
}

// emitRolloutEvent 记录发布级状态通知。去重键带状态事件序号
// （同一发布可能经历多轮暂停/恢复，每条都必须保留）。
func emitRolloutEvent(d *Data, r *Rollout, typ string, now time.Time, extra map[string]any) {
	payload := map[string]any{
		"rollout_id": r.ID,
		"digest":     r.Digest,
		"event":      typ,
		"at":         now,
	}
	for k, v := range extra {
		payload[k] = v
	}
	raw, _ := json.Marshal(payload)
	d.appendOutbox(statusEventKey(r, typ), typ, raw, now)
}

func statusEventKey(r *Rollout, typ string) string {
	// 每条通知紧跟在对应的状态迁移之后产生，此时 History 末尾即该迁移，
	// 其位置在整个单调状态序列中唯一。
	return "rollout-event:" + r.ID + ":" + strconv.Itoa(len(r.History)) + ":" + typ
}
