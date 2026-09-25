package configrollout

import "time"

// Clock 抽象时间来源，便于测试超时与重启恢复。
type Clock interface {
	Now() time.Time
}

type systemClock struct{}

func (systemClock) Now() time.Time { return time.Now().UTC() }

// fixedClock 返回固定时间，测试用。
type fixedClock struct{ t time.Time }

func (c fixedClock) Now() time.Time { return c.t }

// offsetClock 是可手动推进的时钟，测试用。
type offsetClock struct{ t time.Time }

func (c *offsetClock) Now() time.Time          { return c.t }
func (c *offsetClock) advance(d time.Duration) { c.t = c.t.Add(d) }
