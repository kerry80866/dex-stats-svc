package utils

import (
	"sync/atomic"
	"time"
)

// ThrottleLog 限频打印
// lastTime: 上次打印时间指针
// interval: 限频间隔
func ThrottleLog(lastTime *atomic.Int64, interval time.Duration) bool {
	now := time.Now().UnixNano()
	last := lastTime.Load()
	if now-last >= interval.Nanoseconds() {
		return lastTime.CompareAndSwap(last, now)
	}
	return false
}
