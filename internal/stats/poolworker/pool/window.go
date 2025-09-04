package pool

import "dex-stats-sol/internal/stats/types"

const (
	BucketSeconds1s = 1   // 1秒桶大小
	BucketSeconds1m = 60  // 1分钟桶大小，单位秒
	BucketSeconds2m = 120 // 2分钟桶大小，单位秒
)

// WindowSizes 表示每个窗口包含多少桶
var WindowSizes = [types.WindowCount]uint16{
	60,  // 1分钟窗口，60个1秒桶
	300, // 5分钟窗口，300个1秒桶
	60,  // 1小时窗口，60个1分钟桶
	180, // 6小时窗口，180个2分钟桶
	720, // 24小时窗口，720个2分钟桶
}

// SlidingWindowsBuckets 表示各时间窗口及其对应的桶大小，方便遍历使用
var SlidingWindowsBuckets = [types.WindowCount]struct {
	Duration      uint32 // 窗口总时长（秒）
	BucketSeconds uint32 // 桶大小（秒）
}{
	{60, BucketSeconds1s},        // 1分钟窗口，1秒桶
	{300, BucketSeconds1s},       // 5分钟窗口，1秒桶
	{3600, BucketSeconds1m},      // 1小时窗口，1分钟桶
	{6 * 3600, BucketSeconds2m},  // 6小时窗口，2分钟桶
	{24 * 3600, BucketSeconds2m}, // 24小时窗口，2分钟桶
}
