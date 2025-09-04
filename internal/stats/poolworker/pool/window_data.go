package pool

import (
	"math"
	"sync/atomic"
)

// WindowData 用于表示一个时间窗口的统计数据
type WindowData struct {
	buyCount        atomic.Uint32 // 当前窗口内的买入笔数
	sellCount       atomic.Uint32 // 当前窗口内的卖出笔数
	buyVolume       atomic.Uint64 // 当前窗口内的买入成交额（单位：USD，float64的bit表示）
	sellVolume      atomic.Uint64 // 当前窗口内的卖出成交额（单位：USD，float64的bit表示）
	openPrice       atomic.Uint64 // 当前窗口的开盘价（单位：USD，float64的bit表示）
	priceChangeRate atomic.Uint32 // 当前窗口的价格变化率（单位：比例，例如 0.25 表示上涨 25%，-0.10 表示下跌 10%，float32的bit表示）

	// 非导出的字段，只能通过 getter/setter 方法访问
	windowSize  uint16 // 窗口包含的时间桶数量
	openPriceTs uint32 // 开盘价对应的时间戳（内部使用）
	seq         uint32 // 窗口全局逻辑序列号，单调递增标识窗口位置
}

// Getters

func (wd *WindowData) BuyCount() uint32 {
	return wd.buyCount.Load()
}

func (wd *WindowData) SellCount() uint32 {
	return wd.sellCount.Load()
}

func (wd *WindowData) BuyVolume() float64 {
	return math.Float64frombits(wd.buyVolume.Load())
}

func (wd *WindowData) SellVolume() float64 {
	return math.Float64frombits(wd.sellVolume.Load())
}

func (wd *WindowData) OpenPrice() float64 {
	return math.Float64frombits(wd.openPrice.Load())
}

func (wd *WindowData) PriceChangeRate() float32 {
	return math.Float32frombits(wd.priceChangeRate.Load())
}

func (wd *WindowData) WindowSize() uint16 {
	return wd.windowSize
}

func (wd *WindowData) OpenPriceTs() uint32 {
	return wd.openPriceTs
}

func (wd *WindowData) Seq() uint32 {
	return wd.seq
}

// Setters

func (wd *WindowData) setBuyCount(val uint32) {
	wd.buyCount.Store(val)
}

func (wd *WindowData) setSellCount(val uint32) {
	wd.sellCount.Store(val)
}

func (wd *WindowData) setBuyVolume(val float64) {
	wd.buyVolume.Store(math.Float64bits(val))
}

func (wd *WindowData) setSellVolume(val float64) {
	wd.sellVolume.Store(math.Float64bits(val))
}

func (wd *WindowData) setOpenPrice(val float64) {
	wd.openPrice.Store(math.Float64bits(val))
}

func (wd *WindowData) setPriceChangeRate(val float32) {
	wd.priceChangeRate.Store(math.Float32bits(val))
}

func (wd *WindowData) updatePriceChangeRate(currentPrice float64) {
	openPrice := wd.OpenPrice()
	if openPrice != 0 {
		newRate := float32((currentPrice - openPrice) / openPrice)
		wd.setPriceChangeRate(newRate)
	} else {
		wd.setPriceChangeRate(0)
	}
}

func (wd *WindowData) setWindowSize(val uint16) {
	wd.windowSize = val
}

func (wd *WindowData) setOpenPriceTs(val uint32) {
	wd.openPriceTs = val
}

func (wd *WindowData) setSeq(val uint32) {
	wd.seq = val
}
