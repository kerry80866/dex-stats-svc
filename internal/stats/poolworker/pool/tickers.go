package pool

import (
	"dex-stats-sol/internal/pkg/logger"
	"dex-stats-sol/pb"
	"fmt"
	"math"
)

const (
	Ticker1sWindowSize = 300
	Ticker1mWindowSize = 60
	Ticker2mWindowSize = 720
)

type AddTickerResult uint32

const (
	TickerMerged      AddTickerResult = math.MaxUint32
	TickerSkipped     AddTickerResult = math.MaxUint32 - 1
	TickerInsertError AddTickerResult = math.MaxUint32 - 2
)

type Tickers struct {
	tickers []Ticker // 环形缓冲区，存储滑动窗口内的成交数据

	// 滑窗控制（环形缓冲）
	startIndex uint32 // 当前窗口在 tickers 中的起始索引
	count      uint32 // 当前窗口内有效数据数量

	// 价格信息
	currentPrice   float64 // 当前价格（最新有效）
	openPrice      float64 // 开盘价（窗口首个有效）
	currentPriceTs uint32  // 当前价格对应时间戳
	openPriceTs    uint32  // 开盘价对应时间戳

	// 滑窗配置参数
	bucketSeconds uint16 // 每个桶的时间宽度（秒）
	windowSize    uint16 // 窗口跨度（单位：桶数）
	capacity      uint32 // 容量上限（桶数 + 1）
}

func NewTickers1s(openPrice float64, openPriceTs uint32) *Tickers {
	tickers := new([Ticker1sWindowSize + 1]Ticker) // 申请固定内存, 长度不可变
	return &Tickers{
		tickers:        tickers[:],
		windowSize:     Ticker1sWindowSize,
		capacity:       uint32(len(tickers)),
		bucketSeconds:  BucketSeconds1s,
		openPrice:      openPrice,
		openPriceTs:    openPriceTs,
		currentPrice:   openPrice,
		currentPriceTs: openPriceTs,
	}
}

func NewTickers1m(openPrice float64, openPriceTs uint32) *Tickers {
	tickers := new([Ticker1mWindowSize + 1]Ticker)
	return &Tickers{
		tickers:        tickers[:],
		windowSize:     Ticker1mWindowSize,
		capacity:       uint32(len(tickers)),
		bucketSeconds:  BucketSeconds1m,
		openPrice:      openPrice,
		openPriceTs:    openPriceTs,
		currentPrice:   openPrice,
		currentPriceTs: openPriceTs,
	}
}

func NewTickers2m(openPrice float64, openPriceTs uint32) *Tickers {
	tickers := new([Ticker2mWindowSize + 1]Ticker)
	return &Tickers{
		tickers:        tickers[:],
		windowSize:     Ticker2mWindowSize,
		capacity:       uint32(len(tickers)),
		bucketSeconds:  BucketSeconds2m,
		openPrice:      openPrice,
		openPriceTs:    openPriceTs,
		currentPrice:   openPrice,
		currentPriceTs: openPriceTs,
	}
}

// idx 将逻辑索引映射为环形缓冲区的物理索引
func (t *Tickers) idx(i uint32) uint32 {
	if i < t.capacity { // 性能优化：避免不必要的取模
		return i
	}
	return i % t.capacity
}

func (t *Tickers) at(index uint32) *Ticker {
	return &t.tickers[t.idx(index)]
}

func (t *Tickers) removeExpired(latestTime uint32) bool {
	targetStart := t.calcWindowStart(latestTime, t.windowSize)
	for t.count > 0 {
		ticker := t.at(t.startIndex)

		// 如果 ticker 的时间不超出目标时间窗口，则停止移除数据
		if ticker.TimeBucket(t.bucketSeconds) >= targetStart {
			break
		}

		if price := ticker.ClosePrice(); price != 0 {
			t.openPrice = price
			t.openPriceTs = ticker.closeTs
		}

		t.startIndex++
		t.count--
	}

	oldPrice := t.currentPrice
	t.fallbackCurrentPrice()
	return oldPrice != t.currentPrice
}

func (t *Tickers) slideOutWindow(wd *WindowData, latestTime uint32, priceChanged bool) bool {
	targetStart := t.calcWindowStart(latestTime, wd.windowSize)
	endIndex := t.startIndex + t.count
	updated := priceChanged

	// 遍历窗口中的数据并移除超出的部分
	for ; wd.seq < endIndex; wd.seq++ {
		ticker := t.at(wd.seq)

		// 如果 ticker 的时间不超出目标时间窗口，则停止移除数据
		if ticker.TimeBucket(t.bucketSeconds) >= targetStart {
			break
		}

		// 从窗口中移除该 ticker 的数据
		wd.setBuyCount(wd.BuyCount() - ticker.BuyCount())
		wd.setSellCount(wd.SellCount() - ticker.SellCount())
		wd.setBuyVolume(wd.BuyVolume() - ticker.BuyVolume())
		wd.setSellVolume(wd.SellVolume() - ticker.SellVolume())

		// 如果该 ticker 的价格有效，则更新开盘价格和时间戳
		if price := ticker.ClosePrice(); price != 0 {
			wd.setOpenPrice(price)
			wd.setOpenPriceTs(ticker.closeTs)
		}
		updated = true
	}

	if updated {
		wd.updatePriceChangeRate(t.currentPrice)
	}
	return updated
}

// addTicker 添加或合并ticker
func (t *Tickers) addTicker(newTicker Ticker, latestTime uint32) AddTickerResult {
	if newTicker.IsEmpty() {
		return TickerSkipped
	}

	// 1. 处理窗口左侧的历史数据，更新 openPrice
	targetStart := t.calcWindowStart(latestTime, t.windowSize)
	if newTicker.closeTs < targetStart {
		if newTicker.closeTs >= t.openPriceTs && newTicker.ClosePrice() != 0 {
			t.openPriceTs = newTicker.closeTs
			t.openPrice = newTicker.ClosePrice()
			t.fallbackCurrentPrice()
		}
		return TickerMerged
	}

	// 2. 窗口为空
	if t.count == 0 {
		t.count++
		*t.at(t.startIndex) = newTicker
		t.tryUpdateCurrentPrice(t.startIndex)
		return AddTickerResult(t.startIndex)
	}

	lastIndex := t.startIndex + t.count - 1
	lastTicker := t.at(lastIndex)
	lastBucket := lastTicker.TimeBucket(t.bucketSeconds)
	bucket := newTicker.TimeBucket(t.bucketSeconds)

	// 3. 新 ticker 属于最后一个桶 -> 合并
	if bucket == lastBucket {
		lastTicker.addTicker(newTicker)
		t.tryUpdateCurrentPrice(lastIndex)
		return TickerMerged
	}

	// 4. 新 ticker 晚于最后一个桶 -> 追加
	if bucket > lastBucket {
		if t.count >= t.capacity {
			logger.Errorf("[Tickers] BUG: addTicker overflow: count=%d >= cap=%d, start=%d, ts=%d",
				t.count, t.capacity, t.startIndex, latestTime)
			return TickerInsertError
		}
		t.count++
		lastIndex++
		*t.at(lastIndex) = newTicker
		t.tryUpdateCurrentPrice(lastIndex)
		return AddTickerResult(lastIndex)
	}

	// 5. 新 ticker 插入到已有时间桶区间内
	pos, exact := t.findInsertPosition(bucket)
	if exact {
		t.at(pos).addTicker(newTicker)
		t.tryUpdateCurrentPrice(pos)
		return TickerMerged
	}

	// 6. 新 ticker 插入到合适位置
	if t.count >= t.capacity {
		logger.Errorf("[Tickers] BUG: addTicker overflow: count=%d >= cap=%d, start=%d, ts=%d",
			t.count, t.capacity, t.startIndex, latestTime)
		return TickerInsertError
	}
	t.insertAt(pos, newTicker)
	t.tryUpdateCurrentPrice(pos)
	return AddTickerResult(pos)
}

// fallbackCurrentPrice 兜底更新 currentPrice，避免 currentPrice 过旧或未设置
func (t *Tickers) fallbackCurrentPrice() {
	if t.openPriceTs > t.currentPriceTs && t.openPrice != 0 {
		t.currentPriceTs = t.openPriceTs
		t.currentPrice = t.openPrice
	}
}

func (t *Tickers) tryUpdateCurrentPrice(idx uint32) {
	ticker := t.at(idx)
	if ticker.closeTs >= t.currentPriceTs && ticker.ClosePrice() != 0 {
		t.currentPriceTs = ticker.closeTs
		t.currentPrice = ticker.ClosePrice()
	}
}

func (t *Tickers) addTickerToWindow(wd *WindowData, ticker Ticker, insertPos AddTickerResult, latestTime uint32) {
	if insertPos == TickerSkipped || insertPos == TickerInsertError {
		return
	}

	// 计算目标窗口的起始时间
	targetStart := t.calcWindowStart(latestTime, wd.windowSize)

	// 如果 ticker 的时间戳在当前窗口开始时间之前
	if ticker.closeTs < targetStart {
		// 如果 ticker 的时间戳在开盘时间内，且成交价非零，则更新开盘时间和开盘价
		if ticker.closeTs >= wd.openPriceTs && ticker.ClosePrice() != 0 {
			wd.setOpenPriceTs(ticker.closeTs)
			wd.setOpenPrice(ticker.ClosePrice())
		}

		// 如果插入了一个旧 ticker，且新插入位置在 seq 之前，seq 需加 1 保持同步
		if uint32(insertPos) <= wd.seq {
			wd.seq++
		}
	} else {
		// 更新买入、卖出数量以及成交量
		wd.setBuyCount(wd.BuyCount() + ticker.BuyCount())
		wd.setSellCount(wd.SellCount() + ticker.SellCount())
		wd.setBuyVolume(wd.BuyVolume() + ticker.BuyVolume())
		wd.setSellVolume(wd.SellVolume() + ticker.SellVolume())
	}

	wd.updatePriceChangeRate(t.currentPrice)
}

// findInsertPosition 在tickers中查找 tickerBucket 对应的位置。
// 如果找到相同的时间桶，返回索引和 true。
// 如果没找到，返回应插入位置的索引和 false。
func (t *Tickers) findInsertPosition(tickerBucket uint32) (uint32, bool) {
	if t.count == 0 {
		return t.startIndex, false
	}

	low := int(t.startIndex)
	high := int(t.startIndex + t.count - 1)

	for low <= high {
		mid := (low + high) / 2
		midBucket := t.at(uint32(mid)).TimeBucket(t.bucketSeconds)

		if midBucket == tickerBucket {
			return uint32(mid), true // 找到相同时间桶位置
		} else if midBucket < tickerBucket {
			low = mid + 1
		} else {
			high = mid - 1
		}
	}
	return uint32(low), false // 返回应插入位置
}

func (t *Tickers) insertAt(position uint32, ticker Ticker) {
	startIdx := t.idx(position)                  // 计算插入位置的物理索引（环形缓冲区内的实际数组下标）
	lastIdx := t.idx(t.startIndex + t.count - 1) // 计算当前最后一个元素的物理索引

	if startIdx <= lastIdx {
		// 插入位置在尾部元素之前（物理索引连续区间内）
		if lastIdx+1 == t.capacity {
			// 最后一个元素已经处于数组末尾，尾部空间已满，需要特殊处理

			// 1. 先将数组末尾的最后一个元素搬到索引0，防止后续copy覆盖它
			t.tickers[0] = t.tickers[t.capacity-1]

			// 2. 将插入位置到尾部前一个元素的元素整体右移一格，为插入腾出空间
			if startIdx < lastIdx {
				copy(t.tickers[startIdx+1:lastIdx+1], t.tickers[startIdx:lastIdx])
			}
		} else {
			// 尾部未满，可以直接用 copy 操作将 [startIdx:lastIdx+1] 区间内元素右移一格
			// 这样在 startIdx 位置腾出插入新元素的位置
			copy(t.tickers[startIdx+1:lastIdx+2], t.tickers[startIdx:lastIdx+1])
		}
	} else {
		// 插入位置在尾部物理索引后面，说明数据环绕到了数组头部（跨越环形边界）

		// 1. 先将数组开头元素整体右移一格，腾出索引0位置
		copy(t.tickers[1:lastIdx+2], t.tickers[0:lastIdx+1])

		// 2. 将数组末尾的最后一个元素搬到索引0，防止后续copy覆盖
		t.tickers[0] = t.tickers[t.capacity-1]

		// 3. 将插入位置之后的元素右移一格，腾出插入位置
		copy(t.tickers[startIdx+1:], t.tickers[startIdx:])
	}

	// 最后，将新元素放入腾出的插入位置
	t.tickers[startIdx] = ticker
	t.count++ // 元素数量+1
}

func (t *Tickers) calcWindowStart(latestTime uint32, bucketSpan uint16) uint32 {
	bucketSeconds := uint32(t.bucketSeconds)
	startBucket := latestTime/bucketSeconds - uint32(bucketSpan) + 1 // +1 表示包含当前所在桶
	return startBucket * bucketSeconds
}

func (t *Tickers) toProto() *pb.TickersSnapshot {
	ts := &pb.TickersSnapshot{
		OpenPrice:   t.openPrice,
		OpenPriceTs: t.openPriceTs,
		Tickers:     make([]*pb.TickerSnapshot, 0, t.count),
	}

	end := t.startIndex + t.count
	for i := t.startIndex; i < end; i++ {
		ts.Tickers = append(ts.Tickers, t.at(i).toProto())
	}
	return ts
}

// loadSnapshot 用于从快照数据恢复 Tickers 的状态，包括历史 ticker 列表、价格信息、以及各窗口的聚合指标。
// 参数：
// - tickers: protobuf 格式的快照数据
// - windows: 可选的多个时间窗口，用于恢复窗口级别的聚合状态
func (t *Tickers) loadSnapshot(tickers *pb.TickersSnapshot, windows ...*WindowData) error {
	// 如果快照数据长度超过本地环形缓冲区容量，认为是错误状态
	if len(tickers.Tickers) > cap(t.tickers) {
		logger.Errorf("[Tickers] Bug loadSnapshot input length %d exceeds capacity %d, aborting",
			len(tickers.Tickers), cap(t.tickers))
		return fmt.Errorf("[Tickers] loadSnapshot input length %d exceeds capacity %d",
			len(tickers.Tickers), cap(t.tickers))
	}

	// 初始化元数据（环形缓冲区状态）
	t.count = uint32(len(tickers.Tickers))
	t.startIndex = 0
	t.openPriceTs = tickers.OpenPriceTs
	t.currentPriceTs = tickers.OpenPriceTs
	t.openPrice = tickers.OpenPrice
	t.currentPrice = tickers.OpenPrice

	// 初始化全局聚合指标（用于恢复窗口状态）
	var (
		totalBuyCount   uint32
		totalSellCount  uint32
		totalBuyVolume  float64
		totalSellVolume float64
	)

	// 恢复每个 ticker 条目，并同时聚合全局指标
	for i, item := range tickers.Tickers {
		ticker := NewTicker(
			uint16(item.BuyCount), uint16(item.SellCount),
			item.BuyVolume, item.SellVolume,
			item.CloseTs, item.ClosePrice,
		)
		t.tickers[i] = ticker
		t.tryUpdateCurrentPrice(uint32(i)) // 检查是否需要更新当前价格

		totalBuyCount += ticker.BuyCount()
		totalSellCount += ticker.SellCount()
		totalBuyVolume += ticker.BuyVolume()
		totalSellVolume += ticker.SellVolume()
	}

	// 恢复每个窗口的聚合数据与涨跌幅
	for _, wd := range windows {
		wd.setSeq(0)
		wd.setOpenPriceTs(t.openPriceTs)
		wd.setOpenPrice(t.openPrice)
		wd.updatePriceChangeRate(t.currentPrice)

		wd.setBuyCount(totalBuyCount)
		wd.setSellCount(totalSellCount)
		wd.setBuyVolume(totalBuyVolume)
		wd.setSellVolume(totalSellVolume)
	}

	return nil
}
