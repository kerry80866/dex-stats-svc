package types

import (
	"dex-stats-sol/pb"
	"sync/atomic"
)

type TokenPrice struct {
	Price    float64
	Decimals uint8
}

type QuotePrices struct {
	BlockNumber uint32
	BlockTime   uint32
	Prices      map[Pubkey]TokenPrice
}

type GlobalQuotePrices struct {
	ptr atomic.Pointer[QuotePrices]
}

// NewGlobalQuotePrices 初始化全局引用
func NewGlobalQuotePrices() *GlobalQuotePrices {
	g := &GlobalQuotePrices{}
	g.ptr.Store(&QuotePrices{
		Prices: make(map[Pubkey]TokenPrice),
	})
	return g
}

// Load 获取最新的快照 (只读引用，不要修改内部 map)
func (g *GlobalQuotePrices) Load() *QuotePrices {
	return g.ptr.Load()
}

// UpdateFromEvents 基于旧快照增量更新 (用于事件推送场景)
func (g *GlobalQuotePrices) UpdateFromEvents(events *pb.Events) {
	old := g.ptr.Load()
	if old == nil {
		old = &QuotePrices{Prices: make(map[Pubkey]TokenPrice)}
	}

	blockNumber := uint32(events.BlockNumber)
	if blockNumber <= old.BlockNumber {
		// 已经有更新的区块，忽略
		return
	}

	// 拷贝旧的 map，避免并发修改
	l := len(events.QuotePrices) + len(old.Prices)
	prices := make(map[Pubkey]TokenPrice, l)
	for key, val := range old.Prices {
		prices[key] = val
	}

	// 覆盖或新增
	for _, q := range events.QuotePrices {
		tokenAddr, err := PubkeyFromBytes(q.Token)
		if err != nil {
			continue
		}
		prices[tokenAddr] = TokenPrice{
			Price:    q.Price,
			Decimals: uint8(q.Decimals),
		}
	}

	// 写入新快照
	g.ptr.Store(&QuotePrices{
		BlockNumber: blockNumber,
		BlockTime:   uint32(events.BlockTime),
		Prices:      prices,
	})
}

// UpdateFromSnapshot 全量替换快照 (用于恢复/同步场景)
func (g *GlobalQuotePrices) UpdateFromSnapshot(snapshot *QuotePrices) {
	old := g.ptr.Load()
	if old != nil && snapshot.BlockNumber <= old.BlockNumber {
		// 老快照比 snapshot 更新，忽略
		return
	}

	// 深拷贝 map，避免外部修改污染内部状态
	prices := make(map[Pubkey]TokenPrice, len(snapshot.Prices))
	for key, val := range snapshot.Prices {
		prices[key] = val
	}

	g.ptr.Store(&QuotePrices{
		BlockNumber: snapshot.BlockNumber,
		BlockTime:   snapshot.BlockTime,
		Prices:      prices,
	})
}
