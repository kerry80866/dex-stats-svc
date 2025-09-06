package pool

import (
	"dex-stats-sol/internal/consts"
	"dex-stats-sol/internal/stats/types"
	"dex-stats-sol/pb"
)

func (p *Pool) GetPoolStatsData() *pb.TickerData {
	if cache := p.poolStatsCache.Load(); cache != nil {
		return cache
	}
	return p.SyncPoolStatsFull()
}

// SyncLiquidityCache 将最新流动性同步到查询缓存。
//
// ⚠️ 注意：该方法是非安全更新，仅在性能优化场景下使用。
//  1. 这是对外导出的 pb 字段，无法在此处直接使用原子操作或锁来更新。
//     在单线程写入、多线程读取时：读线程可能看到旧值或新值（存在数据竞争）。
//  2. 在 64 位平台上，float64 是原子对齐的，因此不会出现半写入或数据破坏。
//     最坏情况是 Liquidity 值稍有滞后，但不会崩溃或产生不可预期的值。
//  3. 流动性更新涉及的 pool 数量庞大且频繁，若每次都做安全更新，性能开销过高。
//     因此采用轻量策略，容忍少量滞后来换取整体性能。
func (p *Pool) SyncLiquidityCache() {
	if cache := p.poolStatsCache.Load(); cache != nil {
		// 非线程安全更新（见方法注释）
		cache.MarketData.Liquidity = p.Liquidity()
	}
}

// SyncPoolStatsFull 同步整个 pool 的统计数据到 pb 查询缓存
func (p *Pool) SyncPoolStatsFull() *pb.TickerData {
	var tradeParams *pb.MMReport
	if p.HasLeverage() {
		tradeParams = &pb.MMReport{
			LongLeverage:  p.LongLeverage(),
			ShortLeverage: p.ShortLeverage(),
			ListingTime:   int64(p.ListingAtMs() / 1000),
		}
	}

	data := &pb.TickerData{
		TokenAddress: p.tokenAddress,
		PairAddress:  p.poolAddress,
		Chain:        consts.ChainName,
		UpdateTime:   int64(max(p.LastChainEventTs(), p.SharedSupply.UpdatedAt())),
		HolderDistribution: &pb.HolderDistribution{
			HolderCount:       int64(p.HolderCount()),
			Top_10HoldersRate: p.Top10HolderRatio(),
		},
		MarketData:   p.buildMarketData(),
		TradingStats: p.buildTradingStats(),
		TradeParams:  tradeParams,
	}

	p.poolStatsCache.Store(data)
	return data
}

// SyncPoolStatsPartial 只同步部分统计字段，复用窗口缓存数据
func (p *Pool) SyncPoolStatsPartial() *pb.TickerData {
	cache := p.poolStatsCache.Load()
	if cache == nil {
		return p.SyncPoolStatsFull()
	}

	holderDistribution := cache.HolderDistribution
	if holderDistribution.HolderCount != int64(p.HolderCount()) ||
		holderDistribution.Top_10HoldersRate != p.Top10HolderRatio() {
		holderDistribution = &pb.HolderDistribution{
			HolderCount:       int64(p.HolderCount()),
			Top_10HoldersRate: p.Top10HolderRatio(),
		}
	}

	tradeParams := cache.TradeParams
	if p.isTradeParamsChanged(cache.TradeParams) {
		if p.HasLeverage() {
			tradeParams = &pb.MMReport{
				LongLeverage:  p.LongLeverage(),
				ShortLeverage: p.ShortLeverage(),
				ListingTime:   int64(p.ListingAtMs() / 1000),
			}
		} else {
			tradeParams = nil
		}
	}

	data := &pb.TickerData{
		TokenAddress:       p.tokenAddress,
		PairAddress:        p.poolAddress,
		Chain:              consts.ChainName,
		UpdateTime:         int64(max(p.LastChainEventTs(), p.SharedSupply.UpdatedAt())),
		HolderDistribution: holderDistribution,
		MarketData:         p.buildMarketData(),
		TradingStats:       cache.TradingStats, // 复用缓存的 TradingStats
		TradeParams:        tradeParams,
	}

	p.poolStatsCache.Store(data)
	return data
}

func (p *Pool) buildMarketData() *pb.MarketData {
	totalSupplyStr, maxSupplyStr, circulatingSupplyStr := p.SupplyStr()
	w24h := &p.Windows[types.Window24H]
	return &pb.MarketData{
		MarketCap:         p.MarketCap(),
		Fdv:               p.FDV(),
		TotalSupply:       totalSupplyStr,
		MaxSupply:         maxSupplyStr,
		CirculatingSupply: circulatingSupplyStr,
		Liquidity:         p.Liquidity(),
		Price:             p.PriceUsd(),
		Volume_24H:        w24h.BuyVolume() + w24h.SellVolume(),
		PriceChange_24H:   float64(w24h.PriceChangeRate()),
	}
}

func (p *Pool) buildTradingStats() *pb.TradingStats {
	w1m := &p.Windows[types.Window1Min]
	w5m := &p.Windows[types.Window5Min]
	w1h := &p.Windows[types.Window1H]
	w6h := &p.Windows[types.Window6H]
	w24h := &p.Windows[types.Window24H]
	return &pb.TradingStats{
		// 价格变化
		PriceChange_1M:  float64(w1m.PriceChangeRate()),
		PriceChange_5M:  float64(w5m.PriceChangeRate()),
		PriceChange_1H:  float64(w1h.PriceChangeRate()),
		PriceChange_6H:  float64(w6h.PriceChangeRate()),
		PriceChange_24H: float64(w24h.PriceChangeRate()),

		// 买单数量
		Buys_1M:  w1m.BuyCount(),
		Buys_5M:  w5m.BuyCount(),
		Buys_1H:  w1h.BuyCount(),
		Buys_6H:  w6h.BuyCount(),
		Buys_24H: w24h.BuyCount(),

		// 卖单数量
		Sells_1M:  w1m.SellCount(),
		Sells_5M:  w5m.SellCount(),
		Sells_1H:  w1h.SellCount(),
		Sells_6H:  w6h.SellCount(),
		Sells_24H: w24h.SellCount(),

		// 总交易次数
		TotalTrades_1M:  w1m.BuyCount() + w1m.SellCount(),
		TotalTrades_5M:  w5m.BuyCount() + w5m.SellCount(),
		TotalTrades_1H:  w1h.BuyCount() + w1h.SellCount(),
		TotalTrades_6H:  w6h.BuyCount() + w6h.SellCount(),
		TotalTrades_24H: w24h.BuyCount() + w24h.SellCount(),

		// 买入额
		BuyVolume_1M:  w1m.BuyVolume(),
		BuyVolume_5M:  w5m.BuyVolume(),
		BuyVolume_1H:  w1h.BuyVolume(),
		BuyVolume_6H:  w6h.BuyVolume(),
		BuyVolume_24H: w24h.BuyVolume(),

		// 卖出额
		SellVolume_1M:  w1m.SellVolume(),
		SellVolume_5M:  w5m.SellVolume(),
		SellVolume_1H:  w1h.SellVolume(),
		SellVolume_6H:  w6h.SellVolume(),
		SellVolume_24H: w24h.SellVolume(),

		// 总交易额
		Volume_1M:  w1m.BuyVolume() + w1m.SellVolume(),
		Volume_5M:  w5m.BuyVolume() + w5m.SellVolume(),
		Volume_1H:  w1h.BuyVolume() + w1h.SellVolume(),
		Volume_6H:  w6h.BuyVolume() + w6h.SellVolume(),
		Volume_24H: w24h.BuyVolume() + w24h.SellVolume(),
	}
}

// isTradeParamsChanged 判断交易参数是否发生变化。
// 作用：避免在参数未变时重复创建新的 PoolStatsDataPartial 对象，减少内存分配与 GC 压力。
func (p *Pool) isTradeParamsChanged(old *pb.MMReport) bool {
	if old != nil {
		return old.LongLeverage != p.LongLeverage() || old.ShortLeverage != p.ShortLeverage()
	}
	return p.HasLeverage()
}
