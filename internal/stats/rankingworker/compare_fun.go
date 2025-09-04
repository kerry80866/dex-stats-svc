package rankingworker

import (
	"dex-stats-sol/internal/stats/poolworker/pool"
	"dex-stats-sol/internal/stats/types"
	"fmt"
	"golang.org/x/exp/constraints"
)

type CompareFunc func(a, b types.RankingItem[*pool.Pool]) bool

var compareFuncMap = map[types.RankingKey]CompareFunc{}

func getCompareFunc(id types.RankingKey) CompareFunc {
	f, ok := compareFuncMap[id]
	if !ok {
		panic(fmt.Sprintf("invalid ranking key: %v", id))
	}
	return f
}

func getRankingKeyCount() int {
	return len(compareFuncMap)
}

func init() {
	for category := types.RankingCategory(0); category < types.RankingCategoryCount; category++ {
		// 窗口榜单
		for win := types.RankingWindow(0); win < types.WindowCount; win++ {
			for field := types.RankingFieldWindowStart + 1; field < types.RankingFieldWindowEnd; field++ {
				id := types.RankingKey{Category: category, Window: win, Field: field}
				compareFuncMap[id] = getWindowCompareFunc(id)
			}
		}

		// 全局榜单
		for field := types.RankingFieldGlobalStart + 1; field < types.RankingFieldGlobalEnd; field++ {
			if field == types.RankingFieldListingAt {
				continue // ListingAt 单独处理，只在 Hot 分类使用
			}
			id := types.RankingKey{Category: category, Window: types.WindowGlobal, Field: field}
			compareFuncMap[id] = getGlobalCompareFunc(id)
		}

		// 单独添加流动性榜单
		id := types.RankingKey{Category: category, Window: types.WindowGlobal, Field: types.RankingFieldLiquidity}
		compareFuncMap[id] = getGlobalCompareFunc(id)
	}

	// hot 特有排序字段
	for field := types.RankingFieldHotStart + 1; field < types.RankingFieldHotEnd; field++ {
		id := types.RankingKey{Category: types.RankingCategoryHot, Window: types.WindowGlobal, Field: field}
		compareFuncMap[id] = getGlobalCompareFunc(id)
	}
}

func getWindowCompareFunc(id types.RankingKey) func(a, b types.RankingItem[*pool.Pool]) bool {
	switch id.Window {

	case types.Window1Min:
		switch id.Field {
		case types.RankingFieldVolume:
			return func(a, b types.RankingItem[*pool.Pool]) bool {
				return stableDesc(
					a.TickerData.TradingStats.Volume_1M,
					b.TickerData.TradingStats.Volume_1M,
					a.Entity.AddressHash,
					b.Entity.AddressHash,
				)
			}
		case types.RankingFieldTxCount:
			return func(a, b types.RankingItem[*pool.Pool]) bool {
				return stableDesc(
					a.TickerData.TradingStats.TotalTrades_1M,
					b.TickerData.TradingStats.TotalTrades_1M,
					a.Entity.AddressHash,
					b.Entity.AddressHash,
				)
			}
		case types.RankingFieldPriceChange:
			return func(a, b types.RankingItem[*pool.Pool]) bool {
				return stableDesc(
					a.TickerData.TradingStats.PriceChange_1M,
					b.TickerData.TradingStats.PriceChange_1M,
					a.Entity.AddressHash,
					b.Entity.AddressHash,
				)
			}
		}

	case types.Window5Min:
		switch id.Field {
		case types.RankingFieldVolume:
			return func(a, b types.RankingItem[*pool.Pool]) bool {
				return stableDesc(
					a.TickerData.TradingStats.Volume_5M,
					b.TickerData.TradingStats.Volume_5M,
					a.Entity.AddressHash,
					b.Entity.AddressHash,
				)
			}
		case types.RankingFieldTxCount:
			return func(a, b types.RankingItem[*pool.Pool]) bool {
				return stableDesc(
					a.TickerData.TradingStats.TotalTrades_5M,
					b.TickerData.TradingStats.TotalTrades_5M,
					a.Entity.AddressHash,
					b.Entity.AddressHash,
				)
			}
		case types.RankingFieldPriceChange:
			return func(a, b types.RankingItem[*pool.Pool]) bool {
				return stableDesc(
					a.TickerData.TradingStats.PriceChange_5M,
					b.TickerData.TradingStats.PriceChange_5M,
					a.Entity.AddressHash,
					b.Entity.AddressHash,
				)
			}
		}

	case types.Window1H:
		switch id.Field {
		case types.RankingFieldVolume:
			return func(a, b types.RankingItem[*pool.Pool]) bool {
				return stableDesc(
					a.TickerData.TradingStats.Volume_1H,
					b.TickerData.TradingStats.Volume_1H,
					a.Entity.AddressHash,
					b.Entity.AddressHash,
				)
			}
		case types.RankingFieldTxCount:
			return func(a, b types.RankingItem[*pool.Pool]) bool {
				return stableDesc(
					a.TickerData.TradingStats.TotalTrades_1H,
					b.TickerData.TradingStats.TotalTrades_1H,
					a.Entity.AddressHash,
					b.Entity.AddressHash,
				)
			}
		case types.RankingFieldPriceChange:
			return func(a, b types.RankingItem[*pool.Pool]) bool {
				return stableDesc(
					a.TickerData.TradingStats.PriceChange_1H,
					b.TickerData.TradingStats.PriceChange_1H,
					a.Entity.AddressHash,
					b.Entity.AddressHash,
				)
			}
		}

	case types.Window6H:
		switch id.Field {
		case types.RankingFieldVolume:
			return func(a, b types.RankingItem[*pool.Pool]) bool {
				return stableDesc(
					a.TickerData.TradingStats.Volume_6H,
					b.TickerData.TradingStats.Volume_6H,
					a.Entity.AddressHash,
					b.Entity.AddressHash,
				)
			}
		case types.RankingFieldTxCount:
			return func(a, b types.RankingItem[*pool.Pool]) bool {
				return stableDesc(
					a.TickerData.TradingStats.TotalTrades_6H,
					b.TickerData.TradingStats.TotalTrades_6H,
					a.Entity.AddressHash,
					b.Entity.AddressHash,
				)
			}
		case types.RankingFieldPriceChange:
			return func(a, b types.RankingItem[*pool.Pool]) bool {
				return stableDesc(
					a.TickerData.TradingStats.PriceChange_6H,
					b.TickerData.TradingStats.PriceChange_6H,
					a.Entity.AddressHash,
					b.Entity.AddressHash,
				)
			}
		}

	case types.Window24H:
		switch id.Field {
		case types.RankingFieldVolume:
			return func(a, b types.RankingItem[*pool.Pool]) bool {
				return stableDesc(
					a.TickerData.TradingStats.Volume_24H,
					b.TickerData.TradingStats.Volume_24H,
					a.Entity.AddressHash,
					b.Entity.AddressHash,
				)
			}
		case types.RankingFieldTxCount:
			return func(a, b types.RankingItem[*pool.Pool]) bool {
				return stableDesc(
					a.TickerData.TradingStats.TotalTrades_24H,
					b.TickerData.TradingStats.TotalTrades_24H,
					a.Entity.AddressHash,
					b.Entity.AddressHash,
				)
			}
		case types.RankingFieldPriceChange:
			return func(a, b types.RankingItem[*pool.Pool]) bool {
				return stableDesc(
					a.TickerData.TradingStats.PriceChange_24H,
					b.TickerData.TradingStats.PriceChange_24H,
					a.Entity.AddressHash,
					b.Entity.AddressHash,
				)
			}
		}
	}

	panic(fmt.Sprintf("invalid ranking key: %v", id))
}

func getGlobalCompareFunc(id types.RankingKey) CompareFunc {
	switch id.Field {
	case types.RankingFieldMarketCap:
		return func(a, b types.RankingItem[*pool.Pool]) bool {
			return stableDesc(
				a.TickerData.MarketData.MarketCap,
				b.TickerData.MarketData.MarketCap,
				a.Entity.AddressHash,
				b.Entity.AddressHash,
			)
		}

	case types.RankingFieldFDV:
		return func(a, b types.RankingItem[*pool.Pool]) bool {
			return stableDesc(
				a.TickerData.MarketData.Fdv,
				b.TickerData.MarketData.Fdv,
				a.Entity.AddressHash,
				b.Entity.AddressHash,
			)
		}

	case types.RankingFieldLiquidity:
		return func(a, b types.RankingItem[*pool.Pool]) bool {
			return stableDesc(
				a.TickerData.MarketData.Liquidity,
				b.TickerData.MarketData.Liquidity,
				a.Entity.AddressHash,
				b.Entity.AddressHash,
			)
		}

	case types.RankingFieldHolderCount:
		return func(a, b types.RankingItem[*pool.Pool]) bool {
			return stableDesc(
				a.TickerData.HolderDistribution.HolderCount,
				b.TickerData.HolderDistribution.HolderCount,
				a.Entity.AddressHash,
				b.Entity.AddressHash,
			)
		}

	case types.RankingFieldTop10HolderRatio:
		return func(a, b types.RankingItem[*pool.Pool]) bool {
			return stableDesc(
				a.TickerData.HolderDistribution.Top_10HoldersRate,
				b.TickerData.HolderDistribution.Top_10HoldersRate,
				a.Entity.AddressHash,
				b.Entity.AddressHash,
			)
		}

	case types.RankingFieldListingAt:
		return func(a, b types.RankingItem[*pool.Pool]) bool {
			aListingAt := int64(0)
			bListingAt := int64(0)
			if a.TickerData.TradeParams != nil {
				aListingAt = a.TickerData.TradeParams.ListingTime
			}
			if b.TickerData.TradeParams != nil {
				bListingAt = b.TickerData.TradeParams.ListingTime
			}
			return stableDesc(
				aListingAt,
				bListingAt,
				a.Entity.AddressHash,
				b.Entity.AddressHash,
			)
		}

	default:
		panic(fmt.Sprintf("invalid ranking key: %v", id))
	}
}

// stableDesc 比较两个值，若相等则用 AddressHash 做二级排序，确保排序结果稳定。
// AddressHash 使用升序排序（<）作为二级条件，保证稳定性。
func stableDesc[T constraints.Ordered](aVal, bVal T, aHash, bHash uint64) bool {
	if aVal != bVal {
		return aVal > bVal
	}
	return aHash < bHash
}
