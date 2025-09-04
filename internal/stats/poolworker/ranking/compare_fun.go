package ranking

import (
	"dex-stats-sol/internal/stats/poolworker/pool"
	"dex-stats-sol/internal/stats/types"
	"fmt"
	"golang.org/x/exp/constraints"
)

type PoolCompareFunc func(a, b *pool.Pool) bool

func getCompareFunc(id types.RankingKey) PoolCompareFunc {
	if id.Window < types.WindowCount {
		return getWindowCompareFunc(id)
	}

	if id.Window == types.WindowGlobal {
		return getGlobalCompareFunc(id)
	}

	panic(fmt.Sprintf("getCompareFunc, invalid rankingKey: %v", id))
}

func getWindowCompareFunc(id types.RankingKey) PoolCompareFunc {
	idx := id.Window
	switch id.Field {
	case types.RankingFieldVolume:
		return func(a, b *pool.Pool) bool {
			aWin := &a.Windows[idx]
			bWin := &b.Windows[idx]
			return stableDesc(
				aWin.BuyVolume()+aWin.SellVolume(),
				bWin.BuyVolume()+bWin.SellVolume(),
				a.AddressHash,
				b.AddressHash,
			)
		}

	case types.RankingFieldTxCount:
		return func(a, b *pool.Pool) bool {
			aWin := &a.Windows[idx]
			bWin := &b.Windows[idx]
			return stableDesc(
				aWin.BuyCount()+aWin.SellCount(),
				bWin.BuyCount()+bWin.SellCount(),
				a.AddressHash,
				b.AddressHash,
			)
		}

	case types.RankingFieldPriceChange:
		return func(a, b *pool.Pool) bool {
			aWin := &a.Windows[idx]
			bWin := &b.Windows[idx]
			return stableDesc(
				aWin.PriceChangeRate(),
				bWin.PriceChangeRate(),
				a.AddressHash,
				b.AddressHash,
			)
		}
	default:
		panic(fmt.Sprintf("getWindowCompareFunc, invalid rankingKey: %v", id))
	}
}

func getGlobalCompareFunc(id types.RankingKey) PoolCompareFunc {
	switch id.Field {
	case types.RankingFieldMarketCap:
		return func(a, b *pool.Pool) bool {
			return stableDesc(a.MarketCap(), b.MarketCap(), a.AddressHash, b.AddressHash)
		}

	case types.RankingFieldFDV:
		return func(a, b *pool.Pool) bool {
			return stableDesc(a.FDV(), b.FDV(), a.AddressHash, b.AddressHash)
		}

	case types.RankingFieldHolderCount:
		return func(a, b *pool.Pool) bool {
			return stableDesc(a.HolderCount(), b.HolderCount(), a.AddressHash, b.AddressHash)
		}

	case types.RankingFieldTop10HolderRatio:
		return func(a, b *pool.Pool) bool {
			return stableDesc(a.Top10HolderRatio(), b.Top10HolderRatio(), a.AddressHash, b.AddressHash)
		}

	case types.RankingFieldListingAt:
		return func(a, b *pool.Pool) bool {
			return stableDesc(a.ListingAtMs(), b.ListingAtMs(), a.AddressHash, b.AddressHash)
		}

	case types.RankingFieldLiquidity:
		return func(a, b *pool.Pool) bool {
			return stableDesc(a.Liquidity(), b.Liquidity(), a.AddressHash, b.AddressHash)
		}

	default:
		panic(fmt.Sprintf("getGlobalCompareFunc, invalid rankingKey: %v", id))
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
