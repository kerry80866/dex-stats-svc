package ranking

import (
	"dex-stats-sol/internal/stats/poolworker/pool"
	"dex-stats-sol/internal/stats/types"
	"fmt"
)

const OneDaySeconds = 86400 // 24小时对应的秒数

type PoolValidFunc func(p *pool.Pool, latestTime uint32) bool

func getValidFunc(id types.RankingKey) PoolValidFunc {
	if id.Window < types.WindowCount {
		return getWindowValidFunc(id)
	}

	if id.Window == types.WindowGlobal {
		return getGlobalValidFunc(id)
	}

	panic(fmt.Sprintf("getValidFunc, invalid rankingKey: %v", id))
}

func getWindowValidFunc(id types.RankingKey) PoolValidFunc {
	if id.Category == types.RankingCategoryHot {
		return func(p *pool.Pool, latestTime uint32) bool {
			return p.HasLeverage() && latestTime <= p.LastSwapEventTs()+OneDaySeconds
		}
	} else if id.Category == types.RankingCategoryFull {
		expireSeconds := pool.SlidingWindowsBuckets[id.Window].Duration
		return func(p *pool.Pool, latestTime uint32) bool {
			return latestTime <= p.LastSwapEventTs()+expireSeconds
		}
	}
	panic(fmt.Sprintf("getWindowValidFunc, invalid rankingKey: %v", id))
}

func getGlobalValidFunc(id types.RankingKey) PoolValidFunc {
	if id.Category == types.RankingCategoryHot {
		switch id.Field {
		case types.RankingFieldMarketCap:
			return func(p *pool.Pool, latestTime uint32) bool {
				return p.HasLeverage() && p.MarketCap() > 0 && withinOneDay(p, latestTime)
			}

		case types.RankingFieldFDV:
			return func(p *pool.Pool, latestTime uint32) bool {
				return p.HasLeverage() && p.FDV() > 0 && withinOneDay(p, latestTime)
			}

		case types.RankingFieldHolderCount:
			return func(p *pool.Pool, latestTime uint32) bool {
				return p.HasLeverage() && p.HolderCount() > 0 && withinOneDay(p, latestTime)
			}

		case types.RankingFieldTop10HolderRatio:
			return func(p *pool.Pool, latestTime uint32) bool {
				return p.HasLeverage() && p.Top10HolderRatio() > 0 && withinOneDay(p, latestTime)
			}

		case types.RankingFieldListingAt:
			return func(p *pool.Pool, latestTime uint32) bool {
				return p.HasLeverage() && p.ListingAtMs() > 0 && withinOneDay(p, latestTime)
			}

		case types.RankingFieldLiquidity:
			return func(p *pool.Pool, latestTime uint32) bool {
				return p.HasLeverage() && p.Liquidity() > 0 && withinOneDay(p, latestTime)
			}
		}
	} else if id.Category == types.RankingCategoryFull {
		switch id.Field {
		case types.RankingFieldMarketCap:
			return func(p *pool.Pool, latestTime uint32) bool {
				return p.MarketCap() > 0 && withinOneDay(p, latestTime)
			}

		case types.RankingFieldFDV:
			return func(p *pool.Pool, latestTime uint32) bool {
				return p.FDV() > 0 && withinOneDay(p, latestTime)
			}

		case types.RankingFieldHolderCount:
			return func(p *pool.Pool, latestTime uint32) bool {
				return p.HolderCount() > 0 && withinOneDay(p, latestTime)
			}

		case types.RankingFieldTop10HolderRatio:
			return func(p *pool.Pool, latestTime uint32) bool {
				return p.Top10HolderRatio() > 0 && withinOneDay(p, latestTime)
			}

		case types.RankingFieldLiquidity:
			return func(p *pool.Pool, latestTime uint32) bool {
				return p.Liquidity() > 0 && withinOneDay(p, latestTime)
			}
		}
	}

	panic(fmt.Sprintf("getGlobalValidFunc, invalid rankingKey: %v", id))
}

func withinOneDay(p *pool.Pool, latestTime uint32) bool {
	return latestTime <= p.LastChainEventTs()+OneDaySeconds
}
