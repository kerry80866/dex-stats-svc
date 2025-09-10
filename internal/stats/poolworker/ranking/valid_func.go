package ranking

import (
	"dex-stats-sol/internal/stats/poolworker/pool"
	"dex-stats-sol/internal/stats/types"
	"fmt"
)

type PoolValidFunc func(p *pool.Pool, latestTime uint32) bool

func getValidFunc(id types.RankingKey) PoolValidFunc {
	if id.Category == types.RankingCategoryHot {
		return getCategoryHotValidFunc(id)
	}

	if id.Category == types.RankingCategoryFull {
		return getCategoryFullValidFunc(id)
	}

	panic(fmt.Sprintf("getValidFunc, invalid rankingKey: %v", id))
}

func getCategoryHotValidFunc(id types.RankingKey) PoolValidFunc {
	switch id.Field {
	case types.RankingFieldMarketCap:
		return func(p *pool.Pool, latestTime uint32) bool {
			return p.HasLeverage() && hasTrades(p, types.Window24H) //&& p.MarketCap() > 0
		}

	case types.RankingFieldFDV:
		return func(p *pool.Pool, latestTime uint32) bool {
			return p.HasLeverage() && hasTrades(p, types.Window24H) //&& p.FDV() > 0
		}

	case types.RankingFieldHolderCount:
		return func(p *pool.Pool, latestTime uint32) bool {
			return p.HasLeverage() && hasTrades(p, types.Window24H) //&& p.HolderCount() > 0
		}

	case types.RankingFieldTop10HolderRatio:
		return func(p *pool.Pool, latestTime uint32) bool {
			return p.HasLeverage() && hasTrades(p, types.Window24H) //&& p.Top10HolderRatio() > 0
		}

	case types.RankingFieldLiquidity:
		return func(p *pool.Pool, latestTime uint32) bool {
			return p.HasLeverage() && hasTrades(p, types.Window24H) //&& p.Liquidity() > 0
		}
	}

	return func(p *pool.Pool, latestTime uint32) bool {
		return p.HasLeverage() && hasTrades(p, types.Window24H)
	}
}

func getCategoryFullValidFunc(id types.RankingKey) PoolValidFunc {
	if id.Window < types.WindowCount {
		return func(p *pool.Pool, latestTime uint32) bool {
			return hasTrades(p, id.Window)
		}
	}

	switch id.Field {
	case types.RankingFieldMarketCap:
		return func(p *pool.Pool, latestTime uint32) bool {
			return p.MarketCap() > 0 && hasTrades(p, types.Window24H)
		}

	case types.RankingFieldFDV:
		return func(p *pool.Pool, latestTime uint32) bool {
			return p.FDV() > 0 && hasTrades(p, types.Window24H)
		}

	case types.RankingFieldHolderCount:
		return func(p *pool.Pool, latestTime uint32) bool {
			return p.HolderCount() > 0 && hasTrades(p, types.Window24H)
		}

	case types.RankingFieldTop10HolderRatio:
		return func(p *pool.Pool, latestTime uint32) bool {
			return p.Top10HolderRatio() > 0 && hasTrades(p, types.Window24H)
		}

	case types.RankingFieldLiquidity:
		return func(p *pool.Pool, latestTime uint32) bool {
			return p.Liquidity() > 0 && hasTrades(p, types.Window24H)
		}
	}

	panic(fmt.Sprintf("getCategoryFullValidFunc, invalid rankingKey: %v", id))
}

func hasTrades(p *pool.Pool, window types.RankingWindow) bool {
	win := &p.Windows[window]
	return win.BuyCount() > 0 || win.SellCount() > 0
}
