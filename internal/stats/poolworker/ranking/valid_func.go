package ranking

import (
	"dex-stats-sol/internal/stats/poolworker/pool"
	"dex-stats-sol/internal/stats/types"
	"fmt"
)

type PoolValidFunc func(p *pool.Pool, latestTime uint32) bool

func getValidFunc(id types.RankingKey) PoolValidFunc {
	if id.Category == types.RankingCategoryHot {
		return func(p *pool.Pool, latestTime uint32) bool {
			return p.HasLeverage() && hasTrades(p, types.Window24H)
		}
	}

	if id.Window < types.WindowCount {
		return func(p *pool.Pool, latestTime uint32) bool {
			return hasTrades(p, id.Window)
		}
	}

	if id.Window == types.WindowGlobal {
		return getGlobalValidFunc(id)
	}

	panic(fmt.Sprintf("getValidFunc, invalid rankingKey: %v", id))
}

func getGlobalValidFunc(id types.RankingKey) PoolValidFunc {
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

	panic(fmt.Sprintf("getGlobalValidFunc, invalid rankingKey: %v", id))
}

func hasTrades(p *pool.Pool, window types.RankingWindow) bool {
	win := &p.Windows[window]
	return win.BuyCount() > 0 || win.SellCount() > 0
}
