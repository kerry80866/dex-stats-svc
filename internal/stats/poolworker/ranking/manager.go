package ranking

import (
	"dex-stats-sol/internal/stats/poolworker/pool"
	"dex-stats-sol/internal/stats/types"
)

// PoolRankingManager 管理所有维度和排序方向的排行榜
type PoolRankingManager struct {
	Rankings map[types.RankingKey]*PoolRanking // 动态窗口排行榜，按时间窗口和排序键区分
}

// NewPoolRankingManager 初始化排行榜管理器
func NewPoolRankingManager() *PoolRankingManager {
	pm := &PoolRankingManager{
		Rankings: make(map[types.RankingKey]*PoolRanking),
	}

	// 遍历每个分类
	for category := types.RankingCategory(0); category < types.RankingCategoryCount; category++ {
		// 窗口字段（动态榜单）
		for win := types.RankingWindow(0); win < types.WindowCount; win++ {
			for field := types.RankingFieldWindowStart + 1; field < types.RankingFieldWindowEnd; field++ {
				id := types.RankingKey{Category: category, Window: win, Field: field}
				pm.Rankings[id] = newPoolRanking(getCompareFunc(id), getValidFunc(id))
			}
		}

		// 全局字段
		for field := types.RankingFieldGlobalStart + 1; field < types.RankingFieldGlobalEnd; field++ {
			id := types.RankingKey{Category: category, Window: types.WindowGlobal, Field: field}
			pm.Rankings[id] = newPoolRanking(getCompareFunc(id), getValidFunc(id))
		}

		// 单独处理流动性
		id := types.RankingKey{Category: category, Window: types.WindowGlobal, Field: types.RankingFieldLiquidity}
		pm.Rankings[id] = newPoolRanking(getCompareFunc(id), getValidFunc(id))
	}

	// Hot 专用字段
	for field := types.RankingFieldHotStart + 1; field < types.RankingFieldHotEnd; field++ {
		id := types.RankingKey{Category: types.RankingCategoryHot, Window: types.WindowGlobal, Field: field}
		pm.Rankings[id] = newPoolRanking(getCompareFunc(id), getValidFunc(id))
	}

	return pm
}

// RebuildAllRankings 全量重建所有排行榜
func (pm *PoolRankingManager) RebuildAllRankings(poolMap map[types.Pubkey]*pool.Pool, latestTime uint32) {
	for id := range pm.Rankings {
		pm.Rankings[id].rebuildFromPoolMap(poolMap, latestTime)
	}
}

// UpdateWindowRankings 增量更新指定窗口的排行榜
func (pm *PoolRankingManager) UpdateWindowRankings(
	category types.RankingCategory,
	win types.RankingWindow,
	pools []*pool.Pool,
	latestTime uint32,
) {
	filtered := make([]*pool.Pool, len(pools))                // 复用 slice
	updatedSet := make(map[types.Pubkey]struct{}, len(pools)) // 复用 map

	for field := types.RankingFieldWindowStart + 1; field < types.RankingFieldWindowEnd; field++ {
		id := types.RankingKey{
			Category: category,
			Window:   win,
			Field:    field,
		}
		validPools := pm.filterValidPools(pools, pm.Rankings[id].isValid, latestTime, filtered, updatedSet)
		pm.Rankings[id].updateRanking(validPools, updatedSet, latestTime)
	}
}

// UpdateGlobalRankings 增量更新全局排行榜
func (pm *PoolRankingManager) UpdateGlobalRankings(
	category types.RankingCategory,
	pools []*pool.Pool,
	latestTime uint32,
) {
	filtered := make([]*pool.Pool, len(pools))                // 复用 slice
	updatedSet := make(map[types.Pubkey]struct{}, len(pools)) // 复用 map

	// 更新全局字段排行榜
	for field := types.RankingFieldGlobalStart + 1; field < types.RankingFieldGlobalEnd; field++ {
		id := types.RankingKey{
			Category: category,
			Window:   types.WindowGlobal,
			Field:    field,
		}
		validPools := pm.filterValidPools(pools, pm.Rankings[id].isValid, latestTime, filtered, updatedSet)
		pm.Rankings[id].updateRanking(validPools, updatedSet, latestTime)
	}

	// Hot 专用字段（仅当类别为 Hot 时）
	if category == types.RankingCategoryHot {
		for field := types.RankingFieldHotStart + 1; field < types.RankingFieldHotEnd; field++ {
			id := types.RankingKey{
				Category: category,
				Window:   types.WindowGlobal,
				Field:    field,
			}
			validPools := pm.filterValidPools(pools, pm.Rankings[id].isValid, latestTime, filtered, updatedSet)
			pm.Rankings[id].updateRanking(validPools, updatedSet, latestTime)
		}
	}
}

// RebuildLiquidityRanking 全量排序流动性排行榜
func (pm *PoolRankingManager) RebuildLiquidityRanking(
	category types.RankingCategory,
	poolMap map[types.Pubkey]*pool.Pool,
	latestTime uint32,
) {
	id := types.RankingKey{
		Category: category,
		Window:   types.WindowGlobal,
		Field:    types.RankingFieldLiquidity,
	}
	pm.Rankings[id].rebuildFromPoolMap(poolMap, latestTime)
}

// UpdateLiquidityRankings 增量更新流动性排行榜
func (pm *PoolRankingManager) UpdateLiquidityRankings(
	category types.RankingCategory,
	pools []*pool.Pool,
	latestTime uint32,
) {
	filtered := make([]*pool.Pool, len(pools))
	updatedSet := make(map[types.Pubkey]struct{}, len(pools))

	id := types.RankingKey{
		Category: category,
		Window:   types.WindowGlobal,
		Field:    types.RankingFieldLiquidity,
	}
	validPools := pm.filterValidPools(pools, pm.Rankings[id].isValid, latestTime, filtered, updatedSet)
	pm.Rankings[id].updateRanking(validPools, updatedSet, latestTime)
}

func (pm *PoolRankingManager) filterValidPools(
	pools []*pool.Pool,
	validFunc PoolValidFunc,
	latestTime uint32,
	filtered []*pool.Pool, // 可复用 slice，容量 >= len(pools)
	updatedSet map[types.Pubkey]struct{}, // 可复用 map
) []*pool.Pool {
	clear(updatedSet)

	i := 0
	for _, pl := range pools {
		if validFunc(pl, latestTime) {
			updatedSet[pl.Address] = struct{}{}
			filtered[i] = pl
			i++
		}
	}

	return filtered[:i]
}
