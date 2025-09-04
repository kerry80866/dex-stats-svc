package ranking

import (
	"dex-stats-sol/internal/pkg/utils"
	"dex-stats-sol/internal/stats/poolworker/pool"
	"dex-stats-sol/internal/stats/types"
	"sort"
)

type PoolRanking struct {
	ranked []*pool.Pool // 当前有效且已排序的池列表
	cache  []*pool.Pool // 缓存切片，用于临时存储数据，减少频繁分配

	compare PoolCompareFunc // 排序比较函数，定义排序规则（通常降序）
	isValid PoolValidFunc   // 有效性判断函数，用于过滤过期或无效池
}

func newPoolRanking(compareFunc PoolCompareFunc, validFunc PoolValidFunc) *PoolRanking {
	return &PoolRanking{
		compare: compareFunc,
		isValid: validFunc,
		cache:   make([]*pool.Pool, 0),
		ranked:  make([]*pool.Pool, 0),
	}
}

// Pools 返回当前排名列表
func (p *PoolRanking) Pools() []*pool.Pool {
	return p.ranked
}

// rebuildFromPoolMap 基于全量池快照重建完整排名
func (p *PoolRanking) rebuildFromPoolMap(poolMap map[types.Pubkey]*pool.Pool, latestTime uint32) {
	// 统计有效池数量，提前预分配容量，减少扩容次数
	count := 0
	for _, pl := range poolMap {
		if p.isValid(pl, latestTime) {
			count++
		}
	}

	// 清理缓存，防止内存泄漏
	utils.ClearSlice(&p.cache)

	// 预分配 ranked 容量
	if cap(p.ranked) < count {
		// 容量不足，重新分配 slice，并扩容 1.2 倍
		p.ranked = make([]*pool.Pool, 0, count*6/5)
	} else {
		// 容量足够：先清理 count 之后的多余引用，防止内存泄漏
		utils.ClearSliceTail(p.ranked, count)
		p.ranked = p.ranked[:0]
	}

	if count == 0 {
		return
	}

	// 追加所有有效池
	for _, pl := range poolMap {
		if p.isValid(pl, latestTime) {
			p.ranked = append(p.ranked, pl)
		}
	}

	// 对结果排序，生成最终排名
	sort.Slice(p.ranked, func(i, j int) bool {
		return p.compare(p.ranked[i], p.ranked[j])
	})
}

// updateRanking 增量更新排名，合并新增/更新池与旧池列表
func (p *PoolRanking) updateRanking(
	updatedList []*pool.Pool,
	updatedSet map[types.Pubkey]struct{},
	latestTime uint32,
) {
	if len(updatedList) == 0 {
		// 无新增更新，移除过期池并清理缓存
		p.removeExpiredPools(latestTime)
		utils.ClearSlice(&p.cache)
		return
	}

	// 对新增或更新池子排序
	sort.Slice(updatedList, func(i, j int) bool {
		return p.compare(updatedList[i], updatedList[j])
	})

	// 过滤旧池，去除无效和已更新池，得到缓存列表
	filteredList := p.filterValidPools(updatedSet, latestTime)

	// 合并排序后的更新池和过滤后的旧池，生成新排名
	p.ranked = p.mergePools(filteredList, updatedList)

	// 缓存过滤后的旧池供下次复用
	p.cache = filteredList
	// 不在此处调用 p.clearCache()，避免多余的 clear 开销。
	// 此时 p.cache 的元素已包含在新的 p.ranked 中，不会导致内存泄漏。
}

// fullRank 全量更新排名，旧池中排除已更新池，更新后全量排序
func (p *PoolRanking) fullRank(
	updatedList []*pool.Pool,
	updatedSet map[types.Pubkey]struct{},
	latestTime uint32,
) {
	// 过滤旧池，去除无效和已更新池，得到缓存列表
	validList := p.filterValidPools(updatedSet, latestTime)

	// 追加所有新增/更新池
	p.ranked = append(validList, updatedList...)

	// 全量排序
	sort.Slice(p.ranked, func(i, j int) bool {
		return p.compare(p.ranked[i], p.ranked[j])
	})

	// 清空缓存，下次重新构建缓存
	utils.ClearSlice(&p.cache)
}

// removeExpiredPools 移除无效或过期池
func (p *PoolRanking) removeExpiredPools(latestTime uint32) {
	validCount := 0
	for _, pl := range p.ranked {
		if p.isValid(pl, latestTime) {
			p.ranked[validCount] = pl
			validCount++
		}
	}

	utils.ClearSliceTail(p.ranked, validCount)
	p.ranked = p.ranked[:validCount]
}

// filterValidPools 过滤旧池，剔除已更新和无效池，返回有效池列表
func (p *PoolRanking) filterValidPools(updatedSet map[types.Pubkey]struct{}, latestTime uint32) []*pool.Pool {
	list := p.ranked
	count := 0
	for _, pl := range list {
		if p.isValid(pl, latestTime) {
			if _, updated := updatedSet[pl.Address]; !updated {
				list[count] = pl
				count++
			}
		}
	}

	utils.ClearSliceTail(list, count)
	return list[:count]
}

// mergePools 合并两个有序池列表，返回合并后新的有序列表
func (p *PoolRanking) mergePools(list []*pool.Pool, updatedList []*pool.Pool) []*pool.Pool {
	oldLen := len(list)
	updatedLen := len(updatedList)
	newLen := oldLen + updatedLen

	var newList []*pool.Pool
	if cap(p.cache) < newLen {
		// 容量不足，重新分配切片，扩容 1.2 倍
		newList = make([]*pool.Pool, newLen, (newLen*6)/5)
	} else {
		// 复用缓存
		utils.ClearSliceTail(p.cache, newLen) // 清理多余引用，避免内存泄漏
		p.cache = p.cache[:newLen]            // 调整长度
		newList = p.cache
	}

	// 归并两个有序列表
	i, j, k := 0, 0, 0
	for i < oldLen && j < updatedLen {
		if p.compare(list[i], updatedList[j]) {
			newList[k] = list[i]
			i++
		} else {
			newList[k] = updatedList[j]
			j++
		}
		k++
	}

	// 拷贝剩余元素
	if i < oldLen {
		copy(newList[k:], list[i:])
	} else if j < updatedLen {
		copy(newList[k:], updatedList[j:])
	}

	return newList
}
