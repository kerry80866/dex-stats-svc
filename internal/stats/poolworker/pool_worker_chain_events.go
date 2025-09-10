package poolworker

import (
	"dex-stats-sol/internal/consts"
	"dex-stats-sol/internal/pkg/logger"
	"dex-stats-sol/internal/pkg/utils"
	ea "dex-stats-sol/internal/stats/eventadapter"
	"dex-stats-sol/internal/stats/poolworker/pool"
	"dex-stats-sol/internal/stats/poolworker/shared"
	"dex-stats-sol/internal/stats/types"
	"dex-stats-sol/pb"
	"time"
)

// handleChainEvents 处理单个 block 的链上事件列表
func (w *PoolWorker) handleChainEvents(events *pb.Events) {
	start := time.Now() // 总耗时开始
	blockTime := uint32(events.BlockTime)
	latestTime := max(w.lastHandledTime, blockTime)
	blockNumber := uint32(events.BlockNumber)

	logger.Infof("[PoolWorker:%d] start handleChainEvents block %d", w.workerID, events.BlockNumber)

	// Step 1: 更新全局价格
	t1 := time.Now()
	quotePriceUpdated := w.quotePrices.updateFromEvents(events)
	logger.Debugf("[PoolWorker:%d] Step 1: update quotePrice cost %s", w.workerID, time.Since(t1))

	// Step 2: 滑出窗口处理
	t2 := time.Now()
	groupedEvents := ea.GroupEventsByPool(events)
	slidingWindowPools := w.slideOutPools(groupedEvents, w.lastHandledTime, latestTime)
	isSlidingOutWindow24H := len(slidingWindowPools[types.Window24H]) > 0
	logger.Debugf("[PoolWorker:%d] Step 2: slideOutPools cost %s", w.workerID, time.Since(t2))

	// Step 3: 处理本 block 的事件
	t3 := time.Now()
	updatedPools, _ := w.processBlock(events, groupedEvents, latestTime)
	logger.Debugf("[PoolWorker:%d] Step 3: processBlock cost %s", w.workerID, time.Since(t3))

	// Step 4: 更新排行榜
	t4 := time.Now()
	w.updateAllRankings(slidingWindowPools, updatedPools, quotePriceUpdated, latestTime)
	logger.Debugf("[PoolWorker:%d] Step 4: updateAllRankings cost %s", w.workerID, time.Since(t4))

	// Step 5: 同步查询缓存
	t5 := time.Now()
	w.syncAllPoolsStatsCache(slidingWindowPools, updatedPools, quotePriceUpdated)
	logger.Debugf("[PoolWorker:%d] Step 5: syncAllPoolsStatsCache cost %s", w.workerID, time.Since(t5))

	// Step 6: 收集待回调任务
	t6 := time.Now()
	pushTasks := w.collectPushTask(blockNumber, updatedPools, slidingWindowPools)
	tokenMetaTasks, holderCountTasks, topHoldersTasks := w.collectTokenTasks(updatedPools)
	rankingTasks := w.collectRankingUpdates(blockNumber)
	logger.Debugf("[PoolWorker:%d] Step 6: collect tasks cost %s", w.workerID, time.Since(t6))

	// Step 7: 状态重置（清理缓存 + 更新桶 + 标记 block）
	t7 := time.Now()
	w.resetState(updatedPools, latestTime, blockTime, isSlidingOutWindow24H, blockNumber)
	logger.Debugf("[PoolWorker:%d] Step 7: resetState cost %s", w.workerID, time.Since(t7))

	// Step 8: 回调所有任务
	t8 := time.Now()
	w.dispatchAllTasks(pushTasks, tokenMetaTasks, holderCountTasks, topHoldersTasks, rankingTasks)
	logger.Debugf("[PoolWorker:%d] Step 8: dispatchAllTasks cost %s", w.workerID, time.Since(t8))

	// 总耗时
	logger.Infof("[PoolWorker:%d] handleChainEvents block %d total processed in %s",
		w.workerID, events.BlockNumber, time.Since(start))
}

// slideOutPools 检查滑动窗口过期池，返回每个窗口中有变动的池集合
func (w *PoolWorker) slideOutPools(
	groupedEvents map[types.Pubkey]*ea.PoolEvents, // 按池地址分组后的事件
	lastHandledTime, latestTime uint32,
) [types.WindowCount]map[types.Pubkey]struct{} {
	// 初始化每个窗口的池集合
	updatedWindowPools := [types.WindowCount]map[types.Pubkey]struct{}{
		make(map[types.Pubkey]struct{}),
		make(map[types.Pubkey]struct{}),
		make(map[types.Pubkey]struct{}),
		make(map[types.Pubkey]struct{}),
		make(map[types.Pubkey]struct{}),
	}

	// 收集所有需要滑出的池
	slidingOutPools := w.collectSlidingOutPools(groupedEvents, lastHandledTime, latestTime)

	// 处理每个池的滑动过期操作
	for _, p := range slidingOutPools {
		updatedWindows := p.SlideOutExpired(latestTime)
		for i, updated := range updatedWindows {
			if updated {
				updatedWindowPools[i][p.Address] = struct{}{}
			}
		}
	}

	return updatedWindowPools
}

// collectSlidingOutPools 收集滑出窗口的池列表
func (w *PoolWorker) collectSlidingOutPools(
	groupedEvents map[types.Pubkey]*ea.PoolEvents, // 按池地址分组的事件
	lastHandledTime, latestTime uint32,
) []*pool.Pool {
	slidingPoolMap := make(map[types.Pubkey]struct{})
	for poolAddr := range groupedEvents {
		slidingPoolMap[poolAddr] = struct{}{}
	}

	for _, wb := range pool.SlidingWindowsBuckets {
		w.poolBuckets15s.collectSlidingOutPools(
			slidingPoolMap,
			lastHandledTime,
			latestTime,
			wb.Duration,
			wb.BucketSeconds,
		)
	}

	slidingPools := make([]*pool.Pool, 0, len(slidingPoolMap))
	for pubkey := range slidingPoolMap {
		p := w.pools.getPoolUnsafe(pubkey)
		if p != nil {
			slidingPools = append(slidingPools, p)
		}
	}
	return slidingPools
}

// processBlock 处理区块事件，返回更新池和新建池
func (w *PoolWorker) processBlock(
	events *pb.Events,
	groupedEvents map[types.Pubkey]*ea.PoolEvents, // 按池子分组后的事件
	latestTime uint32,
) (
	updatedPools map[types.Pubkey]struct{}, // 更新的池地址
	newPools []*pool.Pool, // 新建的池
) {
	blockNumber := uint32(events.BlockNumber)
	isProcessed := w.blockStates.isBlockProcessed(blockNumber)

	// 初始化返回的池映射和池列表
	updatedPools = make(map[types.Pubkey]struct{}, len(groupedEvents))
	newPools = make([]*pool.Pool, 0, len(groupedEvents))

	// 遍历每个池子的事件
	for poolAddr, poolData := range groupedEvents {
		pl := w.pools.getPoolUnsafe(poolAddr)

		if !isProcessed {
			if pl == nil {
				// 新建池
				pl = w.newPool(poolData, latestTime)
				if pl != nil {
					newPools = append(newPools, pl)
				}
			} else {
				// 更新已有池
				quotePrice := w.quotePrices.priceUnsafe(poolData.QuoteToken)
				pl.Update(poolData, latestTime, quotePrice)
			}
		}

		if pl == nil {
			continue
		}

		// 记录更新的池
		updatedPools[pl.Address] = struct{}{}

		// 更新 TotalSupply
		if poolData.TotalSupply > 0 {
			if tokenInfo := w.tokenMap.GetTokenInfoUnsafe(pl.BaseToken); tokenInfo != nil {
				tokenInfo.UpdateTotalSupply(poolData.TotalSupply, false, poolData.BlockTime, w.pendingTokenPools)
			}
		}
	}

	// 处理 TokenHolderChange 事件
	if !isProcessed {
		w.processHolderChanges(events, blockNumber, w.pendingTokenPools)
	}

	// 批量写入新建池
	w.pools.addPools(newPools)
	w.tokenMap.AddPools(newPools)

	return updatedPools, newPools
}

// newPool 创建新的 Pool
func (w *PoolWorker) newPool(data *ea.PoolEvents, latestTime uint32) *pool.Pool {
	hotPoolData := w.hotPools.getDataUnsafe(data.Pool)
	quotePrice := w.quotePrices.priceUnsafe(data.QuoteToken)

	var (
		supplyInfo  *shared.SupplyInfo
		holderCount *shared.HolderCount
		topHolders  *shared.TopHolders
	)

	if tokenInfo := w.tokenMap.GetTokenInfoUnsafe(data.BaseToken); tokenInfo != nil {
		supplyInfo = tokenInfo.SharedSupply
		holderCount = tokenInfo.SharedHolderCount
		topHolders = tokenInfo.SharedTopHolders
	} else {
		supplyInfo = shared.NewSupplyInfo()
		topHolders = shared.NewTopHolders(data.BlockNumber, data.IsNewCreate)
		holderCount = shared.NewHolderCount()
		if data.IsNewCreate {
			holderCount.ApplyBase(0, data.BlockNumber)
		}
	}

	params := &pool.PoolInitParams{
		Data:          data,
		WorkerID:      w.workerID,
		SupplyInfo:    supplyInfo,
		HolderCount:   holderCount,
		TopHolders:    topHolders,
		LongLeverage:  hotPoolData.LongLeverage,
		ShortLeverage: hotPoolData.ShortLeverage,
		ListingAtMs:   hotPoolData.ListingAtMs,
		LatestTime:    latestTime,
		QuotePriceUSD: quotePrice,
	}

	return pool.NewPool(params)
}

// processHolderChanges 处理 TokenHolderChange 事件
func (w *PoolWorker) processHolderChanges(events *pb.Events, blockNumber uint32, updatedPools map[types.Pubkey]struct{}) {
	holderChangeEvents := ea.ExtractHolderChanges(events)
	for _, event := range holderChangeEvents {
		tokenAddr, err := types.PubkeyFromBytes(event.Token)
		if err != nil {
			continue
		}

		tokenInfo := w.tokenMap.GetTokenInfoUnsafe(tokenAddr)
		if tokenInfo == nil {
			continue
		}

		tokenInfo.UpdateHolderChangeEvent(blockNumber, event, updatedPools)
	}
}

// updateAllRankings 更新全量和热门池排行榜
func (w *PoolWorker) updateAllRankings(
	slidingWindowPools [types.WindowCount]map[types.Pubkey]struct{},
	updatedPools map[types.Pubkey]struct{},
	fullLiquidity bool,
	latestTime uint32,
) {
	// 更新流动性
	if fullLiquidity {
		w.updatePoolLiquidity()
	}

	// 更新全量排行榜
	w.updateRankings(types.RankingCategoryFull, slidingWindowPools, updatedPools, fullLiquidity, latestTime)

	// 更新热门池排行榜
	hotUpdatedPools := utils.MergeSets(updatedPools, w.pendingHotPools)
	w.updateRankings(types.RankingCategoryHot, slidingWindowPools, hotUpdatedPools, fullLiquidity, latestTime)
}

// 更新所有池子流动性
func (w *PoolWorker) updatePoolLiquidity() {
	qp := w.quotePrices
	nativePrice := qp.nativePrice
	usdcPrice := qp.priceUnsafe(consts.USDCMint)

	// 更新池子流动性价格（主线程安全，快速判断 Native/USDC quote）
	for _, pl := range w.pools.pools {
		switch {
		case pl.IsNativeQuote:
			pl.UpdateLiquidity(nativePrice)
		case pl.IsUSDCQuote:
			pl.UpdateLiquidity(usdcPrice)
		default:
			pl.UpdateLiquidity(qp.priceUnsafe(pl.QuoteToken))
		}
	}
}

// updateRankings 更新排行榜
func (w *PoolWorker) updateRankings(
	category types.RankingCategory,
	slidingWindowPools [types.WindowCount]map[types.Pubkey]struct{},
	updatedPools map[types.Pubkey]struct{},
	fullLiquidity bool,
	latestTime uint32,
) {
	// 1. 窗口级排行榜：每个时间窗口单独更新
	for i, slidingPools := range slidingWindowPools {
		// 合并本次更新池子和窗口池子
		windowSet := utils.MergeSets(updatedPools, slidingPools)
		windowPools := w.collectPoolsFromMap(windowSet)
		w.rankingManager.UpdateWindowRankings(category, types.RankingWindow(i), windowPools, latestTime)
	}

	// 2. 全局排行榜（FDV、MC 等全局字段）
	globalSet := utils.MergeSets(updatedPools, w.pendingTokenPools)
	globalPools := w.collectPoolsFromMap(globalSet)
	w.rankingManager.UpdateGlobalRankings(category, globalPools, latestTime)

	// 3. 流动性排行榜
	if fullLiquidity {
		// 全量重建流动性排行榜
		w.rankingManager.RebuildLiquidityRanking(category, w.pools.pools, latestTime)
	} else {
		// 增量更新流动性排行榜
		w.rankingManager.UpdateLiquidityRankings(category, globalPools, latestTime)
	}
}

// collectPoolsFromMap 根据地址集合收集对应的池指针切片
func (w *PoolWorker) collectPoolsFromMap(updatedPools map[types.Pubkey]struct{}) []*pool.Pool {
	pools := make([]*pool.Pool, 0, len(updatedPools))
	for addr := range updatedPools {
		if p := w.pools.getPoolUnsafe(addr); p != nil {
			pools = append(pools, p)
		}
	}
	return pools
}

// syncAllPoolsStatsCache 将所有相关 pool 数据同步到对外查询缓存。
func (w *PoolWorker) syncAllPoolsStatsCache(
	slidingWindowPools [types.WindowCount]map[types.Pubkey]struct{},
	updatedPools map[types.Pubkey]struct{},
	fullLiquidity bool,
) {
	visited := make(map[types.Pubkey]struct{}, 1024)

	// 全量同步：updatedPools
	w.syncSelectedPoolsCache(updatedPools, visited, true)

	// 全量同步：slidingWindowPools
	for _, slidingPools := range slidingWindowPools {
		w.syncSelectedPoolsCache(slidingPools, visited, true)
	}

	// 部分同步：pendingHotPools + pendingTokenPools
	poolSet := utils.MergeSets(w.pendingHotPools, w.pendingTokenPools)
	w.syncSelectedPoolsCache(poolSet, visited, false)

	// 流动性同步（轻量、非安全）
	if fullLiquidity {
		for key, pl := range w.pools.pools {
			if _, ok := visited[key]; !ok && pl != nil {
				pl.SyncPoolStatsPartial() // pl.SyncLiquidityCache()
			}
		}
	}
}

// syncSelectedPoolsCache 同步指定 pool 的缓存（全量或部分）
func (w *PoolWorker) syncSelectedPoolsCache(
	keys map[types.Pubkey]struct{},
	visited map[types.Pubkey]struct{},
	fullSync bool,
) {
	for key := range keys {
		if _, ok := visited[key]; ok {
			continue
		}

		pl := w.pools.getPoolUnsafe(key)
		if pl == nil {
			continue
		}

		if fullSync {
			pl.SyncPoolStatsFull()
		} else {
			pl.SyncPoolStatsPartial()
		}

		visited[key] = struct{}{}
	}
}

// collectPushTasks 收集 PushTask
func (w *PoolWorker) collectPushTask(
	blockNumber uint32,
	updatedPools map[types.Pubkey]struct{},
	slidingWindowPools [types.WindowCount]map[types.Pubkey]struct{},
) []*types.PushTask {
	// 计算所有池子的总数量
	n := len(updatedPools) + len(w.pendingTokenPools) + len(w.pendingHotPools)
	for _, item := range slidingWindowPools {
		n += len(item)
	}

	// 创建一个池集合并合并所有池子
	poolSet := make(map[types.Pubkey]struct{}, n)
	utils.MergeMultipleSets(poolSet, updatedPools, w.pendingTokenPools, w.pendingHotPools)
	for _, slidingPools := range slidingWindowPools {
		utils.MergeMultipleSets(poolSet, slidingPools)
	}

	// 收集 PushTask
	pushTasks := make([]*types.PushTask, 0, len(poolSet))
	for key := range poolSet {
		pl := w.pools.getPoolUnsafe(key)
		if pl == nil || !pl.ShouldPushTicker() {
			continue
		}

		pushTasks = append(pushTasks, &types.PushTask{
			Pool:        key,
			PoolHash:    pl.AddressHash,
			BlockNumber: blockNumber,
			Ticker:      pl.GetPoolStatsData(),
		})
	}

	return pushTasks
}

// collectTokenTasks 收集 TokenTask（Meta / HolderCount / TopHolders）
func (w *PoolWorker) collectTokenTasks(updatedPools map[types.Pubkey]struct{}) (
	tokenMetaTasks []types.TokenTask,
	holderCountTasks []types.TokenTask,
	topHoldersTasks []types.TokenTask,
) {
	tokenMap := make(map[types.Pubkey]struct{}, len(updatedPools))
	for key := range updatedPools {
		if pl := w.pools.getPoolUnsafe(key); pl != nil {
			tokenMap[pl.BaseToken] = struct{}{}
		}
	}

	n := len(tokenMap)
	tokenMetaTasks = make([]types.TokenTask, 0, n)
	holderCountTasks = make([]types.TokenTask, 0, n)
	topHoldersTasks = make([]types.TokenTask, 0, n)

	nowMs := time.Now().UnixMilli()
	for key := range tokenMap {
		tokenInfo := w.tokenMap.GetTokenInfoUnsafe(key)
		if tokenInfo == nil {
			continue
		}

		if tokenInfo.ShouldRequestSupply(true) {
			tokenMetaTasks = append(tokenMetaTasks, types.TokenTask{Token: key, TaskAtMs: nowMs})
		}
		if tokenInfo.ShouldRequestHolderCount(true) {
			holderCountTasks = append(holderCountTasks, types.TokenTask{Token: key, TaskAtMs: nowMs})
		}
		if tokenInfo.ShouldRequestHolderCount(true) {
			topHoldersTasks = append(topHoldersTasks, types.TokenTask{Token: key, TaskAtMs: nowMs})
		}
	}

	return
}

// collectRankingUpdates 收集排行榜更新
func (w *PoolWorker) collectRankingUpdates(blockNumber uint32) types.RankingResult[*pool.Pool] {
	rankings := make(types.RankingByDimension[*pool.Pool], len(w.rankingManager.Rankings))

	for key, ranking := range w.rankingManager.Rankings {
		pools := ranking.Pools()
		items := make([]types.RankingItem[*pool.Pool], 0, len(pools))
		for _, p := range pools {
			items = append(items, types.RankingItem[*pool.Pool]{
				TickerData: p.GetPoolStatsData(),
				Entity:     p,
			})
		}
		rankings[key] = items
	}

	return types.RankingResult[*pool.Pool]{
		WorkerID:    w.workerID,
		BlockNumber: blockNumber,
		Rankings:    rankings,
	}
}

// dispatchAllTasks 将收集到的所有任务回调给对应的 listener。
func (w *PoolWorker) dispatchAllTasks(
	pushTasks []*types.PushTask,
	tokenMetaTasks []types.TokenTask,
	holderCountTasks []types.TokenTask,
	topHoldersTasks []types.TokenTask,
	rankingTasks types.RankingResult[*pool.Pool],
) {
	// 回调 PushTask
	if len(pushTasks) > 0 {
		w.taskListener.OnPoolPushTasks(w.workerID, pushTasks)
	}

	// 回调 TokenTask
	if len(tokenMetaTasks) > 0 {
		w.taskListener.OnPoolTokenTasks(w.workerID, types.TokenTaskMeta, tokenMetaTasks)
	}
	if len(holderCountTasks) > 0 {
		w.taskListener.OnPoolTokenTasks(w.workerID, types.TokenTaskHolderCount, holderCountTasks)
	}
	if len(topHoldersTasks) > 0 {
		w.taskListener.OnPoolTokenTasks(w.workerID, types.TokenTaskTopHolders, topHoldersTasks)
	}

	// 回调 RankingResult
	w.rankingListener.OnPoolRankingUpdate(rankingTasks)
}

// resetState 状态重置（清理缓存 + 更新桶 + 标记 block）
func (w *PoolWorker) resetState(
	updatedPools map[types.Pubkey]struct{},
	latestTime, blockTime uint32,
	isSlidingOutWindow24H bool,
	blockNumber uint32,
) {
	// 清理临时缓存
	clear(w.pendingHotPools)
	clear(w.pendingTokenPools)

	// 清理过期池
	if isSlidingOutWindow24H {
		expiredPools := w.pools.cleanupExpiredPools(latestTime)
		w.tokenMap.RemovePools(expiredPools)
	}

	w.hotPools.updateLastEventTimes(updatedPools, blockTime)
	w.hotPools.cleanExpired(latestTime)

	// 更新 15s 缓存桶
	w.poolBuckets15s.addPools(updatedPools, blockTime)

	// 标记 block 已处理
	w.blockStates.setBlockProcessed(blockNumber)

	// 更新 lastHandledTime
	w.lastHandledTime = latestTime
}
