package poolworker

import (
	"dex-stats-sol/internal/pkg/logger"
	"dex-stats-sol/internal/stats/poolworker/defs"
	"dex-stats-sol/internal/stats/poolworker/pool"
	"dex-stats-sol/internal/stats/poolworker/token"
	"dex-stats-sol/internal/stats/types"
	"fmt"
	"runtime/debug"
	"time"
)

// ============================
// 快照写入
// ============================

// PrepareSnapshot 生成 PoolWorker 的快照数据
func (w *PoolWorker) PrepareSnapshot() ([]types.Serializable, error) {
	// 预估容量，减少切片扩容
	result := make([]types.Serializable, 0, w.poolBuckets15s.len()+w.pools.len()+16)

	// Block状态快照
	result = append(result, w.blockStates)

	// 15秒桶快照
	w.poolBuckets15s.prepareSnapshot(&result)

	// Tokens 数据快照
	w.tokenMap.PrepareSnapshot(&result)

	// Pools 数据快照
	w.pools.prepareSnapshot(&result)

	// 热门池和行情报价快照
	result = append(result, w.hotPools, w.quotePrices)

	return result, nil
}

// ============================
// 快照恢复入口
// ============================

// OnRecoverFromSnapshot 反序列化后发给worker线程
func (w *PoolWorker) OnRecoverFromSnapshot(data []byte) error {
	item, err := w.deserializeSnapshot(data)
	if err != nil {
		return err
	}
	w.sendMsg(&Msg{Type: defs.WorkerMsgTypeSnapshotItem, Data: item})
	return nil
}

// OnRecoverFromSnapshotDone 快照恢复完成
func (w *PoolWorker) OnRecoverFromSnapshotDone() {
	w.sendMsg(&Msg{Type: defs.WorkerMsgTypeSnapshotDone})
}

// deserializeSnapshot 反序列化单条快照数据
func (w *PoolWorker) deserializeSnapshot(data []byte) (res any, err error) {
	defer func() {
		if r := recover(); r != nil {
			logger.Errorf(
				"[PoolWorker:%d] panic recovered in deserializeSnapshot: %v\n%s",
				w.workerID, r, debug.Stack(),
			)
			err = fmt.Errorf("[PoolWorker:%d] panic recovered in deserializeSnapshot: %v", w.workerID, r)
			res = nil
		}
	}()

	if len(data) < 2 {
		return nil, fmt.Errorf("[PoolWorker:%d] snapshot data too short", w.workerID)
	}

	switch data[1] {
	case defs.SnapshotTypeBlockStates:
		return deserializeBlockStates(w.workerID, data)
	case defs.SnapshotTypePoolBucket15s:
		return deserializePoolBucket15s(w.workerID, data)
	case defs.SnapshotTypePoolData:
		return pool.DeserializePool(w.workerID, data)
	case defs.SnapshotTypeTokenData:
		return token.DeserializeTokenInfo(w.workerID, data)
	case defs.SnapshotTypeHotPools:
		return deserializeHotPools(w.workerID, data)
	case defs.SnapshotTypeQuotePrice:
		return deserializeQuotePrice(w.workerID, data)
	default:
		return nil, fmt.Errorf("[PoolWorker:%d] unknown snapshot type: %d", w.workerID, data[1])
	}
}

// ============================
// 快照恢复处理
// ============================

// handleSnapshotItem 根据类型处理快照数据项，更新对应状态
func (w *PoolWorker) handleSnapshotItem(data any) {
	switch item := data.(type) {
	case *BlockStates:
		w.blockStates.replaceWithSnapshot(item)
	case *PoolBucket15s:
		w.poolBuckets15s.replaceWithSnapshotItem(item)
	case *token.TokenInfo:
		w.tokenMap.RestoreToken(item)
	case *pool.Pool:
		w.pools.addPool(item)
	case *HotPools:
		w.hotPools.replaceWithSnapshot(item)
	case *WorkerQuotePrices:
		w.quotePrices.replaceWithSnapshot(item)
	default:
		logger.Warnf("[PoolWorker:%d] unknown snapshot item type: %T", w.workerID, item)
	}
}

// handleSnapshotDone 快照恢复完成后的处理
func (w *PoolWorker) handleSnapshotDone() {
	start := time.Now()

	// ==============================
	// 阶段 1：注入共享状态
	// ==============================
	stage1Start := time.Now()
	latestTime := w.quotePrices.BlockTime
	for _, pl := range w.pools.pools {
		if tokenInfo := w.tokenMap.GetTokenInfoUnsafe(pl.BaseToken); tokenInfo != nil {
			// 注入共享 token 数据
			pl.RestoreSharedTokenData(
				tokenInfo.SharedSupply,
				tokenInfo.SharedHolderCount,
				tokenInfo.SharedTopHolders,
			)
		} else {
			// 如果 tokenInfo 不存在，初始化 TopHolders
			pl.SharedTopHolders.InitIfEmpty()
			w.tokenMap.AddPool(pl)
		}

		// 用于后续 ticker 滑动计算最新时间
		latestTime = max(latestTime, pl.LastChainEventTs())
	}
	logger.Infof("[PoolWorker:%d] handleSnapshotDone: Stage1 cost=%v", w.workerID, time.Since(stage1Start))

	// ==============================
	// 阶段 2：重建 tokenMap + 同步其他 pool 状态
	// ==============================
	stage2Start := time.Now()
	w.tokenMap.Reset() // 确保 tokenMap 与 pools 完全同步
	for _, pl := range w.pools.pools {
		w.tokenMap.AddPool(pl)

		// 热门池信息
		hot := w.hotPools.getDataUnsafe(pl.Address)
		pl.SetLongLeverage(hot.LongLeverage)
		pl.SetShortLeverage(hot.ShortLeverage)
		pl.UpdateListingAt(hot.ListingAtMs)

		// 价格、持仓、市场数据
		pl.UpdateLiquidity(w.quotePrices.priceUnsafe(pl.QuoteToken))
		pl.UpdateHolderCount()
		pl.UpdateTop10HolderRatio()
		pl.UpdateMarketCapFDV()

		// 滑动过期 ticker
		pl.SlideOutExpired(latestTime)
	}
	logger.Infof("[PoolWorker:%d] handleSnapshotDone: Stage2 cost=%v", w.workerID, time.Since(stage2Start))

	// ==============================
	// 阶段 3：重建排行榜
	// ==============================
	stage3Start := time.Now()
	w.rankingManager.RebuildAllRankings(w.pools.pools, latestTime)
	logger.Infof("[PoolWorker:%d] handleSnapshotDone: Stage3 cost=%v", w.workerID, time.Since(stage3Start))

	// 更新最后处理时间
	w.lastHandledTime = latestTime

	// 回调快照恢复完成
	w.rankingListener.OnPoolRankingUpdate(w.collectRankingUpdates(1))
	w.snapshotListener.OnPoolSnapshotRecovered(w.workerID, w.quotePrices.cloneUnsafe())

	logger.Infof("[PoolWorker:%d] handleSnapshotDone: Total cost=%v", w.workerID, time.Since(start))
}
