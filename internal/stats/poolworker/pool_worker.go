package poolworker

import (
	"context"
	"dex-stats-sol/internal/pkg/logger"
	"dex-stats-sol/internal/pkg/utils"
	ea "dex-stats-sol/internal/stats/eventadapter"
	"dex-stats-sol/internal/stats/poolworker/defs"
	"dex-stats-sol/internal/stats/poolworker/pool"
	"dex-stats-sol/internal/stats/poolworker/ranking"
	"dex-stats-sol/internal/stats/poolworker/token"
	"dex-stats-sol/internal/stats/types"
	"dex-stats-sol/pb"
	"errors"
	"runtime/debug"
	"sync/atomic"
	"time"
)

// ==========================================
// 类型定义与结构体
// ==========================================

type Msg struct {
	Type defs.WorkerMsgType
	Data any
}

type PoolSnapshotListener interface {
	OnPoolSnapshotRecovered(workerID uint8, quotePrices *types.QuotePrices)
}

type PoolRankingListener[T any] interface {
	OnPoolRankingUpdate(result types.RankingResult[T])
}

type PoolTaskListener interface {
	OnPoolTokenTasks(workerID uint8, taskType types.TokenTaskType, tasks []types.TokenTask)
	OnPoolPushTasks(workerID uint8, tasks []*types.PushTask)
}

type PoolWorker struct {
	// ----------------- 消息与上下文 -----------------
	msgChan          chan *Msg                       // 消息通道
	ctx              context.Context                 // 上下文
	cancel           func(err error)                 // 取消函数
	snapshotListener PoolSnapshotListener            // 快照回调监听器
	rankingListener  PoolRankingListener[*pool.Pool] // 排行榜回调监听器
	taskListener     PoolTaskListener                // 任务回调监听器

	workerID        uint8        // 线程编号
	lastSendLogTime atomic.Int64 // 阻塞日志限流时间（纳秒）

	// ----------------- 时间/状态管理 -----------------
	lastHandledTime uint32          // 最近处理事件时间戳（秒）
	blockStates     *BlockStates    // block 状态管理
	poolBuckets15s  *PoolBuckets15s // 15秒时间桶缓存

	// ----------------- 池数据 -----------------
	pools       *PoolMap           // 所有池主数据
	hotPools    *HotPools          // 热门池缓存
	tokenMap    *token.TokenMap    // Token 到池映射
	quotePrices *WorkerQuotePrices // 行情报价

	// ----------------- 排行榜 -----------------
	rankingManager *ranking.PoolRankingManager // 全局排名

	// ----------------- 待更新池 -----------------
	pendingHotPools   map[types.Pubkey]struct{} // 热门池待更新
	pendingTokenPools map[types.Pubkey]struct{} // token 级别池待更新
}

// ==========================================
// 构造函数与启动/停止方法
// ==========================================

func NewPoolWorker(
	workerID uint8,
	snapshotListener PoolSnapshotListener,
	rankingListener PoolRankingListener[*pool.Pool],
	taskListener PoolTaskListener,
) *PoolWorker {
	ctx, cancel := context.WithCancelCause(context.Background())
	return &PoolWorker{
		msgChan:           make(chan *Msg, 32),
		ctx:               ctx,
		cancel:            cancel,
		workerID:          workerID,
		snapshotListener:  snapshotListener,
		rankingListener:   rankingListener,
		taskListener:      taskListener,
		lastHandledTime:   0,
		blockStates:       NewBlockStates(workerID),
		poolBuckets15s:    NewPoolBuckets15s(workerID),
		pools:             NewPoolMap(),
		hotPools:          newHotPools(workerID, 128),
		tokenMap:          token.NewTokenMap(),
		quotePrices:       NewWorkerQuotePrices(workerID),
		rankingManager:    ranking.NewPoolRankingManager(),
		pendingHotPools:   make(map[types.Pubkey]struct{}),
		pendingTokenPools: make(map[types.Pubkey]struct{}),
	}
}

func (w *PoolWorker) Start() {
	for {
		select {
		case <-w.ctx.Done():
			return

		case msg := <-w.msgChan:
			w.handleMsg(msg)
		}
	}
}

func (w *PoolWorker) Stop() {
	w.cancel(errors.New("service stop"))
}

// ==========================================
// 外部接口方法（事件接收）
// ==========================================

// EnqueueRecoverTasks 将异步恢复任务加入工作队列（如 leader 切换时）
func (w *PoolWorker) EnqueueRecoverTasks(recoverType defs.RecoverType) {
	w.sendMsg(&Msg{Type: defs.WorkerMsgTypeRecoverTasks, Data: recoverType})
}

// OnReceivedChainEvents 接收链上事件
func (w *PoolWorker) OnReceivedChainEvents(events *pb.Events) {
	w.sendMsg(&Msg{Type: defs.WorkerMsgTypeChainEvents, Data: events})
}

// OnReceivedBalanceEvents 接收余额变化事件
func (w *PoolWorker) OnReceivedBalanceEvents(events *pb.Events) {
	w.sendMsg(&Msg{Type: defs.WorkerMsgTypeBalanceEvents, Data: events})
}

// OnReceivedTokenMetaEvents 接收 TokenMeta 数据事件
func (w *PoolWorker) OnReceivedTokenMetaEvents(events *pb.MetaEvents) {
	w.sendMsg(&Msg{Type: defs.WorkerMsgTypeTokenMetaEvents, Data: events})
}

// OnReceivedLpReportEvents 接收 LP 报告事件
func (w *PoolWorker) OnReceivedLpReportEvents(events *pb.PoolLpReportEvents) {
	w.sendMsg(&Msg{Type: defs.WorkerMsgTypeLpReportEvents, Data: events})
}

// OnReceivedTotalSupplyEvents 接收 TotalSupply 事件
func (w *PoolWorker) OnReceivedTotalSupplyEvents(events *pb.TotalSupplyEvents) {
	w.sendMsg(&Msg{Type: defs.WorkerMsgTypeTotalSupplyEvents, Data: events})
}

// OnReceivedTopHoldersEvents 接收 TopHolders 事件
func (w *PoolWorker) OnReceivedTopHoldersEvents(events *pb.TopHoldersEvents) {
	w.sendMsg(&Msg{Type: defs.WorkerMsgTypeTopHoldersEvents, Data: events})
}

// OnReceivedHolderCountEvents 接收 HolderCount 事件
func (w *PoolWorker) OnReceivedHolderCountEvents(events *pb.HolderCountEvents) {
	w.sendMsg(&Msg{Type: defs.WorkerMsgTypeHolderCountEvents, Data: events})
}

// OnReceivedPoolTickerPushDoneEvents 接收 PoolTicker 推送完成事件
func (w *PoolWorker) OnReceivedPoolTickerPushDoneEvents(events *pb.PoolTickerPushDoneEvents) {
	w.sendMsg(&Msg{Type: defs.WorkerMsgTypePoolTickerPushDoneEvents, Data: events})
}

func (w *PoolWorker) GetPools(poolAddresses []types.Pubkey) ([]*pool.Pool, map[types.Pubkey]HotPoolData) {
	pools, missingKeys := w.pools.getPools(poolAddresses)
	hotPools := w.hotPools.getHotPoolsData(missingKeys)
	return pools, hotPools
}

func (w *PoolWorker) GetTokenInfo(tokenAddress types.Pubkey) *token.TokenInfo {
	return w.tokenMap.GetTokenInfo(tokenAddress)
}

// ==========================================
// 内部消息发送及处理
// ==========================================

// sendMsg 向消息通道投递 Msg，如果通道已满则阻塞重试，并打印节流日志。
func (w *PoolWorker) sendMsg(msg *Msg) {
	for {
		select {
		case w.msgChan <- msg:
			return

		case <-w.ctx.Done():
			// 上下文关闭，退出发送
			logger.Infof("[PoolWorker:%d] context done, stop sending msg", w.workerID)
			return

		default:
			// 通道满，节流打印日志 + 阻塞等待重试
			if utils.ThrottleLog(&w.lastSendLogTime, time.Second) {
				logger.Warnf("[PoolWorker:%d] msgChan full, blocking Send()", w.workerID)
			}
			time.Sleep(30 * time.Millisecond)
		}
	}
}

func (w *PoolWorker) handleMsg(msg *Msg) {
	start := time.Now()
	logger.Infof("[PoolWorker:%d] start handleMsg type=%s", w.workerID, msg.Type)

	defer func() {
		if r := recover(); r != nil {
			logger.Errorf("[PoolWorker:%d] panic in handleMsg: %v\n%s", w.workerID, r, debug.Stack())
		}
		if msg.Type != defs.WorkerMsgTypeSnapshotItem {
			logger.Infof("[PoolWorker:%d] end handleMsg, totalPools=%d hotPools=%d type=%s cost=%s",
				w.workerID, len(w.pools.pools), w.hotPools.lenUnsafe(), msg.Type, time.Since(start))
		}
	}()

	switch msg.Type {
	case defs.WorkerMsgTypeSnapshotItem:
		w.handleSnapshotItem(msg.Data)

	case defs.WorkerMsgTypeSnapshotDone:
		w.handleSnapshotDone()

	case defs.WorkerMsgTypeRecoverTasks:
		w.handleRecoverTasks(msg.Data.(defs.RecoverType))

	case defs.WorkerMsgTypeChainEvents:
		w.handleChainEvents(msg.Data.(*pb.Events))

	case defs.WorkerMsgTypeBalanceEvents:
		w.handleBalanceEvents(msg.Data.(*pb.Events))

	case defs.WorkerMsgTypeTokenMetaEvents:
		w.handleTokenMetaEvents(msg.Data.(*pb.MetaEvents))

	case defs.WorkerMsgTypeTotalSupplyEvents:
		w.handleTotalSupplyEvents(msg.Data.(*pb.TotalSupplyEvents))

	case defs.WorkerMsgTypeTopHoldersEvents:
		w.handleTopHoldersEvents(msg.Data.(*pb.TopHoldersEvents))

	case defs.WorkerMsgTypeHolderCountEvents:
		w.handleHolderCountEvents(msg.Data.(*pb.HolderCountEvents))

	case defs.WorkerMsgTypeLpReportEvents:
		w.handleLpReportEvents(msg.Data.(*pb.PoolLpReportEvents))

	case defs.WorkerMsgTypePoolTickerPushDoneEvents:
		w.handlePoolTickerPushDoneEvent(msg.Data.(*pb.PoolTickerPushDoneEvents))

	default:
		logger.Warnf("[PoolWorker:%d] unknown MsgType %s", w.workerID, msg.Type)
	}
}

// ==========================================
// 事件处理
// ==========================================

func (w *PoolWorker) handleRecoverTasks(recoverType defs.RecoverType) {
	start := time.Now()
	const batchSize = 128

	// 准备任务切片
	tokenMetaTasks := make([]types.TokenTask, 0, batchSize)
	holderCountTasks := make([]types.TokenTask, 0, batchSize)
	topHoldersTasks := make([]types.TokenTask, 0, batchSize)
	pushTasks := make([]*types.PushTask, 0, batchSize)

	nowMs := time.Now().UnixMilli()
	tokenMap := w.tokenMap.GetTokenMapUnsafe() // unsafe 读，主线程安全

	// 遍历 tokenMap，生成 TokenTask
	for _, info := range tokenMap {
		if recoverType == defs.RecoveryHotTokens && !info.HasLeverage() {
			continue
		}

		if info.SharedSupply.ShouldRequest(false) {
			tokenMetaTasks = append(tokenMetaTasks, types.TokenTask{Token: info.Token, TaskAtMs: nowMs})
		}
		if info.SharedHolderCount.ShouldRequest(false) {
			holderCountTasks = append(holderCountTasks, types.TokenTask{Token: info.Token, TaskAtMs: nowMs})
		}
		if info.SharedTopHolders.ShouldRequest(false) {
			topHoldersTasks = append(topHoldersTasks, types.TokenTask{Token: info.Token, TaskAtMs: nowMs})
		}
	}

	if recoverType != defs.RecoveryHotTokens {
		// 遍历池子，生成 PushTask
		for _, pl := range w.pools.pools {
			if pl.ShouldPushTicker() {
				pushTasks = append(pushTasks, &types.PushTask{
					Pool:        pl.Address,
					PoolHash:    pl.AddressHash,
					BlockNumber: pl.BlockNumber(),
					Ticker:      pl.GetPoolStatsData(),
				})
			}
		}
	}

	// 回调任务
	if len(pushTasks) > 0 {
		w.taskListener.OnPoolPushTasks(w.workerID, pushTasks)
	}
	if len(tokenMetaTasks) > 0 {
		w.taskListener.OnPoolTokenTasks(w.workerID, types.TokenTaskMeta, tokenMetaTasks)
	}
	if len(holderCountTasks) > 0 {
		w.taskListener.OnPoolTokenTasks(w.workerID, types.TokenTaskHolderCount, holderCountTasks)
	}
	if len(topHoldersTasks) > 0 {
		w.taskListener.OnPoolTokenTasks(w.workerID, types.TokenTaskTopHolders, topHoldersTasks)
	}

	// 日志打印
	logger.Infof(
		"[PoolWorker:%d] handleRecoverTasks finished in %s | PushTasks=%d, TokenMeta=%d, HolderCount=%d, TopHolders=%d",
		w.workerID, time.Since(start), len(pushTasks), len(tokenMetaTasks), len(holderCountTasks), len(topHoldersTasks),
	)
}

// handleBalanceEvents 处理单个 block 的余额事件
func (w *PoolWorker) handleBalanceEvents(events *pb.Events) {
	if events.Source != 1 { // 只支持 gRPC 推送
		return
	}

	// 按 token 聚合余额事件
	startTime := time.Now()
	groupByToken := ea.GroupBalancesByToken(events, w.pools.tokenAccountSet)
	if len(groupByToken) == 0 {
		return
	}

	// 待同步的 Token 任务列表
	tasks := make([]types.TokenTask, 0, len(groupByToken))
	nowMs := time.Now().UnixMilli()

	forStartTime := time.Now()
	for tokenAddr, balances := range groupByToken {
		tokenInfo := w.tokenMap.GetTokenInfoUnsafe(tokenAddr)
		if tokenInfo == nil {
			continue
		}

		// 更新 TopHolders，并判断是否需要触发同步
		if tokenInfo.UpdateTopHolders(uint32(events.BlockNumber), balances, w.pendingTokenPools) {
			tasks = append(tasks, types.TokenTask{
				Token:    tokenAddr,
				TaskAtMs: nowMs,
			})
		}
	}

	// 回调通知 Listener
	tasksStartTime := time.Now()
	if len(tasks) > 0 {
		w.taskListener.OnPoolTokenTasks(w.workerID, types.TokenTaskTopHolders, tasks)
	}

	// 计算各部分时间
	groupTime := forStartTime.Sub(startTime)
	forLoopTime := tasksStartTime.Sub(forStartTime)
	tasksTime := time.Since(tasksStartTime)
	totalTime := time.Since(startTime)

	// 打印时间日志
	logger.Infof("[PoolWorker:%d] handleBalanceEvents, totalTime: %v, groupTime: %v, forLoopTime: %v, tasksTime: %v, groupByToken length: %d, tasks length: %d",
		w.workerID, totalTime, groupTime, forLoopTime, tasksTime, len(groupByToken), len(tasks))
}

// handleTokenMetaEvents 处理 TokenMeta 事件，更新池子的供应信息
func (w *PoolWorker) handleTokenMetaEvents(events *pb.MetaEvents) {
	for _, event := range events.Events {
		supply := event.GetSupply()
		if supply == nil || supply.Type != pb.MetaEventType_SUPPLY {
			continue
		}

		tokenAddr, err := types.PubkeyFromBytes(supply.TokenAddress)
		if err != nil {
			logger.Warnf(
				"[PoolWorker:%d] handleTokenMetaEvents, invalid token address: %x",
				w.workerID, supply.TokenAddress,
			)
			continue
		}

		if tokenInfo := w.tokenMap.GetTokenInfoUnsafe(tokenAddr); tokenInfo != nil {
			tokenInfo.UpdateSupplyInfo(
				utils.AmountToFloat64(supply.TotalSupply, uint8(supply.Decimals)),
				utils.AmountToFloat64(supply.CirculatingSupply, uint8(supply.Decimals)),
				utils.AmountToFloat64(supply.MaxSupply, uint8(supply.Decimals)),
				false,
				uint32(event.UpdateTime),
				w.pendingTokenPools,
			)
		}
	}
}

// handleTotalSupplyEvents 处理 TotalSupply 事件，更新相关池子的供应信息
func (w *PoolWorker) handleTotalSupplyEvents(events *pb.TotalSupplyEvents) {
	for _, event := range events.Events {
		tokenAddr, err := types.PubkeyFromBytes(event.TokenAddress)
		if err != nil {
			logger.Warnf(
				"[PoolWorker:%d] handleTotalSupplyEvents, invalid token address: %x",
				w.workerID, event.TokenAddress,
			)
			continue
		}

		if tokenInfo := w.tokenMap.GetTokenInfoUnsafe(tokenAddr); tokenInfo != nil {
			tokenInfo.UpdateTotalSupply(
				utils.AmountToFloat64(event.TotalSupply, tokenInfo.Decimals),
				event.IsBurned,
				uint32(events.UpdateTime),
				w.pendingTokenPools,
			)
		}
	}
}

// handleTopHoldersEvents 处理 TopHolders 事件
func (w *PoolWorker) handleTopHoldersEvents(events *pb.TopHoldersEvents) {
	for _, event := range events.Events {
		tokenAddr, err := types.PubkeyFromBytes(event.TokenAddress)
		if err != nil {
			logger.Warnf(
				"[PoolWorker:%d] handleTopHoldersEvents, invalid token address: %x",
				w.workerID, event.TokenAddress,
			)
			continue
		}

		tokenInfo := w.tokenMap.GetTokenInfoUnsafe(tokenAddr)
		if tokenInfo == nil {
			continue
		}

		infos := make([]*ea.AccountBalanceInfo, 0, len(event.Holders))
		for _, h := range event.Holders {
			account, accErr := types.TryPubkeyFromString(h.AccountAddress)
			if accErr != nil {
				continue
			}

			infos = append(infos, &ea.AccountBalanceInfo{
				Account:       account,
				Balance:       utils.AmountToFloat64(h.Balance, tokenInfo.Decimals),
				BlockNumber:   uint32(h.LastEventId >> 32),
				IsPoolAccount: w.pools.isPoolAccountUnsafe(account),
			})
		}

		tokenInfo.SyncTopHolders(infos, w.pendingTokenPools)
	}
}

// handleHolderCountEvents 处理 HolderCount 事件
func (w *PoolWorker) handleHolderCountEvents(events *pb.HolderCountEvents) {
	for _, event := range events.Events {
		tokenAddr, err := types.PubkeyFromBytes(event.TokenAddress)
		if err != nil {
			logger.Warnf(
				"[PoolWorker:%d] handleHolderCountEvents, invalid token address: %x",
				w.workerID, event.TokenAddress,
			)
			continue
		}

		if tokenInfo := w.tokenMap.GetTokenInfoUnsafe(tokenAddr); tokenInfo != nil {
			tokenInfo.UpdateHolderBase(
				int32(event.HolderCount),
				uint32(event.UpdateTime),
				w.pendingTokenPools,
			)
		}
	}
}

// handleLpReportEvents 处理杠杆事件，更新池子的杠杆信息。
func (w *PoolWorker) handleLpReportEvents(events *pb.PoolLpReportEvents) {
	w.hotPools.updateLeverages(events)

	for _, event := range events.Events {
		addr, err := types.PubkeyFromBytes(event.PoolAddress)
		if err != nil {
			logger.Warnf("[PoolWorker:%d] handleLpReportEvents, invalid pool address: %x", w.workerID, event.PoolAddress)
			continue
		}

		pl := w.pools.getPoolUnsafe(addr)
		if pl == nil {
			logger.Debugf("[PoolWorker:%d] handleLpReportEvents, pool not found: %s", w.workerID, addr)
			continue
		}

		// 根据交易方向更新对应杠杆参数，并返回是否发生变化
		var updated bool
		if isLongDirection(event.Direction) {
			updated = pl.SetLongLeverage(event.Leverage)
		} else {
			updated = pl.SetShortLeverage(event.Leverage)
		}

		// 如果 listingAt 或杠杆值发生变化，则标记该 pool 为 pending hot pool
		if pl.UpdateListingAt(event.ListingTimeMs) || updated {
			w.pendingHotPools[addr] = struct{}{}
		}

		// 加打印日志
		direction := "short"
		if isLongDirection(event.Direction) {
			direction = "long"
		}
		logger.Debugf("[PoolWorker:%d] handleLpReportEvents, updated pool %s: leverage=%d, direction=%s",
			w.workerID, addr, event.Leverage, direction)
	}
}

// handlePoolTickerPushDoneEvent 处理 PoolTicker 推送完成事件
func (w *PoolWorker) handlePoolTickerPushDoneEvent(events *pb.PoolTickerPushDoneEvents) {
	updated := 0

	for _, event := range events.Events {
		poolAddr, err := types.PubkeyFromBytes(event.PoolAddress)
		if err != nil {
			logger.Warnf("[PoolWorker:%d] handlePoolTickerPushDoneEvent, invalid pool address: %x",
				w.workerID, event.PoolAddress)
			continue
		}

		if pl := w.pools.getPoolUnsafe(poolAddr); pl != nil {
			pl.SetLastPushBlockNumber(uint32(event.BlockNumber))
			updated++
		}
	}

	if updated > 0 {
		logger.Debugf("[PoolWorker:%d] handlePoolTickerPushDoneEvent: updated=%d", w.workerID, updated)
	}
}
