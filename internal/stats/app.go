package stats

import (
	"dex-stats-sol/internal/consts"
	"dex-stats-sol/internal/pkg/logger"
	"dex-stats-sol/internal/pkg/mq"
	"dex-stats-sol/internal/pkg/nacos"
	"dex-stats-sol/internal/pkg/utils"
	"dex-stats-sol/internal/stats/eventadapter"
	pw "dex-stats-sol/internal/stats/poolworker"
	"dex-stats-sol/internal/stats/poolworker/defs"
	"dex-stats-sol/internal/stats/poolworker/pool"
	"dex-stats-sol/internal/stats/pushworker"
	"dex-stats-sol/internal/stats/raft"
	"dex-stats-sol/internal/stats/rankingworker"
	"dex-stats-sol/internal/stats/taskworker"
	"dex-stats-sol/internal/stats/types"
	"dex-stats-sol/internal/svc"
	"dex-stats-sol/pb"
	"fmt"
	"github.com/apache/rocketmq-client-go/v2/primitive"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	gzsvc "github.com/zeromicro/go-zero/core/service"
	"google.golang.org/protobuf/proto"
	"runtime"
	"runtime/debug"
	"strings"
	"sync/atomic"
	"time"
)

//////////////////////////////
// 常量与类型定义
//////////////////////////////

type MsgType uint8

const (
	MsgTypeChainEvents MsgType = iota + 1
	MsgTypeBalanceEvents
	MsgTypeTokenMetaEvents
	MsgTypeLpReportEvents
	MsgTypeTotalSupplyEvents
	MsgTypeTopHoldersEvents
	MsgTypeHolderCountEvents
	MsgTypePoolTickerPushDoneEvents
)

//////////////////////////////
// App 结构体
//////////////////////////////

type App struct {
	pb.UnimplementedStatsQueryServiceServer

	// 服务相关
	svc  *svc.ServiceContext
	sg   *gzsvc.ServiceGroup
	raft *raft.Raft

	// 工作者
	poolWorkerCount         uint8
	poolWorkers             []*pw.PoolWorker
	holderCountWorker       *taskworker.HolderCountTaskWorker
	topHoldersWorker        *taskworker.TopHoldersTaskWorker
	tokenMetaInternalWorker *taskworker.TokenMetaInternalWorker
	tokenMetaRpcWorker      *taskworker.TokenMetaRpcWorker
	kafkaPushWorker         *pushworker.KafkaPushWorker
	rankingWorker           *rankingworker.RankingWorker

	// 消费者
	chainEventsKC   *mq.KafkaConsumer
	balanceEventsKC *mq.KafkaConsumer
	tokenMetaKC     *mq.KafkaConsumer
	lpReportRC      *mq.RocketMQConsumer

	// 缓冲区
	chainEventsBuf   []byte
	balanceEventsBuf []byte
	tokenMetaBuf     []byte

	// 全局右币价格
	globalQuotePrices *types.GlobalQuotePrices

	// 状态与GC
	needRecoveryAllTasks atomic.Bool
	lastGCTime           time.Time
	lastRecoveryTime     time.Time
}

//////////////////////////////
// 构造与初始化
//////////////////////////////

// NewApp 构造应用实例
func NewApp(svc *svc.ServiceContext) *App {
	app := &App{
		svc:               svc,
		sg:                gzsvc.NewServiceGroup(),
		chainEventsBuf:    make([]byte, 0, 512*1024),
		balanceEventsBuf:  make([]byte, 0, 1024*1024),
		tokenMetaBuf:      make([]byte, 0, 64*1024),
		globalQuotePrices: types.NewGlobalQuotePrices(),
	}

	// 初始化 Mq 消费者
	app.chainEventsKC = mq.NewKafkaConsumer(svc.Cfg.ChainEventsKcConfig, app)
	app.balanceEventsKC = mq.NewKafkaConsumer(svc.Cfg.BalanceEventsKcConfig, app)
	app.tokenMetaKC = mq.NewKafkaConsumer(svc.Cfg.TokenMetaKcConfig, app)
	app.lpReportRC = mq.NewRocketMQConsumer(svc.Cfg.LpReportRcConfig, app)

	// 初始化 Worker
	app.initWorkers(svc)

	// 初始化 Raft
	app.raft = raft.NewRaft(svc.Cfg.Raft, app)
	app.sg.Add(app.raft)

	return app
}

// initWorkers 初始化所有业务 Worker
func (app *App) initWorkers(svc *svc.ServiceContext) {
	// 获取 Nacos 服务，确保必选服务存在
	tokenMetaNacosService := app.mustGetRemoteService(svc, "token-meta")
	ingestNacosService := app.mustGetRemoteService(svc, "ingest")

	// TokenMeta 内部 Worker（处理内部任务）
	app.tokenMetaInternalWorker = taskworker.NewTokenMetaInternalWorker(tokenMetaNacosService, app)
	app.sg.Add(app.tokenMetaInternalWorker)

	// TokenMeta RPC Worker（通过 RPC 拉取 token 元数据）
	app.tokenMetaRpcWorker = taskworker.NewTokenMetaRpcWorker(
		svc.Cfg.Rpc.Endpoint,
		svc.Cfg.Rpc.TimeoutMs,
		app,
	)
	app.sg.Add(app.tokenMetaRpcWorker)

	// TopHolders Worker
	app.topHoldersWorker = taskworker.NewTopHoldersTaskWorker(ingestNacosService, app)
	app.sg.Add(app.topHoldersWorker)

	// HolderCount Worker
	app.holderCountWorker = taskworker.NewHolderCountTaskWorker(ingestNacosService, app)
	app.sg.Add(app.holderCountWorker)

	// Kafka Push Worker
	kafkaPushWorker, err := pushworker.NewKafkaPushWorker(svc.Cfg.KafkaProducerConfig, app)
	if err != nil {
		logger.Errorf("[Init] failed to create KafkaPushWorker: %v", err)
		panic(err)
	}
	app.kafkaPushWorker = kafkaPushWorker
	app.sg.Add(app.kafkaPushWorker)

	// 初始化多个 PoolWorker，用于并发处理事件
	app.poolWorkerCount = uint8(svc.Cfg.PoolWorkerConfig.WorkerCount)
	app.poolWorkers = make([]*pw.PoolWorker, 0, app.poolWorkerCount)
	for i := uint8(0); i < app.poolWorkerCount; i++ {
		worker := pw.NewPoolWorker(i, app, app, app)
		app.poolWorkers = append(app.poolWorkers, worker)
		app.sg.Add(worker)
	}

	// 初始化 RankingWorker
	app.rankingWorker = rankingworker.NewRankingWorker(app.poolWorkerCount, app)
	app.sg.Add(app.rankingWorker)
}

// mustGetRemoteService 从 Nacos 获取服务，若不存在则 panic
func (app *App) mustGetRemoteService(svc *svc.ServiceContext, id string) *nacos.NacosRemoteService {
	s := svc.NacosManager.GetRemoteServiceByID(id)
	if s == nil {
		logger.Errorf("[Init] %s service not found in Nacos", id)
		panic(fmt.Sprintf("missing %s service", id))
	}
	return s
}

//////////////////////////////
// 启动 / 停止
//////////////////////////////

func (app *App) Start() {
	logger.Infof("[App] Starting raft...")
	app.sg.Start()
}

func (app *App) Stop() {
	defer func() {
		if r := recover(); r != nil {
			logger.Errorf("[App] panic during Stop:  %v\n%s", r, debug.Stack())
		}
	}()

	logger.Infof("[App] Deregistering from Nacos...")
	if err := app.svc.NacosManager.DeregisterServer(); err != nil {
		logger.Warnf("[App] DeregisterNacos failed: %v", err)
	}

	logger.Infof("[App] Stopping kafka consumer...")
	app.chainEventsKC.Stop()
	app.balanceEventsKC.Stop()
	app.tokenMetaKC.Stop()
	app.lpReportRC.Stop()
	app.sg.Stop()
}

//////////////////////////////
// Raft 生命周期相关
//////////////////////////////

func (app *App) IsReady() bool {
	return app.raft != nil && app.raft.IsReady()
}

// GetLeaderIP 返回当前 Raft 集群中的 Leader IP 地址
func (app *App) GetLeaderIP() (s string, err error) {
	return app.raft.GetLeaderIP()
}

// AddOrRemoveNode 用于添加或移除 Raft 集群中的节点
func (app *App) AddOrRemoveNode(ip string, addNode bool) (err error) {
	return app.raft.AddOrRemoveNode(ip, addNode)
}

// OnBecameRaftLeader 处理 Raft 成为 Leader 时的回调
func (app *App) OnBecameRaftLeader(first bool) {
	app.holderCountWorker.Resume()
	app.topHoldersWorker.Resume()
	app.tokenMetaInternalWorker.Resume()
	app.tokenMetaRpcWorker.Resume()
	app.kafkaPushWorker.Resume()
	if first {
		// 检查 RankingWorker 是否准备好，准备好则启动恢复任务
		if app.rankingWorker.IsReady() {
			app.StartRecoveryAllTasks() // 如果已准备好，启动恢复任务
		} else {
			// 否则标记为待恢复状态
			app.needRecoveryAllTasks.Store(true)
		}
	}

	// 启动所有消费者和工作者
	app.chainEventsKC.Start()
	app.balanceEventsKC.Start()
	app.tokenMetaKC.Start()
	app.lpReportRC.Start()
}

// OnBecameRaftFollower 处理 Raft 变为 Follower 时的回调
func (app *App) OnBecameRaftFollower(first bool) {
	app.needRecoveryAllTasks.Store(false)
	app.holderCountWorker.Pause()
	app.topHoldersWorker.Pause()
	app.tokenMetaInternalWorker.Pause()
	app.tokenMetaRpcWorker.Pause()
	app.kafkaPushWorker.Pause()

	// 停止所有消费者和工作者
	app.chainEventsKC.Stop()
	app.balanceEventsKC.Stop()
	app.tokenMetaKC.Stop()
	app.lpReportRC.Stop()
}

// OnRaftReady Raft 准备好时的回调，注册 Nacos 服务
func (app *App) OnRaftReady() {
	app.registerNacosServer("Raft is ready")
}

// OnGlobalRankingComplete 排行榜计算完成时的回调，触发恢复任务和垃圾回收
func (app *App) OnGlobalRankingComplete() {
	app.registerNacosServer("Global ranking is complete")

	// 如果是 Leader 且有待触发的恢复任务，执行恢复任务
	if app.raft.IsLeader() && app.needRecoveryAllTasks.Load() {
		app.StartRecoveryAllTasks() // 启动恢复任务
	}

	// 触发垃圾回收
	app.triggerGC()
}

// registerNacosServer 注册 Nacos 服务
func (app *App) registerNacosServer(trigger string) {
	// 如果已注册，直接返回
	if app.svc.NacosManager.IsServerRegistered() {
		logger.Infof("[App] Server is already registered with Nacos")
		return
	}

	// 检查 Raft 和 RankingWorker 是否都准备好
	if !app.rankingWorker.IsReady() {
		logger.Warnf("[App] %s: RankingWorker not ready, skipping Nacos registration", trigger)
		return
	}
	if !app.raft.IsReady() {
		logger.Warnf("[App] %s: Raft not ready, skipping Nacos registration", trigger)
		return
	}

	// 执行 Nacos 注册
	logger.Infof("[App] Both Raft and RankingWorker are ready, attempting to register server with Nacos")
	if err := app.svc.NacosManager.RegisterServer(); err != nil {
		logger.Errorf("[App] RegisterNacos failed after %s trigger: %v", trigger, err)
		return
	}
	logger.Infof("[App] RegisterNacos succeeded after %s trigger", trigger)
}

// StartRecoveryAllTasks 启动所有 PoolWorker 的异步任务恢复
func (app *App) StartRecoveryAllTasks() bool {
	if !app.raft.IsReady() || !app.raft.IsLeader() {
		return false
	}

	// 标记恢复任务已触发
	app.needRecoveryAllTasks.Store(false)

	logger.Infof("[App] Starting recovery tasks")

	// 启动所有 pool worker 的恢复任务
	for _, worker := range app.poolWorkers {
		worker.EnqueueRecoverTasks(defs.RecoveryAll)
	}
	app.lastRecoveryTime = time.Now()
	return true
}

// startHotTokenRecoveryTasksIfNeeded 如果需要的话启动热令牌恢复任务
func (app *App) startHotTokenRecoveryTasksIfNeeded() {
	// 检查是否满足启动条件
	if !app.raft.IsReady() || !app.raft.IsLeader() {
		return
	}

	// 如果距离上次恢复超过 5 分钟，启动恢复任务
	if time.Since(app.lastRecoveryTime) > 5*time.Minute {
		logger.Infof("[App] Starting recovery tasks of type: %v", defs.RecoveryHotTokens)

		// 启动所有 pool worker 的热令牌恢复任务
		for _, worker := range app.poolWorkers {
			worker.EnqueueRecoverTasks(defs.RecoveryHotTokens)
		}

		// 更新最后恢复时间
		app.lastRecoveryTime = time.Now()
	}
}

// triggerGC 手动触发垃圾回收
func (app *App) triggerGC() {
	if time.Since(app.lastGCTime) >= app.svc.Cfg.GcConfig.Interval {
		startTime := time.Now()

		logger.Infof("[App] Triggering runtime.GC()")
		runtime.GC() // 手动触发垃圾回收

		gcDuration := time.Since(startTime)
		logger.Infof("[App] GC completed in: %v", gcDuration)

		if gcDuration > time.Second { // 如果GC耗时超过1秒，发出警告
			logger.Warnf("[App] GC cost longer than expected: %v", gcDuration)
		}

		app.lastGCTime = time.Now()
	}
}

//////////////////////////////
// Kafka/RockMq 消息处理
//////////////////////////////

func (app *App) HandleKafkaMsg(totalPartitions uint8, msg *kafka.Message) {
	defer func() {
		if r := recover(); r != nil {
			logger.Errorf("[HandleKafkaMsg] panic recovered: %v\n%s", r, debug.Stack())
			panic(r) // 遇到严重bug，依然终止程序
		}
	}()

	// 严重错误检查: 消息 topic 为空，直接 panic
	if msg.TopicPartition.Topic == nil {
		panic(fmt.Sprintf("[HandleKafkaMsg] nil topic in message, partition=%d, offset=%d",
			msg.TopicPartition.Partition, msg.TopicPartition.Offset))
	}

	topic := *msg.TopicPartition.Topic
	partition := uint8(msg.TopicPartition.Partition)
	offset := int64(msg.TopicPartition.Offset)

	logFields := fmt.Sprintf("topic=%q, totalPartitions=%d, partition=%d, offset=%d",
		topic, totalPartitions, partition, offset)

	// 空消息警告并跳过
	if len(msg.Value) == 0 {
		logger.Warnf("[HandleKafkaMsg] empty message: %s", logFields)
		return
	}

	// 获取 topic 对应的处理上下文: 内部 buf、消息类型和 KafkaConsumer
	neededCap := len(msg.Value) + 3
	buf, msgType, kc := app.topicHandlerInfo(topic, neededCap)
	if buf == nil || kc == nil {
		logger.Warnf("[HandleKafkaMsg] unknown topic: %s", logFields)
		return
	}

	// 构造 Raft 消息
	buf = append(buf, msgType)
	switch MsgType(msgType) {
	case MsgTypeChainEvents:
		if !app.checkKafkaPartitionValid(totalPartitions, partition) {
			panic(fmt.Sprintf("[HandleKafkaMsg] invalid partition: %s, poolWorkerCount=%d",
				logFields, app.poolWorkerCount))
		}
		buf = append(buf, totalPartitions, partition)
	case MsgTypeBalanceEvents:
		buf = append(buf, totalPartitions, partition)
	default:
	}
	buf = append(buf, msg.Value...)

	logger.Debugf("[HandleKafkaMsg] submitting to Raft: %s, payloadSize=%d", logFields, len(msg.Value))

	// 提交到 Raft
	if err := app.raft.Submit(buf); err != nil {
		logger.Errorf("[HandleKafkaMsg] raft submit failed: %s, err=%v", logFields, err)
	}

	// 提交 Kafka offset
	kc.CommitOffset(int32(partition), offset)
}

// topicHandlerInfo 返回处理 topic 对应的 buffer、类型及 KafkaConsumer
func (app *App) topicHandlerInfo(topic string, neededCap int) (buf []byte, msgType uint8, kc *mq.KafkaConsumer) {
	switch topic {
	case app.chainEventsKC.Config.Topic:
		msgType = byte(MsgTypeChainEvents)
		kc = app.chainEventsKC
		if cap(app.chainEventsBuf) < neededCap {
			app.chainEventsBuf = make([]byte, 0, neededCap)
		}
		buf = app.chainEventsBuf[:0]
		return

	case app.balanceEventsKC.Config.Topic:
		msgType = byte(MsgTypeBalanceEvents)
		kc = app.balanceEventsKC
		if cap(app.balanceEventsBuf) < neededCap {
			app.balanceEventsBuf = make([]byte, 0, neededCap)
		}
		buf = app.balanceEventsBuf[:0]
		return

	case app.tokenMetaKC.Config.Topic:
		msgType = byte(MsgTypeTokenMetaEvents)
		kc = app.tokenMetaKC
		if cap(app.tokenMetaBuf) < neededCap {
			app.tokenMetaBuf = make([]byte, 0, neededCap)
		}
		buf = app.tokenMetaBuf[:0]
		return

	default:
		return nil, 0, nil
	}
}

// HandleRocketMQMsg 处理 RocketMQ 消息，返回 nil 则自动提交
func (app *App) HandleRocketMQMsg(msgs ...*primitive.MessageExt) error {
	defer func() {
		if r := recover(); r != nil {
			logger.Errorf("[HandleRocketMQMsg] panic recovered: %v\n%s", r, debug.Stack())
			panic(r) // 遇到严重 bug，依然终止程序
		}
	}()

	if len(msgs) == 0 {
		logger.Warnf("[HandleRocketMQMsg] received empty message batch")
		return nil
	}

	// 预分配 slice，长度为消息数，避免频繁扩容
	lpEvents := make([]*pb.PoolLpReportEvent, 0, len(msgs))

	for _, msg := range msgs {
		var lpReport types.LpReportReq
		if err := utils.SafeJsonUnmarshal(msg.Body, &lpReport); err != nil {
			logger.Warnf("[HandleRocketMQMsg] failed to unmarshal message: msgId=%s, err=%v", msg.MsgId, err)
			continue
		}

		// 只处理 Solana 链
		if !strings.EqualFold(lpReport.ChainName, consts.ChainName) {
			logger.Infof("[HandleRocketMQMsg] skip non-Solana chain: chain=%s, msgId=%s", lpReport.ChainName, msg.MsgId)
			continue
		}

		poolAddress, err := types.TryPubkeyFromString(lpReport.ContractAddress)
		if err != nil {
			logger.Warnf("[HandleRocketMQMsg] invalid pool address: %s, msgId=%s, err=%v", lpReport.ContractAddress, msg.MsgId, err)
			continue
		}

		// 统一处理报告时间（秒）和上架时间（毫秒）
		reportTime := utils.ToSeconds(msg.BornTimestamp)
		if reportTime == 0 {
			reportTime = uint32(time.Now().Unix())
		}
		listingTimeMs := utils.ToMilliseconds(lpReport.ListingTime)

		// 下架事件 direction 标记为 0
		direction := lpReport.Direction
		if lpReport.UseStatus == 0 {
			direction = 0
		}

		lpEvents = append(lpEvents, &pb.PoolLpReportEvent{
			PoolAddress:   poolAddress[:],
			Leverage:      lpReport.Leverage,
			Direction:     direction,
			ReportTime:    reportTime,
			ListingTimeMs: listingTimeMs,
		})
	}

	logger.Debugf("[HandleRocketMQMsg] processed %d valid LP events from %d messages", len(lpEvents), len(msgs))
	if len(lpEvents) == 0 {
		return nil
	}

	// 提交 Raft
	eventBatch := &pb.PoolLpReportEvents{Events: lpEvents}
	if err := app.submitRaftMessage(MsgTypeLpReportEvents, eventBatch); err != nil {
		logger.Errorf("[HandleRocketMQMsg] failed to submit to Raft: %v", err)
	}

	return nil
}

//////////////////////////////
// Raft Snapshot
//////////////////////////////

func (app *App) OnPrepareSnapshot() ([][]types.Serializable, error) {
	snapshots := make([][]types.Serializable, app.poolWorkerCount)
	for i, worker := range app.poolWorkers {
		data, err := worker.PrepareSnapshot()
		if err != nil {
			return nil, fmt.Errorf("prepare snapshot failed for worker %d: %w", i, err)
		}
		snapshots[i] = data
	}
	return snapshots, nil
}

func (app *App) OnRecoverFromSnapshot(data []byte) error {
	if len(data) < 1 {
		return fmt.Errorf("snapshot data too short")
	}

	workerID := data[0]
	if workerID >= app.poolWorkerCount {
		return fmt.Errorf("invalid workerID: %d (total workers: %d)", workerID, app.poolWorkerCount)
	}

	return app.poolWorkers[workerID].OnRecoverFromSnapshot(data)
}

func (app *App) OnRecoverFromSnapshotDone() error {
	for _, worker := range app.poolWorkers {
		worker.OnRecoverFromSnapshotDone()
	}
	return nil
}

//////////////////////////////
// Raft 数据更新处理
//////////////////////////////

// OnRaftDataUpdated 处理接收到的 raft 数据
func (app *App) OnRaftDataUpdated(data []byte) (err error) {
	defer func() {
		if r := recover(); r != nil {
			logger.Errorf("[OnRaftDataUpdated] panic recovered: %v\n%s", r, debug.Stack())
			panic(r) // 遇到严重 bug，依然终止程序
		}
	}()

	if len(data) == 0 {
		panic("empty raft data")
	}

	msgType := MsgType(data[0])
	switch msgType {
	case MsgTypeChainEvents:
		return app.handleChainRaftData(data)
	case MsgTypeBalanceEvents:
		return app.handleBalanceRaftData(data)
	case MsgTypeTokenMetaEvents:
		return app.handleTokenMetaRaftData(data)
	case MsgTypeLpReportEvents:
		return app.handleLpReportRaftData(data)
	case MsgTypeTotalSupplyEvents:
		return app.handleTotalSupplyRaftData(data)
	case MsgTypeTopHoldersEvents:
		return app.handleTopHoldersRaftData(data)
	case MsgTypeHolderCountEvents:
		return app.handleHolderCountRaftData(data)
	case MsgTypePoolTickerPushDoneEvents:
		return app.handlePoolTickerPushDoneRaftData(data)
	default:
		logger.Warnf("[OnRaftDataUpdated] handling unknown message type=%s, payloadSize=%d", msgType, len(data)-1)
	}
	app.startHotTokenRecoveryTasksIfNeeded()
	return nil
}

func (app *App) handleChainRaftData(data []byte) error {
	msgType := MsgType(data[0])

	// 检查消息长度
	if len(data) < 3 {
		panic(fmt.Sprintf("[handleChainRaftData] raft data too short, type=%s, length=%d", msgType, len(data)))
	}

	totalPartitions := data[1]
	partition := data[2]
	payload := data[3:]

	// 校验分区合法性
	if !app.checkKafkaPartitionValid(totalPartitions, partition) {
		panic(fmt.Sprintf(
			"[handleChainRaftData] invalid partition: type=%s, totalPartitions=%d, partition=%d, poolWorkerCount=%d",
			msgType, totalPartitions, partition, app.poolWorkerCount,
		))
	}

	// 反序列化事件
	events := &pb.Events{}
	err := utils.SafeProtoUnmarshal(payload, events)
	if err != nil {
		logger.Errorf("[handleChainRaftData] unmarshal events failed: totalPartitions=%d, partition=%d, payloadSize=%d, err=%v",
			totalPartitions, partition, len(payload), err)
		return err
	}

	// 更新全局 QuotePrices
	app.globalQuotePrices.UpdateFromEvents(events)

	// 按 worker 分发事件
	workers := make([]uint8, 0, app.poolWorkerCount/totalPartitions)
	for i := partition; i < app.poolWorkerCount; i += totalPartitions {
		workers = append(workers, i)
	}

	eventsMap := eventadapter.RouteEventsToWorkers(events, workers, app.poolWorkerCount)
	for workerID, eventsForWorker := range eventsMap {
		logger.Debugf("[handleChainRaftData] partition=%d routes %d events to workerID=%d",
			partition, len(eventsForWorker.Events), workerID)
		app.poolWorkers[workerID].OnReceivedChainEvents(eventsForWorker)
	}

	return nil
}

// handleBalanceRaftData 处理来自 Kafka 的链上事件消息
func (app *App) handleBalanceRaftData(data []byte) error {
	msgType := MsgType(data[0])

	// 检查消息长度
	if len(data) < 3 {
		panic(fmt.Sprintf("[handleBalanceRaftData] raft data too short, type=%s, length=%d", msgType, len(data)))
	}

	events := &pb.Events{}
	err := utils.SafeProtoUnmarshal(data[3:], events)
	if err != nil {
		logger.Errorf("[handleBalanceRaftData] unmarshal events failed: %v", err)
		return err
	}

	logger.Debugf("[handleBalanceRaftData] received %d events, dispatching to %d poolWorkers",
		len(events.Events), len(app.poolWorkers))

	for _, worker := range app.poolWorkers {
		worker.OnReceivedBalanceEvents(events)
	}
	return nil
}

// handleTokenMetaRaftData 处理来自 Kafka 的链上元数据事件
func (app *App) handleTokenMetaRaftData(data []byte) error {
	events := &pb.MetaEvents{}
	err := utils.SafeProtoUnmarshal(data[1:], events)
	if err != nil {
		logger.Errorf("[handleTokenMetaRaftData] unmarshal events failed: %v", err)
		return err
	}

	if events.ChainId != consts.ChainID {
		logger.Infof("[handleTokenMetaRaftData] ignored events for non-%s chain, chainId=%d", consts.ChainName, events.ChainId)
		return nil
	}

	logger.Debugf("[handleTokenMetaRaftData] received %d meta events, dispatching to %d poolWorkers",
		len(events.Events), len(app.poolWorkers))

	for _, worker := range app.poolWorkers {
		worker.OnReceivedTokenMetaEvents(events)
	}
	return nil
}

// handleLpReportRaftData 处理来自 Kafka 的 LpReport 事件消息
func (app *App) handleLpReportRaftData(data []byte) error {
	events := &pb.PoolLpReportEvents{}
	err := utils.SafeProtoUnmarshal(data[1:], events)
	if err != nil {
		logger.Errorf("[handleLpReportRaftData] unmarshal events failed: %v", err)
		return err
	}

	logger.Debugf("[handleLpReportRaftData] received %d lp report events, dispatching to poolWorkers", len(events.Events))

	for _, worker := range app.poolWorkers {
		worker.OnReceivedLpReportEvents(events)
	}
	return nil
}

// handleTotalSupplyRaftData 处理 TotalSupply 事件
func (app *App) handleTotalSupplyRaftData(data []byte) error {
	events := &pb.TotalSupplyEvents{}
	if err := utils.SafeProtoUnmarshal(data[1:], events); err != nil {
		logger.Errorf("[handleTotalSupplyRaftData] unmarshal failed: %v", err)
		return err
	}

	for _, worker := range app.poolWorkers {
		worker.OnReceivedTotalSupplyEvents(events)
	}
	return nil
}

// handleTopHoldersRaftData 处理 TopHolders 事件
func (app *App) handleTopHoldersRaftData(data []byte) error {
	events := &pb.TopHoldersEvents{}
	if err := utils.SafeProtoUnmarshal(data[1:], events); err != nil {
		logger.Errorf("[handleTopHoldersRaftData] unmarshal failed: %v", err)
		return err
	}

	for _, worker := range app.poolWorkers {
		worker.OnReceivedTopHoldersEvents(events)
	}
	return nil
}

// handleHolderCountRaftData 处理 HolderCount 事件
func (app *App) handleHolderCountRaftData(data []byte) error {
	events := &pb.HolderCountEvents{}
	if err := utils.SafeProtoUnmarshal(data[1:], events); err != nil {
		logger.Errorf("[handleHolderCountRaftData] unmarshal failed: %v", err)
		return err
	}

	for _, worker := range app.poolWorkers {
		worker.OnReceivedHolderCountEvents(events)
	}
	return nil
}

// handlePoolTickerPushDoneRaftData 处理 PoolTickerPushDone 事件
func (app *App) handlePoolTickerPushDoneRaftData(data []byte) error {
	events := &pb.PoolTickerPushDoneEvents{}
	if err := utils.SafeProtoUnmarshal(data[1:], events); err != nil {
		logger.Errorf("[handlePoolTickerPushDoneRaftData] unmarshal failed: %v", err)
		return err
	}

	for _, worker := range app.poolWorkers {
		worker.OnReceivedPoolTickerPushDoneEvents(events)
	}
	return nil
}

//////////////////////////////
// pool worker 回调
//////////////////////////////

func (app *App) OnPoolSnapshotRecovered(workerID uint8, quotePrices *types.QuotePrices) {
	logger.Infof("Worker %d: PoolSnapshotRecovered, updating global quote prices", workerID)
	app.globalQuotePrices.UpdateFromSnapshot(quotePrices)
}

func (app *App) OnPoolRankingUpdate(r types.RankingResult[*pool.Pool]) {
	logger.Infof("Worker %d: PoolRankingUpdate, block %d, results count: %d", r.WorkerID, r.BlockNumber, len(r.Rankings))
	app.rankingWorker.Add(r)
}

func (app *App) OnPoolTokenTasks(workerID uint8, taskType types.TokenTaskType, tasks []types.TokenTask) {
	logger.Infof("Worker %d: PoolTokenTasks, type: %v, task count: %d", workerID, taskType, len(tasks))

	switch taskType {
	case types.TokenTaskMeta:
		app.tokenMetaRpcWorker.Add(tasks)
	case types.TokenTaskTopHolders:
		app.topHoldersWorker.Add(tasks)
	case types.TokenTaskHolderCount:
		app.holderCountWorker.Add(tasks)
	default:
		logger.Errorf("Worker %d: unknown TokenTaskType %v", workerID, taskType)
	}
}

func (app *App) OnPoolPushTasks(workerID uint8, tasks []*types.PushTask) {
	logger.Infof("Worker %d: PoolPushTasks, task count: %d", workerID, len(tasks))
	app.kafkaPushWorker.Add(tasks)
}

//////////////////////////////
// Task / Push Worker 回调
//////////////////////////////

// OnTokenMetaInternalDone 处理内部接口 TokenMeta 数据请求完成的回调
func (app *App) OnTokenMetaInternalDone(results []taskworker.TaskResult[*pb.SupplyInfo]) {
	updateTime := time.Now().Unix()

	events := make([]*pb.MetaEvent, 0, len(results))
	fallbackTasks := make([]types.TokenTask, 0, len(results))

	for _, r := range results {
		if r.Err != nil {
			// 失败的任务回退到 RPC worker 队列
			fallbackTasks = append(fallbackTasks, r.Item)
			continue
		}

		// 构造成功事件
		events = append(events, &pb.MetaEvent{
			Event: &pb.MetaEvent_Supply{
				Supply: &pb.SupplyEvent{
					Type:              pb.MetaEventType_SUPPLY,
					TokenAddress:      r.Item.Token[:],
					Decimals:          r.Data.Decimals,
					TotalSupply:       r.Data.TotalSupply,
					MaxSupply:         r.Data.MaxSupply,
					CirculatingSupply: r.Data.CirculatingSupply,
				},
			},
			UpdateTime: updateTime,
		})
	}

	// 处理失败任务
	if len(fallbackTasks) > 0 {
		logger.Warnf("[OnTokenMetaInternalDone] %d tasks failed, re-adding to RPC worker", len(fallbackTasks))
		app.tokenMetaRpcWorker.Add(fallbackTasks)
	}

	// 没有成功事件则直接返回
	if len(events) == 0 {
		logger.Debugf("[OnTokenMetaInternalDone] no successful token meta events to submit")
		return
	}

	// 提交 Raft 消息
	eventBatch := &pb.MetaEvents{Events: events}
	if err := app.submitRaftMessage(MsgTypeTokenMetaEvents, eventBatch); err != nil {
		logger.Errorf("[OnTokenMetaInternalDone] failed to submit %d token meta events to Raft: %v",
			len(events), err)
	} else {
		logger.Debugf("[OnTokenMetaInternalDone] submitted %d token meta events to Raft", len(events))
	}
}

// OnTokenMetaRpcDone 处理通过 RPC 获取的 TotalSupply 完成的回调
func (app *App) OnTokenMetaRpcDone(results []taskworker.TaskResult[*pb.TotalSupplyEvent]) {
	events := make([]*pb.TotalSupplyEvent, 0, len(results))

	for _, r := range results {
		if r.Err != nil {
			continue
		}
		events = append(events, r.Data)
	}

	if len(events) == 0 {
		logger.Debugf("[OnTokenMetaRpcDone] no valid total supply events to submit")
		return
	}

	// 提交 Raft 消息
	eventBatch := &pb.TotalSupplyEvents{
		Events:     events,
		UpdateTime: time.Now().Unix(),
	}
	if err := app.submitRaftMessage(MsgTypeTotalSupplyEvents, eventBatch); err != nil {
		logger.Errorf("[OnTokenMetaRpcDone] failed to submit %d total supply events to Raft: %v",
			len(events), err)
	} else {
		logger.Debugf("[OnTokenMetaRpcDone] submitted %d total supply events to Raft", len(events))
	}
}

// OnTopHoldersDone 处理 TopHolders 请求完成的回调
func (app *App) OnTopHoldersDone(results []taskworker.TaskResult[[]*pb.Balance]) {
	updateTime := time.Now().Unix()
	events := make([]*pb.TopHolders, 0, len(results))

	for _, r := range results {
		if r.Err != nil {
			continue
		}

		topAccounts := &pb.TopHolders{
			TokenAddress: r.Item.Token[:],
			Holders:      r.Data,
			UpdateTime:   updateTime,
		}
		events = append(events, topAccounts)
	}

	if len(events) == 0 {
		logger.Debugf("[OnTopHoldersDone] no top holders events to submit")
		return
	}

	// 提交 Raft
	eventBatch := &pb.TopHoldersEvents{Events: events}
	if err := app.submitRaftMessage(MsgTypeTopHoldersEvents, eventBatch); err != nil {
		logger.Errorf("[OnTopHoldersDone] failed to submit %d top holders events to Raft: %v", len(events), err)
	} else {
		logger.Debugf("[OnTopHoldersDone] submitted %d top holders events to Raft", len(events))
	}
}

// OnHolderCountDone 处理 HolderCount 请求完成的回调
func (app *App) OnHolderCountDone(results []taskworker.TaskResult[uint64]) {
	updateTime := time.Now().Unix()
	events := make([]*pb.HolderCount, 0, len(results))

	for _, r := range results {
		if r.Err != nil {
			continue
		}

		// 即使 holder_count 为 0，也提交
		events = append(events, &pb.HolderCount{
			TokenAddress: r.Item.Token[:],
			HolderCount:  r.Data,
			UpdateTime:   updateTime,
		})
	}

	if len(events) == 0 {
		logger.Debugf("[OnHolderCountDone] no valid holder count events to submit")
		return
	}

	// 提交 Raft 消息
	eventBatch := &pb.HolderCountEvents{Events: events}
	if err := app.submitRaftMessage(MsgTypeHolderCountEvents, eventBatch); err != nil {
		logger.Errorf("[OnHolderCountDone] failed to submit %d holder count events to Raft: %v",
			len(events), err)
	} else {
		logger.Debugf("[OnHolderCountDone] submitted %d holder count events to Raft", len(events))
	}
}

// OnPoolTickerPushed 处理 Pool Ticker 推送完成的回调
func (app *App) OnPoolTickerPushed(results []pushworker.MsgID) {
	if len(results) == 0 {
		logger.Debugf("[OnPoolTickerPushed] no pool ticker push done events to submit")
		return
	}

	events := make([]*pb.PoolTickerPushDoneEvent, 0, len(results))
	for _, r := range results {
		events = append(events, &pb.PoolTickerPushDoneEvent{
			PoolAddress: r.Pool[:],
			BlockNumber: uint64(r.BlockNumber),
		})
	}

	// 提交 Raft 消息
	eventBatch := &pb.PoolTickerPushDoneEvents{Events: events}
	if err := app.submitRaftMessage(MsgTypePoolTickerPushDoneEvents, eventBatch); err != nil {
		logger.Errorf("[OnPoolTickerPushed] failed to submit %d pool ticker push done events to Raft: %v",
			len(events), err)
	} else {
		logger.Debugf("[OnPoolTickerPushed] submitted %d pool ticker push done events to Raft", len(events))
	}
}

// submitRaftMessage 封装 Raft 提交逻辑，带日志
func (app *App) submitRaftMessage(msgType MsgType, event proto.Message) error {
	// 1. 预分配 buffer
	buf := make([]byte, 0, 1+proto.Size(event)+16)
	buf = append(buf, uint8(msgType))

	// 2. 序列化到 buf
	data, err := utils.SafeProtoMarshal(buf, event)
	if err != nil {
		logger.Errorf("[submitRaftMessage] proto marshal failed, msgType=%s: %v", msgType, err)
		return err
	}

	// 3. 提交到 Raft 集群
	if err := app.raft.Submit(data); err != nil {
		logger.Errorf("[submitRaftMessage] raft submit failed, msgType=%s: %v", msgType, err)
		return err
	}

	logger.Debugf("[submitRaftMessage] successfully submitted %d bytes to Raft, msgType=%s", len(data), msgType)
	return nil
}

//////////////////////////////
// 辅助函数
//////////////////////////////

// checkKafkaPartitionValid 校验 Kafka 分区与 poolWorker 数量是否匹配
func (app *App) checkKafkaPartitionValid(totalPartitions uint8, partition uint8) bool {
	if partition >= totalPartitions {
		return false
	}
	if app.poolWorkerCount < totalPartitions {
		return false
	}
	// 要求 poolWorkerCount 是 totalPartitions 的倍数
	return app.poolWorkerCount%totalPartitions == 0
}

func (m MsgType) String() string {
	switch m {
	case MsgTypeChainEvents:
		return "ChainEvents"
	case MsgTypeBalanceEvents:
		return "BalanceEvents"
	case MsgTypeTokenMetaEvents:
		return "TokenMetaEvents"
	case MsgTypeLpReportEvents:
		return "LpReportEvents"
	case MsgTypeTotalSupplyEvents:
		return "TotalSupplyEvents"
	case MsgTypeTopHoldersEvents:
		return "TopHoldersEvents"
	case MsgTypeHolderCountEvents:
		return "HolderCountEvents"
	case MsgTypePoolTickerPushDoneEvents:
		return "PoolTickerPushDoneEvents"
	default:
		return fmt.Sprintf("Unknown(%d)", uint8(m))
	}
}
