package pushworker

import (
	"context"
	"dex-stats-sol/internal/pkg/logger"
	"dex-stats-sol/internal/pkg/mq"
	"dex-stats-sol/internal/pkg/utils"
	"dex-stats-sol/internal/stats/types"
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"sort"
	"sync/atomic"
	"time"
)

const (
	singleBufSize     = 768              // 单条消息 buffer 大小
	bufPoolPreAlloc   = 128              // BufPool 启动时预分配的 buffer 数量
	sendBatchSize     = 512              // BufPool 最大容量，也是每次发送 Kafka 消息的条数上限
	inputChanSize     = 128              // inputChan 缓冲大小
	initTaskCap       = 1024             // tasks map 初始容量
	taskLimit         = 8192             // tasks map 最大保留条数
	taskMapResetLimit = taskLimit * 2    // 超过该长度就重新分配 map
	kafkaBatchTimeout = 10 * time.Second // Kafka 批量发送超时时间
)

type MsgID struct {
	Pool        types.Pubkey
	BlockNumber uint32
}

type KafkaPushListener interface {
	OnPoolTickerPushed([]MsgID)
}

type KafkaPushWorker struct {
	producer        *kafka.Producer
	inputChan       chan []*types.PushTask // 批量消息 channel
	ctx             context.Context
	cancel          context.CancelFunc
	listener        KafkaPushListener
	tasks           map[types.Pubkey]*types.PushTask
	bufPool         *BufPool // 单线程 buffer 池
	isPaused        atomic.Bool
	topic           string
	partitions      int
	lastSendLogTime atomic.Int64 // 阻塞日志限流时间（纳秒）
}

func NewKafkaPushWorker(config *mq.KafkaProducerConf, listener KafkaPushListener) (*KafkaPushWorker, error) {
	if len(config.Topics) != 1 {
		return nil, fmt.Errorf("kafka config must have exactly 1 topic, got %d", len(config.Topics))
	}

	producer, err := mq.NewKafkaProducer(config)
	if err != nil {
		logger.Errorf("[KafkaPushWorker] failed to create producer for topic %v: %v", config.Topics, err)
		return nil, err
	}

	ctx, cancel := context.WithCancel(context.Background())
	worker := &KafkaPushWorker{
		producer:   producer,
		inputChan:  make(chan []*types.PushTask, inputChanSize),
		ctx:        ctx,
		cancel:     cancel,
		listener:   listener,
		tasks:      make(map[types.Pubkey]*types.PushTask, initTaskCap),
		topic:      config.Topics[0].Topic,
		partitions: config.Topics[0].Partitions,
		bufPool:    NewBufPool(bufPoolPreAlloc, sendBatchSize, singleBufSize),
	}
	worker.isPaused.Store(true)
	return worker, nil
}

// Start 启动处理循环
func (w *KafkaPushWorker) Start() {
	w.loop()
}

// Stop 停止 worker
func (w *KafkaPushWorker) Stop() {
	w.isPaused.Store(true)
	w.cancel()
}

// Resume 恢复
func (w *KafkaPushWorker) Resume() {
	w.isPaused.Store(false)
}

// Pause 暂停
func (w *KafkaPushWorker) Pause() {
	w.isPaused.Store(true)
}

// Add 阻塞批量添加任务，队列满时等待，不丢弃，限频打印
func (w *KafkaPushWorker) Add(list []*types.PushTask) {
	for {
		if w.isPaused.Load() {
			return
		}

		select {
		case <-w.ctx.Done():
			return

		case w.inputChan <- list:
			return

		default:
			// 队列满时限频打印
			if utils.ThrottleLog(&w.lastSendLogTime, 3*time.Second) {
				logger.Warnf("[KafkaPushWorker] inputChan full (%d), waiting to add task batch", len(w.inputChan))
			}
			time.Sleep(30 * time.Millisecond)
		}
	}
}

func (w *KafkaPushWorker) loop() {
	for {
		select {
		case <-w.ctx.Done():
			return

		case list := <-w.inputChan:
			// 初始化 batch，估算一个合理的容量
			batch := make([]*types.PushTask, 0, min(len(w.inputChan), sendBatchSize))

			// 收集第一条消息
			for _, task := range list {
				if task != nil {
					batch = append(batch, task)
				}
			}

			// 收集其余消息
			batch = w.collectBatch(batch)

			for len(batch) > 0 || len(w.tasks) > 0 {
				if w.isPaused.Load() {
					// 在暂停时清理任务
					utils.ClearOrResetMap(&w.tasks, taskMapResetLimit, initTaskCap)
					break
				}

				// 处理任务
				w.handleBatch(batch)

				// 清空 batch 以准备接收新消息
				utils.ClearSlice(&batch)

				// 如果任务数没超过限制，则继续收集
				if len(w.tasks) <= taskLimit {
					batch = w.collectBatch(batch)
				}
			}
		}
	}
}

// collectBatch 尝试非阻塞地收集 inputChan 中的任务
func (w *KafkaPushWorker) collectBatch(batch []*types.PushTask) []*types.PushTask {
	for {
		select {
		case list := <-w.inputChan:
			for _, task := range list {
				if task != nil {
					batch = append(batch, task)
				}
			}
		default:
			return batch
		}
	}
}

func (w *KafkaPushWorker) handleBatch(batch []*types.PushTask) {
	// ------------------------
	// 1. 更新任务 map
	// ------------------------
	for _, task := range batch {
		if old := w.tasks[task.Pool]; old != nil {
			if task.BlockNumber >= old.BlockNumber {
				w.tasks[task.Pool] = task
			}
		} else {
			w.tasks[task.Pool] = task
		}
	}

	// ------------------------
	// 2. 小于 sendBatchSize 条，直接全部发送
	// ------------------------
	if len(w.tasks) <= sendBatchSize {
		toSend := make([]*kafka.Message, 0, len(w.tasks))
		for _, t := range w.tasks {
			if msg := w.toKafkaMessage(t); msg != nil {
				toSend = append(toSend, msg)
			} else {
				delete(w.tasks, t.Pool)
			}
		}
		w.dispatchBatch(toSend)
		return
	}

	// ------------------------
	// 3. 超过 sendBatchSize，按 blockNumber + poolHash 排序，截取前 SendBatch 条发送
	// ------------------------
	allTasks := make([]*types.PushTask, 0, len(w.tasks))
	for _, t := range w.tasks {
		allTasks = append(allTasks, t)
	}

	sort.Slice(allTasks, func(i, j int) bool {
		if allTasks[i].BlockNumber == allTasks[j].BlockNumber {
			return allTasks[i].PoolHash < allTasks[j].PoolHash
		}
		return allTasks[i].BlockNumber < allTasks[j].BlockNumber
	})

	n := min(len(allTasks), sendBatchSize)
	toSend := make([]*kafka.Message, 0, n)
	for i := 0; i < n; i++ {
		if msg := w.toKafkaMessage(allTasks[i]); msg != nil {
			toSend = append(toSend, msg)
		} else {
			delete(w.tasks, allTasks[i].Pool)
		}
	}

	w.dispatchBatch(toSend)
}

// dispatchBatch 发送消息并更新任务 map，回调 listener
func (w *KafkaPushWorker) dispatchBatch(messages []*kafka.Message) {
	if w.isPaused.Load() {
		return
	}

	results := mq.SendKafkaMessagesBatch(w.ctx, w.producer, messages, kafkaBatchTimeout)

	successList := make([]MsgID, 0, len(messages))
	for _, item := range results {
		if item.Completed {
			w.bufPool.Put(item.Msg.Value)
		}

		if !item.Success {
			continue
		}

		msgID, ok := item.Msg.Opaque.(MsgID)
		if !ok {
			continue
		}

		if task, exists := w.tasks[msgID.Pool]; exists {
			if msgID.BlockNumber >= task.BlockNumber {
				delete(w.tasks, msgID.Pool)
			}
			successList = append(successList, msgID)
		}
	}

	// 回调 listener，直接在原线程调用
	if w.listener != nil && len(successList) > 0 && !w.isPaused.Load() {
		w.listener.OnPoolTickerPushed(successList)
	}
}

func (w *KafkaPushWorker) toKafkaMessage(t *types.PushTask) *kafka.Message {
	data, err := utils.SafeProtoMarshal(w.bufPool.Get(), t.Ticker)
	if err != nil {
		logger.Warnf("[KafkaPushWorker] marshal ticker failed for Pool=%s, BlockNumber=%d: %v", t.Pool, t.BlockNumber, err)
		return nil
	}

	partition := kafka.PartitionAny
	if w.partitions > 1 {
		partition = int32(t.PoolHash % uint64(w.partitions))
	}

	return &kafka.Message{
		TopicPartition: kafka.TopicPartition{
			Topic:     &w.topic,
			Partition: partition,
		},
		Value: data,
		Opaque: MsgID{
			Pool:        t.Pool,
			BlockNumber: t.BlockNumber,
		},
	}
}
