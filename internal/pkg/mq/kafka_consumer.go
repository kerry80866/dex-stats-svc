package mq

import (
	"dex-stats-sol/internal/pkg/logger"
	"dex-stats-sol/internal/pkg/utils"
	"errors"
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"os"
	"runtime/debug"
	"strings"
	"sync"
	"time"
)

// KafkaConsumerConf 定义 Kafka 消费者配置（适合单 topic 场景）
// 所有时间相关参数单位均为毫秒
type KafkaConsumerConf struct {
	Brokers               []string `json:"brokers" yaml:"brokers"`                                   // Kafka 集群 broker 地址列表
	Topic                 string   `json:"topic" yaml:"topic"`                                       // 订阅的 topic 名称
	GroupID               string   `json:"group_id" yaml:"group_id"`                                 // 消费者组 ID，同组实现高可用
	SessionTimeoutMs      int      `json:"session_timeout_ms" yaml:"session_timeout_ms"`             // 会话超时时间（ms）
	HeartbeatIntervalMs   int      `json:"heartbeat_interval_ms" yaml:"heartbeat_interval_ms"`       // 心跳间隔（ms）
	ReadTimeoutMs         int      `json:"read_timeout_ms" yaml:"read_timeout_ms"`                   // 拉取消息超时时间（ms）
	ReconnectBackoffMs    int      `json:"reconnect_backoff_ms" yaml:"reconnect_backoff_ms"`         // 第一次重连延迟
	ReconnectBackoffMaxMs int      `json:"reconnect_backoff_max_ms" yaml:"reconnect_backoff_max_ms"` // 最大重连间隔
	RetryBackoffMs        int      `json:"retry_backoff_ms" yaml:"retry_backoff_ms"`                 // 拉取失败重试间隔
}

type KafkaHandler interface {
	HandleKafkaMsg(totalPartitions uint8, msg *kafka.Message)
}

type KafkaConsumer struct {
	Config          *KafkaConsumerConf
	consumer        *kafka.Consumer
	kafkaConfig     *kafka.ConfigMap
	handler         KafkaHandler
	state           ConsumerState
	mu              sync.RWMutex
	totalPartitions uint8
}

// NewKafkaConsumer 创建并初始化 KafkaConsumer 实例
func NewKafkaConsumer(config *KafkaConsumerConf, handler KafkaHandler) *KafkaConsumer {
	kafkaConfig := &kafka.ConfigMap{
		"bootstrap.servers":        strings.Join(config.Brokers, ","),
		"group.id":                 config.GroupID,
		"session.timeout.ms":       config.SessionTimeoutMs,
		"heartbeat.interval.ms":    config.HeartbeatIntervalMs,
		"auto.offset.reset":        "latest", // 默认只消费新数据
		"enable.auto.commit":       false,    // 只支持手动提交
		"client.id":                getClientID(config.GroupID),
		"reconnect.backoff.ms":     config.ReconnectBackoffMs,
		"reconnect.backoff.max.ms": config.ReconnectBackoffMaxMs,
		"retry.backoff.ms":         config.RetryBackoffMs,
	}

	logger.Infof("[KafkaConsumer] New: brokers=%v, group=%s, topic=%s",
		config.Brokers, config.GroupID, config.Topic)

	return &KafkaConsumer{
		Config:      config,
		kafkaConfig: kafkaConfig,
		handler:     handler,
		state:       StateStopped,
	}
}

func (kc *KafkaConsumer) Start() {
	// 快速预检查，避免加写锁
	kc.mu.RLock()
	state := kc.state
	kc.mu.RUnlock()

	if state != StateStopped {
		if state != StateRunning {
			logger.Warnf("[KafkaConsumer] %s already in state: %s", kc.Config.Topic, state)
		}
		return
	}

	// 加写锁，开始启动流程
	kc.mu.Lock()
	defer kc.mu.Unlock()

	if kc.state != StateStopped {
		return
	}

	kc.setStateUnsafe(StateStarting)

	consumer, err := kafka.NewConsumer(kc.kafkaConfig)
	if err != nil {
		logger.Errorf("[KafkaConsumer] %s create error: %v", kc.Config.Topic, err)
		kc.setStateUnsafe(StateStopped)
		return
	}
	kc.consumer = consumer
	logger.Infof("[KafkaConsumer] %s created", kc.Config.Topic)

	meta, err := kc.consumer.GetMetadata(&kc.Config.Topic, false, 2000)
	if err != nil {
		logger.Errorf("[KafkaConsumer] %s get metadata failed: %v", kc.Config.Topic, err)
		kc.closeConsumerUnsafe()
		kc.setStateUnsafe(StateStopped)
		return
	}

	topicMeta, ok := meta.Topics[kc.Config.Topic]
	if !ok || len(topicMeta.Partitions) == 0 {
		logger.Errorf("[KafkaConsumer] %s has no partitions, check if topic exists", kc.Config.Topic)
		kc.closeConsumerUnsafe()
		kc.setStateUnsafe(StateStopped)
		return
	}

	kc.totalPartitions = uint8(len(topicMeta.Partitions))
	logger.Infof("[KafkaConsumer] %s has %d partitions", kc.Config.Topic, kc.totalPartitions)

	if err := kc.consumer.SubscribeTopics([]string{kc.Config.Topic}, nil); err != nil {
		logger.Errorf("[KafkaConsumer] %s subscribe error: %v", kc.Config.GroupID, err)
		kc.closeConsumerUnsafe()
		kc.setStateUnsafe(StateStopped)
		return
	}

	kc.setStateUnsafe(StateRunning)
	logger.Infof("[KafkaConsumer] %s subscribed, run loop started", kc.Config.Topic)
	go kc.runLoop()
}

func (kc *KafkaConsumer) Stop() {
	// 先用读锁预判断,减少锁冲突
	kc.mu.RLock()
	if kc.state == StateStopped || kc.state == StateStopping {
		kc.mu.RUnlock()
		logger.Infof("[KafkaConsumer] %s already stopped or stopping, current state: %s", kc.Config.Topic, kc.state)
		return
	}
	kc.mu.RUnlock()

	// 真正处理
	kc.mu.Lock()
	defer kc.mu.Unlock()

	// 在处理停止操作前，打印当前状态
	logger.Infof("[KafkaConsumer] %s attempting to stop, current state: %s", kc.Config.Topic, kc.state)

	if kc.state != StateStopped && kc.state != StateStopping {
		kc.setStateUnsafe(StateStopping)
		logger.Infof("[KafkaConsumer] %s changed state to stopping", kc.Config.Topic)
	}

	// 继续执行其它停止操作
	logger.Infof("[KafkaConsumer] %s is stopping consumer", kc.Config.Topic)
}

func (kc *KafkaConsumer) runLoop() {
	var lastLogTime time.Time

	for {
		consumer := kc.tryStopIfNeeded()
		if consumer == nil {
			logger.Infof("[KafkaConsumer] %s consumer stopped, exiting run loop", kc.Config.Topic)
			return
		}

		msg, err := consumer.ReadMessage(time.Duration(kc.Config.ReadTimeoutMs) * time.Millisecond)

		if kc.tryStopIfNeeded() == nil {
			logger.Infof("[KafkaConsumer] %s consumer stopping detected, exiting run loop", kc.Config.Topic)
			return
		}

		if err != nil {
			var kafkaErr kafka.Error
			if errors.As(err, &kafkaErr) && kafkaErr.Code() == kafka.ErrTimedOut {
				// 每隔 1 分钟打一次 debug 日志，避免刷屏
				if time.Since(lastLogTime) > time.Minute {
					logger.Debugf("[KafkaConsumer] %s poll timeout, still waiting...", kc.Config.Topic)
					lastLogTime = time.Now()
				}
				continue
			}

			logger.Errorf("[KafkaConsumer] %s read error: %v", kc.Config.Topic, err)
			continue
		}

		kc.handler.HandleKafkaMsg(kc.totalPartitions, msg)
	}
}

func (kc *KafkaConsumer) tryStopIfNeeded() *kafka.Consumer {
	kc.mu.RLock()
	state := kc.state
	consumer := kc.consumer
	kc.mu.RUnlock()

	if state == StateStopped {
		return nil
	}
	if state != StateStopping {
		return consumer
	}

	// 状态为 Stopping，尝试停止
	kc.mu.Lock()
	defer kc.mu.Unlock()

	// 二次确认状态
	if kc.state == StateStopping {
		kc.closeConsumerUnsafe()
		kc.setStateUnsafe(StateStopped)
		return nil
	}
	return kc.consumer
}

func (kc *KafkaConsumer) closeConsumerUnsafe() {
	if kc.consumer == nil {
		return
	}
	if !kc.consumer.IsClosed() {
		if err := kc.consumer.Close(); err != nil {
			logger.Errorf("[KafkaConsumer] %s failed to close consumer: %v", kc.Config.Topic, err)
		} else {
			logger.Infof("[KafkaConsumer] %s consumer closed", kc.Config.Topic)
		}
	}
	kc.consumer = nil
}

func (kc *KafkaConsumer) setStateUnsafe(state ConsumerState) {
	if kc.state == state {
		return
	}
	logger.Infof("[KafkaConsumer] %s State changed: %s → %s", kc.Config.Topic, kc.state, state)
	kc.state = state
}

func (kc *KafkaConsumer) IsActive() bool {
	kc.mu.RLock()
	defer kc.mu.RUnlock()
	return kc.state == StateStarting || kc.state == StateRunning
}

func (kc *KafkaConsumer) CommitOffset(partition int32, offset int64) {
	nextOffset := offset + 1
	if nextOffset <= 0 {
		logger.Errorf("[KafkaConsumer] %s commit offset overflow detected: original offset=%d", kc.Config.Topic, offset)
		return
	}

	defer func() {
		if r := recover(); r != nil {
			logger.Errorf("[KafkaConsumer] %s commit offset panic: partition=%d offset=%d panic=%v\n%s",
				kc.Config.Topic, partition, nextOffset, r, debug.Stack())
		}
	}()

	kc.mu.RLock()
	defer kc.mu.RUnlock()

	if kc.state != StateRunning {
		logger.Warnf("[KafkaConsumer] %s commit offset aborted: partition=%d offset=%d, invalid state: %s",
			kc.Config.Topic, partition, nextOffset, kc.state)
		return
	}
	if kc.consumer == nil {
		logger.Warnf("[KafkaConsumer] %s consumer is nil: partition=%d offset=%d",
			kc.Config.Topic, partition, nextOffset)
		return
	}
	if kc.consumer.IsClosed() {
		logger.Warnf("[KafkaConsumer] %s consumer is closed: partition=%d offset=%d",
			kc.Config.Topic, partition, nextOffset)
		return
	}

	tp := kafka.TopicPartition{
		Topic:     &kc.Config.Topic,
		Partition: partition,
		Offset:    kafka.Offset(nextOffset), // Kafka 提交的是下一条 offset
	}

	_, err := kc.consumer.CommitOffsets([]kafka.TopicPartition{tp})
	if err != nil {
		logger.Errorf("[KafkaConsumer] %s commit offset failed: partition=%d offset=%d err=%v",
			kc.Config.Topic, partition, nextOffset, err)
		return
	}

	logger.Infof("[KafkaConsumer] %s commit offset success: partition=%d offset=%d",
		kc.Config.Topic, partition, nextOffset)
}

func getClientID(service string) string {
	hostname, _ := os.Hostname()
	localIP, _ := utils.GetLocalIP()
	if localIP == "" {
		localIP = "unknown"
	}
	return fmt.Sprintf("%s-%s-%s", service, hostname, localIP)
}
