package mq

import (
	"context"
	"dex-stats-sol/internal/pkg/logger"
	"github.com/apache/rocketmq-client-go/v2"
	"github.com/apache/rocketmq-client-go/v2/consumer"
	"github.com/apache/rocketmq-client-go/v2/primitive"
	"github.com/apache/rocketmq-client-go/v2/rlog"
	"runtime/debug"
	"sync"
	"time"
)

type ConsumeFromWhereType string

const (
	ConsumeFromFirstOffset ConsumeFromWhereType = "first"
	ConsumeFromLastOffset  ConsumeFromWhereType = "last"
)

type RocketMQConsumerConf struct {
	Servers          []string             `json:"servers" yaml:"servers"`                       // NS 地址
	Group            string               `json:"group" yaml:"group"`                           // 消费者组
	Topic            string               `json:"topic" yaml:"topic"`                           // Topic
	ConsumeFromWhere ConsumeFromWhereType `json:"consume_from_where" yaml:"consume_from_where"` // 首次消费位置
	Tag              string               `json:"tag" yaml:"tag"`                               // 消费消息的标签
}

type RocketMQHandler interface {
	HandleRocketMQMsg(msgs ...*primitive.MessageExt) error
}

type RocketMQConsumer struct {
	Config   *RocketMQConsumerConf
	consumer rocketmq.PushConsumer
	options  []consumer.Option
	selector consumer.MessageSelector
	handler  func(context.Context, ...*primitive.MessageExt) (consumer.ConsumeResult, error)
	state    ConsumerState
	mu       sync.RWMutex
}

// NewRocketMQConsumer 创建并初始化 RocketMQConsumer 实例
func NewRocketMQConsumer(config *RocketMQConsumerConf, handler RocketMQHandler) *RocketMQConsumer {
	rlog.SetLogLevel("error") // 设置日志级别

	consumeFrom := consumer.ConsumeFromFirstOffset
	if config.ConsumeFromWhere == ConsumeFromLastOffset {
		consumeFrom = consumer.ConsumeFromLastOffset
	}

	options := []consumer.Option{
		consumer.WithNameServer(config.Servers),
		consumer.WithGroupName(config.Group),            // 同一个 Group 内的消费者会共享消息进度
		consumer.WithConsumerModel(consumer.Clustering), // 表示集群模式（消息只会被同组的一个消费者消费）
		consumer.WithConsumerOrder(true),                // 严格顺序（单队列内消息会保证顺序，但会降低吞吐）
		consumer.WithAutoCommit(true),                   //  SDK 自动定时提交（一般推荐）
		consumer.WithConsumeFromWhere(consumeFrom),
	}

	selector := consumer.MessageSelector{}
	if len(config.Tag) > 0 {
		selector = consumer.MessageSelector{
			Type:       consumer.TAG,
			Expression: config.Tag,
		}
	}

	topic := config.Topic
	fn := func(ctx context.Context, msgs ...*primitive.MessageExt) (consumer.ConsumeResult, error) {
		if len(msgs) == 0 {
			logger.Warnf("[RocketMQConsumer] %s received empty message batch", topic)
			return consumer.ConsumeSuccess, nil
		}

		firstID := msgs[0].MsgId
		lastID := msgs[len(msgs)-1].MsgId
		batchSize := len(msgs)

		start := time.Now()
		if err := handler.HandleRocketMQMsg(msgs...); err != nil {
			logger.Errorf("[RocketMQConsumer] %s batch handling failed: %v, batchSize=%d, firstMsgID=%s, lastMsgID=%s, will retry later",
				topic, err, batchSize, firstID, lastID)
			return consumer.ConsumeRetryLater, nil
		}
		elapsed := time.Since(start)

		logger.Infof("[RocketMQConsumer] %s batch handled successfully, batchSize=%d, firstMsgID=%s, lastMsgID=%s, cost=%s",
			topic, batchSize, firstID, lastID, elapsed)

		return consumer.ConsumeSuccess, nil
	}

	return &RocketMQConsumer{
		Config:   config,
		options:  options,
		handler:  fn,
		selector: selector,
		state:    StateStopped,
	}
}

func (rc *RocketMQConsumer) Start() {
	// 快速预检查，避免加写锁
	rc.mu.RLock()
	state := rc.state
	rc.mu.RUnlock()

	if state != StateStopped {
		if state != StateRunning {
			logger.Warnf("[RocketMQConsumer] %s start skipped, current state=%s", rc.Config.Topic, state)
		}
		return
	}

	// 加写锁，开始启动流程
	rc.mu.Lock()
	defer rc.mu.Unlock()

	if rc.state != StateStopped {
		return
	}

	rc.setStateUnsafe(StateStarting)

	c, err := rocketmq.NewPushConsumer(rc.options...)
	if err != nil {
		logger.Errorf("[RocketMQConsumer] %s failed to create consumer: %v", rc.Config.Topic, err)
		rc.setStateUnsafe(StateStopped)
		return
	}
	logger.Infof("[RocketMQConsumer] %s created", rc.Config.Topic)

	if err := c.Subscribe(rc.Config.Topic, rc.selector, rc.handler); err != nil {
		logger.Errorf("[RocketMQConsumer] %s failed to subscribe: %v", rc.Config.Topic, err)
		rc.setStateUnsafe(StateStopped)
		return
	}

	if err := c.Start(); err != nil {
		logger.Errorf("[RocketMQConsumer] %s consumer start failed: %v", rc.Config.Topic, err)
		rc.setStateUnsafe(StateStopped)
		return
	}

	rc.consumer = c
	rc.setStateUnsafe(StateRunning)
	logger.Infof("[RocketMQConsumer] %s started and running", rc.Config.Topic)
}

func (rc *RocketMQConsumer) Stop() {
	// 先用读锁预判断，减少锁冲突
	rc.mu.RLock()
	if rc.state == StateStopped {
		rc.mu.RUnlock()
		logger.Infof("[RocketMQConsumer] %s already stopped or stopping", rc.Config.Topic)
		return
	}
	rc.mu.RUnlock()

	// 真正处理
	var c rocketmq.PushConsumer
	rc.mu.Lock()
	if rc.state != StateStopped {
		logger.Infof("[RocketMQConsumer] %s stopping, current state=%s", rc.Config.Topic, rc.state)
		c = rc.consumer
		rc.consumer = nil
		rc.setStateUnsafe(StateStopped)
		logger.Infof("[RocketMQConsumer] %s stopped", rc.Config.Topic)
	}
	rc.mu.Unlock()

	rc.closeConsumer(c)
}

func (rc *RocketMQConsumer) closeConsumer(c rocketmq.PushConsumer) {
	if c == nil {
		return
	}

	go func() {
		start := time.Now()
		defer func() {
			if r := recover(); r != nil {
				stack := debug.Stack()
				logger.Errorf(
					"[RocketMQConsumer] panic during shutdown of topic %s: %v\nstacktrace:\n%s",
					rc.Config.Topic, r, stack,
				)
			}
		}()

		if err := c.Shutdown(); err != nil {
			logger.Errorf("[RocketMQConsumer] %s failed to close consumer: %v", rc.Config.Topic, err)
		} else {
			elapsed := time.Since(start)
			logger.Infof("[RocketMQConsumer] %s shutdown completed, took %s", rc.Config.Topic, elapsed)
		}
	}()
}

func (rc *RocketMQConsumer) setStateUnsafe(state ConsumerState) {
	if rc.state == state {
		return
	}
	logger.Infof("[RocketMQConsumer] %s state changed: %s → %s", rc.Config.Topic, rc.state, state)
	rc.state = state
}
