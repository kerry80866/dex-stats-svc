package mq

import (
	"context"
	"dex-stats-sol/internal/pkg/logger"
	"dex-stats-sol/internal/pkg/utils"
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"time"
)

const (
	defaultBatchSize   = 32 * 1024 // 32KB
	defaultLingerMs    = 5         // ms
	defaultMaxInFlight = 5         // 默认 max.in.flight
)

// KafkaProducerConf 定义 Kafka Producer 配置
type KafkaProducerConf struct {
	// ClientID 用于标识 Kafka Producer 客户端，通常用于区分不同应用或实例
	ClientID string `json:"client_id" yaml:"client_id"`

	// Brokers Kafka broker 地址，多个 broker 用英文逗号分隔
	// 示例: "localhost:9092,localhost:9093"
	Brokers string `json:"brokers" yaml:"brokers"`

	// 批处理参数
	BatchSize int `json:"batch_size" yaml:"batch_size"` // 批处理大小（单位字节），Kafka 会把多条消息聚合到一个 batch 中发送，建议根据吞吐量和消息大小调整
	LingerMs  int `json:"linger_ms" yaml:"linger_ms"`   // 批处理最大延迟（毫秒），Kafka 会等待该时间再发送 batch，建议 5~20ms 之间，可提高吞吐量

	// 可靠性参数
	Idempotence bool `json:"idempotence" yaml:"idempotence"`     // 是否开启幂等发送，true 时保证 Producer 消息不会重复且按顺序发送
	MaxInFlight int  `json:"max_in_flight" yaml:"max_in_flight"` // 同一连接上最大未确认请求数（in-flight 请求），开启幂等时最大只能为5

	// Topic 配置
	Topics []struct {
		Topic      string `json:"topic" yaml:"topic"`           // Topic 名称
		Partitions int    `json:"partitions" yaml:"partitions"` // Topic 分区数，决定并发和吞吐
	} `json:"topics" yaml:"topics"`
}

// NewKafkaProducer 创建 Kafka 生产者
func NewKafkaProducer(cfg *KafkaProducerConf) (*kafka.Producer, error) {
	// 确保 topic 存在
	if err := ensureKafkaTopics(cfg); err != nil {
		return nil, err
	}

	// 批处理参数默认值
	batchSize := cfg.BatchSize
	if batchSize <= 0 {
		batchSize = defaultBatchSize
	}

	lingerMs := cfg.LingerMs
	if lingerMs < 0 {
		lingerMs = defaultLingerMs
	}

	localIP, _ := utils.GetLocalIP()
	if localIP == "" {
		localIP = "unknown"
	}

	// 可靠性参数处理
	maxInFlight := cfg.MaxInFlight
	if maxInFlight <= 0 {
		maxInFlight = defaultMaxInFlight // 默认合法值
	}

	acks := "1"
	if cfg.Idempotence {
		acks = "all"
		if maxInFlight > defaultMaxInFlight {
			logger.Warnf("[KafkaProducer] maxInFlight (%d) exceeds maximum allowed (%d) for idempotence, resetting to %d",
				maxInFlight, defaultMaxInFlight, defaultMaxInFlight)
			maxInFlight = defaultMaxInFlight // 幂等开启时最大值限制为 5
		}
	}

	// 打印最终生效的生产者参数
	logger.Infof("[KafkaProducer] Creating producer with config: ClientID=%s, Brokers=%s, Idempotence=%v, Acks=%s, MaxInFlight=%d, BatchSize=%d, LingerMs=%d",
		cfg.ClientID, cfg.Brokers, cfg.Idempotence, acks, maxInFlight, batchSize, lingerMs)

	// 创建生产者
	producer, err := kafka.NewProducer(&kafka.ConfigMap{
		// 基础连接
		"bootstrap.servers": cfg.Brokers,
		"client.id":         fmt.Sprintf("%s-%s", cfg.ClientID, localIP),

		// PLAINTEXT: 不加密(明文传输), 不认证
		// SSL: 只加密，不认证
		// SASL_PLAINTEXT: 只认证，不加密
		// SASL_SSL: 加密 + 认证（
		//"security.protocol":  "SASL_SSL", // 生成环境建议: SASL_SSL

		// 1. PLAIN: 明文传输;
		// 2. SCRAM-SHA-256: 用户名 + 密码 + 哈希认证;
		// 3. SCRAM-SHA-512: 用户名 + 密码 + 哈希认证(更强);
		// 4. GSSAPI: Kerberos 身份认证;
		// 5. OAUTHBEARER: OAuth 令牌认证
		//"sasl.mechanisms":    "SCRAM-SHA-256",

		//"sasl.username":      "user",
		//"sasl.password":      "password",
		//"ssl.ca.location":    "/etc/ssl/certs/ca-certificates.crt",
		//"sasl.oauthbearer.token.endpoint.url": "https://your-auth.com/oauth2/token", // 可选

		// 可靠性保障
		"acks":                                  acks,
		"enable.idempotence":                    cfg.Idempotence,
		"max.in.flight.requests.per.connection": maxInFlight,

		// 超时与重试
		"delivery.timeout.ms": 30000,
		"request.timeout.ms":  30000,
		"retries":             5,   // 重试次数必须 > 0
		"retry.backoff.ms":    100, // 重试间隔

		// 性能优化
		"batch.size":       batchSize,
		"linger.ms":        lingerMs,
		"compression.type": "none",

		// 消息大小
		"message.max.bytes": 2 * 1024 * 1024, // 2MB
	})

	if err != nil {
		return nil, fmt.Errorf("failed to create producer: %w", err)
	}

	return producer, nil
}

type KafkaMsgResult struct {
	Msg       *kafka.Message
	Success   bool
	Completed bool // true 表示已结束
}

func SendKafkaMessagesBatch(
	ctx context.Context,
	producer *kafka.Producer,
	messages []*kafka.Message,
	timeout time.Duration,
) []*KafkaMsgResult {
	if len(messages) == 0 {
		return nil
	}

	startTime := time.Now()
	deliveryChan := make(chan kafka.Event, len(messages))
	results := make([]*KafkaMsgResult, len(messages))
	pending := make(map[any]int)
	successCount := 0

	// Produce 所有消息
	for i, msg := range messages {
		results[i] = &KafkaMsgResult{Msg: msg}

		var deliveryChanToUse chan kafka.Event
		if msg.Opaque != nil {
			// 使用 msg.Opaque 作为唯一标识，用于在 deliveryChan 回调中匹配消息索引
			deliveryChanToUse = deliveryChan
			pending[msg.Opaque] = i
		}

		if err := producer.Produce(msg, deliveryChanToUse); err != nil {
			results[i].Completed = true
			if msg.Opaque != nil {
				delete(pending, msg.Opaque)
			}
			logger.Errorf("[KafkaProducer] Produce failed: %v", err)
			continue
		}

		if msg.Opaque == nil {
			results[i].Completed = true
			results[i].Success = true
			successCount++
		}
	}

	// 等待确认或者超时
	timeoutAfter := time.After(timeout)
	for len(pending) > 0 {
		select {
		case e := <-deliveryChan:
			m, ok := e.(*kafka.Message)
			if !ok || m.Opaque == nil {
				continue
			}

			idx, exists := pending[m.Opaque]
			if !exists {
				continue
			}

			delete(pending, m.Opaque)
			results[idx].Completed = true
			if m.TopicPartition.Error == nil {
				results[idx].Success = true
				successCount++
			}

		case <-timeoutAfter:
			logger.Errorf("[KafkaProducer] Batch timeout after %v, unfinished=%d",
				timeout, len(pending))
			pending = nil // 结束循环

		case <-ctx.Done():
			logger.Warnf("[KafkaProducer] Batch cancelled by context, unfinished=%d",
				len(pending))
			return results
		}
	}

	logger.Infof("[KafkaProducer] Batch finished. Total=%d, Success=%d, Failed=%d, Duration=%v",
		len(messages), successCount, len(messages)-successCount, time.Since(startTime))

	return results
}

// ensureKafkaTopics 确保 Kafka 中配置的 topic 都存在，如果不存在则创建
func ensureKafkaTopics(cfg *KafkaProducerConf) error {
	// 创建管理员客户端来管理 topic
	adminClient, err := kafka.NewAdminClient(&kafka.ConfigMap{
		"bootstrap.servers": cfg.Brokers,
	})
	if err != nil {
		return fmt.Errorf("failed to create admin client: %w", err)
	}
	defer adminClient.Close()

	// 检查 topic 是否存在
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	meta, err := adminClient.GetMetadata(nil, true, 10000)
	if err != nil {
		return fmt.Errorf("failed to get metadata: %w", err)
	}
	brokerCount := len(meta.Brokers)

	// replicationFactor 是 Kafka 主题（Topic）中每个分区（Partition）副本的数量
	replicationFactor := 1
	if brokerCount > 1 {
		replicationFactor = 2
	}
	logger.Infof("[KafkaProducer] Kafka broker count = %d, using replication factor = %d", brokerCount, replicationFactor)

	// 检查需要创建的 topic
	var topicsToCreate []kafka.TopicSpecification
	existingTopics := make(map[string]bool)
	for _, topic := range meta.Topics {
		existingTopics[topic.Topic] = true
	}

	// 如果 topic 不存在，则添加 topic 到创建列表
	for _, topic := range cfg.Topics {
		if !existingTopics[topic.Topic] {
			topicsToCreate = append(topicsToCreate, kafka.TopicSpecification{
				Topic:             topic.Topic,
				NumPartitions:     topic.Partitions,
				ReplicationFactor: replicationFactor,
			})
		}
	}

	// 如果有需要创建的 topic，则创建
	if len(topicsToCreate) > 0 {
		logger.Infof("[KafkaProducer] creating topics: %v", topicsToCreate)
		results, createErr := adminClient.CreateTopics(ctx, topicsToCreate)
		if createErr != nil {
			return fmt.Errorf("failed to create topics: %w", createErr)
		}

		// 检查创建结果
		for _, result := range results {
			if result.Error.Code() != kafka.ErrNoError {
				return fmt.Errorf("failed to create topic %s: %w", result.Topic, result.Error)
			}
			logger.Infof("[KafkaProducer] topic %s created successfully", result.Topic)
		}
	} else {
		logger.Infof("[KafkaProducer] all topics already exist")
	}

	return nil
}
