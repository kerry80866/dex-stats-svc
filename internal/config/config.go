package config

import (
	"dex-stats-sol/internal/pkg/logger"
	"dex-stats-sol/internal/pkg/mq"
	"dex-stats-sol/internal/pkg/nacos"
	"dex-stats-sol/internal/pkg/raft"
	"fmt"
	"github.com/zeromicro/go-zero/zrpc"
	"time"
)

type MonitorConfig struct {
	Port int `json:"port" yaml:"port"` // 监控端口，0 表示关闭
}

type LogConfig struct {
	Format   string `json:"format" yaml:"format"`     // 日志格式，可选 "console"（开发调试）或 "json"（结构化，推荐生产使用）
	LogDir   string `json:"log_dir" yaml:"log_dir"`   // 日志文件目录，可为相对路径或绝对路径
	Level    string `json:"level" yaml:"level"`       // 日志级别：debug / info / warn / error
	Compress bool   `json:"compress" yaml:"compress"` // 是否压缩旧日志文件
}

func (c *LogConfig) ToLogOption() logger.LogOption {
	return logger.LogOption{
		Format:   c.Format,
		LogDir:   c.LogDir,
		Level:    c.Level,
		Compress: c.Compress,
	}
}

type GrpcMiddlewaresConfig struct {
	Prometheus bool `json:"prometheus" yaml:"prometheus"`
}

type GrpcMethodTimeoutConfig struct {
	FullMethod string        `json:"full_method" yaml:"full_method"`
	Timeout    time.Duration `json:"timeout" yaml:"timeout"`
}

// GrpcConfig gRPC 服务相关配置
type GrpcConfig struct {
	Port           int                       `json:"port" yaml:"port"`
	Timeout        int64                     `json:"timeout" yaml:"timeout"`
	CpuThreshold   int64                     `json:"cpu_threshold" yaml:"cpu_threshold"`
	Health         bool                      `json:"health" yaml:"health"`
	Middlewares    GrpcMiddlewaresConfig     `json:"middlewares" yaml:"middlewares"`
	MethodTimeouts []GrpcMethodTimeoutConfig `json:"method_timeouts" yaml:"method_timeouts"`
}

func (c *GrpcConfig) ToRpcServerConf() zrpc.RpcServerConf {
	var mts []zrpc.MethodTimeoutConf
	for _, m := range c.MethodTimeouts {
		mts = append(mts, zrpc.MethodTimeoutConf{
			FullMethod: m.FullMethod,
			Timeout:    m.Timeout,
		})
	}
	return zrpc.RpcServerConf{
		ListenOn:     fmt.Sprintf("0.0.0.0:%d", c.Port),
		Timeout:      c.Timeout,
		CpuThreshold: c.CpuThreshold,
		Health:       c.Health,
		Middlewares: zrpc.ServerMiddlewaresConf{
			Prometheus: c.Middlewares.Prometheus,
		},
		MethodTimeouts: mts,
	}
}

type PoolWorkerConfig struct {
	WorkerCount int `json:"worker_count" yaml:"worker_count"`
}

type RpcConf struct {
	Endpoint  string `yaml:"endpoint" json:"endpoint"`
	TimeoutMs int    `yaml:"timeout_ms" json:"timeout_ms"`
}

type GcConfig struct {
	Interval time.Duration `json:"interval" yaml:"interval"`
}

type Config struct {
	Monitor               MonitorConfig            `json:"monitor" yaml:"monitor"`                     // 监控配置
	LogConf               LogConfig                `json:"logger" yaml:"logger"`                       // 日志配置
	KafkaProducerConfig   *mq.KafkaProducerConf    `json:"kafka_producer" yaml:"kafka_producer"`       // Kafka 生产者配置
	ChainEventsKcConfig   *mq.KafkaConsumerConf    `json:"chain_events_kc" yaml:"chain_events_kc"`     // 链上事件 Kafka 消费者配置
	BalanceEventsKcConfig *mq.KafkaConsumerConf    `json:"balance_events_kc" yaml:"balance_events_kc"` // 余额事件 Kafka 消费者配置
	TokenMetaKcConfig     *mq.KafkaConsumerConf    `json:"token_meta_kc" yaml:"token_meta_kc"`         // Token 元数据 Kafka 消费者配置
	LpReportRcConfig      *mq.RocketMQConsumerConf `json:"lp_report_rc" yaml:"lp_report_rc"`           // RocketMQ 消费者配置
	NacosConfig           *nacos.NacosConfig       `json:"nacos" yaml:"nacos"`                         // Nacos 配置
	Raft                  *raft.RaftConfig         `json:"raft" yaml:"raft"`                           // Raft 配置
	Grpc                  GrpcConfig               `json:"grpc" yaml:"grpc"`                           // gRPC 配置
	PoolWorkerConfig      PoolWorkerConfig         `json:"pool_worker" yaml:"pool_worker"`             // PoolWorker 配置
	GcConfig              GcConfig                 `json:"gc" yaml:"gc"`                               // GC/缓存清理配置
	Rpc                   RpcConf                  `json:"rpc" yaml:"rpc"`
}
