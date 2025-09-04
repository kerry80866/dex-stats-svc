package raft

import (
	"dex-stats-sol/internal/pkg/logger"
	"dex-stats-sol/internal/pkg/utils"
	"fmt"
	"github.com/lni/dragonboat/v3"
	ldcfg "github.com/lni/dragonboat/v3/config"
	ldlogger "github.com/lni/dragonboat/v3/logger"
	"github.com/lni/dragonboat/v3/raftio"
	sm "github.com/lni/dragonboat/v3/statemachine"
	"sync"
	"time"
)

type RaftManager struct {
	NodeHost *dragonboat.NodeHost
	Config   *RaftConfig
	started  bool
	mu       sync.Mutex // 用于并发保护
}

// RaftConfig 为 Raft 管理器的配置项，仅包含必要的部署参数，其他细节由内部封装控制。
type RaftConfig struct {
	NodeID             uint16            `json:"node_id" yaml:"node_id"`                         // 当前节点在集群中的唯一编号（非零）
	ClusterID          uint16            `json:"cluster_id" yaml:"cluster_id"`                   // 所属 Raft 集群的唯一 ID
	Port               uint16            `json:"port" yaml:"port"`                               // 当前节点用于对外通信的监听端口
	DeploymentID       uint16            `json:"deployment_id" yaml:"deployment_id"`             // 所属部署环境 ID（用于隔离 dev/staging/prod 等）
	Join               bool              `json:"join" yaml:"join"`                               // 是否为加入现有集群（true=加入，false=初始化新集群）
	RTTMillisecond     uint32            `json:"rtt_ms" yaml:"rtt_ms"`                           // RTT 时间单位（毫秒），影响选举/心跳的周期基准
	SnapshotEntries    uint32            `json:"snapshot_entries" yaml:"snapshot_entries"`       // 自动生成快照的日志条数间隔（为 0 表示禁用）
	CompactionOverhead uint32            `json:"compaction_overhead" yaml:"compaction_overhead"` // 快照后保留的最近日志数，避免 follower 落后立即触发快照
	WALDir             string            `json:"wal_dir" yaml:"wal_dir"`                         // 用于持久化 WAL 日志的目录（建议使用独立低延迟磁盘）
	NodeHostDir        string            `json:"node_host_dir" yaml:"node_host_dir"`             // 用于存储快照、元数据等其他数据的目录
	InitialMembers     map[uint64]string `json:"initial_members" yaml:"initial_members"`         // 初始节点列表（仅用于 join=false 初始化）
	SubmitTimeout      time.Duration     `json:"submit_timeout" yaml:"submit_timeout"`           // SubmitTimeout 为单次 Raft 提交的最大超时时间，默认 3 秒，建议设置为 1~5 秒。
	SubmitDeadline     time.Duration     `json:"submit_deadline" yaml:"submit_deadline"`         // 整个提交重试的最长时间，默认60秒
}

// NewRaftManager 创建并初始化 Dragonboat NodeHost
func NewRaftManager(
	config *RaftConfig,
	raftListener raftio.IRaftEventListener,
	sysListener raftio.ISystemEventListener,
) (*RaftManager, error) {
	if config.SubmitTimeout == 0 {
		config.SubmitTimeout = 3 * time.Second
	}
	if config.SubmitDeadline == 0 {
		config.SubmitDeadline = 60 * time.Second
	}
	if config.SubmitDeadline < config.SubmitTimeout {
		return nil, fmt.Errorf(
			"submit_deadline (%v) must be greater than or equal to submit_timeout (%v)",
			config.SubmitDeadline, config.SubmitTimeout,
		)
	}

	ip, err := utils.GetLocalIP()
	if err != nil {
		return nil, err
	}
	raftAddress := fmt.Sprintf("%s:%d", ip, config.Port)
	listenAddress := fmt.Sprintf("0.0.0.0:%d", config.Port)

	nhConf := ldcfg.NodeHostConfig{
		DeploymentID:   uint64(config.DeploymentID),   // DeploymentID 用于标识所属部署环境，防止不同环境之间误通信造成数据损坏。
		WALDir:         config.WALDir,                 // WALDir 指定 WAL 日志的存储目录。
		NodeHostDir:    config.NodeHostDir,            // NodeHostDir 是除 WAL 外所有数据的存储路径。
		RTTMillisecond: uint64(config.RTTMillisecond), // RTTMillisecond 是逻辑时钟的基本单位，表示两个节点间平均网络 RTT（毫秒）

		RaftAddress:         raftAddress,   // 对外公开地址（别的节点连你）
		ListenAddress:       listenAddress, // 本地监听地址
		AddressByNodeHostID: false,         // 使用静态ip,不需要

		// 证书参考: https://github.com/lni/dragonboat/wiki/TLS-in-Dragonboat
		MutualTLS: false, // 暂时不需要认证
		CAFile:    "",
		CertFile:  "",
		KeyFile:   "",

		EnableMetrics:       true,  // 启用监控
		NotifyCommit:        false, // 日志在 apply 到状态机后才通知客户端（更安全），false 更稳妥；true 可降低延迟但有幂等风险
		RaftEventListener:   raftListener,
		SystemEventListener: sysListener,

		MaxSendQueueSize:              64 * 1024 * 1024, // 限制 leader 节点发送缓存最大为 64MB
		MaxReceiveQueueSize:           64 * 1024 * 1024, // 限制 leader 节点发送缓存最大为 64MB
		MaxSnapshotSendBytesPerSecond: 0,                // 局域网，不限速，尽快发
		MaxSnapshotRecvBytesPerSecond: 0,                // 局域网，不限速，尽快收
	}

	ldlogger.SetLoggerFactory(func(pkgName string) ldlogger.ILogger {
		return &loggerAdapter{pkg: pkgName}
	})

	nh, err := dragonboat.NewNodeHost(nhConf)
	if err != nil {
		logger.Errorf("failed to create raft node host: %v", err)
		return nil, err
	}

	logger.Infof("raft NodeHost created at %s, address=%s", nhConf.NodeHostDir, nhConf.RaftAddress)
	return &RaftManager{
		NodeHost: nh,
		Config:   config,
	}, nil
}

func (rm *RaftManager) Start(create sm.CreateConcurrentStateMachineFunc) error {
	rm.mu.Lock()
	defer rm.mu.Unlock()

	if rm.started {
		return nil
	}

	conf := ldcfg.Config{
		NodeID:                  uint64(rm.Config.NodeID),             // 当前节点 ID
		ClusterID:               uint64(rm.Config.ClusterID),          // 所属集群 ID
		CheckQuorum:             true,                                 // 启用 CheckQuorum 检查, 防止脑裂
		ElectionRTT:             20,                                   // 选举间隔，单位为 RTT
		HeartbeatRTT:            1,                                    // 心跳间隔，单位为 RTT
		SnapshotEntries:         uint64(rm.Config.SnapshotEntries),    // 每多少条日志自动触发一次快照
		CompactionOverhead:      uint64(rm.Config.CompactionOverhead), // 快照后保留多少条最近日志，便于 follower 追赶
		OrderedConfigChange:     false,                                // 不启用有序变更节点，目前的场景中动态增加移除节点是小概率事件，并且很难乱序
		DisableAutoCompactions:  false,                                // 开启自动清除旧日志
		MaxInMemLogSize:         64 * 1024 * 1024,                     // 限制 leader 节点发送缓存最大为 64MB
		SnapshotCompressionType: ldcfg.NoCompression,                  // 快照默认不压缩
		EntryCompressionType:    ldcfg.NoCompression,                  // 消息默认不压缩
		IsObserver:              false,                                // 非 Observer 节点（无投票权）
		IsWitness:               false,                                // 非 Witness 节点（不参与日志）
		Quiesce:                 false,                                // 不启用 quiesce 模式（避免延迟）
	}

	initialMembers := make(map[uint64]string)
	join := rm.Config.Join
	if !join && len(rm.Config.InitialMembers) > 0 {
		// 初始化新集群，使用用户传入的节点列表
		initialMembers = rm.Config.InitialMembers
	}

	// 启动集群
	if err := rm.NodeHost.StartConcurrentCluster(initialMembers, rm.Config.Join, create, conf); err != nil {
		return fmt.Errorf("failed to start raft cluster: %w", err)
	}

	rm.started = true
	logger.Infof("Raft cluster %d started (join=%v)", rm.Config.ClusterID, rm.Config.Join)
	return nil
}

func (rm *RaftManager) Stop() {
	rm.mu.Lock()
	defer rm.mu.Unlock()

	if !rm.started {
		logger.Warn("RaftManager.Stop called, but not started")
		return
	}

	rm.NodeHost.Stop()
	rm.started = false
	logger.Infof("Raft NodeHost stopped.")
}

type loggerAdapter struct {
	pkg string
}

func (l *loggerAdapter) SetLevel(level ldlogger.LogLevel) {
	// 可根据 level 调整你内部 logger 的级别，当前忽略
}

func (l *loggerAdapter) Debugf(format string, args ...interface{}) {
	logger.Debugf("[dragonboat:%s] %s", l.pkg, fmt.Sprintf(format, args...))
}

func (l *loggerAdapter) Infof(format string, args ...interface{}) {
	logger.Infof("[dragonboat:%s] %s", l.pkg, fmt.Sprintf(format, args...))
}

func (l *loggerAdapter) Warningf(format string, args ...interface{}) {
	logger.Warnf("[dragonboat:%s] %s", l.pkg, fmt.Sprintf(format, args...))
}

func (l *loggerAdapter) Errorf(format string, args ...interface{}) {
	logger.Errorf("[dragonboat:%s] %s", l.pkg, fmt.Sprintf(format, args...))
}

func (l *loggerAdapter) Panicf(format string, args ...interface{}) {
	// 建议使用 panic 直接中断，符合 Dragonboat 的设计
	panic(fmt.Sprintf("[dragonboat:%s] %s", l.pkg, fmt.Sprintf(format, args...)))
}
