package raft

import (
	"context"
	"dex-stats-sol/internal/pkg/logger"
	"dex-stats-sol/internal/pkg/utils"
	"fmt"
	"github.com/lni/dragonboat/v3"
	ldcfg "github.com/lni/dragonboat/v3/config"
	ldlogger "github.com/lni/dragonboat/v3/logger"
	"github.com/lni/dragonboat/v3/raftio"
	sm "github.com/lni/dragonboat/v3/statemachine"
	"strconv"
	"strings"
	"sync"
	"time"
)

type RaftManager struct {
	NodeHost       *dragonboat.NodeHost
	Config         *RaftConfig
	NodeID         uint32
	started        bool
	initialMembers map[uint64]string
	mu             sync.Mutex // 用于并发保护
}

// RaftConfig 为 Raft 管理器的配置项，仅包含必要的部署参数，其他细节由内部封装控制。
type RaftConfig struct {
	DeploymentID       uint16        `json:"deployment_id" yaml:"deployment_id"`             // 所属部署环境 ID（用于隔离 dev/staging/prod 等）
	ClusterID          uint16        `json:"cluster_id" yaml:"cluster_id"`                   // 所属 Raft 集群的唯一 ID
	Join               bool          `json:"join" yaml:"join"`                               // 是否为加入现有集群（true=加入，false=初始化新集群）
	Port               uint16        `json:"port" yaml:"port"`                               // 当前节点用于对外通信的监听端口
	Members            []string      `json:"members" yaml:"members"`                         // 初始节点列表（仅用于 join=false 初始化）
	RTTMillisecond     uint32        `json:"rtt_ms" yaml:"rtt_ms"`                           // RTT 时间单位（毫秒），影响选举/心跳的周期基准
	SnapshotEntries    uint32        `json:"snapshot_entries" yaml:"snapshot_entries"`       // 自动生成快照的日志条数间隔（为 0 表示禁用）
	CompactionOverhead uint32        `json:"compaction_overhead" yaml:"compaction_overhead"` // 快照后保留的最近日志数，避免 follower 落后立即触发快照
	WALDir             string        `json:"wal_dir" yaml:"wal_dir"`                         // 用于持久化 WAL 日志的目录（建议使用独立低延迟磁盘）
	NodeHostDir        string        `json:"node_host_dir" yaml:"node_host_dir"`             // 用于存储快照、元数据等其他数据的目录
	SubmitTimeout      time.Duration `json:"submit_timeout" yaml:"submit_timeout"`           // SubmitTimeout 为单次 Raft 提交的最大超时时间，默认 3 秒，建议设置为 1~5 秒。
	SubmitDeadline     time.Duration `json:"submit_deadline" yaml:"submit_deadline"`         // 整个提交重试的最长时间，默认60秒
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

	currentIp, err := utils.GetLocalIP()
	if err != nil {
		return nil, err
	}
	currentID, err := parseNodeID(currentIp)
	if err != nil {
		return nil, err
	}

	raftAddress := fmt.Sprintf("%s:%d", currentIp, config.Port)
	listenAddress := fmt.Sprintf("0.0.0.0:%d", config.Port)

	initialMembers := make(map[uint64]string)
	if !config.Join {
		if len(config.Members) > 0 {
			for _, member := range config.Members {
				nodeID, ipErr := parseNodeID(member)
				if ipErr != nil {
					return nil, fmt.Errorf("invalid member: %s, error: %v", member, ipErr)
				}
				initialMembers[uint64(nodeID)] = fmt.Sprintf("%s:%d", member, config.Port)
			}
		} else {
			initialMembers[uint64(currentID)] = raftAddress
		}
	}

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
		NodeHost:       nh,
		Config:         config,
		NodeID:         currentID,
		initialMembers: initialMembers,
	}, nil
}

func (rm *RaftManager) Start(create sm.CreateConcurrentStateMachineFunc) error {
	rm.mu.Lock()
	defer rm.mu.Unlock()

	if rm.started {
		return nil
	}

	conf := ldcfg.Config{
		NodeID:                  uint64(rm.NodeID),                    // 当前节点 ID
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

	// 启动集群
	if err := rm.NodeHost.StartConcurrentCluster(rm.initialMembers, rm.Config.Join, create, conf); err != nil {
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

func (rm *RaftManager) GetLeaderIP() (s string, err error) {
	// 使用 defer 和 recover 捕获 panic 错误
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("[GetLeaderIP] recovered from panic: %v", r)
			s = ""
		}
	}()

	// 检查 Raft 节点是否已经启动
	if !rm.started {
		return "", fmt.Errorf("[GetLeaderIP] Raft node not started")
	}

	// 获取 leader ID，确保当前节点是 leader
	clusterID := uint64(rm.Config.ClusterID)
	leaderID, valid, err := rm.NodeHost.GetLeaderID(clusterID)
	if err != nil {
		return "", fmt.Errorf("[GetLeaderIP] failed to get leader ID: %v", err)
	}

	// 检查是否有有效的 leader
	if !valid {
		return "", fmt.Errorf("[GetLeaderIP] no valid leader currently available")
	}

	// 返回 Leader 的 IP 地址
	leaderIP := fmt.Sprintf("%d:x.x.%d.%d", leaderID>>16, (leaderID>>8)&0xff, leaderID&0xff)
	return leaderIP, nil
}

func (rm *RaftManager) AddOrRemoveNode(node string, addNode bool) (err error) {
	// 使用 defer 和 recover 捕获 panic 错误
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("[AddOrRemoveNode] panic %v", r)
		}
	}()

	// 检查 Raft 节点是否已经启动
	if !rm.started {
		return fmt.Errorf("[AddOrRemoveNode] Raft node not started")
	}

	// 将 IP 转换为 NodeID
	newNodeID, err := parseNodeID(node)
	if err != nil {
		return fmt.Errorf("[AddOrRemoveNode] failed to convert IP to NodeID: %v", err)
	}

	// 获取 leader ID，确保当前节点是 leader
	clusterID := uint64(rm.Config.ClusterID)
	leaderID, valid, err := rm.NodeHost.GetLeaderID(clusterID)
	if err != nil {
		return fmt.Errorf("[AddOrRemoveNode] failed to get leader ID: %v", err)
	}

	// 检查是否有有效的 leader
	if !valid {
		return fmt.Errorf("[AddOrRemoveNode] no valid leader currently available")
	}

	// 确保当前节点是 leader
	if uint64(rm.NodeID) != leaderID {
		leaderIP := fmt.Sprintf("x.x.%d.%d", leaderID>>8, leaderID&0xff)
		return fmt.Errorf("[AddOrRemoveNode] current node (%d) is not the leader (%d, IP: %s)", rm.NodeID, leaderID, leaderIP)
	}

	// 设置超时来进行添加/删除节点的请求
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// 添加节点或删除节点
	if addNode {
		// 请求将新节点添加到 Raft 集群
		target := fmt.Sprintf("%s:%d", node, rm.Config.Port)
		err = rm.NodeHost.SyncRequestAddNode(ctx, clusterID, uint64(newNodeID), target, 0)
		if err != nil {
			return fmt.Errorf("[AddOrRemoveNode] failed to add node %s: %v", node, err)
		}
	} else {
		// 请求从 Raft 集群中删除节点
		err = rm.NodeHost.SyncRequestDeleteNode(ctx, clusterID, uint64(newNodeID), 0)
		if err != nil {
			return fmt.Errorf("[AddOrRemoveNode] failed to remove node %s: %v", node, err)
		}
	}
	return nil
}

// parseNodeID 从 "version:ip" 格式的字符串中解析出 NodeID
func parseNodeID(input string) (uint32, error) {
	// 拆分版本和 IP 部分
	parts := strings.Split(input, ":")
	if len(parts) != 2 {
		return 0, fmt.Errorf("invalid input format: %s", input)
	}

	// 获取 version 和 ip 地址
	version, err := strconv.Atoi(parts[0])
	if err != nil {
		return 0, fmt.Errorf("invalid version: %s", parts[0])
	}

	ip := parts[1]
	ipParts := strings.Split(ip, ".")
	if len(ipParts) != 4 {
		return 0, fmt.Errorf("invalid IP address format: %s", ip)
	}

	// 使用倒数第二个字节
	lastByte1, err := strconv.Atoi(ipParts[2])
	if err != nil {
		return 0, fmt.Errorf("invalid byte in IP address: %s", ipParts[2])
	}

	// 使用最后一个字节
	lastByte2, err := strconv.Atoi(ipParts[3])
	if err != nil {
		return 0, fmt.Errorf("invalid byte in IP address: %s", ipParts[3])
	}

	// 生成 NodeID: version << 16 | ip[2] << 8 | ip[3]
	nodeID := uint32(version)<<16 | uint32(lastByte1)<<8 | uint32(lastByte2)
	return nodeID, nil
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
