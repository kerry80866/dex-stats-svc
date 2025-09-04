# Dragonboat 配置项中文说明

## 1. Raft 节点配置：`Config`

```go
// NodeHostConfig 用于配置 Raft 节点容器 NodeHost 的运行参数。
type NodeHostConfig struct {
    // DeploymentID 用于标识所属部署环境，防止不同环境之间误通信造成数据损坏。
    // 建议：生产环境统一设置相同值；测试、开发等环境用不同值。
    // 默认值为 0，表示所有为 0 的节点可互相通信。
    DeploymentID uint64

    // WALDir 指定 WAL 日志的存储目录。
    // 建议使用 NVMe SSD 等低延迟磁盘，并具备断电保护。
    // 若为空，WAL 会与其他数据一起写入 NodeHostDir。
    WALDir string

    // NodeHostDir 是除 WAL 外所有数据的存储路径。
    // 建议与 WALDir 分离以减少磁盘争用。
    NodeHostDir string

    // RTTMillisecond 是逻辑时钟的基本单位，表示两个节点间平均网络 RTT（毫秒）。
    // 影响选举超时（ElectionRTT）、心跳间隔（HeartbeatRTT）。
    // 建议值为 1~10，生产环境中通常设为 1。
    RTTMillisecond uint64

    // RaftAddress 是该节点对外通信地址（IP:端口或域名:端口），用于发送 Raft 消息与快照。
    // 需确保该地址在其他节点中可达，若 ListenAddress 未设置，则用于监听。
    RaftAddress string

    // AddressByNodeHostID 表示是否通过 NodeHostID 而不是固定地址标识节点（适用于动态 IP 场景）。
    // 启用后需配置 Gossip 服务，并使用 NodeHost.ID() 返回值作为目标节点标识。
    AddressByNodeHostID bool

    // ListenAddress 是可选的监听地址，若为空则监听 RaftAddress。
    // 可设为 "0.0.0.0:port" 表示监听所有网卡。
    ListenAddress string

    // MutualTLS 表示是否启用双向 TLS 认证。
    // 启用时需同时配置 CAFile、CertFile、KeyFile。
    MutualTLS bool
    CAFile    string // CA 根证书路径，仅在 MutualTLS=true 时使用
    CertFile  string // 节点证书路径
    KeyFile   string // 节点私钥路径

    // LogDBFactory 和 RaftRPCFactory 已废弃，请使用 Expert.LogDBFactory 替代。
    LogDBFactory   LogDBFactoryFunc
    RaftRPCFactory RaftRPCFactoryFunc

    // EnableMetrics 是否启用 Prometheus 格式指标导出，便于外部监控。
    EnableMetrics bool

    // RaftEventListener 是 Raft 核心事件监听器（如领导变更）。
    // 回调将在单独 goroutine 中触发，避免长时间阻塞。
    RaftEventListener raftio.IRaftEventListener

    // SystemEventListener 是系统事件监听器（如快照生成、日志压缩），主要用于测试或高级场景。
    SystemEventListener raftio.ISystemEventListener

    // MaxSendQueueSize 限制单个节点发送队列最大内存（字节），0 表示不限制。
    // 建议值如：32MB~128MB，防止内存溢出。
    MaxSendQueueSize uint64

    // MaxReceiveQueueSize 限制接收队列大小，规则与发送队列相同。
    MaxReceiveQueueSize uint64

    // MaxSnapshotSendBytesPerSecond 限制快照发送速率（字节/秒），0 表示不限制。
    // 可用于防止快照传输占满带宽。
    MaxSnapshotSendBytesPerSecond uint64

    // MaxSnapshotRecvBytesPerSecond 限制快照接收速率（字节/秒），与发送类似。
    MaxSnapshotRecvBytesPerSecond uint64

    // NotifyCommit 表示是否在日志提交（而非 apply）阶段就通知客户端。
    // 可降低确认延迟，但存在“未应用即返回”风险。
    NotifyCommit bool

    // Gossip 是内部 gossip 服务的配置项，用于动态 IP 场景下的节点地址发现。
    // 启用 AddressByNodeHostID 后必须配置。
    Gossip GossipConfig

    // Expert 高级配置项，慎用。修改不当可能导致无法重启或性能下降。
    Expert ExpertConfig
}

```
---
