package raft

import "dex-stats-sol/internal/stats/types"

// RaftListener 接口定义了 Raft 事件的监听方法
type RaftListener interface {
	// 当前进程成为 Raft Leader
	OnBecameRaftLeader(first bool)

	// 当前进程成为 Raft Follower
	OnBecameRaftFollower(first bool)

	// Raft 节点追上日志并准备就绪
	OnRaftReady()

	// 返回需要持久化的快照数据对象
	OnPrepareSnapshot() ([][]types.Serializable, error)

	// 恢复快照时处理每条原始数据
	OnRecoverFromSnapshot(data []byte) error

	// 快照恢复完成后回调
	OnRecoverFromSnapshotDone() error

	// Raft 日志已提交，传入原始 payload
	OnRaftDataUpdated(data []byte) error
}
