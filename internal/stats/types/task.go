package types

import "dex-stats-sol/pb"

// TokenTaskType 表示 TokenTask 的具体类型
type TokenTaskType uint8

const (
	TokenTaskMeta        TokenTaskType = iota // token meta 相关任务
	TokenTaskTopHolders                       // top holders 更新任务
	TokenTaskHolderCount                      // holder count 更新任务
)

type TokenTask struct {
	Token    Pubkey
	TaskAtMs int64
}

type PushTask struct {
	Pool        Pubkey
	PoolHash    uint64
	BlockNumber uint32
	Ticker      *pb.TickerData
}
