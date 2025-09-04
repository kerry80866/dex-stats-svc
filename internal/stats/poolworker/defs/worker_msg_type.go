package defs

import "fmt"

// WorkerMsgType 定义 Pool Worker 内部消息类型
type WorkerMsgType int

const (
	// 快照相关
	WorkerMsgTypeSnapshotItem WorkerMsgType = iota + 1
	WorkerMsgTypeSnapshotDone

	// 异步任务恢复（leader 切换时触发）
	WorkerMsgTypeRecoverTasks

	// 链上事件
	WorkerMsgTypeChainEvents

	// 余额事件
	WorkerMsgTypeBalanceEvents

	// Token 元数据事件
	WorkerMsgTypeTokenMetaEvents

	// LP 报告事件
	WorkerMsgTypeLpReportEvents

	// 总供应量事件
	WorkerMsgTypeTotalSupplyEvents

	// TopHolders 事件
	WorkerMsgTypeTopHoldersEvents

	// 持有者数量事件
	WorkerMsgTypeHolderCountEvents

	// Pool Ticker 推送完成事件
	WorkerMsgTypePoolTickerPushDoneEvents
)

// String 返回 WorkerMsgType 对应的可读名称，用于日志或调试
func (t WorkerMsgType) String() string {
	switch t {
	case WorkerMsgTypeSnapshotItem:
		return "Worker:SnapshotItem"
	case WorkerMsgTypeSnapshotDone:
		return "Worker:SnapshotDone"
	case WorkerMsgTypeRecoverTasks:
		return "Worker:RecoverTasks"
	case WorkerMsgTypeChainEvents:
		return "Worker:ChainEvents"
	case WorkerMsgTypeBalanceEvents:
		return "Worker:BalanceEvents"
	case WorkerMsgTypeTokenMetaEvents:
		return "Worker:TokenMetaEvents"
	case WorkerMsgTypeLpReportEvents:
		return "Worker:LpReportEvents"
	case WorkerMsgTypeTotalSupplyEvents:
		return "Worker:TotalSupplyEvents"
	case WorkerMsgTypeTopHoldersEvents:
		return "Worker:TopHoldersEvents"
	case WorkerMsgTypeHolderCountEvents:
		return "Worker:HolderCountEvents"
	case WorkerMsgTypePoolTickerPushDoneEvents:
		return "Worker:PoolTickerPushDoneEvents"
	default:
		return fmt.Sprintf("Worker:Unknown(%d)", int(t))
	}
}
