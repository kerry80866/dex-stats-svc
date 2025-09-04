package shared

import (
	"dex-stats-sol/pb"
	"sync/atomic"
)

type HolderCount struct {
	baseCount         atomic.Int32  // 基准持有人数（来自接口/全量数据源）
	baseTs            atomic.Uint32 // 基准值的时间戳
	deltaCount        atomic.Int32  // 增量修正（由事件流累计的加减）
	latestBlockNumber atomic.Uint32 // 已处理到的最新 BlockNumber
}

// NewHolderCount 创建一个空的 HolderCount
func NewHolderCount() *HolderCount {
	return &HolderCount{}
}

// ShouldRequest 判断是否需要请求最新持有人数数据
func (h *HolderCount) ShouldRequest() bool {
	return h.baseTs.Load() == 0
}

// Value 返回当前持有人数（基准 + 增量）
func (h *HolderCount) Value() uint32 {
	if h.baseTs.Load() == 0 {
		return 0
	}

	val := h.baseCount.Load() + h.deltaCount.Load()
	return uint32(max(0, val))
}

// BaseCount 返回基准持有人数
func (h *HolderCount) BaseCount() int32 {
	return h.baseCount.Load()
}

// BaseTimestamp 返回基准值对应的时间戳
func (h *HolderCount) BaseTimestamp() uint32 {
	return h.baseTs.Load()
}

// LatestBlockNumber 返回已推进到的最新 blockNumber
func (h *HolderCount) LatestBlockNumber() uint32 {
	return h.latestBlockNumber.Load()
}

// ApplyDelta 根据事件增量更新持有人数，并推进到对应 blockNumber。
func (h *HolderCount) ApplyDelta(delta int32, blockNumber uint32) bool {
	if blockNumber < h.latestBlockNumber.Load() {
		return false
	}
	h.deltaCount.Add(delta)
	h.latestBlockNumber.Store(blockNumber)
	return true
}

// ApplyBase 更新基准持有人数和对应时间戳，同时重置增量。
func (h *HolderCount) ApplyBase(baseCount int32, baseTs uint32) bool {
	if baseTs <= h.baseTs.Load() {
		return false
	}
	h.baseCount.Store(baseCount)
	h.baseTs.Store(baseTs)
	h.deltaCount.Store(0)
	return true
}

// ToProto 将 HolderCount 转换成 Protobuf Snapshot
func (h *HolderCount) ToProto() *pb.HolderCountSnapshot {
	return &pb.HolderCountSnapshot{
		BaseCount:         h.baseCount.Load(),
		BaseTs:            h.baseTs.Load(),
		DeltaCount:        h.deltaCount.Load(),
		LatestBlockNumber: h.latestBlockNumber.Load(),
	}
}

// NewHolderCountFromProto 根据 Protobuf Snapshot 新建 HolderCount
func NewHolderCountFromProto(p *pb.HolderCountSnapshot) *HolderCount {
	h := &HolderCount{}
	h.baseCount.Store(p.BaseCount)
	h.baseTs.Store(p.BaseTs)
	h.deltaCount.Store(p.DeltaCount)
	h.latestBlockNumber.Store(p.LatestBlockNumber)
	return h
}
