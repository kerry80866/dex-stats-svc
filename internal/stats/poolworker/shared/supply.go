package shared

import (
	"dex-stats-sol/internal/pkg/utils"
	"dex-stats-sol/pb"
	"sync/atomic"
)

// SupplyInfo 表示 base token 的供应信息，包括总量、流通量、最大供应量等
type SupplyInfo struct {
	circulatingSupply utils.AtomicFloat64 // 当前流通量（float64，以 token 为单位）
	totalSupply       utils.AtomicFloat64 // 当前已铸造总量（float64，以 token 为单位）
	maxSupply         utils.AtomicFloat64 // 最大可能供应量（float64，以 token 为单位）
	updatedAt         atomic.Uint32       // 最后更新时间（秒）
	supplyBurned      atomic.Bool         // 是否处于 BurnedSupply 特殊情况
}

// NewSupplyInfo 创建一个空的 SupplyInfo
func NewSupplyInfo() *SupplyInfo {
	return &SupplyInfo{}
}

// ShouldRequest 判断是否需要请求最新供应量数据
func (info *SupplyInfo) ShouldRequest() bool {
	return info.totalSupply.Load() == 0 && !info.supplyBurned.Load()
}

// SupplyBurned 返回是否处于 BurnedSupply 特殊状态
func (info *SupplyInfo) SupplyBurned() bool {
	return info.supplyBurned.Load()
}

// TotalSupply 返回当前总供应量
func (info *SupplyInfo) TotalSupply() float64 {
	return info.totalSupply.Load()
}

// CirculatingSupply 返回当前流通量
func (info *SupplyInfo) CirculatingSupply() float64 {
	return info.circulatingSupply.Load()
}

// MaxSupply 返回最大可能供应量
func (info *SupplyInfo) MaxSupply() float64 {
	return info.maxSupply.Load()
}

// UpdatedAt 返回最后更新时间（Unix 秒）
func (info *SupplyInfo) UpdatedAt() uint32 {
	return info.updatedAt.Load()
}

// UpdateTotalSupply 更新总供应量，如果有实际变化返回 true
func (info *SupplyInfo) UpdateTotalSupply(totalSupply float64, isBurned bool, updatedAt uint32) bool {
	if updatedAt < info.updatedAt.Load() {
		return false
	}

	info.updatedAt.Store(updatedAt)
	return info.updateSupplies(totalSupply, info.CirculatingSupply(), isBurned)
}

// UpdateSupplyInfo 更新总量、流通量和最大供应量，如果有实际变化返回 true
func (info *SupplyInfo) UpdateSupplyInfo(
	totalSupply, circulatingSupply, maxSupply float64,
	isBurned bool, updatedAt uint32,
) bool {
	if updatedAt < info.updatedAt.Load() {
		return false
	}

	changed := info.updateSupplies(totalSupply, circulatingSupply, isBurned)

	if info.maxSupply.Load() != maxSupply {
		info.maxSupply.Store(maxSupply)
		changed = true
	}

	info.updatedAt.Store(updatedAt)
	return changed
}

// updateSupplies 更新总量和流通量，返回是否有实际变化
func (info *SupplyInfo) updateSupplies(totalSupply, circulatingSupply float64, isBurned bool) bool {
	changed := false

	oldTotalSupply := info.totalSupply.Load()
	oldCirculatingSupply := info.circulatingSupply.Load()
	oldSupplyBurned := info.supplyBurned.Load()

	if isBurned {
		totalSupply = 0
		circulatingSupply = 0
	} else if circulatingSupply > totalSupply {
		// 流通量不可能超过总量，兜底置零
		circulatingSupply = 0
	}

	if oldTotalSupply != totalSupply {
		info.totalSupply.Store(totalSupply)
		changed = true
	}
	if oldCirculatingSupply != circulatingSupply {
		info.circulatingSupply.Store(circulatingSupply)
		changed = true
	}
	if oldSupplyBurned != isBurned {
		info.supplyBurned.Store(isBurned)
		changed = true
	}

	return changed
}

// ToProto 将 SupplyInfo 转换成 Protobuf Snapshot
func (info *SupplyInfo) ToProto() *pb.SupplyInfoSnapshot {
	return &pb.SupplyInfoSnapshot{
		CirculatingSupply: info.circulatingSupply.Load(),
		TotalSupply:       info.totalSupply.Load(),
		MaxSupply:         info.maxSupply.Load(),
		UpdatedAtTs:       info.updatedAt.Load(),
		SupplyBurned:      info.supplyBurned.Load(),
	}
}

// NewSupplyInfoFromProto 根据 Protobuf Snapshot 新建 SupplyInfo
func NewSupplyInfoFromProto(p *pb.SupplyInfoSnapshot) *SupplyInfo {
	info := &SupplyInfo{}
	info.circulatingSupply.Store(p.CirculatingSupply)
	info.totalSupply.Store(p.TotalSupply)
	info.maxSupply.Store(p.MaxSupply)
	info.updatedAt.Store(p.UpdatedAtTs)
	info.supplyBurned.Store(p.SupplyBurned)
	return info
}
