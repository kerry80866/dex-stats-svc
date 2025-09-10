package token

import (
	"dex-stats-sol/internal/pkg/utils"
	ea "dex-stats-sol/internal/stats/eventadapter"
	"dex-stats-sol/internal/stats/poolworker/defs"
	"dex-stats-sol/internal/stats/poolworker/pool"
	"dex-stats-sol/internal/stats/poolworker/shared"
	"dex-stats-sol/internal/stats/types"
	"dex-stats-sol/pb"
	"fmt"
	"sync"
)

// ---------------------- 类型定义 ----------------------

// TokenInfo 管理某个 Token 对应的所有 pool 以及共享的 SupplyInfo
type TokenInfo struct {
	mu                sync.Mutex // 保护共享状态
	Token             types.Pubkey
	Decimals          uint8
	WorkerID          uint8
	singlePool        *pool.Pool
	multiPools        map[types.Pubkey]*pool.Pool
	poolAccountMap    map[types.Pubkey]struct{}
	SharedSupply      *shared.SupplyInfo
	SharedHolderCount *shared.HolderCount
	SharedTopHolders  *shared.TopHolders
}

// ---------------------- 构造函数 ----------------------

// NewTokenInfo 创建一个 TokenInfo 实例，初始包含一个 pool
func NewTokenInfo(pl *pool.Pool) *TokenInfo {
	return &TokenInfo{
		Token:             pl.BaseToken,
		Decimals:          pl.BaseDecimals,
		WorkerID:          pl.WorkerID,
		singlePool:        pl,
		multiPools:        nil,
		SharedSupply:      pl.SharedSupply,
		SharedHolderCount: pl.SharedHolderCount,
		SharedTopHolders:  pl.SharedTopHolders,
		poolAccountMap:    make(map[types.Pubkey]struct{}),
	}
}

// ---------------------- 序列化 / 反序列化 ----------------------

// Serialize 将 TokenInfo 转换成 Protobuf + 前缀信息，线程安全
func (t *TokenInfo) Serialize(buf []byte) ([]byte, error) {
	t.mu.Lock()
	defer t.mu.Unlock()

	// 构建 TokenSnapshot
	snapshot := &pb.TokenSnapshot{
		Token:       t.Token[:],
		Decimals:    uint32(t.Decimals),
		SupplyInfo:  t.SharedSupply.ToProto(),
		HolderCount: t.SharedHolderCount.ToProto(),
		TopHolders:  t.SharedTopHolders.ToProto(),
	}

	buf = append(buf, byte(t.WorkerID), byte(defs.SnapshotTypeTokenData))
	return utils.SafeProtoMarshal(buf, snapshot)
}

// DeserializeTokenInfo 从字节数据反序列化 TokenInfo
// 注意：singlePool / multiPools 由上层 Worker 或 Manager 初始化
func DeserializeTokenInfo(workerID uint8, data []byte) (*TokenInfo, error) {
	if len(data) < 2 {
		return nil, fmt.Errorf("[PoolWorker:%d] data too short: got %d bytes, expect at least 2", workerID, len(data))
	}

	snapshotType := data[1]
	if snapshotType != defs.SnapshotTypeTokenData {
		return nil, fmt.Errorf("[PoolWorker:%d] invalid snapshot type: expected %d, got %d",
			workerID, defs.SnapshotTypeTokenData, snapshotType)
	}

	// 反序列化 protobuf
	ps := &pb.TokenSnapshot{}
	if err := utils.SafeProtoUnmarshal(data[2:], ps); err != nil {
		return nil, fmt.Errorf("[PoolWorker:%d] failed to unmarshal TokenSnapshot: %w", workerID, err)
	}

	var token types.Pubkey
	copy(token[:], ps.Token)

	t := &TokenInfo{
		Token:             token,
		Decimals:          uint8(ps.Decimals),
		WorkerID:          workerID,
		SharedSupply:      shared.NewSupplyInfoFromProto(ps.SupplyInfo),
		SharedHolderCount: shared.NewHolderCountFromProto(ps.HolderCount),
		SharedTopHolders:  shared.NewTopHoldersFromProto(ps.TopHolders),
		// singlePool/multiPools 由上层初始化
	}

	return t, nil
}

// ---------------------- 公有接口 ----------------------

func (t *TokenInfo) ShouldRequestSupply(isStrictMode bool) bool {
	return t.SharedSupply.ShouldRequest(isStrictMode)
}

func (t *TokenInfo) ShouldRequestHolderCount(isStrictMode bool) bool {
	topHoldersCount := t.SharedTopHolders.TopHoldersCount()
	return t.SharedHolderCount.ShouldRequest(isStrictMode, topHoldersCount)
}

func (t *TokenInfo) ShouldRequestTopHolders(isStrictMode bool) bool {
	return t.SharedTopHolders.ShouldRequest(isStrictMode)
}

func (t *TokenInfo) HasLeverage() bool {
	if t.singlePool != nil {
		return t.singlePool.HasLeverage()
	}
	for _, pl := range t.multiPools {
		if pl.HasLeverage() {
			return true
		}
	}
	return false
}

// UpdateTotalSupply 更新 totalSupply，并把更新的 pool 写入 updatedPools
func (t *TokenInfo) UpdateTotalSupply(
	totalSupply float64,
	isBurned bool, updatedAt uint32,
	updatedPools map[types.Pubkey]struct{},
) {
	t.updateSupply(func(s *shared.SupplyInfo) bool {
		return s.UpdateTotalSupply(totalSupply, isBurned, updatedAt)
	}, updatedPools)
}

// UpdateSupplyInfo 更新完整 SupplyInfo，并把更新的 pool 写入 updatedPools
func (t *TokenInfo) UpdateSupplyInfo(
	totalSupply, circulatingSupply, maxSupply float64,
	isBurned bool, updatedAt uint32,
	updatedPools map[types.Pubkey]struct{},
) {
	t.updateSupply(func(s *shared.SupplyInfo) bool {
		return s.UpdateSupplyInfo(totalSupply, circulatingSupply, maxSupply, isBurned, updatedAt)
	}, updatedPools)
}

// UpdateHolderBase 更新基准持有人数和对应时间戳，同时重置增量
func (t *TokenInfo) UpdateHolderBase(
	baseCount int32,
	baseTs uint32,
	updatedPools map[types.Pubkey]struct{},
) {
	t.mu.Lock()
	updated := t.SharedHolderCount.ApplyBase(baseCount, baseTs)
	t.mu.Unlock()

	if updated {
		t.forEachPool(func(pl *pool.Pool) {
			pl.UpdateHolderCount()
			if updatedPools != nil {
				updatedPools[pl.Address] = struct{}{}
			}
		})
	}
}

// UpdateHolderChangeEvent 处理 Holder 数量变动事件
func (t *TokenInfo) UpdateHolderChangeEvent(
	blockNumber uint32,
	holderChange *pb.TokenHolderChange,
	updatedPools map[types.Pubkey]struct{},
) {
	t.mu.Lock()
	updated := t.SharedHolderCount.ApplyDelta(holderChange.HolderDelta, blockNumber)
	t.mu.Unlock()

	if updated {
		t.forEachPool(func(pl *pool.Pool) {
			pl.UpdateHolderCount()
			if updatedPools != nil {
				updatedPools[pl.Address] = struct{}{}
			}
		})
	}
}

// SyncTopHolders 使用全量账户信息同步 TopHolders
func (t *TokenInfo) SyncTopHolders(infos []*ea.AccountBalanceInfo, updatedPools map[types.Pubkey]struct{}) {
	t.mu.Lock()
	top10Changed := t.SharedTopHolders.SyncTopHolders(infos)
	t.mu.Unlock()

	if top10Changed {
		t.forEachPool(func(pl *pool.Pool) {
			pl.UpdateTop10HolderRatio()
			if updatedPools != nil {
				updatedPools[pl.Address] = struct{}{}
			}
		})
	}
}

// UpdateTopHolders 更新 token 的 top holders 列表
func (t *TokenInfo) UpdateTopHolders(
	blockNumber uint32,
	infos []*ea.AccountBalanceInfo,
	updatedPools map[types.Pubkey]struct{},
) bool {
	t.mu.Lock()
	top10Changed, syncRequired := t.SharedTopHolders.UpdateTopHolders(blockNumber, infos, t.poolAccountMap)
	t.mu.Unlock()

	if top10Changed {
		t.forEachPool(func(pl *pool.Pool) {
			pl.UpdateTop10HolderRatio()
			if updatedPools != nil {
				updatedPools[pl.Address] = struct{}{}
			}
		})
	}

	return syncRequired
}

// ---------------------- 内部方法 ----------------------

// updateSupply 通用的供应量更新逻辑（模板方法）
func (t *TokenInfo) updateSupply(updateFunc func(*shared.SupplyInfo) bool, updatedPools map[types.Pubkey]struct{}) {
	t.mu.Lock()
	updated := updateFunc(t.SharedSupply)
	t.mu.Unlock()

	if updated {
		t.forEachPool(func(pl *pool.Pool) {
			pl.UpdateMarketCapFDV()
			pl.UpdateTop10HolderRatio()
			if updatedPools != nil {
				updatedPools[pl.Address] = struct{}{}
			}
		})
	}
}

// addPool 内部方法：添加一个 pool
func (t *TokenInfo) addPool(pl *pool.Pool) {
	t.mu.Lock()
	defer t.mu.Unlock()

	t.poolAccountMap[pl.BaseTokenAccount] = struct{}{}

	if t.multiPools != nil {
		t.multiPools[pl.Address] = pl
		return
	}

	if t.singlePool == nil || t.singlePool.Address == pl.Address {
		t.singlePool = pl
		return
	}

	// 升级为 multi map
	t.multiPools = make(map[types.Pubkey]*pool.Pool, 4)
	t.multiPools[t.singlePool.Address] = t.singlePool
	t.multiPools[pl.Address] = pl
	t.singlePool = nil
}

// removePool 内部方法：删除一个 pool
func (t *TokenInfo) removePool(pl *pool.Pool) {
	t.mu.Lock()
	defer t.mu.Unlock()

	if t.multiPools != nil {
		delete(t.multiPools, pl.Address)
	} else if t.singlePool != nil && t.singlePool.Address == pl.Address {
		t.singlePool = nil
	}
}

// isEmpty 内部方法：判断是否没有任何 pool
func (t *TokenInfo) isEmpty() bool {
	return t.singlePool == nil && len(t.multiPools) == 0
}

// forEachPool 遍历所有 pool 执行 fn
func (t *TokenInfo) forEachPool(fn func(*pool.Pool)) {
	if t.singlePool != nil {
		fn(t.singlePool)
		return
	}
	for _, pl := range t.multiPools {
		fn(pl)
	}
}
