package poolworker

import (
	"dex-stats-sol/internal/stats/poolworker/pool"
	"dex-stats-sol/internal/stats/types"
	"sync"
)

const poolsExpireSeconds = 86400 + 900 // 池子过期时间：1天（86400秒） + 额外保留15分钟（900秒）

type PoolMap struct {
	mu              sync.RWMutex
	pools           map[types.Pubkey]*pool.Pool
	tokenAccountSet map[types.Pubkey]struct{}
}

func NewPoolMap() *PoolMap {
	return &PoolMap{
		pools:           make(map[types.Pubkey]*pool.Pool, 8192),
		tokenAccountSet: make(map[types.Pubkey]struct{}, 8192),
	}
}

func (pm *PoolMap) len() int {
	pm.mu.RLock()
	defer pm.mu.RUnlock()
	return len(pm.pools)
}

func (pm *PoolMap) getPool(key types.Pubkey) *pool.Pool {
	pm.mu.RLock()
	defer pm.mu.RUnlock()
	return pm.pools[key]
}

func (pm *PoolMap) getPools(keys []types.Pubkey) []*pool.Pool {
	pm.mu.RLock()
	defer pm.mu.RUnlock()

	result := make([]*pool.Pool, 0, len(keys))
	for _, key := range keys {
		if p, ok := pm.pools[key]; ok && p != nil {
			result = append(result, p)
		}
	}
	return result
}

func (pm *PoolMap) getPoolUnsafe(key types.Pubkey) *pool.Pool {
	return pm.pools[key]
}

func (pm *PoolMap) isPoolAccountUnsafe(tokenAccount types.Pubkey) bool {
	_, exists := pm.tokenAccountSet[tokenAccount]
	return exists
}

func (pm *PoolMap) addPool(pl *pool.Pool) {
	if pl == nil {
		return
	}
	pm.mu.Lock()
	defer pm.mu.Unlock()
	pm.pools[pl.Address] = pl
	pm.tokenAccountSet[pl.BaseTokenAccount] = struct{}{}
}

func (pm *PoolMap) addPools(pools []*pool.Pool) {
	if len(pools) == 0 {
		return
	}
	pm.mu.Lock()
	defer pm.mu.Unlock()
	for _, pl := range pools {
		pm.pools[pl.Address] = pl
		pm.tokenAccountSet[pl.BaseTokenAccount] = struct{}{}
	}
}

func (pm *PoolMap) cleanupExpiredPools(latestTime uint32) []*pool.Pool {
	cutoff := latestTime - poolsExpireSeconds
	expired := make([]*pool.Pool, 0, 128)

	// 系统保证写操作只在主线程，当前函数也只在主线程执行, 因此遍历可以无锁
	for _, pl := range pm.pools {
		if pl.LastSwapEventTs() < cutoff {
			expired = append(expired, pl)
		}
	}
	if len(expired) == 0 {
		return nil
	}

	pm.mu.Lock()
	defer pm.mu.Unlock()

	// 遍历 expired，删除对应的池子
	for _, p := range expired {
		delete(pm.pools, p.Address)
		delete(pm.tokenAccountSet, p.BaseTokenAccount)
	}
	return expired
}

func (pm *PoolMap) prepareSnapshot(dst *[]types.Serializable) {
	pm.mu.RLock()
	defer pm.mu.RUnlock()

	for _, p := range pm.pools {
		*dst = append(*dst, p)
	}
}
