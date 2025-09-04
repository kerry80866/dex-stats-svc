package token

import (
	"dex-stats-sol/internal/stats/poolworker/pool"
	"dex-stats-sol/internal/stats/types"
	"sync"
)

type TokenMap struct {
	mu     sync.RWMutex
	tokens map[types.Pubkey]*TokenInfo
}

func NewTokenMap() *TokenMap {
	return &TokenMap{
		tokens: make(map[types.Pubkey]*TokenInfo, 8192),
	}
}

func (tm *TokenMap) GetTokenMapUnsafe() map[types.Pubkey]*TokenInfo {
	return tm.tokens
}

func (tm *TokenMap) GetTokenInfo(tokenAddress types.Pubkey) *TokenInfo {
	tm.mu.Lock()
	defer tm.mu.Unlock()

	return tm.tokens[tokenAddress]
}

func (tm *TokenMap) GetTokenInfoUnsafe(tokenAddress types.Pubkey) *TokenInfo {
	return tm.tokens[tokenAddress]
}

func (tm *TokenMap) RestoreToken(info *TokenInfo) {
	tm.mu.Lock()
	defer tm.mu.Unlock()

	tm.tokens[info.Token] = info
}

func (tm *TokenMap) Reset() {
	tm.mu.Lock()
	defer tm.mu.Unlock()

	l := max(len(tm.tokens), 8192)
	tm.tokens = make(map[types.Pubkey]*TokenInfo, l)
}

func (tm *TokenMap) AddPool(pl *pool.Pool) {
	tm.mu.Lock()
	defer tm.mu.Unlock()

	tm.addPoolUnsafe(pl)
}

func (tm *TokenMap) AddPools(pools []*pool.Pool) {
	tm.mu.Lock()
	defer tm.mu.Unlock()

	for _, pl := range pools {
		tm.addPoolUnsafe(pl)
	}
}

func (tm *TokenMap) addPoolUnsafe(pl *pool.Pool) {
	if pl != nil {
		if info, ok := tm.tokens[pl.BaseToken]; ok {
			info.addPool(pl)
		} else {
			tm.tokens[pl.BaseToken] = NewTokenInfo(pl)
		}
	}
}

func (tm *TokenMap) RemovePools(pools []*pool.Pool) {
	tm.mu.Lock()
	defer tm.mu.Unlock()

	for _, pl := range pools {
		t, ok := tm.tokens[pl.BaseToken]
		if !ok {
			continue
		}

		t.removePool(pl)
		if t.isEmpty() {
			delete(tm.tokens, pl.BaseToken)
		}
	}
}

func (tm *TokenMap) PrepareSnapshot(dst *[]types.Serializable) {
	tm.mu.Lock()
	defer tm.mu.Unlock()

	for _, item := range tm.tokens {
		*dst = append(*dst, item)
	}
}
