package taskworker

import (
	"context"
	"dex-stats-sol/internal/pkg/logger"
	"dex-stats-sol/internal/pkg/utils"
	"dex-stats-sol/internal/stats/types"
	"dex-stats-sol/pb"
	"encoding/binary"
	"fmt"
	"github.com/blocto/solana-go-sdk/client"
	"runtime/debug"
	"sync/atomic"
	"time"
)

// TokenMetaRpcListener RPC TokenMeta 处理监听器
type TokenMetaRpcListener interface {
	OnTokenMetaRpcDone(results []TaskResult[*pb.TotalSupplyEvent])
}

// TokenMetaRpcWorker 批量处理 RPC TokenMeta 的 Worker
type TokenMetaRpcWorker struct {
	*BaseTaskWorker[*pb.TotalSupplyEvent]
	cli         *client.Client
	timeout     time.Duration
	listener    TokenMetaRpcListener
	lastLogTime atomic.Int64
}

const (
	tokenMetaRpcBatchSize       = 100
	tokenMetaRpcRequestInterval = 10 * time.Second
)

// NewTokenMetaRpcWorker 构造函数
func NewTokenMetaRpcWorker(
	endpoint string,
	timeoutMs int,
	listener TokenMetaRpcListener,
) *TokenMetaRpcWorker {
	worker := &TokenMetaRpcWorker{
		cli:      client.NewClient(endpoint),
		timeout:  time.Duration(timeoutMs) * time.Millisecond,
		listener: listener,
	}

	base := NewBaseTaskWorker[*pb.TotalSupplyEvent](
		"token_meta_rpc",
		tokenMetaRpcRequestInterval,
		tokenMetaRpcBatchSize,
		worker.execute,
		worker.handleResults,
		RemoveOnSuccess,
	)
	worker.BaseTaskWorker = base
	return worker
}

func (w *TokenMetaRpcWorker) handleResults(results []TaskResult[*pb.TotalSupplyEvent]) {
	if w.listener != nil {
		w.listener.OnTokenMetaRpcDone(results)
	}
}

func (w *TokenMetaRpcWorker) execute(ctx context.Context, items []types.TokenTask) (results []TaskResult[*pb.TotalSupplyEvent], err error) {
	defer func() {
		if r := recover(); r != nil {
			logger.Errorf("[TokenMetaRpcWorker] panic in execute: %v\n%s", r, debug.Stack())
			err = fmt.Errorf("panic: %v", r)
			results = nil
		}
	}()

	if len(items) == 0 {
		return nil, nil
	}

	// 构建 token 公钥列表
	tokens := make([]string, len(items))
	for i, item := range items {
		tokens[i] = item.Token.String()
	}

	// 设置超时上下文
	reqCtx, cancel := context.WithTimeout(ctx, w.timeout)
	defer cancel()

	// 批量获取账户信息
	infos, rpcErr := w.cli.GetMultipleAccounts(reqCtx, tokens)
	if rpcErr != nil {
		if utils.ThrottleLog(&w.lastLogTime, 3*time.Second) {
			logger.Errorf("[TokenMetaRpcWorker] GetMultipleAccounts failed: %v", rpcErr)
		}
		return nil, rpcErr
	}
	if len(infos) != len(items) {
		if utils.ThrottleLog(&w.lastLogTime, 3*time.Second) {
			logger.Errorf("[TokenMetaRpcWorker] account count mismatch: got %d, expected %d. tokens: %+v", len(infos), len(items), tokens)
		}
		return nil, fmt.Errorf("GetMultipleAccounts returned %d accounts, expected %d", len(infos), len(items))
	}

	results = make([]TaskResult[*pb.TotalSupplyEvent], len(items))
	for i, item := range items {
		supply, isBurned := parseSupplyFromRpc(infos[i].Data)
		data := &pb.TotalSupplyEvent{
			TokenAddress: item.Token[:],
			TotalSupply:  utils.Uint64ToStr(supply),
			IsBurned:     isBurned,
		}

		results[i] = TaskResult[*pb.TotalSupplyEvent]{Item: item, Data: data, Err: nil}
	}
	return results, nil
}

func parseSupplyFromRpc(data []byte) (uint64, bool) {
	// MintLayout 在 Solana SPL Token 中的偏移：
	// 0-3   : mintAuthorityOption (u32)
	// 4-35  : mintAuthority (PublicKey, 32 bytes)
	// 36-43 : supply (u64, little-endian)
	if len(data) < 44 { // 如果长度不足，视为已销毁
		return 0, true
	}

	// 提取 supply 字段（小端序）
	supply := binary.LittleEndian.Uint64(data[36:44])

	// 如果 supply 为 0，也认为是已销毁
	if supply == 0 {
		return 0, true
	}

	return supply, false
}
