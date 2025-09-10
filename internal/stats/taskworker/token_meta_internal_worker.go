package taskworker

import (
	"context"
	"dex-stats-sol/internal/pkg/logger"
	"dex-stats-sol/internal/pkg/nacos"
	"dex-stats-sol/internal/pkg/utils"
	"dex-stats-sol/internal/stats/types"
	"dex-stats-sol/pb"
	"fmt"
	"runtime/debug"
	"sync/atomic"
	"time"
)

// TokenMetaInternalListener 内部 TokenMeta 处理监听器
type TokenMetaInternalListener interface {
	OnTokenMetaInternalDone(results []TaskResult[*pb.SupplyInfo])
}

// TokenMetaInternalWorker 批量处理内部 TokenMeta 的 Worker
type TokenMetaInternalWorker struct {
	*BaseTaskWorker[*pb.SupplyInfo]
	nacosService *nacos.NacosRemoteService
	listener     TokenMetaInternalListener
	lastLogTime  atomic.Int64
}

const (
	tokenMetaBatchSize       = 50
	tokenMetaRequestInterval = time.Second
	tokenMetaRequestTimeout  = 20 * time.Second
)

// NewTokenMetaInternalWorker 构造函数
func NewTokenMetaInternalWorker(
	nacosService *nacos.NacosRemoteService,
	listener TokenMetaInternalListener,
) *TokenMetaInternalWorker {
	worker := &TokenMetaInternalWorker{
		nacosService: nacosService,
		listener:     listener,
	}

	base := NewBaseTaskWorker[*pb.SupplyInfo](
		"token_meta_internal",
		tokenMetaRequestInterval,
		tokenMetaBatchSize,
		worker.execute,
		worker.handleResults,
		RemoveAll,
	)
	worker.BaseTaskWorker = base
	return worker
}

func (w *TokenMetaInternalWorker) handleResults(results []TaskResult[*pb.SupplyInfo]) {
	if w.listener != nil {
		w.listener.OnTokenMetaInternalDone(results)
	}
}

func (w *TokenMetaInternalWorker) execute(ctx context.Context, items []types.TokenTask) (results []TaskResult[*pb.SupplyInfo], err error) {
	defer func() {
		if r := recover(); r != nil {
			logger.Errorf("[TokenInternalTaskWorker] panic in execute: %v\n%s", r, debug.Stack())
			err = fmt.Errorf("panic: %v", r)
			results = nil
		}
	}()

	if len(items) == 0 {
		return nil, nil
	}

	// 构建请求 token 列表
	tokens := make([]string, len(items))
	for i, item := range items {
		tokens[i] = item.Token.String()
	}
	req := &pb.SupplyRequest{TokenAddress: tokens}

	// 请求超时上下文
	reqCtx, cancel := context.WithTimeout(ctx, tokenMetaRequestTimeout)
	defer cancel()

	conn, err := w.nacosService.GetConn()
	if err != nil {
		if utils.ThrottleLog(&w.lastLogTime, 3*time.Second) {
			logger.Errorf("[TokenInternalTaskWorker] get gRPC connection failed: %v", err)
		}
		return nil, err
	}

	// 执行请求
	start := time.Now()
	client := pb.NewMetadataClient(conn)
	resp, err := client.GetTokenSupplyByAddress(reqCtx, req)
	if err != nil {
		if utils.ThrottleLog(&w.lastLogTime, 3*time.Second) {
			logger.Errorf("[TokenInternalTaskWorker] request failed: %v", err)
		}
		return nil, err
	}
	if resp == nil {
		return nil, fmt.Errorf("internal metadata service returned nil response")
	}

	if duration := time.Since(start); duration > tokenMetaRequestTimeout/2 {
		if utils.ThrottleLog(&w.lastLogTime, 3*time.Second) {
			logger.Warnf("[TokenInternalTaskWorker] request cost long time, duration=%v, token_count=%d", duration, len(tokens))
		}
	}

	// token -> SupplyInfo 映射
	tokenMap := make(map[types.Pubkey]*pb.SupplyInfo, len(resp.Infos))
	for _, r := range resp.Infos {
		tokenAddr, tokenErr := types.PubkeyFromBytes(r.TokenAddress)
		if tokenErr != nil {
			if utils.ThrottleLog(&w.lastLogTime, 3*time.Second) {
				logger.Warnf("[TokenInternalTaskWorker] invalid TokenAddress length=%d, token=%x", len(r.TokenAddress), r.TokenAddress)
			}
			continue
		}

		if utils.IsZeroStr(r.TotalSupply) {
			if utils.ThrottleLog(&w.lastLogTime, 3*time.Second) {
				logger.Warnf("[TokenInternalTaskWorker] totalSupply is 0, token=%s", tokenAddr)
			}
			continue
		}
		tokenMap[tokenAddr] = r
	}

	// 构建结果
	results = make([]TaskResult[*pb.SupplyInfo], len(items))
	for i, item := range items {
		if info, ok := tokenMap[item.Token]; ok {
			results[i] = TaskResult[*pb.SupplyInfo]{Item: item, Data: info, Err: nil}
		} else {
			if utils.ThrottleLog(&w.lastLogTime, 3*time.Second) {
				logger.Warnf("[TokenInternalTaskWorker] GetTokenSupplyByAddress error, token=%s", item.Token)
			}
			results[i] = TaskResult[*pb.SupplyInfo]{Item: item, Data: nil, Err: fmt.Errorf("no supply info for token")}
		}
	}

	return results, nil
}
