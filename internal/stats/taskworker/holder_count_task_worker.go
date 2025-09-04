package taskworker

import (
	"context"
	"dex-stats-sol/internal/pkg/logger"
	"dex-stats-sol/internal/pkg/nacos"
	"dex-stats-sol/internal/pkg/utils"
	"dex-stats-sol/internal/stats/types"
	"dex-stats-sol/pb"
	"errors"
	"fmt"
	"github.com/panjf2000/ants/v2"
	"runtime/debug"
	"sync"
	"sync/atomic"
	"time"
)

const (
	holderCountBatchSize       = 10
	holderCountRequestInterval = time.Second      // 批量请求间隔
	holderCountRequestTimeout  = 30 * time.Second // 单条请求超时时间
)

// HolderCountListener 外部监听器接口
type HolderCountListener interface {
	OnHolderCountDone(results []TaskResult[uint64])
}

// HolderCountTaskWorker 批量处理 Holder Count 的 Worker
type HolderCountTaskWorker struct {
	*BaseTaskWorker[uint64]
	goroutinePool *ants.Pool
	nacosService  *nacos.NacosRemoteService
	listener      HolderCountListener
	lastLogTime   atomic.Int64
}

// NewHolderCountTaskWorker 构造函数，使用非阻塞 ants 池
func NewHolderCountTaskWorker(
	nacosService *nacos.NacosRemoteService,
	listener HolderCountListener,
) *HolderCountTaskWorker {
	pool, _ := ants.NewPool(holderCountBatchSize, ants.WithNonblocking(true))

	worker := &HolderCountTaskWorker{
		goroutinePool: pool,
		nacosService:  nacosService,
		listener:      listener,
	}

	base := NewBaseTaskWorker[uint64](
		"holder_count",
		holderCountRequestInterval,
		holderCountBatchSize,
		worker.executeBatch,
		worker.handleResults,
		RemoveOnSuccess,
	)
	worker.BaseTaskWorker = base
	return worker
}

// 批量任务完成后的回调，通知外部 listener
func (w *HolderCountTaskWorker) handleResults(results []TaskResult[uint64]) {
	if w.listener != nil {
		w.listener.OnHolderCountDone(results)
	}
}

// 执行批量任务，支持非阻塞提交
func (w *HolderCountTaskWorker) executeBatch(ctx context.Context, items []types.TokenTask) ([]TaskResult[uint64], error) {
	results := make([]TaskResult[uint64], len(items))
	var wg sync.WaitGroup

	for i, item := range items {
		idx := i
		it := item

		wg.Add(1)
		submitTask := func() {
			defer wg.Done()
			data, err := w.doRequest(ctx, it)
			results[idx] = TaskResult[uint64]{Item: it, Data: data, Err: err}
		}

		if err := w.goroutinePool.Submit(submitTask); err != nil {
			wg.Done()
			results[idx] = TaskResult[uint64]{Item: it, Data: 0, Err: err}
			logger.Errorf("[HolderCountTaskWorker] Submit task failed: %v, token=%v", err, it.Token)
		}
	}

	wg.Wait()
	return results, nil
}

// 执行单条请求
func (w *HolderCountTaskWorker) doRequest(ctx context.Context, item types.TokenTask) (count uint64, err error) {
	start := time.Now()

	defer func() {
		if r := recover(); r != nil {
			logger.Errorf("[HolderCountTaskWorker] panic in doRequest, token=%s: %v\n%s", item.Token, r, debug.Stack())
			err = fmt.Errorf("panic: %v", r)
			count = 0
		}
	}()

	reqCtx, cancel := context.WithTimeout(ctx, holderCountRequestTimeout)
	defer cancel()

	conn, err := w.nacosService.GetConn()
	if err != nil {
		if utils.ThrottleLog(&w.lastLogTime, 3*time.Second) {
			logger.Errorf("[HolderCountTaskWorker] get gRPC connection failed, token=%s: %v", item.Token, err)
		}
		return 0, err
	}

	req := &pb.TokenReq{
		TokenAddress: item.Token.String(),
	}

	client := pb.NewIngestQueryServiceClient(conn)
	resp, err := client.QueryHolderCountByToken(reqCtx, req)
	if err != nil {
		if utils.ThrottleLog(&w.lastLogTime, 3*time.Second) {
			if errors.Is(err, context.DeadlineExceeded) {
				logger.Warnf("[HolderCountTaskWorker] request timed out, token=%s, duration=%v", item.Token, time.Since(start))
			} else {
				logger.Errorf("[HolderCountTaskWorker] request failed, token=%s: %v", item.Token, err)
			}
		}
		return 0, err
	}

	duration := time.Since(start)
	if duration > holderCountRequestTimeout/2 && utils.ThrottleLog(&w.lastLogTime, 3*time.Second) {
		logger.Warnf("[HolderCountTaskWorker] request cost long time, token=%s, duration=%v", item.Token, duration)
	}

	return resp.Count, nil
}
