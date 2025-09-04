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
	topHoldersBatchSize       = 10
	topHoldersRequestInterval = time.Second      // 批量请求间隔
	topHoldersRequestTimeout  = 30 * time.Second // 单条请求超时时间
	topHoldersLimit           = 50               // 每次请求 Top N 的数量
)

// TopHoldersListener 外部监听器接口
type TopHoldersListener interface {
	OnTopHoldersDone(results []TaskResult[[]*pb.Balance])
}

// TopHoldersTaskWorker 批量处理 TopHolders 的 Worker
type TopHoldersTaskWorker struct {
	*BaseTaskWorker[[]*pb.Balance]
	goroutinePool *ants.Pool
	nacosService  *nacos.NacosRemoteService
	listener      TopHoldersListener
	lastLogTime   atomic.Int64
}

// NewTopHoldersTaskWorker 构造函数，使用非阻塞 ants 池
func NewTopHoldersTaskWorker(
	nacosService *nacos.NacosRemoteService,
	listener TopHoldersListener,
) *TopHoldersTaskWorker {
	pool, _ := ants.NewPool(topHoldersBatchSize, ants.WithNonblocking(true))

	worker := &TopHoldersTaskWorker{
		goroutinePool: pool,
		nacosService:  nacosService,
		listener:      listener,
	}

	worker.BaseTaskWorker = NewBaseTaskWorker[[]*pb.Balance](
		"top_holders",
		topHoldersRequestInterval,
		topHoldersBatchSize,
		worker.executeBatch,
		worker.handleResults,
		RemoveOnSuccess,
	)
	return worker
}

// 批量任务完成后的回调，通知外部 listener
func (w *TopHoldersTaskWorker) handleResults(results []TaskResult[[]*pb.Balance]) {
	if w.listener != nil {
		w.listener.OnTopHoldersDone(results)
	}
}

// 执行批量任务，支持非阻塞提交
func (w *TopHoldersTaskWorker) executeBatch(ctx context.Context, items []types.TokenTask) ([]TaskResult[[]*pb.Balance], error) {
	results := make([]TaskResult[[]*pb.Balance], len(items))
	var wg sync.WaitGroup

	for i, item := range items {
		idx := i
		it := item

		wg.Add(1) // 先加计数
		submitTask := func() {
			defer wg.Done()
			data, err := w.doRequest(ctx, it)
			results[idx] = TaskResult[[]*pb.Balance]{Item: it, Data: data, Err: err}
		}

		// 非阻塞提交
		if err := w.goroutinePool.Submit(submitTask); err != nil {
			// 提交失败，立即完成计数
			wg.Done()
			results[idx] = TaskResult[[]*pb.Balance]{Item: it, Data: nil, Err: err}
			logger.Errorf("[TopHoldersTaskWorker] Submit task failed: %v, token=%v", err, it.Token)
		}
	}

	wg.Wait()
	return results, nil
}

// 执行单条请求
func (w *TopHoldersTaskWorker) doRequest(ctx context.Context, item types.TokenTask) (holders []*pb.Balance, err error) {
	start := time.Now() // 记录开始时间

	defer func() {
		if r := recover(); r != nil {
			logger.Errorf("[TopHoldersTaskWorker] panic in doRequest, token=%s: %v\n%s", item.Token, r, debug.Stack())
			err = fmt.Errorf("panic: %v", r)
			holders = nil
		}
	}()

	// 设置单条请求超时
	reqCtx, cancel := context.WithTimeout(ctx, topHoldersRequestTimeout)
	defer cancel()

	conn, err := w.nacosService.GetConn()
	if err != nil {
		if utils.ThrottleLog(&w.lastLogTime, 3*time.Second) {
			logger.Errorf("[TopHoldersTaskWorker] get gRPC connection failed, token=%s: %v", item.Token, err)
		}
		return nil, err
	}

	limit := uint32(topHoldersLimit)
	req := &pb.TokenTopReq{
		TokenAddress: item.Token.String(),
		Limit:        &limit,
	}

	client := pb.NewIngestQueryServiceClient(conn)
	response, err := client.QueryTopTokenAccountsByToken(reqCtx, req)
	if err != nil {
		if utils.ThrottleLog(&w.lastLogTime, 3*time.Second) {
			if errors.Is(err, context.DeadlineExceeded) {
				// 请求超时
				logger.Warnf("[TopHoldersTaskWorker] request timed out, token=%s, duration=%v", item.Token, time.Since(start))
			} else {
				// 普通错误
				logger.Errorf("[TopHoldersTaskWorker] request failed, token=%s: %v", item.Token, err)
			}
		}
		return nil, err
	}

	// 如果执行时间过长，发出警告
	duration := time.Since(start)
	if duration > topHoldersRequestTimeout/2 && utils.ThrottleLog(&w.lastLogTime, 3*time.Second) {
		logger.Warnf("[TopHoldersTaskWorker] request cost long time, token=%s, duration=%v", item.Token, duration)
	}

	return response.Balances, nil
}
