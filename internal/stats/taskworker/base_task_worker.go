package taskworker

import (
	"context"
	"dex-stats-sol/internal/pkg/logger"
	"dex-stats-sol/internal/pkg/utils"
	"dex-stats-sol/internal/stats/types"
	"runtime/debug"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

// ExecutionPolicy 定义任务执行后的处理策略
type ExecutionPolicy uint8

const (
	RemoveAll       ExecutionPolicy = iota // 成功或失败都移除
	RemoveOnSuccess                        // 只有成功才移除，失败保留
)

const (
	initItemsCap       = 256  // items map 初始容量
	maxItemsForReset   = 1024 // map 超过该长度时重新分配
	warnItemsThreshold = 256  // map 长度接近该值时打印警告
)

type TaskResult[T any] struct {
	Item types.TokenTask
	Data T     // 成功返回的数据
	Err  error // nil 表示成功，否则失败原因
}

type TaskExecutor[T any] func(ctx context.Context, items []types.TokenTask) ([]TaskResult[T], error)
type TaskCallback[T any] func(results []TaskResult[T])

type BaseTaskWorker[T any] struct {
	name            string
	interval        time.Duration
	batchSize       int
	executor        TaskExecutor[T]
	callback        TaskCallback[T]
	executionPolicy ExecutionPolicy

	ctx    context.Context
	cancel context.CancelFunc

	items map[types.Pubkey]int64
	mu    sync.Mutex // 保护 items

	isPaused       atomic.Bool
	lastErrLogTime atomic.Int64
}

// NewBaseTaskWorker 创建 BaseTaskWorker
func NewBaseTaskWorker[T any](
	name string,
	interval time.Duration,
	batchSize int,
	executor TaskExecutor[T],
	callback TaskCallback[T],
	policy ExecutionPolicy,
) *BaseTaskWorker[T] {
	ctx, cancel := context.WithCancel(context.Background())
	worker := &BaseTaskWorker[T]{
		name:            name,
		interval:        interval,
		batchSize:       batchSize,
		executor:        executor,
		callback:        callback,
		executionPolicy: policy,
		ctx:             ctx,
		cancel:          cancel,
		items:           make(map[types.Pubkey]int64, initItemsCap),
	}
	worker.isPaused.Store(true)
	return worker
}

func (w *BaseTaskWorker[T]) Start() {
	ticker := time.NewTicker(w.interval)
	defer ticker.Stop()

	for {
		select {
		case <-w.ctx.Done():
			return

		case <-ticker.C:
			if w.isPaused.Load() {
				drainTicker(ticker)

				w.mu.Lock()
				utils.ClearOrResetMap(&w.items, maxItemsForReset, initItemsCap)
				w.mu.Unlock()
				continue
			}

			drainTicker(ticker)
			w.processBatch()
		}
	}
}

func (w *BaseTaskWorker[T]) Stop() {
	w.isPaused.Store(true)
	w.cancel()
}

func (w *BaseTaskWorker[T]) Resume() {
	w.isPaused.Store(false)
}

func (w *BaseTaskWorker[T]) Pause() {
	w.isPaused.Store(true)
}

func (w *BaseTaskWorker[T]) Add(tokens []types.TokenTask) {
	if w.isPaused.Load() {
		return
	}

	w.mu.Lock()
	defer w.mu.Unlock()

	// double-check
	if w.isPaused.Load() {
		return
	}

	for _, it := range tokens {
		if _, ok := w.items[it.Token]; !ok {
			w.items[it.Token] = it.TaskAtMs
		}
	}
}

func drainTicker(ticker *time.Ticker) {
	for {
		select {
		case <-ticker.C:
		default:
			return
		}
	}
}

// processBatch 执行批次任务
func (w *BaseTaskWorker[T]) processBatch() {
	// 先拷贝数据（只读锁，避免阻塞写）
	w.mu.Lock()
	allItems := make([]types.TokenTask, 0, len(w.items))
	for token, value := range w.items {
		allItems = append(allItems, types.TokenTask{
			Token:    token,
			TaskAtMs: value,
		})
	}
	w.mu.Unlock()

	n := len(allItems)
	if n == 0 {
		return
	}

	if utils.ThrottleLog(&w.lastErrLogTime, 3*time.Second) {
		logger.Infof("[BaseTaskWorker:%s] items size: %d", w.name, n)
	}

	if n > w.batchSize {
		n = w.batchSize
		sort.Slice(allItems, func(i, j int) bool {
			return allItems[i].TaskAtMs < allItems[j].TaskAtMs
		})
	}
	batch := allItems[:n]

	// 执行 executor 并记录耗时
	startExec := time.Now()
	results, err := w.executor(w.ctx, batch)
	durationExec := time.Since(startExec)
	if err != nil {
		if utils.ThrottleLog(&w.lastErrLogTime, 3*time.Second) {
			logger.Errorf("[BaseTaskWorker:%s] executor failed: %v, duration=%v", w.name, err, durationExec)
		}
		return
	}

	// 根据策略更新状态（写锁）
	var callbackDatas []TaskResult[T]
	w.mu.Lock()
	switch w.executionPolicy {
	case RemoveAll:
		callbackDatas = results
		for _, it := range batch {
			delete(w.items, it.Token)
		}
	case RemoveOnSuccess:
		callbackDatas = make([]TaskResult[T], 0, len(results))
		for _, r := range results {
			if r.Err == nil {
				callbackDatas = append(callbackDatas, r)
				delete(w.items, r.Item.Token)
			}
		}
	}
	w.mu.Unlock()

	if len(callbackDatas) > 0 && !w.isPaused.Load() {
		w.runCallback(callbackDatas)
	}
}

func (w *BaseTaskWorker[T]) runCallback(results []TaskResult[T]) {
	if w.callback == nil {
		return
	}

	start := time.Now()
	defer func() {
		if r := recover(); r != nil {
			logger.Errorf("[BaseTaskWorker:%s] callback panicked: %v\n%s", w.name, r, string(debug.Stack()))
		}
		duration := time.Since(start)
		logger.Debugf("[BaseTaskWorker:%s] callback duration=%v for batch size=%d", w.name, duration, len(results))
	}()

	w.callback(results)
}
