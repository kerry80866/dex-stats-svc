package rankingworker

import (
	"context"
	"dex-stats-sol/internal/pkg/logger"
	"dex-stats-sol/internal/pkg/utils"
	"dex-stats-sol/internal/stats/poolworker/pool"
	"dex-stats-sol/internal/stats/types"
	"runtime"
	"sync"
	"sync/atomic"
	"time"
)

const (
	MergeInterval = 3 * time.Second
)

type GlobalRankingListener interface {
	OnGlobalRankingComplete() // 全局排行榜合并完成时调用
}

type RankingWorker struct {
	inputChan           chan types.RankingResult[*pool.Pool]
	ctx                 context.Context
	cancel              context.CancelFunc
	rankingsByWorker    []types.RankingResult[*pool.Pool] // 长度 = workerCount
	workerCount         uint8
	lastMergeTime       time.Time                                            // 上一次合并的时间
	globalRanking       atomic.Pointer[types.RankingByDimension[*pool.Pool]] // 全局合并结果
	mergeExecutor       *utils.BlockingParallelExecutor                      // 用于并行合并
	listener            GlobalRankingListener                                // 用于回调的监听器
	lastThrottleLogTime atomic.Int64                                         // 限频日志用（纳秒）
}

func NewRankingWorker(workerCount uint8, listener GlobalRankingListener) *RankingWorker {
	ctx, cancel := context.WithCancel(context.Background())
	return &RankingWorker{
		inputChan:        make(chan types.RankingResult[*pool.Pool], 64),
		ctx:              ctx,
		cancel:           cancel,
		rankingsByWorker: make([]types.RankingResult[*pool.Pool], workerCount),
		workerCount:      workerCount,
		mergeExecutor:    utils.NewBlockingParallelExecutor(runtime.NumCPU()),
		listener:         listener,
	}
}

func (rw *RankingWorker) IsReady() bool {
	return rw.globalRanking.Load() != nil
}

func (rw *RankingWorker) GetRanking(key types.RankingKey) []types.RankingItem[*pool.Pool] {
	if globalRanking := rw.globalRanking.Load(); globalRanking != nil {
		return (*globalRanking)[key]
	}
	return nil
}

// Add 将结果写入 inputChan，队列满时限频打印并等待
func (rw *RankingWorker) Add(data types.RankingResult[*pool.Pool]) {
	for {
		select {
		case <-rw.ctx.Done():
			return
		case rw.inputChan <- data:
			return
		default:
			if utils.ThrottleLog(&rw.lastThrottleLogTime, 3*time.Second) {
				logger.Warnf("[RankingWorker] inputChan full (%d), waiting to add task for WorkerID=%d, BlockNumber=%d",
					len(rw.inputChan), data.WorkerID, data.BlockNumber)
			}
			time.Sleep(30 * time.Millisecond)
		}
	}
}

func (rw *RankingWorker) Start() {
	rw.loop()
}

func (rw *RankingWorker) Stop() {
	rw.cancel()
}

func (rw *RankingWorker) loop() {
	const interval = MergeInterval + 20*time.Millisecond
	timer := time.NewTimer(interval)
	defer timer.Stop()

	for {
		select {
		case <-rw.ctx.Done():
			return

		case result := <-rw.inputChan:
			// 批量读取 inputChan
			rw.rankingsByWorker[result.WorkerID] = result
			rw.drainInput()
			rw.tryMerge()
			rw.resetTimer(timer, interval)

		case <-timer.C:
			rw.tryMerge()
			rw.resetTimer(timer, interval)
		}
	}
}

func (rw *RankingWorker) drainInput() {
	for {
		select {
		case r := <-rw.inputChan:
			rw.rankingsByWorker[r.WorkerID] = r
		default:
			return
		}
	}
}

func (rw *RankingWorker) tryMerge() {
	if time.Since(rw.lastMergeTime) >= MergeInterval || rw.allBlockNumbersAligned() {
		rw.doMerge()
		rw.lastMergeTime = time.Now()
	}
}

func (rw *RankingWorker) resetTimer(timer *time.Timer, interval time.Duration) {
	elapsed := max(0, time.Since(rw.lastMergeTime))
	next := max(interval-elapsed, 0)

	// 安全停止旧定时器
	if !timer.Stop() {
		select {
		case <-timer.C:
		default:
		}
	}
	timer.Reset(next)
}

// 检查所有 worker 的 BlockNumber 是否一致
func (rw *RankingWorker) allBlockNumbersAligned() bool {
	var expected uint32
	for i, r := range rw.rankingsByWorker {
		if r.BlockNumber == 0 {
			// 还没收到这个 worker 的数据
			return false
		}
		if i == 0 {
			expected = r.BlockNumber
		} else if r.BlockNumber != expected {
			return false
		}
	}
	return true
}

func (rw *RankingWorker) doMerge() {
	// 将各个 worker 的排行榜数据按 RankingKey 分类
	rankingsGroupedByKey := make(map[types.RankingKey][][]types.RankingItem[*pool.Pool], getRankingKeyCount())
	for _, rankingResult := range rw.rankingsByWorker {
		for key, rankingList := range rankingResult.Rankings {
			if _, exists := rankingsGroupedByKey[key]; !exists {
				rankingsGroupedByKey[key] = make([][]types.RankingItem[*pool.Pool], 0, len(rw.rankingsByWorker))
			}
			rankingsGroupedByKey[key] = append(rankingsGroupedByKey[key], rankingList)
		}
	}
	if len(rankingsGroupedByKey) == 0 {
		logger.Infof("[RankingWorker] doMerge: no rankings to merge")
		return
	}

	// 初始化全局排行榜
	newGlobalRanking := make(types.RankingByDimension[*pool.Pool], getRankingKeyCount())
	var mu sync.Mutex
	var wg sync.WaitGroup

	// 并行合并各维度的排行榜
	startTime := time.Now()
	for key, lists := range rankingsGroupedByKey {
		keyCopy := key

		wg.Add(1)
		rw.mergeExecutor.Submit(func() {
			defer wg.Done()

			// 合并排行榜数据
			merged := mergeSortedRankings(getCompareFunc(keyCopy), lists)

			mu.Lock()
			newGlobalRanking[keyCopy] = merged
			mu.Unlock()
		})
	}

	wg.Wait()
	logger.Infof("[RankingWorker] doMerge, cost: %v", time.Since(startTime))

	// 更新全局排行榜
	rw.globalRanking.Store(&newGlobalRanking)

	if rw.listener != nil {
		rw.listener.OnGlobalRankingComplete()
	}
}
