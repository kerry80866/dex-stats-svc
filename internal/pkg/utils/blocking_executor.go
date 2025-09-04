package utils

import "sync"

type BlockingParallelExecutor struct {
	taskCh chan func()
	wg     sync.WaitGroup
}

// NewBlockingParallelExecutor 创建一个执行器，指定并行工作协程数。
// taskCh 无缓冲，保证 Submit 是阻塞式的：提交者会等待直到任务被 worker 接收。
func NewBlockingParallelExecutor(workerNum int) *BlockingParallelExecutor {
	exec := &BlockingParallelExecutor{
		taskCh: make(chan func()), // 无缓冲，保证租塞式提交
	}

	for i := 0; i < workerNum; i++ {
		exec.wg.Add(1)
		go func() {
			defer exec.wg.Done()
			for task := range exec.taskCh {
				task() // worker 执行任务
			}
		}()
	}

	return exec
}

// Submit 阻塞式提交一个任务，直到任务被某个 worker 接收
func (exec *BlockingParallelExecutor) Submit(task func()) {
	exec.taskCh <- task
}

// Close 关闭执行器，等待所有工作协程退出
func (exec *BlockingParallelExecutor) Close() {
	close(exec.taskCh)
	exec.wg.Wait()
}
