package raft

import (
	"bytes"
	"context"
	"dex-stats-sol/internal/pkg/logger"
	"dex-stats-sol/internal/pkg/raft"
	"dex-stats-sol/internal/stats/types"
	"encoding/binary"
	"errors"
	"fmt"
	"github.com/lni/dragonboat/v3"
	"github.com/lni/dragonboat/v3/raftio"
	sm "github.com/lni/dragonboat/v3/statemachine"
	"io"
	"runtime/debug"
	"sync"
	"sync/atomic"
	"time"
)

const (
	magicHeader          = "PBUF"           // 快照文件头标识
	magicHeaderSize      = len(magicHeader) // Header 长度
	maxPayloadErrorCount = 10               // 最大允许的错误 payload 数量
	payloadLengthSize    = 4                // 每个 payload 的长度字段（uint32）
	payloadCrcSize       = 4                // CRC 校验字段长度(CRC32)

	initialPayloadBufSize = 2 * 1024 * 1024   // 初始 payload 缓冲区大小：2MB
	maxPayloadBufSize     = 128 * 1024 * 1024 // 最大 payload 缓冲区大小：128MB
)

type Raft struct {
	manager  *raft.RaftManager
	listener RaftListener

	isReady  atomic.Bool // 是否已经 Ready
	isLeader atomic.Bool

	checkingReadyOnce sync.Once // 只触发一次检查
	becameLeaderOnce  sync.Once // 确保 leaderLoop 只启动一次
}

func NewRaft(config *raft.RaftConfig, listener RaftListener) *Raft {
	if listener == nil {
		panic("[Raft] listener must not be nil")
	}
	r := &Raft{listener: listener}
	var err error
	r.manager, err = raft.NewRaftManager(config, r, nil) // 注册两个监听器接口
	if err != nil {
		panic(err)
	}
	return r
}

func (r *Raft) IsReady() bool {
	return r.isReady.Load()
}

func (r *Raft) IsLeader() bool {
	return r.isLeader.Load()
}

func (r *Raft) Start() {
	err := r.manager.Start(func(clusterID, nodeID uint64) sm.IConcurrentStateMachine {
		return r
	})
	if err != nil {
		logger.Errorf("[Raft] failed to start: %v", err)
		panic(err)
	}
}

func (r *Raft) Stop() {
	r.manager.Stop()
}

func (r *Raft) Submit(data []byte) error {
	if len(data) == 0 {
		return errors.New("submit: data must not be empty")
	}

	start := time.Now()
	deadline := start.Add(r.manager.Config.SubmitDeadline)
	backoff := 50 * time.Millisecond
	maxBackoff := 500 * time.Millisecond

	node := r.manager.NodeHost
	clusterID := uint64(r.manager.Config.ClusterID)

	for {
		// 设置提交超时
		ctx, cancel := context.WithTimeout(context.Background(), r.manager.Config.SubmitTimeout)
		_, err := node.SyncPropose(ctx, node.GetNoOPSession(clusterID), data)
		cancel()

		if err == nil {
			elapsed := time.Since(start)
			logger.Infof("[Raft] Submit succeeded, cost=%s, isLeader: %v", elapsed, r.IsLeader())
			return nil
		}

		// --------------------
		// 不可重试错误 → panic
		// 这些错误一般是永久性或不可恢复的，继续重试无意义
		// --------------------
		if errors.Is(err, dragonboat.ErrInvalidSession) || // Session 无效
			errors.Is(err, dragonboat.ErrClosed) || // NodeHost 已关闭
			errors.Is(err, dragonboat.ErrClusterNotFound) || // 集群不存在
			errors.Is(err, dragonboat.ErrPayloadTooBig) || // 数据太大
			errors.Is(err, dragonboat.ErrClusterClosed) { // 集群已关闭
			panic(fmt.Sprintf("[Raft] unrecoverable error after %s: %v", time.Since(start), err))
		}

		// --------------------
		// 超过 deadline → panic
		// --------------------
		if time.Now().After(deadline) {
			panic(fmt.Errorf("[Raft] submit timeout after %s: last error: %w", r.manager.Config.SubmitDeadline, err))
		}

		// --------------------
		// 可重试错误 → 记录日志并重试
		// 包括：ErrSystemBusy, ErrClusterNotReady, ErrTimeout, ErrAborted, ErrRejected 等
		// --------------------
		logger.Warnf("[Raft] submit failed, will retry: %v, elapsed=%s", err, time.Since(start))
		time.Sleep(backoff)

		// 指数退避
		backoff = min(backoff*2, maxBackoff)
	}
}

func (r *Raft) LeaderUpdated(info raftio.LeaderInfo) {
	logger.Infof("[Raft] LeaderUpdated: clusterID=%d, nodeID=%d, leaderID=%d",
		info.ClusterID, info.NodeID, info.LeaderID)

	if info.LeaderID == info.NodeID {
		r.becameLeaderOnce.Do(func() {
			go r.leaderLoop() // 启动一次 leader 检查循环
		})
	} else {
		if r.isLeader.CompareAndSwap(true, false) {
			logger.Infof("[Raft] This node became Follower, call OnBecameRaftFollower")
			r.listener.OnBecameRaftFollower(true)
		} else {
			r.listener.OnBecameRaftFollower(false)
		}
	}
}

func (r *Raft) Update(entries []sm.Entry) ([]sm.Entry, error) {
	r.checkingReadyOnce.Do(func() {
		go r.checkReadyLoop()
	})

	for i := range entries {
		data := entries[i].Cmd
		if len(data) == 0 {
			continue
		}
		err := r.listener.OnRaftDataUpdated(data)
		if err != nil {
			logger.Errorf("[Raft] OnRaftDataUpdated failed: %v", err)
			return nil, err
		}
	}

	return entries, nil
}

func (r *Raft) PrepareSnapshot() (interface{}, error) {
	start := time.Now()

	items, err := r.listener.OnPrepareSnapshot()
	elapsed := time.Since(start) // 计算耗时

	if err != nil {
		logger.Errorf("[Raft] OnPrepareSnapshot failed: %v, cost %s", err, elapsed)
		return nil, err
	}

	logger.Infof("[Raft] PrepareSnapshot completed, cost %s, items count: %d", elapsed, len(items))
	return items, nil
}

func (r *Raft) SaveSnapshot(data interface{}, w io.Writer, _ sm.ISnapshotFileCollection, stopc <-chan struct{}) error {
	start := time.Now() // 开始计时

	// 写入 magic header
	if _, err := w.Write([]byte(magicHeader)); err != nil {
		return errorfWithLog("SaveSnapshot: failed to write magic header: %v", err)
	}

	nested, ok := data.([][]types.Serializable)
	if !ok {
		return errorfWithLog("SaveSnapshot: unexpected snapshot data type: %T", data)
	}

	var (
		lengthBuf  = make([]byte, payloadLengthSize)
		crcBuf     = make([]byte, payloadCrcSize)
		payloadBuf = make([]byte, initialPayloadBufSize)
		totalCount int
	)
	for threadIdx, items := range nested {
		for i, item := range items {
			select {
			case <-stopc:
				logger.Warnf("[SaveSnapshot] aborted at thread %d, item %d", threadIdx, i)
				return sm.ErrSnapshotStopped
			default:
			}

			payload, err := item.Serialize(payloadBuf[:0])
			if err != nil {
				return errorfWithLog("SaveSnapshot: failed to serialize thread %d item %d: %v", threadIdx, i, err)
			}

			payloadLen := uint32(len(payload))
			if payloadLen == 0 {
				continue
			}

			binary.BigEndian.PutUint32(lengthBuf, payloadLen)
			if _, err := w.Write(lengthBuf); err != nil {
				return errorfWithLog("SaveSnapshot: write length failed at thread %d item %d: %v", threadIdx, i, err)
			}
			if _, err := w.Write(payload); err != nil {
				return errorfWithLog("SaveSnapshot: write payload failed at thread %d item %d: %v", threadIdx, i, err)
			}

			crc := crc32Checksum(payload)
			binary.BigEndian.PutUint32(crcBuf, crc)
			if _, err := w.Write(crcBuf); err != nil {
				return errorfWithLog("SaveSnapshot: write crc failed at thread %d item %d: %v", threadIdx, i, err)
			}

			totalCount++
		}
	}

	logger.Infof("[SaveSnapshot] completed, total payloads: %d, total time: %s", totalCount, time.Since(start))
	return nil
}

func (r *Raft) RecoverFromSnapshot(reader io.Reader, files []sm.SnapshotFile, stopc <-chan struct{}) error {
	start := time.Now() // 记录开始时间

	// 读取 magic header
	magicBuf := make([]byte, magicHeaderSize)
	if err := readFullWithLog(reader, magicBuf, "RecoverSnapshot: reading magic header failed"); err != nil {
		return err
	}

	// 校验 magic header 内容
	if !bytes.Equal(magicBuf, []byte(magicHeader)) {
		return errorfWithLog("RecoverSnapshot: invalid magic header, expected %q, got %x", magicHeader, magicBuf)
	}

	var (
		errCount   int
		payloadNum int
		ok         bool
		lengthBuf  = make([]byte, payloadLengthSize)
		crcBuf     = make([]byte, payloadCrcSize)
		payloadBuf = make([]byte, initialPayloadBufSize)
	)

	for {
		select {
		case <-stopc:
			logger.Warnf("[RecoverSnapshot] aborted due to stop signal, total recovered: %d, errors: %d, cost %s", payloadNum, errCount, time.Since(start))
			return sm.ErrSnapshotStopped
		default:
		}

		// 读取长度
		if err := readFullWithLog(reader, lengthBuf, "RecoverSnapshot: reading payload length failed"); err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return err
		}
		payloadLen := binary.BigEndian.Uint32(lengthBuf)
		if payloadLen == 0 {
			logger.Infof("[Raft] RecoverSnapshot: payload #%d is empty (0-length), validating CRC", payloadNum)
		}

		// 读取 payload
		payloadBuf, ok = growPayloadBuf(payloadBuf, int(payloadLen), maxPayloadBufSize)
		if !ok {
			return errorfWithLog("RecoverSnapshot: payload size %d exceeds max %d", payloadLen, maxPayloadBufSize)
		}
		if err := readFullWithLog(reader, payloadBuf, "RecoverSnapshot: reading payload failed"); err != nil {
			return err
		}

		// 读取 CRC
		if err := readFullWithLog(reader, crcBuf, "RecoverSnapshot: reading crc failed"); err != nil {
			return err
		}
		crc := binary.BigEndian.Uint32(crcBuf)
		expectedCRC := crc32Checksum(payloadBuf)
		if crc != expectedCRC {
			errCount++
			logger.Warnf("[Raft] RecoverSnapshot: payload #%d CRC mismatch: got=%x want=%x", payloadNum, crc, expectedCRC)
			if errCount >= maxPayloadErrorCount {
				return errorfWithLog("RecoverSnapshot: too many crc errors (%d), aborting", errCount)
			}
			continue // 跳过错误数据
		}

		// 恢复 payload
		if err := r.listener.OnRecoverFromSnapshot(payloadBuf); err != nil {
			errCount++
			logger.Warnf("[Raft] RecoverSnapshot: OnRecoverFromSnapshot failed for payload #%d: %v", payloadNum, err)
			if errCount >= maxPayloadErrorCount {
				return errorfWithLog("RecoverSnapshot: too many OnRecoverFromSnapshot errors (%d), aborting", errCount)
			}
			continue
		}

		payloadNum++
	}

	logger.Infof("[Raft] RecoverSnapshot completed, total recovered payloads: %d, skipped: %d, total time: %s", payloadNum, errCount, time.Since(start))

	if err := r.listener.OnRecoverFromSnapshotDone(); err != nil {
		logger.Warnf("[Raft] OnRecoverFromSnapshotDone error: %v", err)
	}
	return nil
}

func growPayloadBuf(buf []byte, required int, maxCap int) ([]byte, bool) {
	if required > maxCap {
		// 超出最大允许容量，返回原始 buf 和 ok=false，调用者可选择报错或忽略
		return buf, false
	}
	if cap(buf) >= required {
		// 复用旧 buffer，仅调整长度为 required，相当于 buf[0:required]
		// 每轮返回的新 slice 依然指向同一底层数组，并且从 0 开始， 因此可以在下一轮循环中继续 buf[:required] 使用。
		return buf[:required], true
	}
	// 扩容，最多不超过 maxCap
	newCap := min(required*3/2, maxCap)
	return make([]byte, required, newCap), true
}

func (r *Raft) Lookup(interface{}) (interface{}, error) {
	return "LOOKUP_CALLED", nil
}

func (r *Raft) Close() error {
	return nil
}

// leaderLoop 持续检查自身是否为 leader
func (r *Raft) leaderLoop() {
	node := r.manager.NodeHost
	clusterID := uint64(r.manager.Config.ClusterID)
	nodeID := uint64(r.manager.NodeID)

	var lastLogTime time.Time
	var lastErrLogTime time.Time

	for {
		time.Sleep(2 * time.Second) // 初始延迟，防止竞选抖动

		leaderID, valid, err := node.GetLeaderID(clusterID)
		if err != nil {
			if time.Since(lastErrLogTime) > 10*time.Second {
				lastErrLogTime = time.Now()
				logger.Warnf("[Raft] GetLeaderID failed: %v", err)
			}
			continue
		}

		if valid {
			// 每分钟打印一次 I'm still the leader 的日志
			if time.Since(lastLogTime) > time.Minute {
				lastLogTime = time.Now()
				if leaderID == nodeID {
					logger.Debugf("[Raft] I'm still the leader (leaderID=%d)", leaderID)
				} else {
					logger.Debugf("[Raft] Current leader is %d, I'm node %d", leaderID, nodeID)
				}
			}

			if leaderID == nodeID {
				if r.isLeader.CompareAndSwap(false, true) {
					logger.Infof("[Raft] This node became Leader, call OnBecameRaftLeader")
					r.listener.OnBecameRaftLeader(true)
				} else {
					r.listener.OnBecameRaftLeader(false)
				}
			}
		}
	}
}

// 检查启动是否已就绪
func (r *Raft) checkReadyLoop() {
	for !r.isReady.Load() {
		if r.tryMarkReadyOnce() {
			return
		}
		time.Sleep(2 * time.Second)
	}
}

func (r *Raft) tryMarkReadyOnce() bool {
	defer func() {
		if rec := recover(); rec != nil {
			logger.Errorf("[Raft] panic in tryMarkReadyOnce: %v\n%s", rec, debug.Stack())
		}
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()

	// 发起基于一致性协议（ReadIndex）的读取请求， 成功返回说明已追上最新日志进度。
	_, err := r.manager.NodeHost.SyncRead(ctx, uint64(r.manager.Config.ClusterID), "lastApplied")
	if err != nil {
		logger.Debugf("[Raft] SyncRead not ready: %v", err)
		return false
	}

	if r.isReady.CompareAndSwap(false, true) {
		logger.Infof("[Raft] Node is ready (SyncRead succeeded)")
		r.listener.OnRaftReady()
	}
	return true
}
