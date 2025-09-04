package poolworker

import (
	"dex-stats-sol/internal/stats/poolworker/defs"
	"encoding/binary"
	"fmt"
	"sync"
	"unsafe"
)

// 每秒约 2.5 个 blockNumber，覆盖 24.5 小时（= 2.5 × 3600 × 24.5 = 220500 个 blockNumber 状态位）
const NumBlockStates = uint32(2.5 * 3600 * 24.5)

type BlockStates struct {
	workerID uint8
	states   *[NumBlockStates]uint32 // 固定长度数组指针，底层连续内存
	mu       sync.Mutex
}

func NewBlockStates(workerID uint8) *BlockStates {
	return &BlockStates{
		workerID: workerID,
		states:   new([NumBlockStates]uint32), // 这里分配连续固定内存
	}
}

func (s *BlockStates) replaceWithSnapshot(snapshot *BlockStates) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.workerID = snapshot.workerID
	s.states = snapshot.states // 直接浅拷贝就可以
}

// isBlockProcessed 只在 worker 线程中调用，无并发，s.states[index] 存最大已处理 blockNumber，判断当前 blockNumber 是否已处理。
func (s *BlockStates) isBlockProcessed(blockNumber uint32) bool {
	// s.states[index] 存储对应索引已处理的最大 blockNumber； 如果当前 blockNumber ≤ 已处理最大 blockNumber，则视为已处理。
	index := blockNumber % NumBlockStates
	return blockNumber <= s.states[index]
}

// setBlockProcessed 只由 worker 线程写入，但因 blockNumber 可能被其他线程读取，必须加锁写
func (s *BlockStates) setBlockProcessed(blockNumber uint32) {
	index := blockNumber % NumBlockStates
	if blockNumber > s.states[index] {
		s.mu.Lock()
		defer s.mu.Unlock()
		s.states[index] = blockNumber
	}
}

func (s *BlockStates) snapshotType() uint8 {
	return defs.SnapshotTypeBlockStates
}

// Serialize 将 BlockStates 序列化为字节流格式：
func (s *BlockStates) Serialize(buf []byte) ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	count := int(NumBlockStates)

	// [1B workerID][1B type][4B count][count * 4B blockNumber values]
	totalLen := 1 + 1 + 4 + count*4
	if cap(buf) < totalLen {
		buf = make([]byte, totalLen)
	} else {
		buf = buf[:totalLen]
	}

	buf[0] = s.workerID
	buf[1] = s.snapshotType()
	binary.BigEndian.PutUint32(buf[2:], uint32(count))

	// Unsafe memory copy
	stateBytes := unsafe.Slice((*byte)(unsafe.Pointer(&s.states[0])), count*4)
	copy(buf[6:], stateBytes)

	return buf, nil
}

func deserializeBlockStates(workerID uint8, data []byte) (*BlockStates, error) {
	// [1B workerID][1B type][4B count][count * 4B blockNumber values]
	const headerLen = 6
	if len(data) < headerLen {
		return nil, fmt.Errorf("[PoolWorker:%d] [BlockStates] invalid snapshot blockNumber data: too short, len=%d", workerID, len(data))
	}

	snapshotType := data[1]
	if snapshotType != defs.SnapshotTypeBlockStates {
		return nil, fmt.Errorf("[PoolWorker:%d] [BlockStates] invalid item type: expected %d, got %d",
			workerID, defs.SnapshotTypeBlockStates, snapshotType)
	}

	count := binary.BigEndian.Uint32(data[2:6])
	if count != NumBlockStates {
		return nil, fmt.Errorf("[PoolWorker:%d] [BlockStates] unexpected blockNumber count: got %d, expect %d",
			workerID, count, NumBlockStates)
	}

	if len(data) != headerLen+int(count)*4 {
		return nil, fmt.Errorf("[PoolWorker:%d] [BlockStates] invalid data length: expect %d, got %d",
			workerID, headerLen+int(count)*4, len(data))
	}

	s := NewBlockStates(workerID)
	src := data[headerLen:]
	dstBytes := unsafe.Slice((*byte)(unsafe.Pointer(&s.states[0])), count*4)
	copy(dstBytes, src)
	return s, nil
}
