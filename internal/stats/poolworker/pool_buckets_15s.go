package poolworker

import (
	"dex-stats-sol/internal/pkg/utils"
	"dex-stats-sol/internal/stats/poolworker/defs"
	"dex-stats-sol/internal/stats/types"
	"encoding/binary"
	"fmt"
	"sync"
)

const (
	num15sBuckets    = uint32(4 * 60 * 25) // 6000个桶，覆盖25小时，每桶15秒
	bucketSeconds15s = uint32(15)          // 每个桶时间长度15秒
	maxPoolCacheLen  = 1024                // poolCache最大容量阈值，超过则重置
	initPoolCacheCap = 256                 // poolCache初始容量
)

type PoolBucket15s struct {
	mu        sync.Mutex
	workerID  uint8
	startTime uint32         // 当前桶的起始时间戳，必须是 15 秒对齐
	pools     []types.Pubkey // 当前时间段内活跃的 pool 列表
}

// PoolBuckets15s 管理固定数量的时间桶。
// 所有桶必须在初始化时填满，运行期间禁止替换指针，只允许加锁修改内容。
type PoolBuckets15s struct {
	buckets   [num15sBuckets]*PoolBucket15s
	poolCache map[types.Pubkey]struct{} // addPools 用于去重的临时缓存，单线程安全
}

// NewPoolBuckets15s 初始化所有桶，确保运行时 buckets[i] 永远非 nil。
func NewPoolBuckets15s(workerID uint8) *PoolBuckets15s {
	var pbs = PoolBuckets15s{
		poolCache: make(map[types.Pubkey]struct{}, initPoolCacheCap),
	}
	for i := range pbs.buckets {
		pbs.buckets[i] = &PoolBucket15s{
			workerID: workerID,
			pools:    make([]types.Pubkey, 0, 64),
		}
	}
	return &pbs
}

func (pbs *PoolBuckets15s) len() int {
	return initPoolCacheCap
}

// replaceWithSnapshotItem 用于快照恢复时更新指定时间桶的内容。
// 注意：仅更新字段，不替换 buckets[i] 指针，保持结构稳定性。
func (pbs *PoolBuckets15s) replaceWithSnapshotItem(bucket *PoolBucket15s) {
	if bucket == nil {
		return
	}

	index := timeBucket15s(bucket.startTime) % num15sBuckets
	pbs.buckets[index].mu.Lock()
	defer pbs.buckets[index].mu.Unlock()

	pbs.buckets[index].workerID = bucket.workerID
	pbs.buckets[index].startTime = bucket.startTime
	pbs.buckets[index].pools = bucket.pools
}

// collectSlidingOutPools 用于收集滑出时间范围的 pool 列表
func (pbs *PoolBuckets15s) collectSlidingOutPools(
	dst map[types.Pubkey]struct{},
	lastHandledTime, latestTime, window, bucketSeconds uint32,
) {
	// 如果上次处理时间比最新时间晚或相等，则无需处理
	if lastHandledTime >= latestTime {
		return
	}

	// 首次处理时，lastHandledTime为0，初始化为最新时间向前推window长度，确保首次能收集整个窗口范围内的pool
	if lastHandledTime == 0 {
		lastHandledTime = latestTime - window
	}

	// 计算 alignedLast 和 alignedLatest: 将 lastHandledTime 和 latestTime 减去 window，得到滑动窗口的起始时间点
	// 然后向下对齐到桶粒度 bucketSeconds，确保时间点对齐到对应的桶边界
	alignedLast := (lastHandledTime - window) / bucketSeconds * bucketSeconds
	alignedLatest := (latestTime - window) / bucketSeconds * bucketSeconds

	// 将对齐后的时间转换为以15秒为单位的桶索引，方便遍历相应时间段内的桶
	lastBucketIndex := timeBucket15s(alignedLast)
	latestBucketIndex := timeBucket15s(alignedLatest)

	// 遍历 lastHandledTime 之后到 latest 之间所有相关的桶
	for i := lastBucketIndex + 1; i <= latestBucketIndex; i++ {
		// 环形数组取桶，确保桶索引循环使用
		bucket15s := pbs.buckets[i%num15sBuckets]

		// 当前桶预期的起始时间戳
		expectedTime := i * bucketSeconds15s

		// 确认桶的startTime和预期一致，防止时间错乱导致误判
		if bucket15s.startTime == expectedTime {
			// 收集该桶内所有活跃pool到结果集
			for _, pool := range bucket15s.pools {
				dst[pool] = struct{}{}
			}
		}
	}
}

// addPools 向指定时间段的桶添加pool，防止重复添加，必须单线程调用
func (pbs *PoolBuckets15s) addPools(pools map[types.Pubkey]struct{}, blockTime uint32) {
	// 1. 将 blockTime 向下对齐到 15 秒的时间窗口，计算桶索引及对应的起始时间
	bucketIndex := timeBucket15s(blockTime)
	startTime := bucketIndex * bucketSeconds15s
	bucket := pbs.buckets[bucketIndex%num15sBuckets]

	// 2. 避免写入过旧时间的数据，保证时间单调递增
	if startTime < bucket.startTime {
		return
	}

	// 3. 清理或重置 map 缓存，防止容量无限增长
	utils.ClearOrResetMap(&pbs.poolCache, maxPoolCacheLen, initPoolCacheCap)

	// 4. 如果时间段没变，则将旧池子加入去重 map 缓存
	if startTime == bucket.startTime {
		for _, pl := range bucket.pools {
			pbs.poolCache[pl] = struct{}{}
		}
	}

	// 5. 构造新增 pool 列表，只添加不重复的
	addedPools := make([]types.Pubkey, 0, len(pools))
	for pl := range pools {
		if _, exists := pbs.poolCache[pl]; !exists {
			addedPools = append(addedPools, pl)
		}
	}

	// 6. 加锁写入桶数据，保证并发安全
	bucket.mu.Lock()
	defer bucket.mu.Unlock()

	// 7. 如果时间更新，说明是新桶，清空旧数据
	if startTime > bucket.startTime {
		bucket.pools = bucket.pools[:0]
		bucket.startTime = startTime
	}

	// 8. 扩容桶内切片，避免频繁扩容
	newLen := len(bucket.pools) + len(addedPools)
	if cap(bucket.pools) < newLen {
		newPools := make([]types.Pubkey, 0, newLen*6/5) // 扩容1.2倍
		newPools = append(newPools, bucket.pools...)
		bucket.pools = newPools
	}

	// 9. 追加新增池子
	bucket.pools = append(bucket.pools, addedPools...)
}

func timeBucket15s(t uint32) uint32 {
	return t / bucketSeconds15s
}

// prepareSnapshot 收集所有桶的指针用于快照。
// buckets[i] 在初始化阶段已填充为非 nil 且不可替换；仅内部字段会新，而此处仅读取指针本身，因此无需加锁。
func (pbs *PoolBuckets15s) prepareSnapshot(dst *[]types.Serializable) {
	for _, bucket := range pbs.buckets {
		if len(bucket.pools) > 0 {
			*dst = append(*dst, bucket)
		}
	}
}

func (p *PoolBucket15s) snapshotType() uint8 {
	return defs.SnapshotTypePoolBucket15s
}

func (p *PoolBucket15s) Serialize(buf []byte) ([]byte, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	count := len(p.pools)
	if count == 0 {
		return buf[:0], nil
	}

	// [1B workerID][1B type][4B startTime][4B count][count x 32B pubkey]
	totalLen := 1 + 1 + 4 + 4 + count*32
	if cap(buf) < totalLen {
		buf = make([]byte, totalLen)
	} else {
		buf = buf[:totalLen]
	}

	buf[0] = p.workerID
	buf[1] = p.snapshotType()
	binary.BigEndian.PutUint32(buf[2:], p.startTime)
	binary.BigEndian.PutUint32(buf[6:], uint32(count))

	pos := 10
	for _, k := range p.pools {
		copy(buf[pos:], k[:])
		pos += 32
	}

	return buf, nil
}

func deserializePoolBucket15s(workerID uint8, data []byte) (*PoolBucket15s, error) {
	// [1B workerID][1B type][4B startTime][4B count][count x 32B pubkey]
	const headerLen = 10
	if len(data) < headerLen {
		return nil, fmt.Errorf("[PoolWorker:%d] [PoolBucket15s] data too short: got %d bytes, expect at least %d", workerID, len(data), headerLen)
	}

	snapshotType := data[1]
	if snapshotType != defs.SnapshotTypePoolBucket15s {
		return nil, fmt.Errorf("[PoolWorker:%d] [PoolBucket15s] invalid snapshot type: expected %d, got %d",
			workerID, defs.SnapshotTypePoolBucket15s, snapshotType)
	}

	startTime := binary.BigEndian.Uint32(data[2:6])
	count := int(binary.BigEndian.Uint32(data[6:10]))
	expectedLen := headerLen + count*32
	if len(data) != expectedLen {
		return nil, fmt.Errorf("[PoolWorker:%d] [PoolBucket15s] invalid snapshot length: expect %d, got %d",
			workerID, expectedLen, len(data))
	}

	pools := make([]types.Pubkey, 0, count)
	m := make(map[types.Pubkey]struct{}, count)

	pos := headerLen
	for i := 0; i < count; i++ {
		var key types.Pubkey
		copy(key[:], data[pos:pos+32])
		pos += 32

		if _, exit := m[key]; exit {
			continue
		}

		m[key] = struct{}{}
		pools = append(pools, key)
	}

	return &PoolBucket15s{
		pools:     pools,
		workerID:  workerID,
		startTime: startTime,
	}, nil
}
