package poolworker

import (
	"dex-stats-sol/internal/pkg/logger"
	"dex-stats-sol/internal/pkg/utils"
	"dex-stats-sol/internal/stats/poolworker/defs"
	"dex-stats-sol/internal/stats/types"
	"dex-stats-sol/pb"
	"encoding/binary"
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

const (
	hotPoolsExpireSeconds  = 15 * 86400 // 15天永久删除
	hotPoolsCleanInterval  = 600        // 10分钟清理一次
	hotPoolsCleanThreshold = 30000      // 触发清理的最小数量
	maxHotPoolCount        = 1_000_000
)

const (
	hotPoolOffsetAddr      = 0
	hotPoolOffsetLong      = 32
	hotPoolOffsetShort     = 36
	hotPoolOffsetListingAt = 40
	hotPoolOffsetUpdated   = 48
	hotPoolOffsetLastEvent = 52
	hotPoolEntrySize       = 56
	hotPoolHeaderSize      = 6 // 1B workerID + 1B type + 4B count
)

type HotPoolData struct {
	LongLeverage  int32  // 多头杠杆倍数
	ShortLeverage int32  // 空头杠杆倍数
	ListingAtMs   uint64 // 最早开仓时间（Unix 毫秒）
	LastUpdatedAt uint32 // 最后更新时间（Unix 秒）
	LastEventAt   uint32 // 最后的链上事件时间（Unix 秒）
}

type HotPools struct {
	mu            sync.RWMutex
	workerID      uint8
	allHotPools   map[types.Pubkey]HotPoolData
	lastCleanedAt uint32 // 上次清理时间，Unix 秒
	lastLogTime   atomic.Int64
}

func newHotPools(workerID uint8, capacity int) *HotPools {
	return &HotPools{
		workerID:    workerID,
		allHotPools: make(map[types.Pubkey]HotPoolData, capacity),
	}
}

func (h *HotPools) replaceWithSnapshot(snapshot *HotPools) {
	h.mu.Lock()
	defer h.mu.Unlock()

	h.workerID = snapshot.workerID
	h.allHotPools = snapshot.allHotPools
	h.lastCleanedAt = snapshot.lastCleanedAt
}

func (h *HotPools) getHotPoolsData(poolAddrs []types.Pubkey) map[types.Pubkey]HotPoolData {
	if len(poolAddrs) == 0 {
		return make(map[types.Pubkey]HotPoolData)
	}

	h.mu.Lock()
	defer h.mu.Unlock()

	result := make(map[types.Pubkey]HotPoolData, len(poolAddrs))
	for _, poolAddr := range poolAddrs {
		result[poolAddr] = h.allHotPools[poolAddr]
	}
	return result
}

func (h *HotPools) lenUnsafe() int {
	return len(h.allHotPools)
}

func (h *HotPools) getDataUnsafe(poolAddr types.Pubkey) HotPoolData {
	return h.allHotPools[poolAddr] // map 中不存在时返回 zero value
}

func (h *HotPools) updateLastEventTimes(addrSet map[types.Pubkey]struct{}, blockTime uint32) {
	h.mu.Lock()
	defer h.mu.Unlock()

	for addr := range addrSet {
		if data, ok := h.allHotPools[addr]; ok {
			if blockTime > data.LastEventAt {
				data.LastEventAt = blockTime
				h.allHotPools[addr] = data
			}
		}
	}
}

func (h *HotPools) updateLeverages(events *pb.PoolLpReportEvents) {
	h.mu.Lock()
	defer h.mu.Unlock()

	for _, event := range events.Events {
		addr, err := types.PubkeyFromBytes(event.PoolAddress)
		if err != nil {
			continue
		}

		data, exists := h.allHotPools[addr]

		// 统一方向判断
		isLong := isLongDirection(event.Direction)
		dir := "short"
		if isLong {
			dir = "long"
		}

		if event.Leverage == 0 {
			// 下架逻辑
			if !exists {
				continue
			}

			if isLong {
				data.LongLeverage = 0
			} else {
				data.ShortLeverage = 0
			}

			// 如果 long/short 都为 0，则移除
			if data.LongLeverage == 0 && data.ShortLeverage == 0 {
				delete(h.allHotPools, addr)
				logger.Debugf("[HotPools] HotPool removed: %s (direction: %s)", addr, dir)
				continue
			}
		} else {
			// 上架/更新逻辑
			if isLong {
				data.LongLeverage = event.Leverage
			} else {
				data.ShortLeverage = event.Leverage
			}
		}

		// 公共更新逻辑
		if event.ListingTimeMs > 0 {
			data.ListingAtMs = event.ListingTimeMs
		} else {
			if utils.ThrottleLog(&h.lastLogTime, 3*time.Second) {
				logger.Warnf("[HotPools] Missing listing time for pool %s (direction: %s)", addr, dir)
			}
		}

		data.LastUpdatedAt = max(data.LastUpdatedAt, event.ReportTime)
		data.LastEventAt = max(data.LastEventAt, event.ReportTime)
		h.allHotPools[addr] = data

		logger.Debugf("[HotPools] HotPool updated: %s (direction: %s, long: %d, short: %d, listingAtMs: %d, lastUpdatedAt: %d)",
			addr, dir, data.LongLeverage, data.ShortLeverage, data.ListingAtMs, data.LastUpdatedAt)
	}
}

func (h *HotPools) cleanExpired(latestTime uint32) {
	if len(h.allHotPools) < hotPoolsCleanThreshold || latestTime <= h.lastCleanedAt+hotPoolsCleanInterval {
		return
	}

	h.mu.Lock()
	defer h.mu.Unlock()

	cutoff := latestTime - hotPoolsExpireSeconds
	for addr, data := range h.allHotPools {
		if data.LastEventAt < cutoff {
			delete(h.allHotPools, addr)
			logger.Debugf("[HotPools] HotPool cleaned (expired): %s", addr)
		}
	}
	h.lastCleanedAt = latestTime
}

func isLongDirection(direction int32) bool {
	return direction == 0
}

func (h *HotPools) snapshotType() uint8 {
	return defs.SnapshotTypeHotPools
}

func (h *HotPools) Serialize(buf []byte) ([]byte, error) {
	h.mu.RLock()
	defer h.mu.RUnlock()

	count := len(h.allHotPools)
	if count == 0 {
		return buf[:0], nil
	}

	totalLen := hotPoolHeaderSize + count*hotPoolEntrySize
	if cap(buf) < totalLen {
		buf = make([]byte, totalLen)
	} else {
		buf = buf[:totalLen]
	}

	buf[0] = h.workerID
	buf[1] = h.snapshotType()
	binary.BigEndian.PutUint32(buf[2:], uint32(count))

	pos := hotPoolHeaderSize
	for addr, data := range h.allHotPools {
		entry := buf[pos : pos+hotPoolEntrySize]

		copy(entry[hotPoolOffsetAddr:], addr[:])
		binary.BigEndian.PutUint32(entry[hotPoolOffsetLong:], uint32(data.LongLeverage))
		binary.BigEndian.PutUint32(entry[hotPoolOffsetShort:], uint32(data.ShortLeverage))
		binary.BigEndian.PutUint64(entry[hotPoolOffsetListingAt:], data.ListingAtMs)
		binary.BigEndian.PutUint32(entry[hotPoolOffsetUpdated:], data.LastUpdatedAt)
		binary.BigEndian.PutUint32(entry[hotPoolOffsetLastEvent:], data.LastEventAt)

		pos += hotPoolEntrySize
	}

	return buf, nil
}

func deserializeHotPools(workerID uint8, data []byte) (*HotPools, error) {
	if len(data) < hotPoolHeaderSize {
		return nil, fmt.Errorf("[PoolWorker:%d] [HotPools] data too short for header", workerID)
	}

	snapshotType := data[1]
	if snapshotType != defs.SnapshotTypeHotPools {
		return nil, fmt.Errorf("[PoolWorker:%d] [HotPools] invalid snapshot type: %d", workerID, snapshotType)
	}

	count := binary.BigEndian.Uint32(data[2:6])
	if count > maxHotPoolCount {
		return nil, fmt.Errorf("[PoolWorker:%d] [HotPools] hotPools count too large: %d", workerID, count)
	}

	expectedLen := hotPoolHeaderSize + int(count)*hotPoolEntrySize
	if len(data) != expectedLen {
		return nil, fmt.Errorf("[PoolWorker:%d] [HotPools] data length mismatch: expected %d, got %d",
			workerID, expectedLen, len(data))
	}

	h := newHotPools(workerID, int(count))

	var addr types.Pubkey
	for pos := hotPoolHeaderSize; pos < expectedLen; pos += hotPoolEntrySize {
		entry := data[pos : pos+hotPoolEntrySize]

		// 复制 addr 的 32 字节
		copy(addr[:], entry[hotPoolOffsetAddr:hotPoolOffsetAddr+32])

		longLeverage := int32(binary.BigEndian.Uint32(entry[hotPoolOffsetLong : hotPoolOffsetLong+4]))
		shortLeverage := int32(binary.BigEndian.Uint32(entry[hotPoolOffsetShort : hotPoolOffsetShort+4]))
		listingAtMs := binary.BigEndian.Uint64(entry[hotPoolOffsetListingAt : hotPoolOffsetListingAt+8])
		lastUpdatedAt := binary.BigEndian.Uint32(entry[hotPoolOffsetUpdated : hotPoolOffsetUpdated+4])
		lastEventAt := binary.BigEndian.Uint32(entry[hotPoolOffsetLastEvent : hotPoolOffsetLastEvent+4])

		h.allHotPools[addr] = HotPoolData{
			LongLeverage:  longLeverage,
			ShortLeverage: shortLeverage,
			ListingAtMs:   listingAtMs,
			LastUpdatedAt: lastUpdatedAt,
			LastEventAt:   lastEventAt,
		}
	}

	return h, nil
}
