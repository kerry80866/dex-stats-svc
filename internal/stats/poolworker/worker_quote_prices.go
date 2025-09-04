package poolworker

import (
	"dex-stats-sol/internal/consts"
	"dex-stats-sol/internal/pkg/utils"
	"dex-stats-sol/internal/stats/poolworker/defs"
	"dex-stats-sol/internal/stats/types"
	"dex-stats-sol/pb"
	"fmt"
	"math"
	"sync"
)

type WorkerQuotePrices struct {
	mu                    sync.Mutex               // 并发安全控制
	workerID              uint8                    // 所属线程ID
	nativePrice           float64                  // 快速访问原生 Token 报价
	lastSignificantPrices map[types.Pubkey]float64 // 上次用于显著变化判断的价格
	types.QuotePrices                              // 嵌入
}

func NewWorkerQuotePrices(workerID uint8) *WorkerQuotePrices {
	return &WorkerQuotePrices{
		workerID:              workerID,
		lastSignificantPrices: make(map[types.Pubkey]float64, 8),
		QuotePrices: types.QuotePrices{
			Prices: make(map[types.Pubkey]types.TokenPrice, 8),
		},
	}
}

func (qp *WorkerQuotePrices) replaceWithSnapshot(snapshot *WorkerQuotePrices) {
	qp.mu.Lock()
	defer qp.mu.Unlock()

	qp.workerID = snapshot.workerID
	qp.BlockNumber = snapshot.BlockNumber
	qp.BlockTime = snapshot.BlockTime
	qp.nativePrice = snapshot.nativePrice
	qp.Prices = snapshot.Prices
}

func (qp *WorkerQuotePrices) updateFromEvents(events *pb.Events) (priceSignificant bool) {
	// 如果事件 BlockNumber 小于等于当前 blockNumber，直接跳过
	if uint32(events.BlockNumber) <= qp.BlockNumber {
		return false
	}

	const singleThreshold = 0.0003      // 单次变化阀值, 0.03%
	const accumulatedThreshold = 0.0006 // 累计变化阀值, 0.06%

	qp.mu.Lock()
	defer qp.mu.Unlock()

	// 更新最新 block 信息
	qp.BlockNumber = uint32(events.BlockNumber)
	qp.BlockTime = uint32(events.BlockTime)

	// 遍历所有 quotePrice 事件
	for _, quotePrice := range events.QuotePrices {
		tokenAddr, err := types.PubkeyFromBytes(quotePrice.Token)
		if err != nil {
			continue // 跳过无法解析的 token
		}

		// 获取旧价格，用于单次变化判断
		oldPrice := 0.0
		if old, exists := qp.Prices[tokenAddr]; exists {
			oldPrice = old.Price
		}

		// 更新最新价格到缓存
		newPrice := quotePrice.Price
		qp.Prices[tokenAddr] = types.TokenPrice{
			Price:    newPrice,
			Decimals: uint8(quotePrice.Decimals),
		}

		// 更新 nativePrice（统一处理，无需单独 lastSignificant 字段）
		if tokenAddr == consts.WNativeToken || tokenAddr == consts.NativeToken {
			qp.nativePrice = newPrice
		}

		// 如果已经触发显著变化，则跳过判断
		if priceSignificant {
			continue
		}

		// 单次变化判断
		ratio := 1.0
		if oldPrice > 0 {
			ratio = math.Abs(newPrice-oldPrice) / oldPrice
		}
		if ratio > singleThreshold {
			priceSignificant = true
			continue
		}

		// 累计变化判断
		ratio = 1.0
		if last, exists := qp.lastSignificantPrices[tokenAddr]; exists && last > 0 {
			ratio = math.Abs(newPrice-last) / last
		}
		if ratio > accumulatedThreshold {
			priceSignificant = true
		}
	}

	// 如果触发显著变化，则更新 lastSignificantPrices
	if priceSignificant {
		for tokenAddr, p := range qp.Prices {
			qp.lastSignificantPrices[tokenAddr] = p.Price
		}
	}

	return
}

func (qp *WorkerQuotePrices) cloneUnsafe() *types.QuotePrices {
	cp := &types.QuotePrices{
		BlockNumber: qp.BlockNumber,
		BlockTime:   qp.BlockTime,
		Prices:      make(map[types.Pubkey]types.TokenPrice, len(qp.Prices)),
	}
	for k, v := range qp.Prices {
		cp.Prices[k] = v
	}
	return cp
}

func (qp *WorkerQuotePrices) priceUnsafe(tokenAddr types.Pubkey) float64 {
	price, ok := qp.Prices[tokenAddr]
	if !ok {
		return 0
	}
	return price.Price
}

func (qp *WorkerQuotePrices) snapshotType() uint8 {
	return defs.SnapshotTypeQuotePrice
}

func (qp *WorkerQuotePrices) Serialize(buf []byte) ([]byte, error) {
	qp.mu.Lock()
	defer qp.mu.Unlock()

	qs := &pb.QuotePriceSnapshot{
		BlockNumber: qp.BlockNumber,
		BlockTime:   qp.BlockTime,
		Prices:      make([]*pb.QuotePriceProto, 0, len(qp.Prices)),
	}

	for token, price := range qp.Prices {
		qs.Prices = append(qs.Prices, &pb.QuotePriceProto{
			Token:    token[:],
			Price:    price.Price,
			Decimals: uint32(price.Decimals),
		})
	}

	buf = append(buf, byte(qp.workerID), qp.snapshotType())
	return utils.SafeProtoMarshal(buf, qs)
}

func deserializeQuotePrice(workerID uint8, data []byte) (*WorkerQuotePrices, error) {
	if len(data) < 2 {
		return nil, fmt.Errorf("[PoolWorker:%d] [QuotePrices] snapshot data too short: got %d bytes", workerID, len(data))
	}

	snapshotType := data[1]
	if snapshotType != defs.SnapshotTypeQuotePrice {
		return nil, fmt.Errorf("[PoolWorker:%d] [QuotePrices] invalid snapshot type: expected %d, got %d",
			workerID, defs.SnapshotTypeQuotePrice, snapshotType)
	}

	// 反序列化 protobuf
	qs := &pb.QuotePriceSnapshot{}
	if err := utils.SafeProtoUnmarshal(data[2:], qs); err != nil {
		return nil, fmt.Errorf("[PoolWorker:%d] [QuotePrices] failed to unmarshal QuotePriceSnapshot: %w",
			workerID, err)
	}

	// 构建基础对象
	qp := &WorkerQuotePrices{
		workerID: workerID,
		QuotePrices: types.QuotePrices{
			BlockNumber: qs.BlockNumber,
			BlockTime:   qs.BlockTime,
			Prices:      make(map[types.Pubkey]types.TokenPrice),
		},
	}

	for _, price := range qs.Prices {
		tokenAddr, tokenErr := types.PubkeyFromBytes(price.Token)
		if tokenErr != nil {
			return nil, fmt.Errorf("[PoolWorker:%d] [QuotePrices] failed to deserialize token: %w", workerID, tokenErr)
		}

		if tokenAddr == consts.WNativeToken || tokenAddr == consts.NativeToken {
			qp.nativePrice = price.Price
		}
		qp.Prices[tokenAddr] = types.TokenPrice{
			Price:    price.Price,
			Decimals: uint8(price.Decimals),
		}
	}

	return qp, nil
}
