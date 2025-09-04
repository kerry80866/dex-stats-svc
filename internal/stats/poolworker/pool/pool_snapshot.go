package pool

import (
	"dex-stats-sol/internal/consts"
	"dex-stats-sol/internal/pkg/utils"
	"dex-stats-sol/internal/stats/poolworker/defs"
	"dex-stats-sol/internal/stats/poolworker/shared"
	"dex-stats-sol/internal/stats/types"
	"dex-stats-sol/pb"
	"fmt"
	"github.com/cespare/xxhash/v2"
)

func SerializePool(pl *Pool, buf []byte) ([]byte, error) {
	ps := &pb.PoolSnapshot{
		Address:   pl.Address[:],
		Dex:       uint32(pl.Dex),
		CreatedAt: pl.CreatedAt(),

		BaseToken:     pl.BaseToken[:],
		QuoteToken:    pl.QuoteToken[:],
		BaseDecimals:  uint32(pl.BaseDecimals),
		QuoteDecimals: uint32(pl.QuoteDecimals),

		BaseTokenAccount:  pl.BaseTokenAccount[:],
		QuoteTokenAccount: pl.QuoteTokenAccount[:],
		BaseTokenLiq:      pl.liquidity.BaseLiq(),
		QuoteTokenLiq:     pl.liquidity.QuoteLiq(),

		LastChainEventTs:    pl.LastChainEventTs(),
		LastSwapEventTs:     pl.LastSwapEventTs(),
		LastPushBlockNumber: pl.lastPushBlockNumber,
		BlockNumber:         pl.blockNumber,
		BlockBitmap:         pl.blockBitmap[:],

		Tickers1S: pl.tickers1s.toProto(),
		Tickers1M: pl.tickers1m.toProto(),
		Tickers2M: pl.tickers2m.toProto(),
	}

	buf = append(buf, byte(pl.WorkerID), byte(defs.SnapshotTypePoolData))
	return utils.SafeProtoMarshal(buf, ps)
}

func DeserializePool(workerID uint8, data []byte) (pl *Pool, err error) {
	// 校验数据合法性
	if len(data) < 2 {
		return nil, fmt.Errorf("[PoolWorker:%d] data too short: got %d bytes, expect at least 2", workerID, len(data))
	}

	snapshotType := data[1]
	if snapshotType != defs.SnapshotTypePoolData {
		return nil, fmt.Errorf("[PoolWorker:%d] invalid snapshot type: expected %d, got %d", workerID, defs.SnapshotTypePoolData, snapshotType)
	}

	// 反序列化 protobuf
	ps := &pb.PoolSnapshot{}
	if err := utils.SafeProtoUnmarshal(data[2:], ps); err != nil {
		return nil, fmt.Errorf("[PoolWorker:%d] failed to unmarshal PoolSnapshot: %w", workerID, err)
	}

	// 地址字段还原
	address, err := types.PubkeyFromBytes(ps.Address)
	if err != nil {
		return nil, fmt.Errorf("[PoolWorker:%d] DeserializePool invalid address: %w", workerID, err)
	}
	baseToken, err := types.PubkeyFromBytes(ps.BaseToken)
	if err != nil {
		return nil, fmt.Errorf("[PoolWorker:%d] DeserializePool invalid baseToken: %w", workerID, err)
	}
	quoteToken, err := types.PubkeyFromBytes(ps.QuoteToken)
	if err != nil {
		return nil, fmt.Errorf("[PoolWorker:%d] DeserializePool invalid quoteToken: %w", workerID, err)
	}
	baseTokenAccount, err := types.PubkeyFromBytes(ps.BaseTokenAccount)
	if err != nil {
		return nil, fmt.Errorf("[PoolWorker:%d] DeserializePool invalid baseTokenAccount: %w", workerID, err)
	}
	quoteTokenAccount, err := types.PubkeyFromBytes(ps.QuoteTokenAccount)
	if err != nil {
		return nil, fmt.Errorf("[PoolWorker:%d] DeserializePool invalid quoteTokenAccount: %w", workerID, err)
	}

	// 构建基础对象
	pl = &Pool{
		Address:     address,
		AddressHash: xxhash.Sum64(address[:]),
		Dex:         uint16(ps.Dex),
		WorkerID:    workerID,

		BaseToken:     baseToken,
		QuoteToken:    quoteToken,
		IsNativeQuote: quoteToken == consts.WNativeToken || quoteToken == consts.NativeToken,
		IsUSDCQuote:   quoteToken == consts.USDCMint,
		BaseDecimals:  uint8(ps.BaseDecimals),
		QuoteDecimals: uint8(ps.QuoteDecimals),

		BaseTokenAccount:  baseTokenAccount,
		QuoteTokenAccount: quoteTokenAccount,

		liquidity: NewLiquidityInfo(),

		// Shared 数据先初始化为空，之后会从 token 数据恢复
		SharedSupply:      shared.NewSupplyInfo(),
		SharedHolderCount: shared.NewHolderCount(),
		SharedTopHolders:  shared.NewEmptyTopHolders(),

		lastPushBlockNumber: ps.LastPushBlockNumber,
		blockNumber:         ps.BlockNumber,

		tickers1s: NewTickers1s(ps.Tickers1S.OpenPrice, ps.Tickers1S.OpenPriceTs),
		tickers1m: NewTickers1m(ps.Tickers1M.OpenPrice, ps.Tickers1M.OpenPriceTs),
		tickers2m: NewTickers2m(ps.Tickers2M.OpenPrice, ps.Tickers2M.OpenPriceTs),

		poolAddress:  address.String(),
		tokenAddress: baseToken.String(),
	}

	// 初始化窗口
	for i := range pl.Windows {
		pl.Windows[i].setWindowSize(WindowSizes[i])
	}

	// 加载tickers快照, 并设置window
	if err := pl.tickers1s.loadSnapshot(ps.Tickers1S, &pl.Windows[types.Window1Min], &pl.Windows[types.Window5Min]); err != nil {
		return nil, fmt.Errorf("[PoolWorker:%d] tickers1s loadSnapshot failed: %w", workerID, err)
	}
	if err := pl.tickers1m.loadSnapshot(ps.Tickers1M, &pl.Windows[types.Window1H]); err != nil {
		return nil, fmt.Errorf("[PoolWorker:%d] tickers1m loadSnapshot failed: %w", workerID, err)
	}
	if err := pl.tickers2m.loadSnapshot(ps.Tickers2M, &pl.Windows[types.Window6H], &pl.Windows[types.Window24H]); err != nil {
		return nil, fmt.Errorf("[PoolWorker:%d] tickers2m loadSnapshot failed: %w", workerID, err)
	}

	// 基础数据设置
	pl.setCreatedAt(ps.CreatedAt)
	l := min(len(pl.blockBitmap), len(ps.BlockBitmap))
	copy(pl.blockBitmap[:l], ps.BlockBitmap[:l])

	// 设置链上事件时间戳
	pl.setLastChainEventTs(ps.LastChainEventTs)
	pl.setLastSwapEventTs(ps.LastSwapEventTs)

	// 设置当前价格和流动性
	priceUsd := pl.tickers2m.currentPrice
	pl.setPriceUsd(priceUsd)
	pl.liquidity.SetLiquidityInfo(ps.BaseTokenLiq, ps.QuoteTokenLiq, priceUsd)

	return pl, nil
}
