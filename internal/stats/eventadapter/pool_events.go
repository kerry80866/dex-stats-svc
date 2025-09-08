package eventadapter

import (
	"bytes"
	"dex-stats-sol/internal/pkg/utils"
	"dex-stats-sol/internal/stats/types"
	"dex-stats-sol/pb"
	"math"
)

const minTradeThreshold = 0.0001 // 最小交易阈值，低于此值的交易会被跳过
const minAmountCoefficient = 0.6 // 针对精度的系数

// PoolEvents 代表某个池子的一批链上事件及相关基础信息
type PoolEvents struct {
	Pool              types.Pubkey
	BaseToken         types.Pubkey
	QuoteToken        types.Pubkey
	BaseTokenAccount  types.Pubkey
	QuoteTokenAccount types.Pubkey
	QuotePriceUsd     float64
	BaseDecimals      uint8
	QuoteDecimals     uint8
	Dex               uint16
	IsNewCreate       bool
	TotalSupply       float64
	BlockTime         uint32
	BlockNumber       uint32
	Events            []*pb.Event
}

// AggregateResult 是事件聚合后的统计结果结构体
type AggregateResult struct {
	OpenPrice  float64
	ClosePrice float64
	CloseTs    uint32
	BuyCount   uint16
	SellCount  uint16
	BuyVolume  float64
	SellVolume float64
	BaseLiq    float64
	QuoteLiq   float64
}

func (p *PoolEvents) Aggregate() (r AggregateResult) {
	for _, event := range p.Events {
		switch evt := event.Event.(type) {
		case *pb.Event_Trade:
			if evt.Trade.Type == pb.EventType_TRADE_BUY || evt.Trade.Type == pb.EventType_TRADE_SELL {
				if evt.Trade.Type == pb.EventType_TRADE_BUY {
					r.BuyCount++
					r.BuyVolume += evt.Trade.AmountUsd
				} else {
					r.SellCount++
					r.SellVolume += evt.Trade.AmountUsd
				}

				// 更新金库余额
				r.BaseLiq = utils.AmountToFloat64(evt.Trade.PairTokenBalance, uint8(evt.Trade.TokenDecimals))
				r.QuoteLiq = utils.AmountToFloat64(evt.Trade.PairQuoteBalance, uint8(evt.Trade.QuoteDecimals))

				// 更新价格
				if evt.Trade.PriceUsd > 0 && evt.Trade.AmountUsd >= minTradeThreshold {
					tokenAmount := utils.AmountToFloat64(evt.Trade.TokenAmount, uint8(evt.Trade.TokenDecimals)) // 转换为 token 为单位
					minAmount := math.Pow10(int(-math.Round(float64(evt.Trade.TokenDecimals) * minAmountCoefficient)))
					if tokenAmount >= minAmount {
						r.ClosePrice = evt.Trade.PriceUsd
						if r.OpenPrice == 0 {
							r.OpenPrice = evt.Trade.PriceUsd
						}
					}
				}
			}

		case *pb.Event_Liquidity:
			r.BaseLiq = utils.AmountToFloat64(evt.Liquidity.PairTokenBalance, uint8(evt.Liquidity.TokenDecimals))
			r.QuoteLiq = utils.AmountToFloat64(evt.Liquidity.PairQuoteBalance, uint8(evt.Liquidity.QuoteDecimals))

		case *pb.Event_Migrate:
			if evt.Migrate.Type == pb.EventType_MIGRATE {
				// 根据迁移来源或目标更新对应池子的金库余额
				if bytes.Equal(p.Pool[:], evt.Migrate.SrcPairAddress) {
					r.BaseLiq = utils.AmountToFloat64(evt.Migrate.SrcPairTokenBalance, uint8(evt.Migrate.TokenDecimals))
					r.QuoteLiq = utils.AmountToFloat64(evt.Migrate.SrcPairQuoteBalance, uint8(evt.Migrate.QuoteDecimals))
				} else if bytes.Equal(p.Pool[:], evt.Migrate.DestPairAddress) {
					r.BaseLiq = utils.AmountToFloat64(evt.Migrate.DestPairTokenBalance, uint8(evt.Migrate.TokenDecimals))
					r.QuoteLiq = utils.AmountToFloat64(evt.Migrate.DestPairQuoteBalance, uint8(evt.Migrate.QuoteDecimals))
				}
			}
		}
	}

	// 对买卖量进行四舍五入
	r.BuyVolume = utils.Float64Round(r.BuyVolume)
	r.SellVolume = utils.Float64Round(r.SellVolume)

	// 设置关闭时间戳
	r.CloseTs = p.BlockTime
	return
}
