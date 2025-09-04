package pool

import (
	"dex-stats-sol/internal/pkg/utils"
	ea "dex-stats-sol/internal/stats/eventadapter"
	"dex-stats-sol/pb"
)

type Ticker struct {
	closeTs   uint32 // 收盘时间（秒级 Unix 时间戳）
	buyCount  uint16 // 买入成交笔数
	sellCount uint16 // 卖出成交笔数

	buyVolumeFrac  uint32 // 买入成交量（float40 尾数部分）
	sellVolumeFrac uint32 // 卖出成交量（float40 尾数部分）
	closePriceFrac uint32 // 收盘价（float40 尾数部分）

	buyVolumeExp  uint8 // 买入成交量（float40 指数部分）
	sellVolumeExp uint8 // 卖出成交量（float40 指数部分）
	closePriceExp uint8 // 收盘价（float40 指数部分）
}

func TickerFromAggregate(r ea.AggregateResult) Ticker {
	return NewTicker(
		r.BuyCount,
		r.SellCount,
		r.BuyVolume,
		r.SellVolume,
		r.CloseTs,
		r.ClosePrice,
	)
}

func NewTicker(
	buyCount, sellCount uint16,
	buyVolume, sellVolume float64,
	closeTs uint32,
	closePrice float64,
) Ticker {
	buyExp, buyFrac := utils.Float64ToFloat40(buyVolume)
	sellExp, sellFrac := utils.Float64ToFloat40(sellVolume)
	closeExp, closeFrac := utils.Float64ToFloat40(closePrice)

	return Ticker{
		closeTs:        closeTs,
		buyCount:       buyCount,
		sellCount:      sellCount,
		buyVolumeFrac:  buyFrac,
		sellVolumeFrac: sellFrac,
		closePriceFrac: closeFrac,
		buyVolumeExp:   buyExp,
		sellVolumeExp:  sellExp,
		closePriceExp:  closeExp,
	}
}

// TimeBucket 返回ticker所在的时间桶，interval单位为秒
func (t *Ticker) TimeBucket(interval uint16) uint32 {
	if interval == 0 || interval == 1 {
		return t.closeTs
	}
	return t.closeTs / uint32(interval)
}

func (t *Ticker) ClosePrice() float64 {
	return utils.Float40ToFloat64(t.closePriceExp, t.closePriceFrac)
}

func (t *Ticker) BuyVolume() float64 {
	return utils.Float40ToFloat64(t.buyVolumeExp, t.buyVolumeFrac)
}

func (t *Ticker) SellVolume() float64 {
	return utils.Float40ToFloat64(t.sellVolumeExp, t.sellVolumeFrac)
}

func (t *Ticker) BuyCount() uint32 {
	return uint32(t.buyCount)
}

func (t *Ticker) SellCount() uint32 {
	return uint32(t.sellCount)
}

func (t *Ticker) IsEmpty() bool {
	return t.sellCount == uint16(0) && t.buyCount == uint16(0)
}

func (t *Ticker) IsValid() bool {
	return t.sellCount > uint16(0) || t.buyCount > uint16(0)
}

// addTicker 合并另一个ticker的数据
func (t *Ticker) addTicker(other Ticker) {
	// 累加买入、卖出笔数
	t.buyCount += other.buyCount
	t.sellCount += other.sellCount

	// 累加买入、卖出成交额，转换为float40存储
	totalBuy := t.BuyVolume() + other.BuyVolume()
	totalSell := t.SellVolume() + other.SellVolume()
	t.buyVolumeExp, t.buyVolumeFrac = utils.Float64ToFloat40(totalBuy)
	t.sellVolumeExp, t.sellVolumeFrac = utils.Float64ToFloat40(totalSell)

	// 更新最新价格，优先选择时间戳更晚的
	if other.ClosePrice() != 0 {
		if t.ClosePrice() == 0 || other.closeTs >= t.closeTs {
			t.closeTs = other.closeTs
			t.closePriceExp = other.closePriceExp
			t.closePriceFrac = other.closePriceFrac
		}
	}
}

func (t *Ticker) toProto() *pb.TickerSnapshot {
	return &pb.TickerSnapshot{
		CloseTs:    t.closeTs,
		BuyCount:   uint32(t.buyCount),
		SellCount:  uint32(t.sellCount),
		BuyVolume:  t.BuyVolume(),
		SellVolume: t.SellVolume(),
		ClosePrice: t.ClosePrice(),
	}
}
