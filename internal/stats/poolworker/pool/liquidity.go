package pool

import (
	"dex-stats-sol/internal/pkg/utils"
)

type LiquidityInfo struct {
	baseLiq    utils.AtomicFloat64 // base token 流动性余额（float64，以 token 为单位）
	quoteLiq   utils.AtomicFloat64 // quote token 流动性余额（float64，以 token 为单位）
	baseLiqUsd utils.AtomicFloat64 // base token 流动性对应的 USD 价值（float64，以美元为单位）
}

func NewLiquidityInfo() *LiquidityInfo {
	return &LiquidityInfo{}
}

func (liq *LiquidityInfo) SetLiquidityInfo(baseLiq, quoteLiq, priceUsd float64) {
	liq.baseLiq.Store(baseLiq)
	liq.baseLiqUsd.Store(baseLiq * priceUsd)
	liq.quoteLiq.Store(quoteLiq)
}

func (liq *LiquidityInfo) BaseLiq() float64 {
	return liq.baseLiq.Load()
}

func (liq *LiquidityInfo) BaseLiqUsd() float64 {
	return liq.baseLiqUsd.Load()
}

func (liq *LiquidityInfo) QuoteLiq() float64 {
	return liq.quoteLiq.Load()
}
