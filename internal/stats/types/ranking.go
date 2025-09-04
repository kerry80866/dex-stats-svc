package types

import (
	"dex-stats-sol/pb"
	"fmt"
)

type RankingKey struct {
	Category RankingCategory
	Field    RankingField
	Window   RankingWindow
}

type RankingItem[T any] struct {
	TickerData *pb.TickerData
	Entity     T
}

type RankingByDimension[T any] map[RankingKey][]RankingItem[T]

type RankingResult[T any] struct {
	WorkerID    uint8
	BlockNumber uint32
	Rankings    RankingByDimension[T]
}

type RankingCategory uint8

const (
	RankingCategoryFull RankingCategory = iota // 全量
	RankingCategoryHot                         // 热门池
	RankingCategoryCount
)

type RankingField uint8

const (
	// 流动性单独字段
	RankingFieldLiquidity RankingField = iota

	// 全局字段
	RankingFieldGlobalStart
	RankingFieldMarketCap
	RankingFieldFDV
	RankingFieldHolderCount
	RankingFieldTop10HolderRatio
	RankingFieldGlobalEnd

	// 窗口字段
	RankingFieldWindowStart
	RankingFieldVolume
	RankingFieldTxCount
	RankingFieldPriceChange
	RankingFieldWindowEnd

	// Hot 专用字段
	RankingFieldHotStart
	RankingFieldListingAt
	RankingFieldHotEnd
)

type RankingWindow uint8

const (
	Window1Min RankingWindow = iota
	Window5Min
	Window1H
	Window6H
	Window24H
	WindowCount

	WindowGlobal = 128 // 全局榜单，单独值
)

func (k RankingKey) String() string {
	return fmt.Sprintf("%v-%v-%v", k.Category, k.Field, k.Window)
}

func (c RankingCategory) String() string {
	switch c {
	case RankingCategoryFull:
		return "Full"
	case RankingCategoryHot:
		return "Hot"
	default:
		return fmt.Sprintf("RankingCategory(%d)", c)
	}
}

func (f RankingField) String() string {
	switch f {
	case RankingFieldLiquidity:
		return "Liquidity"
	case RankingFieldMarketCap:
		return "MarketCap"
	case RankingFieldFDV:
		return "FDV"
	case RankingFieldHolderCount:
		return "HolderCount"
	case RankingFieldTop10HolderRatio:
		return "Top10HolderRatio"
	case RankingFieldListingAt:
		return "ListingAt"
	case RankingFieldVolume:
		return "Volume"
	case RankingFieldTxCount:
		return "TxCount"
	case RankingFieldPriceChange:
		return "PriceChange"
	default:
		return fmt.Sprintf("RankingField(%d)", f)
	}
}

func (w RankingWindow) String() string {
	switch w {
	case Window1Min:
		return "1Min"
	case Window5Min:
		return "5Min"
	case Window1H:
		return "1H"
	case Window6H:
		return "6H"
	case Window24H:
		return "24H"
	case WindowGlobal:
		return "Global"
	default:
		return fmt.Sprintf("RankingWindow(%d)", w)
	}
}
