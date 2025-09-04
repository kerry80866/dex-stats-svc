package eventadapter

import (
	"bytes"
	"dex-stats-sol/internal/pkg/utils"
	"dex-stats-sol/internal/stats/types"
	"dex-stats-sol/pb"
)

// ExtractHolderChanges 遍历事件列表，返回首个 HolderChange 事件中的变化列表
func ExtractHolderChanges(events *pb.Events) []*pb.TokenHolderChange {
	for _, e := range events.Events {
		if hc := e.GetHolderChange(); hc != nil {
			return hc.HolderChange
		}
	}
	return nil
}

// GroupEventsByPool 按池子地址分组事件，并关联基础信息（token、报价、是否新池子等）
func GroupEventsByPool(events *pb.Events) map[types.Pubkey]*PoolEvents {
	poolMap := make(map[types.Pubkey]*PoolEvents, len(events.Events))
	tokenMap := make(map[types.Pubkey]float64, 16)

	for _, e := range events.Events {
		parsed := parseEvent(e)
		if parsed == nil {
			continue
		}

		// 收集 Token 总供应量
		if parsed.totalSupply > 0 {
			if tokenAddr, err := types.PubkeyFromBytes(parsed.base); err == nil {
				tokenMap[tokenAddr] = parsed.totalSupply
			}
			continue
		}

		// Pool 事件处理
		poolAddr, err := types.PubkeyFromBytes(parsed.pl)
		if err != nil {
			continue
		}

		entry, exists := poolMap[poolAddr]
		if !exists {
			entry = newPoolEvents(poolAddr, parsed, events)
			if entry == nil {
				continue
			}
			poolMap[poolAddr] = entry
		}

		if parsed.isNewCreate {
			entry.IsNewCreate = true
		}

		entry.Events = append(entry.Events, e)
	}

	// 填充 total supply
	for _, entry := range poolMap {
		if supply, ok := tokenMap[entry.BaseToken]; ok {
			entry.TotalSupply = supply
		}
	}

	return poolMap
}

// newPoolEvents 根据 parsedEvent 创建 PoolEvents
func newPoolEvents(poolAddr types.Pubkey, parsed *parsedEvent, events *pb.Events) *PoolEvents {
	baseAddr, err := types.PubkeyFromBytes(parsed.base)
	if err != nil {
		return nil
	}
	quoteAddr, err := types.PubkeyFromBytes(parsed.quote)
	if err != nil {
		return nil
	}
	baseAccount, err := types.PubkeyFromBytes(parsed.baseAccount)
	if err != nil {
		return nil
	}
	quoteAccount, err := types.PubkeyFromBytes(parsed.quoteAccount)
	if err != nil {
		return nil
	}

	quotePrice := findQuotePrice(events, parsed.quote)
	if quotePrice == nil {
		return nil
	}

	return &PoolEvents{
		Events:            make([]*pb.Event, 0, 16),
		Pool:              poolAddr,
		BaseToken:         baseAddr,
		QuoteToken:        quoteAddr,
		BaseTokenAccount:  baseAccount,
		QuoteTokenAccount: quoteAccount,
		BaseDecimals:      parsed.baseDecimals,
		QuoteDecimals:     uint8(quotePrice.Decimals),
		QuotePriceUsd:     quotePrice.Price,
		Dex:               parsed.dex,
		IsNewCreate:       false,
		BlockTime:         uint32(events.BlockTime),
		BlockNumber:       uint32(events.BlockNumber),
	}
}

// parsedEvent 临时承载解析结果
type parsedEvent struct {
	pl           []byte
	base         []byte
	quote        []byte
	baseAccount  []byte
	quoteAccount []byte
	dex          uint16
	baseDecimals uint8
	isNewCreate  bool
	totalSupply  float64
}

// parseEvent 解析单个 Event
func parseEvent(event *pb.Event) *parsedEvent {
	switch evt := event.Event.(type) {
	case *pb.Event_Trade:
		if evt.Trade.Type == pb.EventType_TRADE_BUY || evt.Trade.Type == pb.EventType_TRADE_SELL {
			return &parsedEvent{
				pl:           evt.Trade.PairAddress,
				base:         evt.Trade.Token,
				quote:        evt.Trade.QuoteToken,
				baseAccount:  evt.Trade.TokenAccount,
				quoteAccount: evt.Trade.QuoteTokenAccount,
				baseDecimals: uint8(evt.Trade.TokenDecimals),
				dex:          uint16(evt.Trade.Dex),
			}
		}

	case *pb.Event_Liquidity:
		switch evt.Liquidity.Type {
		case pb.EventType_ADD_LIQUIDITY, pb.EventType_REMOVE_LIQUIDITY, pb.EventType_CREATE_POOL:
			return &parsedEvent{
				pl:           evt.Liquidity.PairAddress,
				base:         evt.Liquidity.Token,
				quote:        evt.Liquidity.QuoteToken,
				baseAccount:  evt.Liquidity.TokenAccount,
				quoteAccount: evt.Liquidity.QuoteTokenAccount,
				baseDecimals: uint8(evt.Liquidity.TokenDecimals),
				dex:          uint16(evt.Liquidity.Dex),
				isNewCreate:  evt.Liquidity.Type == pb.EventType_CREATE_POOL,
			}
		}

	case *pb.Event_Migrate:
		if evt.Migrate.Type == pb.EventType_MIGRATE {
			return &parsedEvent{
				pl:           evt.Migrate.SrcPairAddress,
				base:         evt.Migrate.Token,
				quote:        evt.Migrate.SrcQuoteToken,
				baseAccount:  evt.Migrate.SrcTokenAccount,
				quoteAccount: evt.Migrate.SrcQuoteTokenAccount,
				baseDecimals: uint8(evt.Migrate.TokenDecimals),
				dex:          uint16(evt.Migrate.SrcDex),
			}
		}

	case *pb.Event_Token:
		if evt.Token.Type == pb.EventType_LAUNCHPAD_TOKEN && !utils.IsZeroStr(evt.Token.TotalSupply) {
			return &parsedEvent{
				base:        evt.Token.Token,
				totalSupply: utils.AmountToFloat64(evt.Token.TotalSupply, uint8(evt.Token.Decimals)),
			}
		}
	}

	return nil
}

// findQuotePrice 从报价列表中找到对应 token 的报价信息
func findQuotePrice(events *pb.Events, quote []byte) *pb.TokenPrice {
	for _, qp := range events.QuotePrices {
		if bytes.Equal(qp.Token, quote) {
			return qp
		}
	}
	return nil
}
