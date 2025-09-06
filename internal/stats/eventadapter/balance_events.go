package eventadapter

import (
	"dex-stats-sol/internal/pkg/utils"
	"dex-stats-sol/internal/stats/types"
	"dex-stats-sol/pb"
)

type AccountBalanceInfo struct {
	Account       types.Pubkey // 账户地址
	PreBalance    float64
	Balance       float64
	BlockNumber   uint32
	IsPoolAccount bool // 是否是池子账户
}

// GroupBalancesByToken 按 token 分组账户余额变化
func GroupBalancesByToken(
	events *pb.Events,
	poolAccountSet map[types.Pubkey]struct{},
) map[types.Pubkey][]*AccountBalanceInfo {
	result := make(map[types.Pubkey][]*AccountBalanceInfo)

	for _, e := range events.Events {
		balance := e.GetBalance()
		if balance == nil {
			continue
		}

		// 转换账户和 token
		account, err := types.PubkeyFromBytes(balance.Account)
		if err != nil {
			continue
		}
		token, err := types.PubkeyFromBytes(balance.Token)
		if err != nil {
			continue
		}

		info := &AccountBalanceInfo{
			Account:     account,
			PreBalance:  utils.AmountToFloat64(balance.PreBalance, uint8(balance.Decimals)),
			Balance:     utils.AmountToFloat64(balance.PostBalance, uint8(balance.Decimals)),
			BlockNumber: uint32(balance.BlockNumber),
		}

		// 判断是否为池子账户
		if _, ok := poolAccountSet[account]; ok {
			info.IsPoolAccount = true
		}

		// 预分配 slice 容量，减少扩容
		list, ok := result[token]
		if !ok {
			list = make([]*AccountBalanceInfo, 0, 16)
		}
		result[token] = append(list, info)
	}

	return result
}
