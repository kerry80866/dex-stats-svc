package consts

import "dex-stats-sol/internal/stats/types"

// Base58 地址常量（可读性高，适合配置与日志使用）
const (
	// USD 计价基础报价币（具有稳定市场价格）
	SOLMintStr  = "So11111111111111111111111111111111111111111"
	WSOLMintStr = "So11111111111111111111111111111111111111112"
	USDCMintStr = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v"
	USDTMintStr = "Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB"
)

var (
	// 稳定报价币（USD 估值）
	SOLMint  = types.PubkeyFromString(SOLMintStr)
	WSOLMint = types.PubkeyFromString(WSOLMintStr)
	USDCMint = types.PubkeyFromString(USDCMintStr)
	USDTMint = types.PubkeyFromString(USDTMintStr)

	NativeToken  = SOLMint
	WNativeToken = WSOLMint
)
