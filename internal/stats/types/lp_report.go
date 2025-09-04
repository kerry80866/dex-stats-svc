package types

type LpReportReq struct {
	ChainName        string `json:"chainName"`        // 链名称
	ContractAddress  string `json:"contractAddress"`  // 池子合约地址
	ListingTime      int64  `json:"openTime"`         // 最开始的上架时间（时间戳，秒）
	MaxProfitability int64  `json:"maxProfitability"` // 最大盈利能力
	Leverage         int32  `json:"leverage"`         // 杠杆倍数（0表示不可用）
	LpMemberId       string `json:"lpMemberId"`       // LP 用户 ID
	MaxMargin        string `json:"maxMargin"`        // 最大保证金
	Direction        int32  `json:"direction"`        // 持仓方向（0=多头，1=空头）
	UseStatus        int32  `json:"useStatus"`        // 是否可用（0=不可用，1=可用）
}
