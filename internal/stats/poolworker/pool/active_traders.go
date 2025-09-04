package pool

import (
	"sync"
)

type ActiveTraders struct {
	m    map[uint64]*TraderNode
	head *TraderNode
	tail *TraderNode
	mu   sync.Mutex

	window1m  ActiveTraderWindow
	window5m  ActiveTraderWindow
	window1h  ActiveTraderWindow
	window6h  ActiveTraderWindow
	window24h ActiveTraderWindow
}

type ActiveTraderWindow struct {
	node  *TraderNode // 当前窗口的链表起点
	count uint32      // 当前窗口内的活跃 trader 数量（去重）
}

type TraderNode struct {
	Trader      uint64 // Trader 地址
	ActiveTime1 uint32 // Packed：高位标记方向，低位时间戳（如买/卖时间）
	ActiveTime2 uint32 // Packed：另一方向或对手方时间（单位：秒）
	prev, next  *TraderNode
}
