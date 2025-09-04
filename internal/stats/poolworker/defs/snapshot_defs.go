package defs

// SnapshotItemType 是 snapshot payload 的类型标识
const (
	SnapshotTypeBlockStates = iota + 1
	SnapshotTypePoolBucket15s
	SnapshotTypePoolData
	SnapshotTypeTokenData
	SnapshotTypeHotPools
	SnapshotTypeQuotePrice
)
