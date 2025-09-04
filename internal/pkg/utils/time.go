package utils

// 时间阈值判断：2025-01-01 10倍秒数，用于区分秒级/毫秒级时间
const unix2025Jan1_10x = 1735689600 * 10

// ToMilliseconds 将输入时间统一转换为毫秒
// 输入可能是秒级或毫秒级时间戳
func ToMilliseconds(t int64) uint64 {
	if t >= unix2025Jan1_10x {
		// 已经是毫秒级，直接返回
		return uint64(t)
	}
	// 秒级时间，转换为毫秒
	return uint64(t * 1000)
}

// ToSeconds 将输入时间统一转换为秒
// 输入可能是秒级或毫秒级时间戳
func ToSeconds(t int64) uint32 {
	if t >= unix2025Jan1_10x {
		// 毫秒级，转换为秒
		return uint32(t / 1000)
	}
	// 已经是秒级，直接返回
	return uint32(t)
}
