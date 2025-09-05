package utils

// ClearOrResetMap 根据 map 当前长度判断是清空还是重新分配
// - m: 待清理的 map
// - maxLen: 超过这个长度就重新分配
// - initCap: 初始容量，用于重新分配
func ClearOrResetMap[K comparable, V any](m *map[K]V, maxLen, initCap int) {
	n := len(*m)
	if n == 0 {
		return
	}

	if n > maxLen {
		*m = make(map[K]V, initCap) // 容量过大，重新分配新空间，释放旧内存
	} else {
		clear(*m) // 容量适中，直接清空复用内存
	}
}

// MergeSets 将 s1 和 s2 的 key 合并，返回一个新的 map。
// 如果其中一个 map 为空，则直接返回另一个 map（假设不会被修改）。
func MergeSets[K comparable](s1, s2 map[K]struct{}) map[K]struct{} {
	if len(s1) == 0 {
		return s2
	}
	if len(s2) == 0 {
		return s1
	}

	result := make(map[K]struct{}, len(s1)+len(s2))

	for k := range s1 {
		result[k] = struct{}{}
	}

	for k := range s2 {
		result[k] = struct{}{}
	}

	return result
}

func MergeMultipleSets[K comparable](output map[K]struct{}, maps ...map[K]struct{}) {
	// 遍历所有传入的 map
	for _, m := range maps {
		// 合并当前 map 的 key
		for k := range m {
			output[k] = struct{}{}
		}
	}
}
