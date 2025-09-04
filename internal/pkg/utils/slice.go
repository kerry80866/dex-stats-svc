package utils

// ClearSliceTail 清理 slice 中从 newLen 开始到末尾的元素引用（避免内存泄漏）。
// T 必须是可赋零值的类型（指针、interface 等）。
func ClearSliceTail[T any](s []T, newLen int) {
	if len(s) > newLen {
		clear(s[newLen:]) // 内建 clear，安全释放引用
	}
}

func ClearSlice[T any](s *[]T) {
	if s == nil || len(*s) == 0 {
		return
	}

	clear(*s)
	*s = (*s)[:0]
}
