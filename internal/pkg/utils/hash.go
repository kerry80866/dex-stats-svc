package utils

func PartitionHash(b []byte) uint32 {
	if len(b) < 28 {
		return 0
	}
	// fallback 路径：组合多个字节避免 hash 冲突
	return uint32(b[7])<<24 | uint32(b[15])<<16 | uint32(b[19])<<8 | uint32(b[27])
}
