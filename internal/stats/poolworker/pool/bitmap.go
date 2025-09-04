package pool

const bitmapSize = 8
const bitmapCapacity = uint32(bitmapSize * 64)

// Bitmap 是固定大小位图，由多个 uint64 组成。
// bit 0 对应数组第0个元素的最高有效位（bit 63），
// bit 1 是第0个元素的次高位（bit 62），依次到 bit 63 是最低位（bit 0）。
// bit 64 是数组第1个元素的最高位，依此类推。
type Bitmap [bitmapSize]uint64

// shiftRight 向右移动位图中的所有位，移出的位被丢弃，左边新位用0填充。
func (b *Bitmap) shiftRight(shift uint32) {
	if shift == 0 {
		return
	}

	if shift >= bitmapCapacity {
		b.reset()
		return
	}

	blockShift := int(shift / 64)
	bitShift := shift % 64

	if bitShift == 0 {
		// 如果 bitShift 为0，直接块拷贝即可；
		copy(b[blockShift:], b[:bitmapSize-blockShift])
	} else {
		// 计算整体右移时，先按64位块整体移动，再处理跨块的位移（bitShift）。
		for i := bitmapSize - 1; i >= blockShift; i-- {
			j := i - blockShift
			val := b[j] >> bitShift
			if j > 0 {
				val |= b[j-1] << (64 - bitShift)
			}
			b[i] = val
		}
	}

	for i := 0; i < blockShift; i++ {
		b[i] = 0
	}
}

func (b *Bitmap) isSet(offset uint32) bool {
	if offset >= bitmapCapacity {
		return false
	}

	idx := offset / 64
	bit := 63 - offset%64
	return (b[idx] & (1 << bit)) != 0
}

func (b *Bitmap) setBit(offset uint32) {
	if offset >= bitmapCapacity {
		return
	}

	idx := offset / 64
	bit := 63 - offset%64
	b[idx] |= 1 << bit
}

func (b *Bitmap) reset() {
	for i := range b {
		b[i] = 0
	}
}
