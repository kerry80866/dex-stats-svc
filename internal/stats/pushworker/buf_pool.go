package pushworker

// BufPool 是一个简单的单线程字节切片池，带容量限制
type BufPool struct {
	pool    [][]byte
	bufSize int
	maxSize int
}

// NewBufPool 创建一个 BufPool，预分配 preAlloc 个 buf，每个 buf 大小为 bufSize
func NewBufPool(preAlloc, maxSize, bufSize int) *BufPool {
	p := &BufPool{
		pool:    make([][]byte, 0, maxSize),
		bufSize: bufSize,
		maxSize: maxSize,
	}
	for i := 0; i < preAlloc; i++ {
		p.pool = append(p.pool, make([]byte, bufSize))
	}
	return p
}

// Get 从池中获取一个 buf，如果池空则创建一个新的
func (p *BufPool) Get() []byte {
	if n := len(p.pool); n > 0 {
		buf := p.pool[n-1]
		p.pool[n-1] = nil // 清空 pool 的引用，避免 GC 泄漏
		p.pool = p.pool[:n-1]
		return buf[:0]
	}
	return make([]byte, 0, p.bufSize)
}

// Put 将 buf 放回池中供复用，如果已达到 maxSize 则丢弃
func (p *BufPool) Put(buf []byte) {
	if len(p.pool) >= p.maxSize {
		// 已达到池上限，丢弃 buf
		return
	}
	if buf != nil && cap(buf) > 0 {
		// 复用 buf，清空长度
		p.pool = append(p.pool, buf[:0])
	}
}
