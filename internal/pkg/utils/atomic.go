package utils

import (
	"math"
	"sync/atomic"
)

type noCopy struct{}

func (*noCopy) Lock()   {}
func (*noCopy) Unlock() {}

type align64 struct{}

// AtomicFloat64 是一个原子 float64 类型。
// 实现方式参考 Go 标准库 atomic，将 float64 的二进制存储在 uint64 中。
type AtomicFloat64 struct {
	_ noCopy
	_ align64
	v uint64
}

func (x *AtomicFloat64) Load() float64 {
	return math.Float64frombits(atomic.LoadUint64(&x.v))
}

func (x *AtomicFloat64) Store(val float64) {
	atomic.StoreUint64(&x.v, math.Float64bits(val))
}

func (x *AtomicFloat64) Swap(new float64) (old float64) {
	return math.Float64frombits(atomic.SwapUint64(&x.v, math.Float64bits(new)))
}

func (x *AtomicFloat64) CompareAndSwap(old, new float64) (swapped bool) {
	return atomic.CompareAndSwapUint64(&x.v, math.Float64bits(old), math.Float64bits(new))
}

type AtomicFloat32 struct {
	_ noCopy
	v uint32
}

func (x *AtomicFloat32) Load() float32 {
	return math.Float32frombits(atomic.LoadUint32(&x.v))
}

func (x *AtomicFloat32) Store(val float32) {
	atomic.StoreUint32(&x.v, math.Float32bits(val))
}

func (x *AtomicFloat32) Swap(new float32) (old float32) {
	return math.Float32frombits(atomic.SwapUint32(&x.v, math.Float32bits(new)))
}

func (x *AtomicFloat32) CompareAndSwap(old, new float32) (swapped bool) {
	return atomic.CompareAndSwapUint32(&x.v, math.Float32bits(old), math.Float32bits(new))
}
