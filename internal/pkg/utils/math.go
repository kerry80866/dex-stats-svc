package utils

import (
	"math"
	"math/big"
	"strconv"
)

// Float64Round2 对 float64 保留最多两位小数，适用于市值、流动性等指标
func Float64Round2(v float64) float64 {
	return math.Round(v*100) / 100
}

func Pow10(n uint8) float64 {
	switch n {
	case 6:
		return 1e6
	case 9:
		return 1e9
	case 18:
		return 1e18
	case 0:
		return 1
	case 8:
		return 1e8
	default:
		return math.Pow10(int(n)) // 对于其它情况使用 math.Pow10
	}
}

func AmountToFloat64(value string, decimals uint8) float64 {
	if i, err := strconv.ParseUint(value, 10, 64); err == nil {
		return float64(i) / Pow10(decimals)
	}

	bi, ok := new(big.Int).SetString(value, 10)
	if !ok {
		return 0
	}

	bf := new(big.Float).SetInt(bi)
	bf.Quo(bf, new(big.Float).SetFloat64(Pow10(decimals)))

	result, _ := bf.Float64()
	return result
}

func Uint64ToStr(value uint64) string {
	return strconv.FormatUint(value, 10)
}

func IsZeroStr(value string) bool {
	if value == "" {
		return true
	}
	for i := 0; i < len(value); i++ {
		if value[i] != '0' {
			return false
		}
	}
	return true
}

// float64 转 float40
func Float64ToFloat40(f float64) (uint8, uint32) {
	bits := math.Float64bits(f)

	exp64 := int((bits >> 52) & 0x7FF)
	frac64 := bits & ((1 << 52) - 1)

	if exp64 == 0x7FF {
		// NaN / Inf
		return 0xFF, ^uint32(0) // saturate to max
	}
	if exp64 == 0 && frac64 == 0 {
		return 0, 0
	}

	exp40 := exp64 - (1023 - 127)
	if exp40 < 1 {
		// underflow / subnormal，全部归零
		return 0, 0
	}
	if exp40 > 0xFF {
		// overflow，全部变最大
		return 0xFF, ^uint32(0)
	}

	frac40 := uint32(frac64 >> (52 - 32))
	return uint8(exp40), frac40
}

// float40 转 float64
func Float40ToFloat64(exp40 uint8, frac40 uint32) float64 {
	if exp40 == 0 {
		return 0
	}

	// saturate: exp40 >= 0xFF 视为 float64 的最大非 Inf
	exp64 := uint64(exp40) + (1023 - 127)
	if exp64 >= 0x7FF {
		exp64 = 0x7FE
		frac40 = ^uint32(0)
	}
	frac64 := uint64(frac40) << (52 - 32)
	bits := (exp64 << 52) | frac64
	return math.Float64frombits(bits)
}
