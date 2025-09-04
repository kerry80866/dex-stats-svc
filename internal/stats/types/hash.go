package types

import (
	"fmt"
	"github.com/mr-tron/base58"
)

type Hash [32]byte

func (h Hash) String() string {
	return base58.Encode(h[:])
}

func (h Hash) Equals(other Hash) bool {
	return h == other
}

func HashFromBase58(s string) (Hash, error) {
	var h Hash
	data, err := base58.Decode(s)
	if err != nil {
		return h, err
	}
	if len(data) != 32 {
		return h, fmt.Errorf("invalid pubkey length")
	}
	copy(h[:], data)
	return h, nil
}
