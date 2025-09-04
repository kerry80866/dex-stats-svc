package types

import (
	"fmt"
	"github.com/mr-tron/base58"
)

type Pubkey [32]byte

func (p Pubkey) String() string {
	return base58.Encode(p[:])
}

func (p Pubkey) Equals(other Pubkey) bool {
	return p == other
}

func PubkeyFromBytes(b []byte) (Pubkey, error) {
	if len(b) != 32 {
		return Pubkey{}, fmt.Errorf("invalid pubkey: length = %d, want 32", len(b))
	}
	var pk Pubkey
	copy(pk[:], b)
	return pk, nil
}

func TryPubkeyFromString(s string) (Pubkey, error) {
	data, err := base58.Decode(s)
	if err != nil {
		return Pubkey{}, fmt.Errorf("failed to decode base58 pubkey %q: %w", s, err)
	}
	if len(data) != 32 {
		return Pubkey{}, fmt.Errorf("invalid pubkey length: got %d, want 32, input=%q", len(data), s)
	}
	var p Pubkey
	copy(p[:], data)
	return p, nil
}

func PubkeyFromString(s string) Pubkey {
	data, err := base58.Decode(s)
	if err != nil {
		panic(fmt.Errorf("failed to decode base58 pubkey %q: %w", s, err))
	}
	if len(data) != 32 {
		panic(fmt.Errorf("invalid pubkey length: got %d, want 32, input=%q", len(data), s))
	}
	var p Pubkey
	copy(p[:], data)
	return p
}
