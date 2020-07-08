package boolgen

import (
	"math/rand"
	"time"
)

type Boolgen struct {
	src rand.Source
	cache int64
	remaining int
}

// Bool - generates a random boolean
// not safe for concurrent use
func (b *Boolgen) Bool() bool {
	if b.remaining == 0 {
		b.cache, b.remaining = b.src.Int63(), 63
	}

	result := b.cache&0x01 == 1
	b.cache >>= 1

	return result
}

func New() *Boolgen {
	return &Boolgen{src: rand.NewSource(time.Now().UnixNano())}
}
