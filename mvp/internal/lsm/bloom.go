package lsm

import (
	"encoding/binary"
	"hash/fnv"
	"math"
)

// Bloom is a classic bit-array bloom filter using k hash functions derived from
// two FNV-1a hashes (Kirsch–Mitzenmacher double hashing).
type Bloom struct {
	Bits []byte
	K    uint32 // number of hash funcs
	M    uint32 // number of bits
}

// NewBloom builds a filter sized for n entries at the given false-positive rate.
func NewBloom(n int, fpRate float64) *Bloom {
	if n <= 0 {
		n = 1
	}
	if fpRate <= 0 || fpRate >= 1 {
		fpRate = 0.01
	}
	// m = -n * ln(p) / (ln2)^2
	m := uint32(math.Ceil(-float64(n) * math.Log(fpRate) / (math.Ln2 * math.Ln2)))
	if m < 64 {
		m = 64
	}
	// k = (m/n) * ln2
	k := uint32(math.Ceil(float64(m) / float64(n) * math.Ln2))
	if k < 1 {
		k = 1
	}
	if k > 16 {
		k = 16
	}
	return &Bloom{
		Bits: make([]byte, (m+7)/8),
		K:    k,
		M:    m,
	}
}

func hashPair(key []byte) (uint32, uint32) {
	h1 := fnv.New32a()
	h1.Write(key)
	a := h1.Sum32()
	h2 := fnv.New32()
	h2.Write(key)
	b := h2.Sum32()
	if b == 0 {
		b = 0x9e3779b1
	}
	return a, b
}

func (b *Bloom) Add(key []byte) {
	h1, h2 := hashPair(key)
	for i := uint32(0); i < b.K; i++ {
		pos := (h1 + i*h2) % b.M
		b.Bits[pos/8] |= 1 << (pos % 8)
	}
}

func (b *Bloom) MayContain(key []byte) bool {
	h1, h2 := hashPair(key)
	for i := uint32(0); i < b.K; i++ {
		pos := (h1 + i*h2) % b.M
		if b.Bits[pos/8]&(1<<(pos%8)) == 0 {
			return false
		}
	}
	return true
}

// Encode serializes the bloom filter into bytes: [k:u32][m:u32][bits...].
func (b *Bloom) Encode() []byte {
	buf := make([]byte, 8+len(b.Bits))
	binary.BigEndian.PutUint32(buf[0:4], b.K)
	binary.BigEndian.PutUint32(buf[4:8], b.M)
	copy(buf[8:], b.Bits)
	return buf
}

// DecodeBloom reads the format written by Encode.
func DecodeBloom(buf []byte) (*Bloom, error) {
	if len(buf) < 8 {
		return nil, errShortBuf
	}
	k := binary.BigEndian.Uint32(buf[0:4])
	m := binary.BigEndian.Uint32(buf[4:8])
	bits := make([]byte, len(buf)-8)
	copy(bits, buf[8:])
	return &Bloom{Bits: bits, K: k, M: m}, nil
}
