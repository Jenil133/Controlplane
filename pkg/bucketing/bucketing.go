// Package bucketing maps units (user IDs, account IDs, ...) to stable buckets
// so that every service, in every language, makes the same rollout and
// experiment decisions without coordinating.
//
// The algorithm is a cross-language contract and must never change once
// frozen: changing it silently moves units between rollout cohorts and
// experiment arms.
//
//	bucket(salt, u) = murmur3_x86_32(utf8(salt + ":" + u), seed=0) mod 10000
//	threshold(p)    = round(p * 100) clamped to [0, 10000]
//
// Experiment assignment: weights w_i, W = Σw_i; point = bucket * W / 10000
// (integer math, uint64); pick the first i with w_0 + … + w_i > point.
// Disabled experiment or empty unit or W == 0 → not enrolled.
//
// A unit is in a p% rollout iff bucket(salt, u) < threshold(p), so raising p
// only ever adds units. Details for other implementations: murmur3_x86_32 is
// MurmurHash3_x86_32 from Austin Appleby's SMHasher, reading 4-byte blocks
// little-endian, and its result is an unsigned 32-bit integer; round is half
// away from zero (reachable only with more than two decimals, which
// validation rejects) and NaN counts as 0. Compare the bucket with the
// rounded threshold, never with p*100 itself: in binary floating point that
// product lands just above the integer for 573 of the 10,001 valid
// percentages (0.07*100 is 7.000000000000001), and comparing with it would
// let one bucket too many in.
//
// This package implements the hashing and the weighted pick; package eval
// applies the enrollment and flag rules on top.
package bucketing

import (
	"encoding/binary"
	"math"
	"math/bits"
)

// Buckets is the number of buckets units are hashed into, which gives
// percentages a resolution of 0.01.
const Buckets = 10000

// MurmurHash3 x86_32 block mixing constants.
const (
	c1 = 0xcc9e2d51
	c2 = 0x1b873593
)

// Hash32 returns the MurmurHash3_x86_32 hash of data with seed, as the
// SMHasher reference computes it on a little-endian machine. The reference
// reads blocks in native byte order; Hash32 always reads them little-endian,
// so it returns that canonical value on every platform.
func Hash32(data []byte, seed uint32) uint32 {
	h := seed
	n := len(data)
	for len(data) >= 4 {
		h ^= mixK(binary.LittleEndian.Uint32(data))
		h = bits.RotateLeft32(h, 13)*5 + 0xe6546b64
		data = data[4:]
	}
	var k uint32
	switch len(data) {
	case 3:
		k ^= uint32(data[2]) << 16
		fallthrough
	case 2:
		k ^= uint32(data[1]) << 8
		fallthrough
	case 1:
		k ^= uint32(data[0])
		h ^= mixK(k)
	}
	// The reference mixes in the length as a 32-bit value.
	h ^= uint32(n)
	return fmix32(h)
}

func mixK(k uint32) uint32 {
	k *= c1
	k = bits.RotateLeft32(k, 15)
	return k * c2
}

// fmix32 is the MurmurHash3 finalizer; it makes every input bit affect every
// output bit.
func fmix32(h uint32) uint32 {
	h ^= h >> 16
	h *= 0x85ebca6b
	h ^= h >> 13
	h *= 0xc2b2ae35
	h ^= h >> 16
	return h
}

// Bucket returns the bucket of unitID under salt, in [0, Buckets).
func Bucket(salt, unitID string) uint32 {
	// Building the key in a stack buffer keeps flag evaluation free of heap
	// allocations for typical keys; longer keys spill to the heap.
	var buf [128]byte
	key := append(append(append(buf[:0], salt...), ':'), unitID...)
	return Hash32(key, 0) % Buckets
}

// Threshold returns how many buckets, counting from bucket 0, a rollout of
// percent covers: round(percent*100) clamped to [0, Buckets]. NaN yields 0.
func Threshold(percent float64) uint32 {
	t := math.Round(percent * 100)
	switch {
	case !(t > 0): // also NaN, which fails every comparison
		return 0
	case t >= Buckets:
		return Buckets
	}
	return uint32(t)
}

// InRollout reports whether unitID is in a percent rollout salted with salt,
// i.e. Bucket(salt, unitID) < Threshold(percent). A unit that is in at some
// percentage stays in at every higher one. An empty unitID is hashed like any
// other; the rule that keeps it out of partial rollouts lives in package eval.
func InRollout(salt, unitID string, percent float64) bool {
	return Bucket(salt, unitID) < Threshold(percent)
}

// Choose returns the index of the weighted option unitID is assigned to: the
// first i with weights[0] + … + weights[i] > Bucket(salt, unitID) * W /
// Buckets, where W is the sum of all weights. Options with weight 0 are never
// chosen. Returns -1 when weights is empty or sums to 0. Like InRollout it
// does not special-case an empty unitID.
func Choose(salt, unitID string, weights []uint32) int {
	var total uint64
	for _, w := range weights {
		total += uint64(w)
	}
	if total == 0 {
		return -1
	}
	point := uint64(Bucket(salt, unitID)) * total / Buckets
	var sum uint64
	for i, w := range weights {
		sum += uint64(w)
		if sum > point {
			return i
		}
	}
	// point < total because Bucket < Buckets, so the loop always returns.
	panic("bucketing: weighted pick ran past the last option")
}
