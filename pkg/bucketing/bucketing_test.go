package bucketing

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"math"
	"strconv"
	"strings"
	"testing"
)

// Every frozen value in this file was computed with two independent
// MurmurHash3 implementations (github.com/spaolacci/murmur3 and the SMHasher
// C code), which agreed with each other, before being written down here.

func TestHash32Vectors(t *testing.T) {
	tests := []struct {
		data string
		seed uint32
		want uint32
	}{
		{"", 0, 0x00000000},
		{"", 1, 0x514E28B7},
		{"", 0xffffffff, 0x81F16F39},
		{"\x00\x00\x00\x00", 0, 0x2362F9DE},
		{"hello", 0, 0x248BFA47},
		{"Hello, world!", 1234, 0xFAF6CDB3},
		{"The quick brown fox jumps over the lazy dog", 0, 0x2E4FF723},
		{"\xff\xff\xff\xff", 0, 0x76293B50},
		{"\x21\x43\x65\x87", 0, 0xF55B516B},
		{"\x21\x43\x65\x87", 0x5082EDEE, 0x2362F9DE},
		{"\x21\x43\x65", 0, 0x7E4A8634},
		{"\x21\x43", 0, 0xA0F7B07A},
		{"\x21", 0, 0x72661CF4},
		{"\x00\x00\x00", 0, 0x85F0B427},
		{"\x00\x00", 0, 0x30F4C306},
		{"\x00", 0, 0x514E28B7},
		{"aaaa", 0x9747b28c, 0x5A97808A},
		{"aaa", 0x9747b28c, 0x283E0130},
		{"aa", 0x9747b28c, 0x5D211726},
		{"a", 0x9747b28c, 0x7FA09EA6},
		{"abcd", 0x9747b28c, 0xF0478627},
		{"abc", 0x9747b28c, 0xC84A62DD},
		{"ab", 0x9747b28c, 0x74875592},
		{"abc", 0, 0xB3DD93FA},
		{"Hello, world!", 0x9747b28c, 0x24884CBA},
		{"ππππππππ", 0x9747b28c, 0xD58063C1},
		{"abcdbcdecdefdefgefghfghighijhijkijkljklmklmnlmnomnopnopq", 0, 0xEE925B90},
		{"The quick brown fox jumps over the lazy dog", 0x9747b28c, 0x2FA826CD},
	}
	for _, tt := range tests {
		if got := Hash32([]byte(tt.data), tt.seed); got != tt.want {
			t.Errorf("Hash32(%q, %#x) = %#08x, want %#08x", tt.data, tt.seed, got, tt.want)
		}
	}
}

// TestHash32SMHasherVerification runs SMHasher's self-test, which hashes keys
// of every length from 0 to 255, each with a different seed, and compares the
// result with the published verification value for MurmurHash3_x86_32.
func TestHash32SMHasherVerification(t *testing.T) {
	key := make([]byte, 256)
	hashes := make([]byte, 0, 256*4)
	for i := range 256 {
		key[i] = byte(i)
		hashes = binary.LittleEndian.AppendUint32(hashes, Hash32(key[:i], uint32(256-i)))
	}
	if got := Hash32(hashes, 0); got != 0xB0F57EE3 {
		t.Fatalf("verification value = %#08x, want 0xb0f57ee3", got)
	}
}

// splitMix64 is a tiny, fully specified PRNG, so the corpus below is the same
// on every Go version and can be regenerated in any language.
type splitMix64 uint64

func (s *splitMix64) next() uint64 {
	*s += 0x9e3779b97f4a7c15
	z := uint64(*s)
	z = (z ^ (z >> 30)) * 0xbf58476d1ce4e5b9
	z = (z ^ (z >> 27)) * 0x94d049bb133111eb
	return z ^ (z >> 31)
}

// fill writes little-endian bytes of successive outputs into b.
func (s *splitMix64) fill(b []byte) {
	for i := 0; i < len(b); i += 8 {
		v := s.next()
		for j := 0; j < 8 && i+j < len(b); j++ {
			b[i+j] = byte(v >> (8 * j))
		}
	}
}

// TestHash32ReferenceCorpus hashes 300 pseudo-random inputs of every length
// from 0 to 64 (all block counts and tail sizes) under each class of seed, and
// compares a SHA-256 digest of the results with the digest the reference
// implementations produced for the same corpus.
func TestHash32ReferenceCorpus(t *testing.T) {
	seeds := []struct {
		seed   uint32
		random bool
	}{{seed: 0}, {seed: 1}, {seed: 0x9747b28c}, {seed: 0x7fffffff}, {seed: 0x80000000}, {seed: 0xffffffff}, {random: true}}
	rng := splitMix64(0x6275636b6574)
	digest := sha256.New()
	data := make([]byte, 64)
	var word [4]byte
	for n := 0; n <= 64; n++ {
		for _, s := range seeds {
			for range 300 {
				seed := s.seed
				if s.random {
					seed = uint32(rng.next())
				}
				rng.fill(data[:n])
				binary.LittleEndian.PutUint32(word[:], Hash32(data[:n], seed))
				digest.Write(word[:])
			}
		}
	}
	const want = "c2a85c50cee73a39cc4eb9ece39cc9100083d68d82400d7590cc2939e37188c4"
	if got := hex.EncodeToString(digest.Sum(nil)); got != want {
		t.Fatalf("corpus digest = %s, want %s", got, want)
	}
}

// TestBucketGolden freezes the cross-service contract: every SDK, in every
// language, must put these units in exactly these buckets.
func TestBucketGolden(t *testing.T) {
	tests := []struct {
		salt, unit string
		want       uint32
	}{
		{"new-cart", "user-42", 7601},
		{"new-cart", "user-43", 7814},
		{"new-cart", "user-6936", 7},
		{"new-cart", "", 1548},
		{"", "user-42", 4191},
		{"", "", 7430},
		{"NEW-CART", "user-42", 7298},
		{"new-cart", "user-42 ", 2365},
		{"checkout-button", "7f9c2ba4-e88f-4c3b-8a1e-2d6b5c0e9a31", 2773},
		{"cta-color", "alice@example.com", 607},
		{"payments/v2", "12345", 1660},
		{"ünïcödé", "用户-1", 3412},
		// Both hash "a:b:c": the separator is not escaped.
		{"a:b", "c", 8997},
		{"a", "b:c", 8997},
		// Keys longer than Bucket's stack buffer.
		{"long", strings.Repeat("x", 200), 4577},
		{strings.Repeat("s", 127), "u", 5508},
	}
	for _, tt := range tests {
		if got := Bucket(tt.salt, tt.unit); got != tt.want {
			t.Errorf("Bucket(%q, %q) = %d, want %d", tt.salt, tt.unit, got, tt.want)
		}
	}
}

func TestBucketHashesSaltColonUnit(t *testing.T) {
	// Lengths straddle the stack buffer, so both the stack and heap paths run.
	for n := 0; n <= 300; n++ {
		salt, unit := strings.Repeat("s", n%17), strings.Repeat("u", n)
		want := Hash32([]byte(salt+":"+unit), 0) % Buckets
		if got := Bucket(salt, unit); got != want {
			t.Fatalf("Bucket(%q, %q) = %d, want %d", salt, unit, got, want)
		}
	}
}

func TestBucketDoesNotAllocate(t *testing.T) {
	allocs := testing.AllocsPerRun(100, func() {
		Bucket("checkout-button", "7f9c2ba4-e88f-4c3b-8a1e-2d6b5c0e9a31")
	})
	if allocs != 0 {
		t.Fatalf("Bucket allocates %v times per call, want 0", allocs)
	}
}

// TestBucketDistribution checks that sequential IDs, the most regular input a
// hash sees in practice, spread evenly: every decile of the bucket range holds
// 10% ± 3% of 100,000 units, and a chi-square test over 100 slices passes.
func TestBucketDistribution(t *testing.T) {
	const units = 100_000
	for _, salt := range []string{"new-cart", "cta-color"} {
		var deciles [10]int
		var hundredths [100]int
		for i := range units {
			b := Bucket(salt, "user-"+strconv.Itoa(i))
			deciles[b/1000]++
			hundredths[b/100]++
		}
		for d, n := range deciles {
			if n < 9700 || n > 10300 {
				t.Errorf("salt %q: decile %d holds %d units, want 10000 ± 3%%", salt, d, n)
			}
		}
		var chi2 float64
		for _, n := range hundredths {
			diff := float64(n) - units/100
			chi2 += diff * diff / (units / 100)
		}
		// 148.2 is the 0.1% critical value of chi-square with 99 degrees of freedom.
		if chi2 > 148.2 {
			t.Errorf("salt %q: chi-square over 100 slices = %.1f, want <= 148.2", salt, chi2)
		}
	}
}

// TestSaltsAreIndependent checks that two flags at 50% pick unrelated halves
// of the population, so being in one rollout says nothing about another.
func TestSaltsAreIndependent(t *testing.T) {
	const units = 100_000
	both := 0
	for i := range units {
		u := "user-" + strconv.Itoa(i)
		if InRollout("new-cart", u, 50) && InRollout("dark-mode", u, 50) {
			both++
		}
	}
	if got := float64(both) / units; math.Abs(got-0.25) > 0.01 {
		t.Fatalf("%.4f of units are in both 50%% rollouts, want 0.25 ± 0.01", got)
	}
}

func TestThreshold(t *testing.T) {
	tests := []struct {
		percent float64
		want    uint32
	}{
		{0, 0},
		{0.01, 1},
		{0.5, 50},
		{1, 100},
		{12.34, 1234},
		{33.33, 3333},
		{99.99, 9999},
		{100, Buckets},
		// Out-of-range input is clamped, never wrapped.
		{100.01, Buckets},
		{1e300, Buckets},
		{math.Inf(1), Buckets},
		{-0.01, 0},
		{math.Copysign(0, -1), 0},
		{math.Inf(-1), 0},
		{math.NaN(), 0},
		// More than two decimals: round half away from zero.
		{0.004, 0},
		{0.005, 1},
		{12.345, 1235},
	}
	for _, tt := range tests {
		if got := Threshold(tt.percent); got != tt.want {
			t.Errorf("Threshold(%v) = %d, want %d", tt.percent, got, tt.want)
		}
	}
}

// TestThresholdExactForValidPercents checks that every percentage validation
// accepts, 0 to 100 in steps of 0.01, covers exactly percent*100 buckets.
func TestThresholdExactForValidPercents(t *testing.T) {
	for k := 0; k <= Buckets; k++ {
		p := float64(k) / 100
		if got := Threshold(p); got != uint32(k) {
			t.Fatalf("Threshold(%v) = %d, want %d", p, got, k)
		}
	}
}

// TestInRolloutIsMonotonic checks that raising the percentage only ever adds
// units, which lets a staged rollout grow without flipping anyone back off,
// and that coverage tracks the percentage.
func TestInRolloutIsMonotonic(t *testing.T) {
	percents := []float64{0, 0.01, 0.5, 1, 5, 10, 25, 33.33, 50, 75, 99.99, 100}
	const units = 20_000
	in := make([]int, len(percents))
	for i := range units {
		u := "user-" + strconv.Itoa(i)
		b := Bucket("new-cart", u)
		wasIn := false
		for j, p := range percents {
			got := InRollout("new-cart", u, p)
			if got != (b < Threshold(p)) {
				t.Fatalf("InRollout(new-cart, %s, %v) = %v with bucket %d", u, p, got, b)
			}
			if wasIn && !got {
				t.Fatalf("%s is in at %v%% but out at %v%%", u, percents[j-1], p)
			}
			if got {
				in[j]++
			}
			wasIn = got
		}
	}
	for j, p := range percents {
		if got := float64(in[j]) / units * 100; math.Abs(got-p) > 1 {
			t.Errorf("%v%% rollout covers %.2f%% of units", p, got)
		}
	}
	if in[0] != 0 || in[len(in)-1] != units {
		t.Errorf("0%% covers %d units and 100%% covers %d, want 0 and %d", in[0], in[len(in)-1], units)
	}
}

// TestInRolloutAtEveryThreshold checks both edges of the threshold of every
// percentage validation accepts: the last bucket in and the first one out.
// For 573 of them p*100 lands just above the integer in binary floating point
// (0.07*100 is 7.000000000000001), so comparing buckets with that product
// instead of round(p*100) would let one bucket too many in.
func TestInRolloutAtEveryThreshold(t *testing.T) {
	// unitIn[b] is the first sequential ID in bucket b. user-108058 is the
	// last bucket's; the cap only stops a broken Bucket from looping forever.
	var unitIn [Buckets]string
	for i, found := 0, 0; found < Buckets; i++ {
		if i == 200_000 {
			t.Fatalf("the first %d IDs reach only %d buckets", i, found)
		}
		u := "user-" + strconv.Itoa(i)
		if b := Bucket("new-cart", u); unitIn[b] == "" {
			unitIn[b] = u
			found++
		}
	}
	for k := 0; k <= Buckets; k++ {
		p := float64(k) / 100
		if k > 0 && !InRollout("new-cart", unitIn[k-1], p) {
			t.Fatalf("%v%% rollout leaves out %s, in bucket %d", p, unitIn[k-1], k-1)
		}
		if k < Buckets && InRollout("new-cart", unitIn[k], p) {
			t.Fatalf("%v%% rollout lets in %s, in bucket %d", p, unitIn[k], k)
		}
	}
}

func TestInRolloutEmptyUnitIsNotSpecial(t *testing.T) {
	// Bucket("new-cart", "") is 1548.
	if InRollout("new-cart", "", 15.48) || !InRollout("new-cart", "", 15.49) {
		t.Fatal("InRollout treats the empty unit differently from its bucket")
	}
}

// TestChooseGolden freezes experiment assignment, which is as much a part of
// the cross-service contract as the buckets it is derived from.
func TestChooseGolden(t *testing.T) {
	tests := []struct {
		salt, unit string
		weights    []uint32
		want       int
	}{
		{"new-cart", "user-42", []uint32{50, 50}, 1},                                      // bucket 7601
		{"new-cart", "user-43", []uint32{1, 1, 1}, 2},                                     // bucket 7814
		{"new-cart", "", []uint32{10, 20, 70}, 1},                                         // bucket 1548
		{"", "user-42", []uint32{1, 0, 3}, 2},                                             // bucket 4191
		{"", "", []uint32{math.MaxUint32, math.MaxUint32}, 1},                             // bucket 7430
		{"NEW-CART", "user-42", []uint32{7}, 0},                                           // bucket 7298
		{"new-cart", "user-42 ", []uint32{50, 50}, 0},                                     // bucket 2365
		{"checkout-button", "7f9c2ba4-e88f-4c3b-8a1e-2d6b5c0e9a31", []uint32{1, 1, 1}, 0}, // bucket 2773
	}
	for _, tt := range tests {
		if got := Choose(tt.salt, tt.unit, tt.weights); got != tt.want {
			t.Errorf("Choose(%q, %q, %v) = %d, want %d", tt.salt, tt.unit, tt.weights, got, tt.want)
		}
	}
}

func TestChooseBoundaries(t *testing.T) {
	// With W = 10000 the point equals the bucket, so bucket 7601 lands in the
	// first option exactly when that option's weight exceeds 7601.
	if got := Choose("new-cart", "user-42", []uint32{7602, 2398}); got != 0 {
		t.Errorf("weight 7602 covers bucket 7601: got option %d, want 0", got)
	}
	if got := Choose("new-cart", "user-42", []uint32{7601, 2399}); got != 1 {
		t.Errorf("weight 7601 stops below bucket 7601: got option %d, want 1", got)
	}
}

// TestChooseProportions checks that every option gets its share of 100,000
// units within one percentage point, and options with weight 0 get none.
func TestChooseProportions(t *testing.T) {
	const units = 100_000
	for _, weights := range [][]uint32{{1, 1}, {10, 20, 70}, {1, 0, 3}, {1, 2, 3, 4}, {math.MaxUint32, math.MaxUint32}} {
		counts := make([]int, len(weights))
		var total float64
		for _, w := range weights {
			total += float64(w)
		}
		for i := range units {
			counts[Choose("cta-color", "user-"+strconv.Itoa(i), weights)]++
		}
		for j, w := range weights {
			got, want := float64(counts[j])/units, float64(w)/total
			if math.Abs(got-want) > 0.01 || (w == 0 && counts[j] != 0) {
				t.Errorf("weights %v: option %d got %.4f of units, want %.4f", weights, j, got, want)
			}
		}
	}
}

func TestChooseEdgeCases(t *testing.T) {
	for _, weights := range [][]uint32{nil, {}, {0}, {0, 0, 0}} {
		if got := Choose("cta-color", "user-1", weights); got != -1 {
			t.Errorf("Choose with weights %v = %d, want -1", weights, got)
		}
	}
	for i := range 1000 {
		u := "user-" + strconv.Itoa(i)
		if got := Choose("cta-color", u, []uint32{7}); got != 0 {
			t.Fatalf("single option: Choose(%s) = %d, want 0", u, got)
		}
		if got := Choose("cta-color", u, []uint32{0, 0, 3, 0}); got != 2 {
			t.Fatalf("one non-zero weight: Choose(%s) = %d, want 2", u, got)
		}
	}
}

func BenchmarkHash32(b *testing.B) {
	for _, n := range []int{8, 32, 128} {
		data := make([]byte, n)
		b.Run(strconv.Itoa(n), func(b *testing.B) {
			b.SetBytes(int64(n))
			for b.Loop() {
				Hash32(data, 0)
			}
		})
	}
}

func BenchmarkBucket(b *testing.B) {
	b.ReportAllocs()
	for b.Loop() {
		Bucket("checkout-button", "7f9c2ba4-e88f-4c3b-8a1e-2d6b5c0e9a31")
	}
}

func BenchmarkChoose(b *testing.B) {
	weights := []uint32{50, 30, 20}
	b.ReportAllocs()
	for b.Loop() {
		Choose("cta-color", "7f9c2ba4-e88f-4c3b-8a1e-2d6b5c0e9a31", weights)
	}
}
