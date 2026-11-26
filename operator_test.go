package rx

import (
	"context"
	"errors"
	"slices"
	"strconv"
	"sync/atomic"
	"testing"
)

func TestOperators(t *testing.T) {
	src := Range(1, 6) // 1..6
	isEven := func(v int) bool { return v%2 == 0 }
	square := func(v int) int { return v * v }
	sum := func(acc, v int) int { return acc + v }

	tests := []struct {
		name     string
		got      Observable[int]
		expected []int
	}{
		{name: "filter then map", got: src.Filter(isEven).Map(square), expected: []int{4, 16, 36}},
		{name: "skip then take", got: src.Skip(2).Take(2), expected: []int{3, 4}},
		{name: "take zero", got: src.Take(0), expected: nil},
		{name: "scan", got: src.Scan(0, sum), expected: []int{1, 3, 6, 10, 15, 21}},
		{name: "reduce", got: src.Reduce(0, sum), expected: []int{21}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := collect(t, tt.got); !slices.Equal(got, tt.expected) {
				t.Errorf("got %v, expected %v", got, tt.expected)
			}
		})
	}

	if got := collect(t, src.Take(2).Map(strconv.Itoa)); !slices.Equal(got, []string{"1", "2"}) {
		t.Errorf("Map to string = %v", got)
	}
	var tapped []int
	collect(t, Just(7).Tap(func(v int) { tapped = append(tapped, v) }))
	if !slices.Equal(tapped, []int{7}) {
		t.Errorf("Tap = %v", tapped)
	}
}

func TestTakeStopsInfiniteSource(t *testing.T) {
	var produced atomic.Int32
	infinite := FromSeq(func(yield func(int) bool) {
		for i := 0; ; i++ {
			produced.Add(1)
			if !yield(i) {
				return
			}
		}
	})
	if got := collect(t, infinite.Take(3)); !slices.Equal(got, []int{0, 1, 2}) {
		t.Fatalf("Take = %v", got)
	}
	if n := produced.Load(); n > 4 {
		t.Fatalf("source kept producing after Take: %d values", n)
	}
}

func TestMerge(t *testing.T) {
	got := collect(t, Merge(Just(1, 2), Just(3)))
	slices.Sort(got)
	if !slices.Equal(got, []int{1, 2, 3}) {
		t.Errorf("Merge = %v", got)
	}
	if got := collect(t, Just(1).MergeWith(Just(2))); len(got) != 2 {
		t.Errorf("MergeWith = %v", got)
	}
	boom := errors.New("boom")
	if _, err := Merge(Just(1), Fail[int](boom)).Collect(t.Context()); !errors.Is(err, boom) {
		t.Errorf("Merge error = %v", err)
	}
	if got := collect(t, Merge[int]()); len(got) != 0 {
		t.Errorf("Merge() = %v", got)
	}
}

var sinkInts []int

// BenchmarkPipeline1000 pushes 1000 values through Filter, Map and Collect;
// the loop variant computes the same result directly.
func BenchmarkPipeline1000(b *testing.B) {
	isEven := func(n int) bool { return n%2 == 0 }
	square := func(n int) int { return n * n }
	b.Run("loop", func(b *testing.B) {
		for b.Loop() {
			var out []int
			for n := range 1000 {
				if isEven(n) {
					out = append(out, square(n))
				}
			}
			sinkInts = out
		}
	})
	b.Run("Collect", func(b *testing.B) {
		ctx := b.Context()
		for b.Loop() {
			sinkInts, _ = Range(0, 1000).Filter(isEven).Map(square).Collect(ctx)
		}
	})
	b.Run("All", func(b *testing.B) {
		ctx := b.Context()
		for b.Loop() {
			var out []int
			for v := range Range(0, 1000).Filter(isEven).Map(square).All(ctx) {
				out = append(out, v)
			}
			sinkInts = out
		}
	})
}

func TestOperatorsOnEmptyAndFailedSources(t *testing.T) {
	sum := func(acc, v int) int { return acc + v }
	if got := collect(t, Empty[int]().Reduce(5, sum)); !slices.Equal(got, []int{5}) {
		t.Errorf("Reduce on Empty = %v, want the seed", got)
	}
	if got := collect(t, New[int](nil).Map(strconv.Itoa)); len(got) != 0 {
		t.Errorf("Map on New(nil) = %v", got)
	}
	boom := errors.New("boom")
	if _, err := Fail[int](boom).Filter(func(int) bool { return true }).Map(strconv.Itoa).Collect(t.Context()); !errors.Is(err, boom) {
		t.Errorf("error through operators = %v", err)
	}
}

func TestSerialSourceError(t *testing.T) {
	boom := errors.New("boom")
	src := newSerial(func(_ context.Context, e Emitter[int]) func() {
		e.Next(1)
		e.Error(boom)
		e.Next(2)     // dropped after the terminal event
		e.Error(boom) // dropped
		e.Complete()  // dropped
		return nil
	})
	got, err := src.Collect(t.Context())
	if !slices.Equal(got, []int{1}) || !errors.Is(err, boom) {
		t.Fatalf("Collect = %v, %v", got, err)
	}
}
