package rx

import (
	"errors"
	"slices"
	"testing"
	"testing/synctest"
	"time"
)

func TestSources(t *testing.T) {
	if got := collect(t, Just(1, 2, 3)); !slices.Equal(got, []int{1, 2, 3}) {
		t.Errorf("Just = %v", got)
	}
	if got := collect(t, Range(5, 3)); !slices.Equal(got, []int{5, 6, 7}) {
		t.Errorf("Range = %v", got)
	}
	if got := collect(t, Empty[int]()); len(got) != 0 {
		t.Errorf("Empty = %v", got)
	}
	var zero Observable[int]
	if got := collect(t, zero); len(got) != 0 {
		t.Errorf("zero Observable = %v", got)
	}
	boom := errors.New("boom")
	if _, err := Fail[int](boom).Collect(t.Context()); !errors.Is(err, boom) {
		t.Errorf("Fail error = %v", err)
	}

	ch := make(chan string, 2)
	ch <- "a"
	ch <- "b"
	close(ch)
	if got := collect(t, FromChannel(ch)); !slices.Equal(got, []string{"a", "b"}) {
		t.Errorf("FromChannel = %v", got)
	}
}

func TestIntervalWithTake(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		start := time.Now()
		got, err := Interval(time.Second).Take(3).Collect(t.Context())
		if err != nil || !slices.Equal(got, []int{0, 1, 2}) {
			t.Fatalf("Interval.Take = %v, %v", got, err)
		}
		if elapsed := time.Since(start); elapsed != 3*time.Second {
			t.Fatalf("elapsed = %v, expected 3s of fake time", elapsed)
		}
	})
}
