package rx

import (
	"context"
	"errors"
	"slices"
	"testing"
	"testing/synctest"
	"time"
)

func TestContextEndStopsSubscription(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithTimeout(t.Context(), 2500*time.Millisecond)
		defer cancel()
		got, err := Interval(time.Second).Collect(ctx)
		if !errors.Is(err, context.DeadlineExceeded) || !slices.Equal(got, []int{0, 1}) {
			t.Fatalf("Collect = %v, %v", got, err)
		}
	})
}

func TestAll(t *testing.T) {
	var got []int
	for v, err := range Range(1, 5).All(t.Context()) {
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		got = append(got, v)
	}
	if !slices.Equal(got, []int{1, 2, 3, 4, 5}) {
		t.Errorf("All = %v", got)
	}

	boom := errors.New("boom")
	var values []int
	var lastErr error
	for v, err := range Just(1, 2).MergeWith(Fail[int](boom)).All(t.Context()) {
		if err != nil {
			lastErr = err
			continue
		}
		values = append(values, v)
	}
	if !slices.Equal(values, []int{1, 2}) || !errors.Is(lastErr, boom) {
		t.Errorf("All with error = %v, %v", values, lastErr)
	}
}

func TestAllBreakLeavesNoGoroutine(t *testing.T) {
	// synctest.Test fails if a goroutine started in the bubble is still
	// blocked when the function returns, so this also checks for leaks.
	synctest.Test(t, func(t *testing.T) {
		var got []int
		for v, err := range Interval(time.Second).All(t.Context()) {
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got = append(got, v); len(got) == 3 {
				break
			}
		}
		if !slices.Equal(got, []int{0, 1, 2}) {
			t.Errorf("All = %v", got)
		}
	})
}

func TestAllReportsContextEnd(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithTimeout(t.Context(), 1500*time.Millisecond)
		defer cancel()
		var got []int
		var lastErr error
		for v, err := range Interval(time.Second).All(ctx) {
			if err != nil {
				lastErr = err
				break
			}
			got = append(got, v)
		}
		if !slices.Equal(got, []int{0}) || !errors.Is(lastErr, context.DeadlineExceeded) {
			t.Errorf("All = %v, %v", got, lastErr)
		}
	})
}
