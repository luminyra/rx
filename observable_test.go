package rx

import (
	"context"
	"errors"
	"slices"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
)

func collect[T any](t *testing.T, source Observable[T]) []T {
	t.Helper()
	values, err := source.Collect(t.Context())
	if err != nil {
		t.Fatalf("Collect error = %v", err)
	}
	return values
}

func TestNoEventsAfterTerminal(t *testing.T) {
	misbehaving := New(func(_ context.Context, e Emitter[int]) func() {
		e.Next(1)
		e.Complete()
		e.Next(2)
		e.Error(errors.New("late"))
		return nil
	})
	var events []string
	sub := misbehaving.Subscribe(t.Context(), Observer[int]{
		Next:     func(v int) { events = append(events, strconv.Itoa(v)) },
		Error:    func(error) { events = append(events, "error") },
		Complete: func() { events = append(events, "complete") },
	})
	<-sub.Done()
	if !slices.Equal(events, []string{"1", "complete"}) {
		t.Fatalf("events = %v", events)
	}

	// The zero Observer ignores every event.
	sub = Fail[int](errors.New("ignored")).Subscribe(t.Context(), Observer[int]{})
	<-sub.Done()
}

func TestTeardownRunsOnUnsubscribe(t *testing.T) {
	var isTorn atomic.Bool
	src := New(func(context.Context, Emitter[int]) func() {
		return func() { isTorn.Store(true) }
	})
	sub := src.Subscribe(t.Context(), Observer[int]{})
	if isTorn.Load() {
		t.Fatal("teardown ran before Unsubscribe")
	}
	sub.Unsubscribe()
	sub.Unsubscribe()
	<-sub.Done()
	if !isTorn.Load() {
		t.Fatal("teardown did not run")
	}
}

// TestTeardownMayWaitForProducer covers a producer whose teardown waits for
// its goroutine to exit while that goroutine is blocked emitting a value.
func TestTeardownMayWaitForProducer(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		src := New(func(ctx context.Context, e Emitter[int]) func() {
			var wg sync.WaitGroup
			wg.Go(func() {
				for i := 0; ctx.Err() == nil; i++ {
					e.Next(i)
				}
			})
			return wg.Wait
		})
		got, err := src.Take(3).Collect(t.Context())
		if err != nil || !slices.Equal(got, []int{0, 1, 2}) {
			t.Fatalf("Collect = %v, %v", got, err)
		}
	})
}
