package rx

import (
	"context"
	"errors"
	"slices"
	"strconv"
	"sync/atomic"
	"testing"
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
