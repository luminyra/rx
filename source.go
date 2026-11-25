package rx

import (
	"context"
	"iter"
	"slices"
	"time"
)

// Just emits values in order, then completes.
func Just[T any](values ...T) Observable[T] {
	return FromSeq(slices.Values(values))
}

// FromSeq emits the values of seq, then completes. seq runs once per
// subscription and stops early when the subscription ends.
func FromSeq[T any](seq iter.Seq[T]) Observable[T] {
	return New(func(ctx context.Context, e Emitter[T]) func() {
		for v := range seq {
			if ctx.Err() != nil {
				return nil
			}
			e.Next(v)
		}
		e.Complete()
		return nil
	})
}

// Range emits count consecutive integers starting at start, then completes.
func Range(start, count int) Observable[int] {
	return FromSeq(func(yield func(int) bool) {
		for i := range max(count, 0) {
			if !yield(start + i) {
				return
			}
		}
	})
}

// Empty completes without emitting values.
func Empty[T any]() Observable[T] {
	return Observable[T]{}
}

// Fail fails with err without emitting values.
func Fail[T any](err error) Observable[T] {
	return New(func(_ context.Context, e Emitter[T]) func() {
		e.Error(err)
		return nil
	})
}

// FromChannel emits values received from ch and completes when ch is closed.
// It runs on its own goroutine.
func FromChannel[T any](ch <-chan T) Observable[T] {
	return New(func(ctx context.Context, e Emitter[T]) func() {
		go func() {
			for {
				select {
				case <-ctx.Done():
					return
				case v, ok := <-ch:
					if !ok {
						e.Complete()
						return
					}
					e.Next(v)
				}
			}
		}()
		return nil
	})
}

// Interval emits 0, 1, 2, ... every period until the subscription ends. It
// never completes on its own; combine it with Take or a context. Interval
// panics if period is not positive.
func Interval(period time.Duration) Observable[int] {
	if period <= 0 {
		panic("rx: Interval period must be positive")
	}
	return New(func(ctx context.Context, e Emitter[int]) func() {
		ticker := time.NewTicker(period)
		go func() {
			defer ticker.Stop()
			for i := 0; ; i++ {
				select {
				case <-ctx.Done():
					return
				case <-ticker.C:
					e.Next(i)
				}
			}
		}()
		return nil
	})
}
