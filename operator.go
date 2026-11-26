package rx

import (
	"context"
	"sync"
	"sync/atomic"
)

// lift builds an Observable that runs src with the callbacks op builds from
// the downstream Emitter. The operator is fused into the source: it adds no
// subscription, goroutine or lock, and runs under the source's guard.
func lift[T, R any](src Observable[T], op func(dst Emitter[R]) Observer[T]) Observable[R] {
	return Observable[R]{run: func(ctx context.Context, dst Emitter[R]) func() {
		obs := op(dst)
		if src.run == nil { // an empty source completes at once
			emitter[T]{obs}.Complete()
			return nil
		}
		return src.run(ctx, emitter[T]{obs})
	}}
}

// Map transforms every value with f. The result type may differ from T.
func (o Observable[T]) Map[R any](f func(T) R) Observable[R] {
	return lift(o, func(dst Emitter[R]) Observer[T] {
		return Observer[T]{Next: func(v T) { dst.Next(f(v)) }, Error: dst.Error, Complete: dst.Complete}
	})
}

// Filter emits only the values for which keep reports true.
func (o Observable[T]) Filter(keep func(T) bool) Observable[T] {
	return lift(o, func(dst Emitter[T]) Observer[T] {
		return Observer[T]{
			Next: func(v T) {
				if keep(v) {
					dst.Next(v)
				}
			},
			Error:    dst.Error,
			Complete: dst.Complete,
		}
	})
}

// Tap calls fn for every value and passes the value through unchanged.
func (o Observable[T]) Tap(fn func(T)) Observable[T] {
	return o.Filter(func(v T) bool {
		fn(v)
		return true
	})
}

// Take emits the first n values, then completes and unsubscribes from the
// source.
func (o Observable[T]) Take(n int) Observable[T] {
	if n <= 0 {
		return Empty[T]()
	}
	return lift(o, func(dst Emitter[T]) Observer[T] {
		seen := 0
		return Observer[T]{
			Next: func(v T) {
				seen++
				dst.Next(v)
				if seen == n {
					dst.Complete()
				}
			},
			Error:    dst.Error,
			Complete: dst.Complete,
		}
	})
}

// Skip drops the first n values and emits the rest.
func (o Observable[T]) Skip(n int) Observable[T] {
	return lift(o, func(dst Emitter[T]) Observer[T] {
		skipped := 0
		return Observer[T]{
			Next: func(v T) {
				if skipped < n {
					skipped++
					return
				}
				dst.Next(v)
			},
			Error:    dst.Error,
			Complete: dst.Complete,
		}
	})
}

// Scan emits each intermediate result of combining the values with f,
// starting from seed.
func (o Observable[T]) Scan[R any](seed R, f func(acc R, v T) R) Observable[R] {
	return lift(o, func(dst Emitter[R]) Observer[T] {
		acc := seed
		return Observer[T]{
			Next: func(v T) {
				acc = f(acc, v)
				dst.Next(acc)
			},
			Error:    dst.Error,
			Complete: dst.Complete,
		}
	})
}

// Reduce combines all values with f, starting from seed, and emits the final
// result when the source completes.
func (o Observable[T]) Reduce[R any](seed R, f func(acc R, v T) R) Observable[R] {
	return lift(o, func(dst Emitter[R]) Observer[T] {
		acc := seed
		return Observer[T]{
			Next:  func(v T) { acc = f(acc, v) },
			Error: dst.Error,
			Complete: func() {
				dst.Next(acc)
				dst.Complete()
			},
		}
	})
}

// MergeWith is Merge(o, others...).
func (o Observable[T]) MergeWith(others ...Observable[T]) Observable[T] {
	return Merge(append([]Observable[T]{o}, others...)...)
}

// Merge emits the values of all sources as they arrive. It completes when
// every source has completed and fails as soon as any source fails.
func Merge[T any](sources ...Observable[T]) Observable[T] {
	if len(sources) == 0 {
		return Empty[T]()
	}
	return New(func(ctx context.Context, dst Emitter[T]) func() {
		var remaining atomic.Int32
		remaining.Store(int32(len(sources)))
		var mu sync.Mutex
		subs := make([]*Subscription, 0, len(sources))
		for _, src := range sources {
			if ctx.Err() != nil {
				break
			}
			sub := src.Subscribe(ctx, Observer[T]{
				Next:  dst.Next,
				Error: dst.Error,
				Complete: func() {
					if remaining.Add(-1) == 0 {
						dst.Complete()
					}
				},
			})
			mu.Lock()
			subs = append(subs, sub)
			mu.Unlock()
		}
		return func() {
			mu.Lock()
			defer mu.Unlock()
			for _, sub := range subs {
				sub.Unsubscribe()
			}
		}
	})
}
