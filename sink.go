package rx

import (
	"context"
	"iter"
	"sync"
)

// Collect subscribes, waits for the subscription to end, and returns the
// emitted values. It returns the source's error, or ctx.Err() when ctx ends
// first.
func (o Observable[T]) Collect(ctx context.Context) ([]T, error) {
	var (
		mu          sync.Mutex
		values      []T
		err         error
		isCompleted bool
	)
	sub := o.Subscribe(ctx, Observer[T]{
		Next: func(v T) {
			mu.Lock()
			values = append(values, v)
			mu.Unlock()
		},
		Error: func(e error) {
			mu.Lock()
			err = e
			mu.Unlock()
		},
		Complete: func() {
			mu.Lock()
			isCompleted = true
			mu.Unlock()
		},
	})
	<-sub.Done()

	mu.Lock()
	defer mu.Unlock()
	if err == nil && !isCompleted {
		err = ctx.Err()
	}
	return values, err
}

// All returns an iterator that subscribes when ranged over and yields the
// values one at a time with a nil error. When the source fails, or ctx ends
// before the source completes, the last pair carries the error. Breaking out
// of the loop unsubscribes and waits for the subscription to end, so no
// goroutine is left behind.
//
//	for v, err := range obs.All(ctx) {
//		if err != nil {
//			return err
//		}
//		...
//	}
//
// The source runs on its own goroutine and waits for the loop body before
// emitting the next value.
func (o Observable[T]) All(ctx context.Context) iter.Seq2[T, error] {
	return func(yield func(T, error) bool) {
		type event struct {
			value  T
			err    error
			isLast bool
		}
		ctx, cancel := context.WithCancel(ctx)
		events := make(chan event)
		finished := make(chan struct{})
		send := func(ev event) {
			select {
			case events <- ev:
			case <-ctx.Done():
			}
		}
		go func() {
			defer close(finished)
			sub := o.Subscribe(ctx, Observer[T]{
				Next:     func(v T) { send(event{value: v}) },
				Error:    func(err error) { send(event{err: err, isLast: true}) },
				Complete: func() { send(event{isLast: true}) },
			})
			<-sub.Done()
		}()
		defer func() {
			cancel()
			<-finished
		}()

		var zero T
		for {
			select {
			case ev := <-events:
				if ev.isLast {
					if ev.err != nil {
						yield(zero, ev.err)
					}
					return
				}
				if !yield(ev.value, nil) {
					return
				}
			case <-finished:
				// The subscription ended without a terminal event, so ctx
				// ended first.
				yield(zero, ctx.Err())
				return
			}
		}
	}
}
