package rx

import (
	"context"
	"iter"
	"sync"
)

// Collect subscribes, waits for the subscription to end, and returns the
// emitted values. It returns the source's error, or [context.Cause] of ctx
// when ctx ends first.
//
// If the stream panics, on this goroutine or on one that rx runs for it,
// Collect re-panics with a [conc.Panic] holding the original value and stack
// once the subscription has ended. A [runtime.Goexit] on another goroutine
// returns an error matching [conc.ErrGoexit].
func (o Observable[T]) Collect(ctx context.Context) ([]T, error) {
	var (
		mu          sync.Mutex
		values      []T
		err         error
		isCompleted bool
	)
	streamCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	f := watched(cancel)
	consume(streamCtx, f, o, Observer[T]{
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
	p, failure := f.take()
	if p != nil {
		panic(*p)
	}

	mu.Lock()
	defer mu.Unlock()
	switch {
	case failure != nil:
		err = failure
	case err == nil && !isCompleted:
		err = context.Cause(ctx)
	}
	return values, err
}

// All returns an iterator that subscribes when ranged over and yields the
// values one at a time with a nil error. When the source fails, or ctx ends
// before the source completes, the last pair carries the error, or
// [context.Cause] of ctx. Breaking out of the loop unsubscribes and waits for
// the subscription to end, so no goroutine is left behind.
//
//	for v, err := range obs.All(ctx) {
//		if err != nil {
//			return err
//		}
//		...
//	}
//
// The source runs on its own goroutine and waits for the loop body before
// emitting the next value. If the stream panics, the loop re-panics with a
// [conc.Panic] holding the original value and stack once the subscription
// has ended, and a [runtime.Goexit] in the stream ends the loop with an error
// matching [conc.ErrGoexit].
func (o Observable[T]) All(ctx context.Context) iter.Seq2[T, error] {
	return func(yield func(T, error) bool) {
		type event struct {
			value  T
			err    error
			isLast bool
		}
		streamCtx, cancel := context.WithCancel(ctx)
		f := watched(cancel)
		events := make(chan event)
		finished := make(chan struct{})
		send := func(ev event) {
			select {
			case events <- ev:
			case <-streamCtx.Done():
			}
		}
		go func() {
			defer close(finished) // also after a runtime.Goexit in the stream
			consume(streamCtx, f, o, Observer[T]{
				Next:     func(v T) { send(event{value: v}) },
				Error:    func(err error) { send(event{err: err, isLast: true}) },
				Complete: func() { send(event{isLast: true}) },
			})
		}()
		// However the loop ends, the subscription has ended before All
		// returns, and a panic in the stream is raised here.
		defer func() {
			cancel()
			<-finished
			if p, _ := f.take(); p != nil {
				panic(*p)
			}
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
				// The subscription ended without a terminal event: the stream
				// failed, or ctx ended first.
				switch p, err := f.failed(); {
				case p != nil: // raised by the deferred function
				case err != nil:
					yield(zero, err)
				default:
					yield(zero, context.Cause(ctx))
				}
				return
			}
		}
	}
}
