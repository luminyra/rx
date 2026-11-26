package rx

import (
	"context"
	"runtime"
	"sync"

	"github.com/luminyra/conc"
)

// MapAsync transforms every value with f on up to limit goroutines at once
// and emits the results as they finish, in any order. A zero or negative
// limit uses runtime.GOMAXPROCS(0). It is for slow transformations, such as a
// call to another service per value.
//
// While limit values are being transformed, the source waits before emitting
// the next one. The first error from f fails the stream, which cancels the
// context passed to the other calls and drops their results. The stream
// completes after the source has completed and every result has been
// emitted. A panic in f reaches the consumer as described at
// [Observable.Subscribe].
func (o Observable[T]) MapAsync[R any](limit int, f func(context.Context, T) (R, error)) Observable[R] {
	return mapAsync(o, limit, f, false)
}

// MapAsyncOrdered is like [Observable.MapAsync] but emits the results in the
// order of the source values. A result that finishes early waits for the
// results before it and keeps holding its place in the limit, so a slow
// value holds back at most limit values.
func (o Observable[T]) MapAsyncOrdered[R any](limit int, f func(context.Context, T) (R, error)) Observable[R] {
	return mapAsync(o, limit, f, true)
}

func mapAsync[T, R any](src Observable[T], limit int, f func(context.Context, T) (R, error), isOrdered bool) Observable[R] {
	if limit <= 0 {
		limit = runtime.GOMAXPROCS(0)
	}
	if src.run == nil {
		return Empty[R]()
	}
	return Observable[R]{run: func(ctx context.Context, dst Emitter[R]) func() {
		m := &asyncMapper[T, R]{
			ctx:       ctx,
			dst:       dst,
			f:         f,
			slots:     conc.NewSemaphore(int64(limit)),
			isOrdered: isOrdered,
		}
		if isOrdered {
			m.pending = make(map[int]R)
		}
		teardown := src.run(ctx, emitter[T]{Observer[T]{Next: m.next, Error: m.fail, Complete: m.complete}})
		return func() {
			m.close()
			if teardown != nil {
				teardown()
			}
		}
	}}
}

// asyncMapper runs the calls of one MapAsync subscription. The source calls
// next, fail and complete one at a time; the calls deliver from their own
// goroutines, and mu serializes everything they send downstream.
type asyncMapper[T, R any] struct {
	ctx       context.Context
	dst       Emitter[R]
	f         func(context.Context, T) (R, error)
	slots     *conc.Semaphore // one unit per value admitted and not yet emitted
	isOrdered bool
	calls     sync.WaitGroup

	mu           sync.Mutex
	seq          int       // index of the next source value
	emitted      int       // index of the next result to emit, when ordered
	pending      map[int]R // finished results waiting for earlier ones, when ordered
	outstanding  int       // values admitted and not yet emitted or dropped
	isSourceDone bool
	isDone       bool // a terminal event was sent downstream
	isClosed     bool // the subscription is being torn down
}

func (m *asyncMapper[T, R]) next(v T) {
	if m.slots.Acquire(m.ctx, 1) != nil {
		return // the stream is ending
	}
	m.mu.Lock()
	if m.isDone || m.isClosed {
		m.mu.Unlock()
		m.slots.Release(1)
		return
	}
	i := m.seq
	m.seq++
	m.outstanding++
	m.calls.Add(1)
	m.mu.Unlock()
	Go(m.ctx, func() {
		defer m.calls.Done()
		r, err := m.f(m.ctx, v)
		m.deliver(i, r, err)
	})
}

// deliver sends a finished result downstream, or holds it until the results
// before it have been sent when the mapper is ordered.
func (m *asyncMapper[T, R]) deliver(i int, r R, err error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	switch {
	case m.isDone:
		m.drop(1)
		return
	case err != nil:
		m.drop(1)
		m.isDone = true
		m.dst.Error(err)
		return
	case !m.isOrdered:
		m.emit(r)
	default:
		m.pending[i] = r
		for r, ok := m.pending[m.emitted]; ok; r, ok = m.pending[m.emitted] {
			delete(m.pending, m.emitted)
			m.emitted++
			m.emit(r)
		}
	}
	m.completeIfDone()
}

// emit sends r downstream and frees its slot. m.mu must be held.
func (m *asyncMapper[T, R]) emit(r R) {
	m.dst.Next(r)
	m.drop(1)
}

// drop frees the slots of n values that are emitted or will never be.
// m.mu must be held.
func (m *asyncMapper[T, R]) drop(n int) {
	m.outstanding -= n
	m.slots.Release(int64(n))
}

func (m *asyncMapper[T, R]) fail(err error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if !m.isDone {
		m.isDone = true
		m.dst.Error(err)
	}
}

func (m *asyncMapper[T, R]) complete() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.isSourceDone = true
	m.completeIfDone()
}

// completeIfDone completes the stream once the source has completed and
// every result has been emitted. m.mu must be held.
func (m *asyncMapper[T, R]) completeIfDone() {
	if m.isSourceDone && m.outstanding == 0 && !m.isDone {
		m.isDone = true
		m.dst.Complete()
	}
}

// close stops admitting values and waits for the running calls, which see
// the end of the subscription context.
func (m *asyncMapper[T, R]) close() {
	m.mu.Lock()
	m.isClosed = true
	m.mu.Unlock()
	m.calls.Wait()
}
