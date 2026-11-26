package rx

import (
	"context"
	"sync"
	"sync/atomic"
)

// Emitter receives the events that a Producer emits: zero or more values,
// then at most one terminal event, Error or Complete.
type Emitter[T any] interface {
	Next(value T)
	Error(err error)
	Complete()
}

// Observer holds the callbacks of a subscription. Nil callbacks are ignored,
// so the zero Observer just drives the Observable.
type Observer[T any] struct {
	Next     func(T)
	Error    func(error)
	Complete func()
}

// Producer emits events to e. It should stop when ctx is done and may return
// a teardown function, called once when the subscription ends.
type Producer[T any] func(ctx context.Context, e Emitter[T]) (teardown func())

// Observable is a source of events. Operators are methods, so pipelines read
// left to right:
//
//	rx.Range(1, 20).Filter(isEven).Map(square).Take(4)
//
// Map, Scan and Reduce are Go 1.27 generic methods and may change the value
// type. An Observable is an immutable value; every subscription runs its
// producer again. The zero Observable completes immediately.
type Observable[T any] struct {
	// run starts the stream and sends its events to dst. A nil run is an
	// empty stream.
	run func(ctx context.Context, dst Emitter[T]) (teardown func())
}

// New returns an Observable that runs produce for every subscription.
func New[T any](produce Producer[T]) Observable[T] {
	if produce == nil {
		return Observable[T]{}
	}
	return Observable[T]{run: func(ctx context.Context, dst Emitter[T]) func() {
		return produce(ctx, &guard[T]{ctx: ctx, dst: dst})
	}}
}

// Subscribe starts delivering events to obs until a terminal event,
// Unsubscribe, or the end of ctx. Synchronous sources such as Just run on the
// calling goroutine, so Subscribe returns after they finish.
func (o Observable[T]) Subscribe(ctx context.Context, obs Observer[T]) *Subscription {
	ctx, cancel := context.WithCancel(ctx)
	sub := &Subscription{cancel: cancel, done: make(chan struct{})}
	s := &subscriber[T]{sub: sub, obs: obs}
	stop := context.AfterFunc(ctx, sub.Unsubscribe)
	sub.addTeardown(func() { stop() })
	if o.run == nil {
		s.Complete()
		return sub
	}
	sub.addTeardown(o.run(ctx, s))
	return sub
}

// Subscription controls a running subscription.
type Subscription struct {
	cancel   context.CancelFunc
	isClosed atomic.Bool
	once     sync.Once
	done     chan struct{}

	mu        sync.Mutex
	teardowns []func()
}

// Unsubscribe stops delivery and runs the teardown functions. It is
// idempotent.
func (s *Subscription) Unsubscribe() {
	s.once.Do(func() {
		s.isClosed.Store(true)
		s.cancel()
		s.mu.Lock()
		tds := s.teardowns
		s.teardowns = nil
		s.mu.Unlock()
		for _, td := range tds {
			td()
		}
		close(s.done)
	})
}

// end stops the subscription after a terminal event. It only cancels the
// context: the teardown functions then run through the context.AfterFunc
// registered by Subscribe, on another goroutine. A terminal event is
// delivered while a guard holds its lock, and running teardowns there could
// deadlock with a teardown that waits for a producer goroutine blocked on
// that lock.
func (s *Subscription) end() {
	s.cancel()
}

// Done is closed once the subscription has ended for any reason.
func (s *Subscription) Done() <-chan struct{} {
	return s.done
}

// addTeardown registers td, or runs it at once when the subscription has
// already ended.
func (s *Subscription) addTeardown(td func()) {
	if td == nil {
		return
	}
	s.mu.Lock()
	if s.isClosed.Load() {
		s.mu.Unlock()
		td()
		return
	}
	s.teardowns = append(s.teardowns, td)
	s.mu.Unlock()
}

// newSerial is New for this package's own producers that emit from a single
// goroutine and check ctx before every event, such as FromSeq. They cannot
// race with themselves, so they skip the guard's lock; on a synchronous
// pipeline the lock is most of the per-event cost.
func newSerial[T any](produce Producer[T]) Observable[T] {
	return Observable[T]{run: func(ctx context.Context, dst Emitter[T]) func() {
		return produce(ctx, &serialGuard[T]{dst: dst})
	}}
}

// serialGuard enforces the terminal-event rule for a newSerial producer.
type serialGuard[T any] struct {
	dst    Emitter[T]
	isDone bool
}

func (g *serialGuard[T]) Next(v T) {
	if !g.isDone {
		g.dst.Next(v)
	}
}

func (g *serialGuard[T]) Error(err error) {
	if !g.isDone {
		g.isDone = true
		g.dst.Error(err)
	}
}

func (g *serialGuard[T]) Complete() {
	if !g.isDone {
		g.isDone = true
		g.dst.Complete()
	}
}

// guard is the Emitter given to a producer. It is the only place where the
// event contract is enforced: events are serialized, and nothing passes after
// a terminal event or the end of the context. Operators between the guard and
// the subscriber run inside the guard's critical section, so they need no
// synchronization of their own; a pipeline costs one lock per event, however
// many operators it has.
type guard[T any] struct {
	ctx    context.Context
	dst    Emitter[T]
	mu     sync.Mutex
	isDone bool
}

func (g *guard[T]) Next(v T) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if !g.isDone && g.ctx.Err() == nil {
		g.dst.Next(v)
	}
}

func (g *guard[T]) Error(err error) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if !g.isDone && g.ctx.Err() == nil {
		g.isDone = true
		g.dst.Error(err)
	}
}

func (g *guard[T]) Complete() {
	g.mu.Lock()
	defer g.mu.Unlock()
	if !g.isDone && g.ctx.Err() == nil {
		g.isDone = true
		g.dst.Complete()
	}
}

// subscriber delivers events to the Observer of a subscription. Its events
// arrive serialized by a guard, so it only has to stop delivering once the
// subscription has ended.
type subscriber[T any] struct {
	sub *Subscription
	obs Observer[T]
}

func (s *subscriber[T]) Next(v T) {
	if !s.sub.isClosed.Load() && s.obs.Next != nil {
		s.obs.Next(v)
	}
}

func (s *subscriber[T]) Error(err error) {
	if s.sub.isClosed.CompareAndSwap(false, true) {
		if s.obs.Error != nil {
			s.obs.Error(err)
		}
		s.sub.end()
	}
}

func (s *subscriber[T]) Complete() {
	if s.sub.isClosed.CompareAndSwap(false, true) {
		if s.obs.Complete != nil {
			s.obs.Complete()
		}
		s.sub.end()
	}
}

// emitter adapts the callbacks of an operator to an Emitter.
type emitter[T any] struct{ obs Observer[T] }

func (e emitter[T]) Next(v T) {
	if e.obs.Next != nil {
		e.obs.Next(v)
	}
}

func (e emitter[T]) Error(err error) {
	if e.obs.Error != nil {
		e.obs.Error(err)
	}
}

func (e emitter[T]) Complete() {
	if e.obs.Complete != nil {
		e.obs.Complete()
	}
}
