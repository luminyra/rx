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
	produce Producer[T]
}

// New returns an Observable that runs produce for every subscription.
func New[T any](produce Producer[T]) Observable[T] {
	return Observable[T]{produce: produce}
}

// Subscribe starts delivering events to obs until a terminal event,
// Unsubscribe, or the end of ctx. Synchronous sources such as Just run on the
// calling goroutine, so Subscribe returns after they finish.
func (o Observable[T]) Subscribe(ctx context.Context, obs Observer[T]) *Subscription {
	ctx, cancel := context.WithCancel(ctx)
	sub := &Subscription{cancel: cancel, done: make(chan struct{})}
	s := &subscriber[T]{ctx: ctx, sub: sub, obs: obs}
	stop := context.AfterFunc(ctx, sub.Unsubscribe)
	sub.addTeardown(func() { stop() })
	if o.produce == nil {
		s.Complete()
		return sub
	}
	sub.addTeardown(o.produce(ctx, s))
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

// subscriber is the Emitter given to a producer. It enforces the event
// contract: events are serialized, and nothing is delivered after a terminal
// event, Unsubscribe, or the end of the context.
type subscriber[T any] struct {
	ctx context.Context
	sub *Subscription
	obs Observer[T]
	mu  sync.Mutex // serializes events
}

func (s *subscriber[T]) isActive() bool {
	return !s.sub.isClosed.Load() && s.ctx.Err() == nil
}

func (s *subscriber[T]) Next(v T) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.isActive() && s.obs.Next != nil {
		s.obs.Next(v)
	}
}

func (s *subscriber[T]) Error(err error) {
	s.terminate(func() {
		if s.obs.Error != nil {
			s.obs.Error(err)
		}
	})
}

func (s *subscriber[T]) Complete() {
	s.terminate(func() {
		if s.obs.Complete != nil {
			s.obs.Complete()
		}
	})
}

func (s *subscriber[T]) terminate(deliver func()) {
	s.mu.Lock()
	if !s.isActive() {
		s.mu.Unlock()
		return
	}
	s.sub.isClosed.Store(true)
	deliver()
	s.mu.Unlock()
	s.sub.Unsubscribe()
}
