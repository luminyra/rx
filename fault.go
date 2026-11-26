package rx

import (
	"context"
	"sync"

	"github.com/luminyra/conc"
)

// fault records how a stream ended abnormally: a panic or a runtime.Goexit in
// a goroutine that serves it, such as the goroutine of Interval or the one
// that runs a teardown. One fault serves a root subscription and every
// subscription nested in it, such as the inner subscriptions of Merge; they
// find it through the context.
//
// A consumer that waits for the stream, Collect or All, watches the fault and
// raises a panic on its own goroutine once the stream has ended; so does
// Unsubscribe on a root subscription while it runs. Without a watcher, a
// panic is raised again where it happened, as an unrecovered panic in that
// goroutine would be, so it is never lost.
type fault struct {
	cancel context.CancelFunc // ends the root subscription

	mu        sync.Mutex
	err       error // conc.ErrGoexit
	panic     *conc.Panic
	isWatched bool
}

type faultKey struct{}

// withFault returns a context whose streams report to f.
func withFault(ctx context.Context, f *fault) context.Context {
	return context.WithValue(ctx, faultKey{}, f)
}

// faultOf returns the fault that the streams of ctx report to, or nil.
func faultOf(ctx context.Context) *fault {
	f, _ := ctx.Value(faultKey{}).(*fault)
	return f
}

// report records the outcome of conc.Run and ends the stream if it was
// abnormal. Only the first failure is kept for the watcher; a later panic is
// dropped while the stream is watched, as conc.Group drops later panics. A nil
// fault keeps nothing, so a panic is raised again at once.
func (f *fault) report(err error, p *conc.Panic) {
	if err == nil && p == nil {
		return
	}
	if p != nil {
		if inner, ok := p.Value.(conc.Panic); ok {
			p = &inner // already carried across a goroutine, such as by resil.Hedge
		}
	}
	isWatched := false
	if f != nil {
		f.mu.Lock()
		if f.err == nil && f.panic == nil {
			f.err, f.panic = err, p
		}
		isWatched = f.isWatched
		f.mu.Unlock()
		f.cancel()
	}
	if p != nil && !isWatched {
		panic(*p)
	}
}

// watch makes the caller the watcher until it calls take.
func (f *fault) watch() {
	f.mu.Lock()
	f.isWatched = true
	f.mu.Unlock()
}

// take stops watching and hands over the failure, so that only one watcher
// raises it. A failure reported after take is raised where it happens.
func (f *fault) take() (*conc.Panic, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	p, err := f.panic, f.err
	f.panic, f.err, f.isWatched = nil, nil, false
	return p, err
}

// failed returns the recorded failure without ending the watch.
func (f *fault) failed() (*conc.Panic, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.panic, f.err
}

// Go runs fn on a new goroutine that serves a stream. ctx must be the
// context a [Producer] received, or one derived from it. Use Go instead of a
// go statement in a Producer that emits from its own goroutine: a panic or
// [runtime.Goexit] in fn, including in the operators and callbacks that its
// events reach, then ends the subscription and reaches the consumer as
// described at [Observable.Subscribe], instead of crashing the program with
// nobody able to recover it.
func Go(ctx context.Context, fn func()) {
	go conc.Run(func() error {
		fn()
		return nil
	}, faultOf(ctx).report)
}

// watched returns a watched fault for a consumer whose stream ends when
// cancel is called.
func watched(cancel context.CancelFunc) *fault {
	return &fault{cancel: cancel, isWatched: true}
}

// consume subscribes obs to o on the calling goroutine and waits until the
// subscription has ended. A panic anywhere in the stream, on this goroutine
// or on another one, is recorded in f instead of escaping. A runtime.Goexit
// on the calling goroutine is recorded too and still ends the goroutine,
// after the subscription has been cleaned up.
func consume[T any](ctx context.Context, f *fault, o Observable[T], obs Observer[T]) {
	conc.Run(func() error {
		<-o.Subscribe(withFault(ctx, f), obs).Done()
		return nil
	}, f.report)
}
