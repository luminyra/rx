package rx

import (
	"context"
	"errors"
	"runtime"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/luminyra/conc"
)

// catchPanic returns the value fn panicked with, or nil.
func catchPanic(fn func()) (v any) {
	defer func() { v = recover() }()
	fn()
	return nil
}

// checkStreamPanic checks that got is the conc.Panic raised for a stream
// that panicked with want, carrying the original goroutine's stack.
func checkStreamPanic(t *testing.T, got, want any) {
	t.Helper()
	p, ok := got.(conc.Panic)
	if !ok {
		t.Fatalf("panic = %#v, want a conc.Panic", got)
	}
	if p.Value != want || !strings.Contains(p.Stack, "rx.panicAt") {
		t.Fatalf("panic = %v with stack\n%s\nwant %v from panicAt", p.Value, p.Stack, want)
	}
}

func panicAt(n int) func(int) int {
	return func(v int) int {
		if v == n {
			panic("bug")
		}
		return v
	}
}

func goexitAt(n int) func(int) int {
	return func(v int) int {
		if v == n {
			runtime.Goexit()
		}
		return v
	}
}

// sources run the same values synchronously and from a goroutine.
var sources = map[string]func(values ...int) Observable[int]{
	"sync": Just[int],
	"async": func(values ...int) Observable[int] {
		ch := make(chan int, len(values))
		for _, v := range values {
			ch <- v
		}
		close(ch)
		return FromChannel(ch)
	},
}

func TestStreamPanicReachesConsumer(t *testing.T) {
	for name, source := range sources {
		t.Run(name+"/Collect", func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				got := catchPanic(func() { _, _ = source(1, 2, 3).Map(panicAt(2)).Collect(t.Context()) })
				checkStreamPanic(t, got, "bug")
			})
		})
		t.Run(name+"/All", func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				var values []int
				got := catchPanic(func() {
					for v, err := range source(1, 2, 3).Map(panicAt(2)).All(t.Context()) {
						if err != nil {
							t.Errorf("unexpected error %v", err)
						}
						values = append(values, v)
					}
				})
				checkStreamPanic(t, got, "bug")
				if !slices.Equal(values, []int{1}) {
					t.Errorf("values before the panic = %v, want [1]", values)
				}
			})
		})
	}
}

func TestStreamGoexitIsAnError(t *testing.T) {
	t.Run("All", func(t *testing.T) {
		for name, source := range sources {
			t.Run(name, func(t *testing.T) {
				synctest.Test(t, func(t *testing.T) {
					var values []int
					var errs []error
					for v, err := range source(1, 2, 3).Map(goexitAt(2)).All(t.Context()) {
						if err != nil {
							errs = append(errs, err)
							continue
						}
						values = append(values, v)
					}
					if !slices.Equal(values, []int{1}) || len(errs) != 1 || !errors.Is(errs[0], conc.ErrGoexit) {
						t.Fatalf("All = %v, %v; want [1] then ErrGoexit", values, errs)
					}
				})
			})
		}
	})
	t.Run("Collect/async", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			got, err := sources["async"](1, 2, 3).Map(goexitAt(2)).Collect(t.Context())
			if !errors.Is(err, conc.ErrGoexit) || !slices.Equal(got, []int{1}) {
				t.Fatalf("Collect = %v, %v; want [1], ErrGoexit", got, err)
			}
		})
	})
	t.Run("Collect/sync", func(t *testing.T) {
		// A Goexit on the consumer's own goroutine ends that goroutine, after
		// the subscription has been cleaned up.
		synctest.Test(t, func(t *testing.T) {
			var isTorn atomic.Bool
			src := New(func(ctx context.Context, e Emitter[int]) func() {
				context.AfterFunc(ctx, func() { isTorn.Store(true) })
				e.Next(1)
				return nil
			})
			returned := false
			done := make(chan struct{})
			go func() {
				defer close(done)
				_, _ = src.Map(goexitAt(1)).Collect(t.Context())
				returned = true
			}()
			<-done
			synctest.Wait()
			if returned || !isTorn.Load() {
				t.Fatalf("Collect returned %t, subscription ended %t; want false, true", returned, isTorn.Load())
			}
		})
	})
}

func TestPlainSubscribeGoexitEndsSubscription(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var events []string
		sub := Interval(time.Second).Map(goexitAt(1)).Subscribe(t.Context(), Observer[int]{
			Next:     func(int) { events = append(events, "next") },
			Error:    func(error) { events = append(events, "error") },
			Complete: func() { events = append(events, "complete") },
		})
		<-sub.Done() // closes instead of hanging forever
		if !slices.Equal(events, []string{"next"}) {
			t.Fatalf("events = %v, want one value and no terminal event", events)
		}
	})
}

func TestStreamPanicStopsMergedSources(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var ticks atomic.Int32
		healthy := Interval(time.Second).Tap(func(int) { ticks.Add(1) })
		failing := Interval(1500 * time.Millisecond).Map(panicAt(1))
		got := catchPanic(func() { _, _ = Merge(healthy, failing).Collect(t.Context()) })
		checkStreamPanic(t, got, "bug")
		n := ticks.Load()
		time.Sleep(10 * time.Second)
		if ticks.Load() != n {
			t.Fatal("a merged source kept running after the panic")
		}
	})
}

func TestSynchronousPanicCleansUp(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		// Merge subscribes to the Interval before Just panics on this
		// goroutine; the Interval must stop although Merge never returned its
		// teardown.
		var ticks atomic.Int32
		healthy := Interval(time.Second).Tap(func(int) { ticks.Add(1) })
		got := catchPanic(func() {
			Merge(healthy, Just(1).Map(panicAt(1))).Subscribe(t.Context(), Observer[int]{})
		})
		if got != "bug" {
			t.Fatalf("Subscribe panic = %#v, want the original value", got)
		}
		time.Sleep(10 * time.Second)
		if ticks.Load() != 0 {
			t.Fatalf("the Interval ticked %d times after the panic", ticks.Load())
		}
	})
}

func TestTeardownPanic(t *testing.T) {
	t.Run("Unsubscribe", func(t *testing.T) {
		// The inner subscriptions of Merge end both through Merge's teardown
		// and through their own contexts, on other goroutines; either way the
		// panic reaches the caller of Unsubscribe.
		for range 100 {
			var first, second atomic.Bool
			src := New(func(ctx context.Context, e Emitter[int]) func() {
				return func() { first.Store(true); panic("teardown bug") }
			}).MergeWith(New(func(ctx context.Context, e Emitter[int]) func() {
				return func() { second.Store(true) }
			}))
			sub := src.Subscribe(t.Context(), Observer[int]{})
			got := catchPanic(sub.Unsubscribe)
			if p, ok := got.(conc.Panic); !ok || p.Value != "teardown bug" {
				t.Fatalf("Unsubscribe panic = %#v, want the teardown's conc.Panic", got)
			}
			select {
			case <-sub.Done():
			default:
				t.Fatal("Done is open after a teardown panicked")
			}
			if !first.Load() || !second.Load() {
				t.Fatalf("teardowns ran: %t, %t; want both", first.Load(), second.Load())
			}
			if catchPanic(sub.Unsubscribe) != nil {
				t.Fatal("a second Unsubscribe raised the panic again")
			}
		}
	})
	t.Run("Collect", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			src := New(func(ctx context.Context, e Emitter[int]) func() {
				e.Next(1)
				e.Complete()
				return func() { panic("teardown bug") }
			})
			got := catchPanic(func() { _, _ = src.Collect(t.Context()) })
			p, ok := got.(conc.Panic)
			if !ok || p.Value != "teardown bug" {
				t.Fatalf("Collect panic = %#v, want the teardown's conc.Panic", got)
			}
		})
	})
	t.Run("All after break", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			src := New(func(ctx context.Context, e Emitter[int]) func() {
				Go(ctx, func() {
					for i := 0; ctx.Err() == nil; i++ {
						e.Next(i)
					}
				})
				return func() { panic("teardown bug") }
			})
			got := catchPanic(func() {
				for range src.All(t.Context()) {
					break
				}
			})
			if p, ok := got.(conc.Panic); !ok || p.Value != "teardown bug" {
				t.Fatalf("All panic = %#v, want the teardown's conc.Panic", got)
			}
		})
	})
}

func TestUnwatchedFaultPanicsInPlace(t *testing.T) {
	var canceled bool
	f := &fault{cancel: func() { canceled = true }}
	failure := &conc.Panic{Value: "bug"}
	if got := catchPanic(func() { f.report(nil, failure) }); got != *failure || !canceled {
		t.Fatalf("report panic = %#v, canceled %t", got, canceled)
	}
	if got := catchPanic(func() { (*fault)(nil).report(nil, failure) }); got != *failure {
		t.Fatalf("report on a nil fault panic = %#v", got)
	}

	// After the consumer stopped watching, a late panic is not lost.
	w := watched(func() {})
	w.report(conc.ErrGoexit, nil)
	if p, err := w.take(); p != nil || !errors.Is(err, conc.ErrGoexit) {
		t.Fatalf("take = %v, %v", p, err)
	}
	if got := catchPanic(func() { w.report(nil, failure) }); got != *failure {
		t.Fatalf("late report panic = %#v", got)
	}
}

func TestCollectReportsCause(t *testing.T) {
	cause := errors.New("shutting down")
	ctx, cancel := context.WithCancelCause(t.Context())
	cancel(cause)
	if _, err := Interval(time.Hour).Collect(ctx); !errors.Is(err, cause) {
		t.Fatalf("Collect error = %v, want the cause", err)
	}
	for _, err := range Interval(time.Hour).All(ctx) {
		if !errors.Is(err, cause) {
			t.Fatalf("All error = %v, want the cause", err)
		}
	}
}

func TestNestedConcPanicIsNotWrappedTwice(t *testing.T) {
	inner := conc.Panic{Value: "deep bug", Stack: "original stack"}
	got := catchPanic(func() {
		_, _ = Just(1).Map(func(int) int { panic(inner) }).Collect(t.Context())
	})
	if got != inner {
		t.Fatalf("panic = %#v, want the inner conc.Panic unchanged", got)
	}
}
