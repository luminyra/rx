package rx

import (
	"context"
	"errors"
	"slices"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"
)

// slowDouble doubles v after v*10ms and records the peak concurrency.
func slowDouble(running, peak *atomic.Int32) func(context.Context, int) (int, error) {
	return func(ctx context.Context, v int) (int, error) {
		n := running.Add(1)
		defer running.Add(-1)
		for p := peak.Load(); n > p && !peak.CompareAndSwap(p, n); p = peak.Load() {
		}
		select {
		case <-time.After(time.Duration(v) * 10 * time.Millisecond):
			return 2 * v, nil
		case <-ctx.Done():
			return 0, ctx.Err()
		}
	}
}

func TestMapAsync(t *testing.T) {
	for name, source := range sources {
		t.Run(name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				var running, peak atomic.Int32
				start := time.Now()
				got, err := source(5, 1, 4, 2, 3).MapAsync(2, slowDouble(&running, &peak)).Collect(t.Context())
				if err != nil {
					t.Fatal(err)
				}
				// Two at a time: 5 and 1 start; 1 ends at 10ms, 4 starts; 5
				// ends at 50ms, 4 at 50ms; 2 and 3 run until 70ms and 80ms.
				if !slices.Equal(slices.Sorted(slices.Values(got)), []int{2, 4, 6, 8, 10}) || got[0] != 2 {
					t.Fatalf("results = %v, want every value doubled, fastest first", got)
				}
				if peak.Load() != 2 || time.Since(start) != 80*time.Millisecond {
					t.Fatalf("peak %d after %s, want 2 after 80ms", peak.Load(), time.Since(start))
				}
			})
		})
	}
}

func TestMapAsyncOrdered(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var running, peak atomic.Int32
		got, err := Just(5, 1, 4, 2, 3).MapAsyncOrdered(3, slowDouble(&running, &peak)).Collect(t.Context())
		if err != nil || !slices.Equal(got, []int{10, 2, 8, 4, 6}) {
			t.Fatalf("results = %v, %v; want source order", got, err)
		}
		if peak.Load() > 3 {
			t.Fatalf("peak concurrency %d, want at most 3", peak.Load())
		}
	})
}

func TestMapAsyncOrderedBoundsHeldResults(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		// The first value is slow; the next ones finish at once but keep
		// their slots until it is emitted, so at most limit values are
		// admitted.
		var admitted atomic.Int32
		release := make(chan struct{})
		stream := Range(0, 10).MapAsyncOrdered(3, func(ctx context.Context, v int) (int, error) {
			admitted.Add(1)
			if v == 0 {
				<-release
			}
			return v, nil
		})
		done := make(chan []int)
		go func() {
			got, _ := stream.Collect(t.Context())
			done <- got
		}()
		synctest.Wait()
		if n := admitted.Load(); n != 3 {
			t.Fatalf("admitted %d values behind a slow one, want 3", n)
		}
		close(release)
		if got := <-done; !slices.Equal(got, []int{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}) {
			t.Fatalf("results = %v", got)
		}
	})
}

func TestMapAsyncErrorCancelsOthers(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		boom := errors.New("boom")
		var waiting, canceled atomic.Int32
		got, err := Range(1, 100).MapAsync(4, func(ctx context.Context, v int) (int, error) {
			if v == 3 {
				return 0, boom
			}
			waiting.Add(1)
			<-ctx.Done()
			canceled.Add(1)
			return 0, ctx.Err()
		}).Collect(t.Context())
		if !errors.Is(err, boom) || len(got) != 0 {
			t.Fatalf("Collect = %v, %v; want boom", got, err)
		}
		// 1 and 2 are running when 3 fails; 4 may or may not have started.
		if w, c := waiting.Load(), canceled.Load(); c != w || c < 2 || c > 3 {
			t.Fatalf("%d of %d waiting calls saw the cancellation, want all of 2 or 3", c, w)
		}
	})
}

func TestMapAsyncSourceErrorAndTake(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		boom := errors.New("source failed")
		double := func(_ context.Context, v int) (int, error) { return 2 * v, nil }
		if _, err := Fail[int](boom).MapAsync(2, double).Collect(t.Context()); !errors.Is(err, boom) {
			t.Fatalf("source error = %v", err)
		}
		if got, err := Empty[int]().MapAsync(2, double).Collect(t.Context()); err != nil || len(got) != 0 {
			t.Fatalf("empty source = %v, %v", got, err)
		}
		var calls atomic.Int32
		got, err := Interval(time.Millisecond).MapAsyncOrdered(0, func(ctx context.Context, v int) (int, error) {
			calls.Add(1)
			return v, nil
		}).Take(3).Collect(t.Context())
		if err != nil || !slices.Equal(got, []int{0, 1, 2}) {
			t.Fatalf("Take(3) = %v, %v", got, err)
		}
		n := calls.Load()
		time.Sleep(time.Second)
		if calls.Load() != n {
			t.Fatal("the source kept running after Take completed")
		}
	})
}

func TestMapAsyncPanic(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		got := catchPanic(func() {
			for range Range(1, 10).MapAsync(3, func(_ context.Context, v int) (int, error) {
				return panicAt(4)(v), nil
			}).All(t.Context()) {
			}
		})
		checkStreamPanic(t, got, "bug")
	})
}

func BenchmarkMapAsync(b *testing.B) {
	double := func(_ context.Context, v int) (int, error) { return 2 * v, nil }
	ctx := context.Background()
	for _, ordered := range []bool{false, true} {
		name := map[bool]string{false: "unordered", true: "ordered"}[ordered]
		b.Run(name, func(b *testing.B) {
			src := Range(0, 1000)
			stream := src.MapAsync(8, double)
			if ordered {
				stream = src.MapAsyncOrdered(8, double)
			}
			for b.Loop() {
				if _, err := stream.Collect(ctx); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
