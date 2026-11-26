package rx_test

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/luminyra/conc"
	"github.com/luminyra/rx"
)

func Example() {
	labels := rx.Range(1, 20).
		Filter(func(n int) bool { return n%3 == 0 }).
		Map(func(n int) string { return "#" + strconv.Itoa(n) }).
		Take(3)

	values, err := labels.Collect(context.Background())
	fmt.Println(values, err)
	// Output: [#3 #6 #9] <nil>
}

func ExampleObservable_All() {
	for v, err := range rx.Just("a", "b", "c").All(context.Background()) {
		if err != nil {
			fmt.Println("error:", err)
			break
		}
		fmt.Println(v)
		if v == "b" {
			break // stops the source
		}
	}
	// Output:
	// a
	// b
}

func ExampleObservable_Scan() {
	running, _ := rx.Just(3, 1, 4).Scan(0, func(sum, n int) int { return sum + n }).Collect(context.Background())
	fmt.Println(running)
	// Output: [3 4 8]
}

func ExampleMerge() {
	_, err := rx.Merge(rx.Just(1, 2), rx.Fail[int](errors.New("upstream down"))).Collect(context.Background())
	fmt.Println(err)
	// Output: upstream down
}

func ExampleObservable_MapAsyncOrdered() {
	lookup := func(ctx context.Context, id int) (string, error) {
		time.Sleep(time.Duration(4-id) * time.Millisecond) // later ids answer sooner
		return "user-" + strconv.Itoa(id), nil
	}
	names, err := rx.Just(1, 2, 3).MapAsyncOrdered(3, lookup).Collect(context.Background())
	fmt.Println(names, err)
	// Output: [user-1 user-2 user-3] <nil>
}

func ExampleGo() {
	ticks := rx.New(func(ctx context.Context, e rx.Emitter[int]) func() {
		rx.Go(ctx, func() { // instead of a go statement
			for i := 1; i <= 3; i++ {
				e.Next(i)
			}
			e.Complete()
		})
		return nil
	})
	bug := ticks.Map(func(n int) int {
		if n == 2 {
			panic("bug in Map")
		}
		return n
	})
	defer func() {
		p := recover().(conc.Panic) // raised on this goroutine, not on the producer's
		fmt.Println("recovered:", p.Value)
	}()
	_, _ = bug.Collect(context.Background())
	// Output: recovered: bug in Map
}

func ExampleNew() {
	countdown := rx.New(func(ctx context.Context, e rx.Emitter[int]) func() {
		for n := 3; n > 0 && ctx.Err() == nil; n-- {
			e.Next(n)
		}
		e.Complete()
		return nil
	})

	sub := countdown.Subscribe(context.Background(), rx.Observer[int]{
		Next:     func(n int) { fmt.Println(n) },
		Complete: func() { fmt.Println("liftoff") },
	})
	<-sub.Done()
	// Output:
	// 3
	// 2
	// 1
	// liftoff
}
