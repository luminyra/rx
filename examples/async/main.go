// Command async uses asynchronous rx sources: a channel, a timer interval
// consumed with a range loop, and Merge reporting the first error.
package main

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/luminyra/rx"
)

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// region: channel
	// The source completes when the channel is closed.
	steps := make(chan string)
	go func() {
		defer close(steps)
		for _, s := range []string{"build", "test", "deploy"} {
			steps <- s
		}
	}()
	upper, err := rx.FromChannel(steps).Map(strings.ToUpper).Collect(ctx)
	// endregion
	fmt.Println(upper, err)

	// region: interval
	// Interval never completes on its own. Breaking out of the range loop
	// stops the timer and waits for it to exit.
	for tick, err := range rx.Interval(10 * time.Millisecond).All(ctx) {
		if err != nil {
			fmt.Println("error:", err)
			break
		}
		fmt.Println("tick", tick)
		if tick == 2 {
			break
		}
	}
	// endregion

	// region: merge
	// When any source fails, Merge ends with that error; the values received
	// so far are still returned.
	merged, err := rx.Merge(
		rx.Just(1, 2),
		rx.Fail[int](errors.New("downstream service unavailable")),
		rx.Just(3, 4),
	).Collect(ctx)
	// endregion
	fmt.Println(merged, err)
}
