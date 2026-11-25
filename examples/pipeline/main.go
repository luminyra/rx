// Command pipeline builds rx pipelines by chaining operator methods and
// consumes them with Collect, All and Subscribe.
package main

import (
	"context"
	"fmt"
	"strconv"

	"github.com/luminyra/rx"
)

func main() {
	ctx := context.Background()

	// region: chain
	// Multiples of 3 between 1 and 20, squared, first four only. Operators
	// are methods, so the pipeline reads left to right.
	squares := rx.Range(1, 20).
		Filter(func(n int) bool { return n%3 == 0 }).
		Map(func(n int) int { return n * n }).
		Take(4)
	values, err := squares.Collect(ctx)
	// endregion
	fmt.Println(values, err)

	// region: scan
	// Scan emits every intermediate result, Reduce only the final one. Both
	// are generic methods, so the result type may differ from the input.
	words := rx.Just("go", "is", "fun")
	sentence, _ := words.Scan("", func(acc, w string) string {
		if acc == "" {
			return w
		}
		return acc + " " + w
	}).Collect(ctx)
	totalLen, _ := words.Reduce(0, func(sum int, w string) int { return sum + len(w) }).Collect(ctx)
	// endregion
	fmt.Printf("%q\n", sentence)
	fmt.Println(totalLen)

	// region: range
	// All hands the results to a range loop, like iterating over a slice.
	for label, err := range rx.Range(1, 3).Map(strconv.Itoa).All(ctx) {
		if err != nil {
			fmt.Println("error:", err)
			break
		}
		fmt.Println("received #" + label)
	}
	// endregion

	// region: subscribe
	// Subscribe delivers events to the callbacks of an Observer; leave out
	// the callbacks you do not need.
	sub := rx.Just(1, 2).Subscribe(ctx, rx.Observer[int]{
		Next:     func(n int) { fmt.Println("value", n) },
		Complete: func() { fmt.Println("done") },
	})
	<-sub.Done()
	// endregion
}
