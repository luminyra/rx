<img align="right" src="https://images.luminyra.dev/luminyra/github/rx/logo.webp" alt="Indigo and coral event pulses converging into one shared stream." width="96" height="96">

# rx

[![Go Reference](https://pkg.go.dev/badge/github.com/luminyra/rx.svg)](https://pkg.go.dev/github.com/luminyra/rx)
[![CI](https://github.com/luminyra/rx/actions/workflows/ci.yml/badge.svg)](https://github.com/luminyra/rx/actions/workflows/ci.yml)
[![Go Report Card](https://goreportcard.com/badge/github.com/luminyra/rx)](https://goreportcard.com/report/github.com/luminyra/rx)
[![Go 1.27+](https://img.shields.io/badge/Go-1.27%2B-00ADD8?logo=go&logoColor=white)](https://go.dev/doc/devel/release)

rx processes streams of values that arrive over time, such as events from a channel, ticks from a timer or messages from a subscription, with the operators known from ReactiveX: `Filter`, `Map`, `Take`, `Scan`, `Merge` and friends.

You describe the pipeline once, left to right, and rx takes care of the goroutines, cancellation and ordering. When you just want the values, consume the stream with an ordinary `for … range` loop.

- **Readable pipelines.** Operators are methods: `source.Filter(…).Map(…).Take(10)`. `Map`, `Scan` and `Reduce` are Go 1.27 generic methods that can change the value type.
- **Range over a stream.** `All(ctx)` hands values to a `for` loop; `break` stops the source and leaves no goroutine behind.
- **Context native.** Every subscription takes a `context.Context`; cancellation and deadlines end it cleanly.
- **Strict delivery.** Values reach an observer one at a time, and nothing arrives after completion, an error or unsubscribing.
- **Low overhead.** Operators are fused into their source, so a value passes through a whole pipeline with no extra locks: about 12 ns per value.

<p align="center">
  <img src="https://images.luminyra.dev/luminyra/github/rx/cover.webp" alt="Indigo and coral signal pulses converge into a single stream." width="640">
</p>

## Contents

- [When to use it](#when-to-use-it) · [Install](#install) · [Getting started](#getting-started)
- [Common tasks](#common-tasks) · [How it works](#how-it-works) · [Performance](#performance) · [API at a glance](#api-at-a-glance)

## When to use it

rx helps when values arrive asynchronously and you want to combine, filter, accumulate or cut off the stream declaratively: merging several event sources, taking the first few ticks of a timer, keeping a running total, or stopping everything when a request is cancelled.

For data that is already in memory, a slice and a loop, or [coll](https://github.com/luminyra/coll), is simpler. For a single producer and a single consumer, a plain channel is enough.

## Install

```sh
go get github.com/luminyra/rx
```

rx requires Go 1.27 or later. Its only dependency is [conc](https://github.com/luminyra/conc), another Luminyra library with no dependencies.

## Getting started

Copy the program into `main.go` and run it with `go run .`:

```go
package main

import (
	"context"
	"fmt"
	"log"
	"strconv"

	"github.com/luminyra/rx"
)

func main() {
	ctx := context.Background()

	// A pipeline reads left to right: numbers 1 to 20, keep multiples of 3,
	// turn them into labels, stop after four.
	labels := rx.Range(1, 20).
		Filter(func(n int) bool { return n%3 == 0 }).
		Map(func(n int) string { return "#" + strconv.Itoa(n) }).
		Take(4)

	// Consume it with a plain for loop...
	for label, err := range labels.All(ctx) {
		if err != nil {
			log.Fatal(err)
		}
		fmt.Println("got", label)
	}

	// ...or collect everything at once.
	all, err := labels.Collect(ctx)
	fmt.Println(all, err)
}
```

```text
got #3
got #6
got #9
got #12
[#3 #6 #9 #12] <nil>
```

`labels` is a description of a stream, not the stream itself: each call to `All` or `Collect` runs it again from the start.

<p align="center">
  <a href="https://images.luminyra.dev/luminyra/github/rx/demo.webp"><img src="https://images.luminyra.dev/luminyra/github/rx/demo.webp" alt="Two asynchronous sources feed one stream. Take ends the subscription after three events." width="760"></a>
</p>

## Common tasks

### Create a stream

```go
rx.Just("a", "b", "c")          // fixed values
rx.Range(1, 10)                 // 1, 2, …, 10
rx.FromSeq(slices.Values(jobs)) // any iter.Seq
rx.FromChannel(updates)         // until the channel is closed
rx.Interval(time.Second)        // 0, 1, 2, … every second, until cancelled
rx.Fail[int](err)               // an immediate error
```

### Transform, filter and limit

```go
alerts := readings.
	Filter(func(r Reading) bool { return r.Value > threshold }).
	Map(func(r Reading) Alert { return Alert{Sensor: r.Sensor} }).
	Take(5) // stop after five alerts
```

`Skip(n)` drops the first values, and `Tap(fn)` observes values without changing them, which is handy for logging.

### Keep a running result

```go
running := rx.Just(3, 1, 4).Scan(0, func(sum, n int) int { return sum + n }) // 3, 4, 8
total := rx.Just(3, 1, 4).Reduce(0, func(sum, n int) int { return sum + n }) // 8, when the stream ends
```

### Combine streams

```go
all := rx.Merge(fromQueue, fromWebhook, fromPolling) // values as they arrive; fails on the first error
more := fromQueue.MergeWith(fromWebhook)
```

### Consume a stream

Pick the style that fits the call site:

```go
// 1. A for loop; break stops the source.
for event, err := range events.All(ctx) {
	if err != nil {
		return err
	}
	handle(event)
}

// 2. Everything at once.
values, err := events.Take(100).Collect(ctx)

// 3. Callbacks, for long-running subscriptions.
sub := events.Subscribe(ctx, rx.Observer[Event]{
	Next:  handle,
	Error: func(err error) { log.Print(err) },
})
defer sub.Unsubscribe()
```

### Stop a stream

Cancel the context, or call `Unsubscribe` on the subscription. `Done()` is closed once the subscription has ended, for whatever reason:

```go
ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
defer cancel()
values, err := rx.Interval(time.Second).Collect(ctx) // the ticks so far, and context.DeadlineExceeded
```

### Write your own source

```go
lines := rx.New(func(ctx context.Context, e rx.Emitter[string]) func() {
	scanner := bufio.NewScanner(os.Stdin)
	for ctx.Err() == nil && scanner.Scan() {
		e.Next(scanner.Text())
	}
	if err := scanner.Err(); err != nil {
		e.Error(err)
	} else {
		e.Complete()
	}
	return nil // or a cleanup function, called when the subscription ends
})
```

A producer that emits from its own goroutine starts it with `rx.Go(ctx, fn)` instead of a `go` statement, so a panic downstream reaches the consumer instead of crashing the program:

```go
ticks := rx.New(func(ctx context.Context, e rx.Emitter[time.Time]) func() {
	ticker := time.NewTicker(time.Second)
	rx.Go(ctx, func() {
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case t := <-ticker.C:
				e.Next(t)
			}
		}
	})
	return nil
})
```

## How it works

- An `Observable` is a value that describes a stream. Subscribing runs its producer; operators return new Observables.
- Synchronous sources such as `Just`, `Range` and `FromSeq` run on the subscribing goroutine, so `Subscribe` returns once they finish. Asynchronous sources such as `Interval` and `FromChannel` run on their own goroutine.
- Events to one observer are serialized. After `Complete`, `Error`, `Unsubscribe` or the end of the context, nothing more is delivered, even if a producer misbehaves.
- `All` runs the subscription on a goroutine and hands values over one at a time; the source waits for your loop body before producing the next value.
- Callbacks must not unsubscribe their own subscription and then wait for it to finish.
- A panic in a stream reaches whoever waits for it. `Collect`, the loop over `All` and `Unsubscribe` re-panic with a `conc.Panic` that holds the original value and stack, after the subscription has ended and every teardown has run, even when the panic happened on another goroutine: one started by `Interval`, `FromChannel` or `rx.Go`, or the one that runs teardowns. A `runtime.Goexit` there ends the stream with an error matching `conc.ErrGoexit`. With a plain `Subscribe` nobody waits, so such a panic crashes the program like any unrecovered panic, and a synchronous source's panic simply continues on your goroutine, as any function call's would.

## Performance

Measured with the benchmarks in this repository (`make bench`; Go 1.27.4, linux/arm64, 5 CPUs, median of 6 runs). Run them on your hardware for numbers that matter to you.

Pushing 1,000 values through `Range(…).Filter(…).Map(…)`:

| Consumer | Time | Per value |
| --- | --- | --- |
| A hand-written loop (for comparison) | 1.8 µs | 1.8 ns |
| `Collect` | 11.9 µs | 12 ns |
| `for … range All(ctx)` | 106 µs | 106 ns |

Operators are fused into their source: a pipeline takes one lock per value, however many operators it has, and sources such as `Range`, `Just` and `FromSeq` take none. `All` hands every value from the source's goroutine to your loop, which costs about 100 ns; for high-throughput streams, use `Collect` or `Subscribe`.

## API at a glance

| Task | API |
| --- | --- |
| Sources | `Just`, `Range`, `FromSeq`, `FromChannel`, `Interval`, `Empty`, `Fail`, `New` |
| Operators | `Map`, `Filter`, `Tap`, `Take`, `Skip`, `Scan`, `Reduce`, `MergeWith`, `Merge` |
| Consume | `All`, `Collect`, `Subscribe`, `Observer`, `Subscription` |
| Write sources | `New`, `Producer`, `Emitter`, `Go` |

The full reference with runnable examples is on [pkg.go.dev](https://pkg.go.dev/github.com/luminyra/rx). Complete programs live in [`examples/`](examples): [`pipeline`](examples/pipeline/main.go) and [`async`](examples/async/main.go).

## Versioning

rx follows [semantic versioning](https://semver.org). Until v1.0.0, a minor release may change the API; every change is listed in the [changelog](CHANGELOG.md). It supports the two most recent Go releases, starting with Go 1.27.

## Contributing

Questions, bug reports and pull requests are welcome. See the [contributing guide](https://github.com/luminyra/.github/blob/main/CONTRIBUTING.md), and report security issues as described in the [security policy](https://github.com/luminyra/.github/blob/main/SECURITY.md).

rx was inspired by ReactiveX (RxJava, RxJS) and [samber/ro](https://github.com/samber/ro). It is released under the [MIT License](LICENSE).
