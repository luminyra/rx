# rx

[![Go Reference](https://pkg.go.dev/badge/github.com/luminyra/rx.svg)](https://pkg.go.dev/github.com/luminyra/rx)
[![CI](https://github.com/luminyra/rx/actions/workflows/ci.yml/badge.svg)](https://github.com/luminyra/rx/actions/workflows/ci.yml)

ReactiveX-style streams for Go 1.27 with method chaining, generic operators and range-over-func consumption.

```go
labels := rx.Range(1, 20).
	Filter(func(n int) bool { return n%3 == 0 }).
	Map(strconv.Itoa). // Observable[int] becomes Observable[string]
	Take(4)

for label, err := range labels.All(ctx) {
	if err != nil {
		return err
	}
	fmt.Println(label)
}
```

## Install

```sh
go get github.com/luminyra/rx
```

Requires Go 1.27 or later. The module has no dependencies.

## Highlights

- **Pipelines read left to right.** Operators are methods on `Observable`; `Map`, `Scan` and `Reduce` are Go 1.27 generic methods that may change the value type.
- **Three ways to consume.** `Subscribe` with an `Observer` of callbacks, `Collect` into a slice, or `All` to iterate with `range`. Breaking out of the loop stops the source and leaves no goroutine behind.
- **Context native.** Every subscription takes a `context.Context`; cancellation and deadlines end it cleanly.
- **Strict delivery.** Events to one observer are serialized, and nothing is delivered after a terminal event, `Unsubscribe`, or the end of the context.

## Usage

```go
// Sources: Just, Range, FromSeq, FromChannel, Interval, Empty, Fail, Merge.
ticks := rx.Interval(time.Second).Take(3)

// Custom sources emit through an Emitter.
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
	return nil
})

sub := lines.Subscribe(ctx, rx.Observer[string]{
	Next:  func(s string) { fmt.Println(s) },
	Error: func(err error) { log.Print(err) },
})
<-sub.Done()
```

## Examples

Runnable programs live in [`examples/`](examples):

| Example | Shows |
| --- | --- |
| [`pipeline`](examples/pipeline/main.go) | chaining, `Scan` and `Reduce`, `range`, `Subscribe` |
| [`async`](examples/async/main.go) | channels, intervals, `Merge` errors |

```sh
go run ./examples/pipeline
```

## Design

rx is inspired by ReactiveX (RxJava, RxJS) and [samber/ro](https://github.com/samber/ro). It targets Go 1.27 only: `Observable` is a concrete type whose operators are methods, so pipelines need no `Pipe` helpers, and `All` connects streams to Go's iterators.

## License

[MIT](LICENSE)
