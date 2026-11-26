// Package rx implements ReactiveX-style reactive streams for Go 1.27.
//
// An [Observable] emits zero or more values, then at most one terminal event:
// an error or completion.
//
// # Pipelines
//
// Operators are methods on Observable, so a pipeline reads left to right.
// [Observable.Map], [Observable.Scan] and [Observable.Reduce] are generic
// methods that may change the value type:
//
//	labels := rx.Range(1, 20).Filter(isEven).Map(strconv.Itoa).Take(4)
//
// Sources include [Just], [Range], [FromSeq], [FromChannel], [Interval],
// [Empty], [Fail] and [Merge]; [New] builds a custom source from a
// [Producer] that emits through an [Emitter].
//
// # Consuming results
//
// [Observable.Subscribe] delivers events to the callbacks of an [Observer],
// [Observable.Collect] gathers the values into a slice, and [Observable.All]
// yields them to a range loop, stopping the source when the loop breaks:
//
//	for label, err := range labels.All(ctx) {
//		if err != nil {
//			return err
//		}
//		fmt.Println(label)
//	}
//
// # Performance
//
// Operators are fused into their source: values pass from one operator to
// the next by direct calls, and a pipeline takes at most one lock per value,
// however many operators it has. [Observable.All] hands each value from the
// source's goroutine to the loop, which costs more; prefer
// [Observable.Collect] or [Observable.Subscribe] for high-throughput streams.
//
// # Delivery guarantees
//
// Subscribe runs synchronous sources, such as Just, on the calling goroutine
// and returns when they finish; asynchronous sources, such as Interval, emit
// from their own goroutine. Events delivered to one Observer are serialized,
// and nothing is delivered after a terminal event, after
// [Subscription.Unsubscribe], or after the subscription context is done.
// Callbacks must not unsubscribe their own subscription and then wait for it.
//
// # Panics
//
// A panic in a stream reaches whoever waits for it. [Observable.Collect],
// the loop over [Observable.All] and [Subscription.Unsubscribe] re-panic with
// a [conc.Panic] holding the original value and stack once the subscription
// has ended, even when the panic happened on another goroutine. A
// [runtime.Goexit] there ends the stream with an error matching
// [conc.ErrGoexit]. A producer that emits from its own goroutine starts it
// with [Go] to get the same guarantees.
//
// The package is inspired by ReactiveX (RxJava, RxJS) and samber/ro.
package rx
