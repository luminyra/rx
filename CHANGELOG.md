# Changelog

All notable changes to this project are documented in this file. The format
is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and this
project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- Reactive streams with method-based operators, `Collect`, range-over-func consumption with `All`, and context-aware subscriptions.
- Operators are fused into their source, so a pipeline takes at most one lock per value; built-in synchronous sources take none.
- A panic in a stream reaches whoever waits for it: `Collect`, the loop over `All` and `Unsubscribe` re-panic with a `conc.Panic` holding the original value and stack, even when it happened on the goroutine of `Interval`, `FromChannel` or a teardown. A `runtime.Goexit` there ends the stream with an error matching `conc.ErrGoexit` instead of a spurious zero value or a hang.
- `MapAsync` and `MapAsyncOrdered` run a transformation on up to `limit` goroutines, in completion or source order, hold back the source while every slot is busy, and fail the stream on the first error.
- `Go` starts a producer goroutine with the same guarantees.
- Every teardown runs and `Done` is closed even if a teardown panics, including the inner subscriptions of `Merge`; a synchronous source that panics is unsubscribed before the panic continues.
- `Collect` and `All` report `context.Cause` when the context ends first.
- Benchmarks (`make bench`).
- A Makefile with the development tasks (`make help`).
- Requires Go 1.27 or later. Depends on `github.com/luminyra/conc`.
