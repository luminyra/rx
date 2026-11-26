# Changelog

All notable changes to this project are documented in this file. The format
is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and this
project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- Reactive streams with method-based operators, `Collect`, range-over-func consumption with `All`, and context-aware subscriptions.
- Operators are fused into their source, so a pipeline takes at most one lock per value; built-in synchronous sources take none.
- Benchmarks (`make bench`).
- A Makefile with the development tasks (`make help`).
- Requires Go 1.27 or later.
