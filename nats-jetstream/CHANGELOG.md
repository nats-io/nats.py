# 0.2.0

### Added

- Ordered consumers with automatic sequence tracking, gap detection, and consumer recovery (#832)
- `close()` and async context manager support on `Consumer` protocol (#872)
- `stop()` method on `MessageStream` protocol (#871)

### Changed

- `MessageBatch` and `MessageStream` protocols extend `AsyncIterable` (#866, #865)
- `Consumer.messages()` returns `MessageStream` instead of `AsyncIterator` (#864)
- `ConsumerConfig` timestamp fields (`opt_start_time`, `pause_until`) use `datetime` instead of raw values (#826)
- Build backend from setuptools to uv_build (#813)

# 0.1.0

Initial release
