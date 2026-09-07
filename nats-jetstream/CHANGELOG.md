# 0.3.0

### Added

- Atomic batch publishing — `allow_atomic`/`allow_batched` on `StreamConfig`, `Nats-Batch-*` header constants, and `batch_id`/`batch_size` on `PublishAck` (ADR-50, #922)
- Message schedules — `allow_msg_schedules` on `StreamConfig` and `Nats-Schedule-*` header constants (ADR-51, #923)
- Counter streams — `allow_msg_counter` on `StreamConfig` and `value` on `PublishAck` (ADR-49, #926)
- Stream sourcing with pre-created push-durable consumers — `StreamConsumerSource` on `StreamSource` (ADR-60, #921)
- `Stream.reset_consumer()` for the `$JS.API.CONSUMER.RESET` endpoint, plus `ConsumerReset` and `ConsumerInvalidResetError` (#920)
- `persist_mode` on `StreamConfig` for R1 async persistence (ADR-56, #929)
- Re-export of `new` from `nats.jetstream` (#880)

### Fixed

- `Stream.get_message()` now raises `MessageNotFoundError` on a 404 from the direct-get path instead of leaking the underlying status error (#881)

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
