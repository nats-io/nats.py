# Changelog

All notable changes to `nats-py` are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [2.17.0] - 2026-11-17

### Added

- JetStream pull consumer priority groups, including pinned, overflow and prioritized policies, consumer unpinning and priority-group state (#1015)
- Metadata support in `KeyValueConfig` and `create_key_value` (#1045)
- `StreamAlternate` and `StreamInfo.alternates` for alternate locations of mirrored stream data (#1098)

### Changed

- Reject durable consumer and queue-group settings when creating or binding ordered consumers (#1047)

### Fixed

- Enforce configured TLS across seed selection, discovery, server-pool replacement and reconnects, including WebSocket connections. Connections that previously fell back to plaintext now fail when TLS is required (#1118)
- Discard server discovery received in the plaintext INFO greeting before a TLS upgrade; accept fresh discovery received over the encrypted connection (#1119)
- Ignore credentials in server-advertised addresses. Preserve client URL credentials separately, keep discovery credentials bound to the active connection, and retain inherited credentials across server-pool replacement (#1120)

- Publish key-value mutations through the configured JetStream API prefix for domain contexts (#1011)
- Surface credential file permission errors instead of retrying them during connection and reconnect (#1024)
- Process queued callback messages before stopping at the auto-unsubscribe limit (#1035)
- Reconnect when authentication expires and report `AuthenticationExpiredError` (#1029)
- Keep stale-connection detection working after cancelled or timed-out flushes (#1031)
- Send explicit passwords alongside signed NKey/JWT CONNECT credentials, including callable passwords (#1043)
- Interrupt pending pull fetches on connection loss with `FetchDisconnectedError`, a subclass of `nats.errors.TimeoutError`, instead of waiting indefinitely or until the fetch timeout (#1065)
- Raise `nats.errors.TimeoutError` when the pull-fetch deadline expires before a lingering request can be sent (#980)
- Parse V2 ACK subjects correctly in ordered consumers and Object Store reads (#1071)
- Bind Object Store reads and watchers to the backing stream by name so mirrored buckets support `get`, `watch` and `list` (#976)
- Use timezone-aware UTC timestamps in micro services without changing their wire format (#1115)

## [2.16.0] - 2026-09-14

### Added

- Client-side subject validation for publish, subscribe and request, with a `skip_subject_validation` connect option to opt out (#1016)
- Allow `user` and `password` to be callables for credential refresh on reconnect (#891)
- `StreamSource.consumer` and `AckPolicy.FLOW_CONTROL` for pre-created sourcing consumers (#937)
- `JetStreamManager.reset_consumer` and `ConsumerInvalidResetError` (#941)
- Counter stream wire support (#940)
- Atomic batch publish headers and `PubAck` fields (#938)
- Message schedule header constants (#939)

### Changed

- Validate header keys and sanitize header values before publishing (#1017)
- Subscribing to subjects with empty tokens or misplaced wildcards now fails client-side with `BadSubjectError` instead of a server error (#1016)

### Fixed

- Default a missing port to 4222 and preserve the scheme and userinfo for every server in a URL list (#913, #1009)
- Resolve the WebSocket close task when a connection was never established, so `close()` no longer hangs (#993)

## [2.15.0] - 2026-06-04

### Added

- Lame duck mode handling for graceful reconnection (#869)
- `rtt` method to `Client` for measuring round-trip time (#858)
- `client_ip` property to `Client` (#861)
- `consumer_limits` support to `StreamConfig` (#780)
- `first_seq` support to `StreamConfig` (#779)
- `limit_marker_ttl` support for KV watchers (#911)
- `updates_only` mode to object store watch (#666)
- Stream created datetime to `StreamInfo` (#772)
- JetStream direct get timestamp parsing (#810)
- Send `auth_token` alongside nkey/JWT in `CONNECT` (#900)

### Changed

- Replace `email.parser` path in `_process_headers` with a byte-level parser (#928)
- Restrict `msg_ttl` to the create and purge key-value operations (#834)
- Validate stream and consumer names before API requests (#890)
- Avoid `$JS.API.STREAM.NAMES` call since the stream name is known (#807)
- Use a string default in `connect()` to avoid a mutable default argument (#884)
- Replace deprecated `asyncio.iscoroutinefunction` (#932)
- Migrate the build from setuptools to `uv_build` (#813)
- Include the `LICENSE` file in the nats-py sdist (#840)

### Fixed

- Avoid hang when `KeyWatcher.stop()` runs on a full queue (#899)
- Fix `PullSubscription.fetch` hang due to an orphan lingering request (#934)
- Fix flush hanging when internal tasks are cancelled externally (#853)
- Avoid cancelling the current task on close (#841)
- Guard `close()` against a `None` `_io_reader` (#839)
- Fix dropped message when cancelling the subscription messages iterator
- Handle CAS error code 10164 in KV update (#883)
- Strip whitespace from nkeys seed input (#882)
- Disable WebSocket max message size (#855)
- Handle non-binary WebSocket frame types (#893)
- Normalize ISO timestamp fractional seconds to 6 digits for Python <3.11 (#796)
- Fix error in UTC to ISO conversion (#846)

[2.17.0]: https://github.com/nats-io/nats.py/compare/v2.16.0...v2.17.0
[2.16.0]: https://github.com/nats-io/nats.py/compare/v2.15.0...v2.16.0
[2.15.0]: https://github.com/nats-io/nats.py/compare/v2.14.0...v2.15.0
