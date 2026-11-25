# 0.3.0

### Added

- WebSocket connections with the optional `websocket` extra (#906)
- Multiple seed URLs with complete per-server handshakes and credentials (#902)
- `Client.force_reconnect()` for operator-initiated failover (#910)
- `Subscription.unsubscribe_after()` for capped message delivery (#961)
- Methods to remove registered client event callbacks (#957)
- Lame duck mode support and `ServerInfo.server_name` (#885, #904)
- `name`, `verbose`, and `pedantic` connection options (#903, #949)
- Typed exceptions for server-reported `-ERR` messages (#956)
- Explicit TLS policy, hostname, and verification options, plus certificate diagnostics (#1106, #1112)
- `ignore_discovered_servers` to connect only to configured seeds (#1109)

### Changed

- Default to 60 failed reconnect attempts per server; preserve retry history and evict exhausted servers (#905, #960, #1108)
- Start the first reconnect immediately and bound each connection attempt through the first PONG; direct DNS resolution has a separate timeout budget (#1105, #1111)
- Require explicit `tls_skip_verify=True` to disable certificate verification, including with custom TLS contexts (#1106)
- Percent-decode URL credentials and give them precedence over credential options and bare NKey authentication (#889, #1107)
- Make `Headers` a case-insensitive mutable mapping (#954)
- Validate subjects, reply subjects, queue groups, header keys, and payload sizes locally; sanitize header values (#897, #1018, #888)
- Export `connect()` from `nats.client` and document migration from `nats.aio` (#879, #901, #1110)

### Fixed

- Enforce configured TLS intent, reject buffered plaintext before TLS upgrades, and continue secure reconnect after rejecting plaintext peers (#912, #1054, #1055, #1106)
- Apply the same TLS flow during reconnect and verify that the server accepts CONNECT (#907, #908)
- Preserve WebSocket transport during discovery, ignore advertised credentials and transport overrides, and trust only eligible INFO sources after TLS (#1056, #1107, #1109)
- Preserve discovered-server identity, credentials, TLS names, and retry history across server-list updates (#1108)
- Handle handshake traffic and malformed protocol input without leaving connections stalled (#1104)
- Stop reconnect recovery reliably when closing, close sockets after failed connection setup, and reset keepalive state on reconnect (#981, #1072, #980)
- Redact URL credentials in connection diagnostics and avoid logging CONNECT payloads (#1007, #955)
- Use unpadded base64url for nonce signatures and defer optional `nkeys` imports until authentication setup (#886, #979)
- Reset subscription pending counters when unsubscribing (#887)

# 0.2.0

### Added

- `Client.rtt()` method to measure round-trip time to the server (#859)

### Changed

- Request/reply multiplexed over a single inbox subscription instead of creating a new subscription per request (#825)
- Build backend from setuptools to uv_build (#813)

### Fixed

- Various type annotation issues in client and protocol handling (#827)

# 0.1.0

Initial release
