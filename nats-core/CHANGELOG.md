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
