# CLAUDE.md

Async Python client and tooling for the [NATS](https://nats.io) messaging system. This is a **uv workspace** with four packages sharing the `nats` namespace.

## Workspace Packages

| Package | Directory | PyPI name | Python | Status |
|---------|-----------|-----------|--------|--------|
| nats-py (legacy client) | `nats/` | `nats-py` | >=3.7 | Production |
| nats-core (modern client) | `nats-core/` | `nats-core` | >=3.13 | In development |
| nats-jetstream | `nats-jetstream/` | `nats-jetstream` | >=3.11 | In development |
| nats-server (test helper) | `nats-server/` | `nats-server` | >=3.11 | Production |

Namespace packaging: `nats-py` provides `nats`, `nats.aio`, `nats.js`, `nats.micro`; `nats-core` provides `nats.client`; `nats-jetstream` provides `nats.jetstream`; `nats-server` provides `nats.server`.

## Prerequisites

- [uv](https://docs.astral.sh/uv/getting-started/installation/) package manager
- `nats-server` binary in PATH ([install](https://docs.nats.io/running-a-nats-service/introduction/installation)) -- required for tests

## Build / Dev Commands

```bash
# Install all workspace dependencies
uv sync

# Run all tests (uses nats/tests by default, see [tool.pytest.ini_options] in root pyproject.toml)
uv run pytest

# Run tests for a specific package
uv run pytest nats/tests                  # legacy client
uv run pytest nats-core/tests             # modern client
uv run pytest nats-server/tests           # server management
uv run pytest nats-jetstream/tests        # jetstream

# Run specific test file or function
uv run pytest nats/tests/test_client.py
uv run pytest nats-core/tests/test_client.py::test_connect_succeeds_with_valid_url

# Parallel tests
uv run pytest -n auto

# Verbose with output (matches CI)
uv run pytest -x -vv -s --continue-on-collection-errors

# Lint (ruff -- checks E/F/W/I rules, ignores E501)
uv run ruff check

# Format check (ruff format)
uv run ruff format --check

# Auto-format
uv run ruff format

# Spell check
uv run codespell

# Type check (ty, only nats-server currently)
uv run ty check nats-server
```

## CI Checks (`.github/workflows/`)

**check.yml** -- runs on every push/PR:
- `ruff format --check` (format)
- `ruff check` (lint)
- `codespell` (spelling)
- `ty check nats-server` (type check)

**test.yml** -- runs on every push/PR:
- Legacy client (`nats/tests`): Python 3.8-3.13, latest nats-server, Ubuntu
- Modern packages (`nats-core`, `nats-server`): Python 3.13, latest nats-server, Ubuntu/macOS/Windows

## Project Structure

```
nats/                        # Legacy asyncio NATS client (nats-py)
  src/nats/
    __init__.py              # Entry: nats.connect() -> Client
    aio/
      client.py              # Main Client class (connection, pub/sub, reconnect)
      msg.py                 # Msg class
      subscription.py        # Subscription class
      transport.py           # TCP and WebSocket transports
    js/                      # JetStream support (streams, consumers, KV, object store)
      client.py              # JetStream context
      api.py                 # API request/response types
      kv.py                  # Key-Value store
      object_store.py        # Object store
    micro/                   # Microservices framework
    protocol/                # NATS protocol parsing/encoding
      parser.py              # Protocol parser
      command.py             # Command encoding
    errors.py                # Error types
    nuid.py                  # NUID generation
  tests/                     # unittest-based, uses NATSD helper class from utils.py
  scripts/install_nats.sh    # Downloads nats-server binary for CI

nats-core/                   # Modern NATS client (Python 3.13+)
  src/nats/client/
    __init__.py              # Entry: nats.client.connect() -> Client
    connection.py            # Connection protocol + TCP implementation
    message.py               # Message, Headers, Status dataclasses
    subscription.py          # Subscription handling
    errors.py                # Error types
    protocol/                # Protocol encoding/decoding
  tests/                     # pytest-asyncio, uses fixtures from conftest.py

nats-jetstream/              # JetStream package (depends on nats-core)
  src/nats/jetstream/
    __init__.py              # Entry: nats.jetstream.new(client) -> JetStream
    stream.py                # Stream types and operations
    consumer/                # Consumer types (pull-based)
    message.py               # JetStream message types
    api/                     # API client and types
    errors.py                # JetStream-specific errors
  tests/                     # pytest-asyncio, uses fixtures from conftest.py

nats-server/                 # Server management for testing
  src/nats/server/
    __init__.py              # run(), run_cluster(), Server, ServerCluster
  tests/                     # pytest-asyncio, uses fixtures from conftest.py
```

## Code Style and Conventions

### Formatting and Linting
- **Formatter**: `ruff format` (double quotes, spaces, 120 char line length)
- **Linter**: `ruff` with rules E, F, W, I enabled; E501 (line length) ignored globally
- **Relaxed rules**: `nats/examples/*`, `nats/benchmark/*`, `nats/tests/*` have additional ignores (see `[tool.ruff.lint.per-file-ignores]` in root `pyproject.toml`)
- **Spell check**: `codespell` with custom ignore words: nats, nuid, ack, nak, hdr

### Type Annotations
- Legacy code (`nats/`): uses `from __future__ import annotations` and `typing` module (Union, Optional, List, etc.) for Python 3.7+ compat
- Modern code (`nats-core/`, `nats-jetstream/`, `nats-server/`): uses native Python 3.11+/3.13+ type syntax (`str | None`, `list[str]`, `Self`)
- Type checking with `ty` applies only to `nats-server` currently (see `[tool.ty.src]` exclude in root pyproject.toml)

### Testing Patterns
- **Legacy tests** (`nats/tests/`): `unittest.TestCase` subclasses with helper base classes in `utils.py` (e.g., `SingleServerTestCase`, `SingleJetStreamServerTestCase`). Uses `@async_test` decorator. Manages nats-server processes directly via `NATSD` helper class with manual start/stop.
- **Modern tests** (`nats-core/tests/`, `nats-server/tests/`, `nats-jetstream/tests/`): plain `pytest` + `pytest-asyncio` with `asyncio_mode = "auto"`. Uses `pytest_asyncio.fixture` in `conftest.py` for `server` and `client` fixtures. Servers use `nats.server.run(port=0)` for automatic port assignment.
- All tests require a `nats-server` binary in PATH
- pytest-asyncio uses `asyncio_default_fixture_loop_scope = "function"`

### Errors
- Legacy: error classes in `nats/src/nats/errors.py`, all inherit from `Error(Exception)`, use `__str__` returning `"nats: ..."` prefix
- Modern: error classes per package (e.g., `nats.client.errors`, `nats.jetstream.errors`)

### Code Patterns
- Legacy client uses `from __future__ import annotations` everywhere
- Modern code uses `@dataclass` and `@dataclass(slots=True)` extensively
- nats-core uses `Protocol` (structural typing) for connection interface
- Entry points: `nats.connect()` (legacy), `nats.client.connect()` (modern), `nats.jetstream.new(client)` (jetstream)
- License header: Apache 2.0 in legacy `nats/` files; MIT in modern packages

### Dependencies
- nats-jetstream depends on nats-core (workspace dependency)
- nats-core and nats-jetstream tests depend on nats-server (workspace dependency)
- Optional extras for nats-py: `nkeys`, `aiohttp`, `fast_parse` (fast-mail-parser)
