# NATS Server for Python

Manage NATS server instances from python.

## Features

- Asynchronous server control

## Installation

```bash
pip install nats-server
```

Requires Python 3.11+ and the NATS server binary in your PATH.

## Usage

```python
import asyncio
import nats.testing.server

async def main():
    # Start server with auto port assignment
    server = await nats.testing.server.run(port=0, jetstream=True)
    print(f"Server running on {server.host}:{server.port}")

    # Do something with the server...
    # e.g connect with server.client_url

    # Clean shutdown
    await server.shutdown()

asyncio.run(main())
```

## License

MIT

## Contributing

Contributions welcome. Submit a Pull Request on GitHub.
