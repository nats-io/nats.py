# NATS Client

A Python client for the NATS messaging system.

## Features

- Support for publish/subscribe
- Support for request/reply
- Support for queue groups
- Support for multi-value message headers

## Installation

```bash
pip install nats-core
```

## Usage

```python
import asyncio
from nats.client import connect

async def main():
    client = await connect("nats://localhost:4222")

    # Subscribe
    async with await client.subscribe("foo") as subscription:
        # Publish
        await client.publish("foo", "Hello World!")

        # Receive message
        message = await subscription.next()
        print(f"Received: {message.data}")

    await client.close()

if __name__ == "__main__":
    asyncio.run(main())
```

## WebSocket discovery and TLS names

Discovered WebSocket peers retain the transport and TLS of their reporting
connection. Advertisements must be `host:port` entries; entries with a URL scheme
or a malformed address are skipped. User information in an advertisement is
ignored. Configured server URLs keep their own credentials.

For `wss://` discovery, an advertised IP address is verified against the DNS
name used to verify the reporting server, including across multiple failovers.
A discovered DNS name is verified against itself. Set `tls_hostname` to override
the verified name for every server, for example when connecting through a proxy.

If the configured server is an IP address, there is no DNS name to inherit.
Configure `tls_hostname`, or use `tls_verify_discovered_by_ip=True` for a cluster
whose certificates contain IP subject alternative names. Use the IP option only
with a CA that is not shared with others. An explicit `tls_hostname` takes
precedence over this option. Both options request TLS.

For nodes with individual DNS certificates, configure each node to advertise
its DNS name instead of its IP address.
