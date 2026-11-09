# NATS Client

A Python client for the NATS messaging system.

## Features

- Support for publish/subscribe
- Support for request/reply
- Support for queue groups
- Support for multi-value message headers

Control lines, including large INFO discovery lists, are limited to 8 MiB.
WebSocket messages are limited to 72 MiB, allowing a maximum 64 MiB NATS payload
plus protocol overhead. Oversized input fails the connection rather than growing
memory without a bound.

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

## Discovery and TLS names

Discovered TCP and WebSocket peers retain the transport and TLS of their reporting
connection. Advertisements must be `host:port` entries; entries with a URL scheme
or a malformed address are skipped. User information in an advertisement is
ignored. Configured server URLs keep their own credentials.

When discovery arrives over verified TLS, an advertised IP address is verified against the DNS
name used to verify the reporting server, including across multiple failovers.
A discovered DNS name is verified against itself. Set `tls_hostname` to override
the verified name for every server, for example when connecting through a proxy.

If the configured server is an IP address, there is no DNS name to inherit.
Configure `tls_hostname`, or use `tls_verify_discovered_by_ip=True` for a cluster
whose certificates contain IP subject alternative names. Use the IP option only
with a CA that is not shared with others. An explicit `tls_hostname` takes
precedence over this option. Both options request TLS unless `tls_optional=True`.

For nodes with individual DNS certificates, configure each node to advertise
its DNS name instead of its IP address.


A nonempty list received on an established connection replaces discovered peers;
configured servers and the current connection remain. Missing or empty lists do
not change membership. Set `ignore_discovered_servers=True` to use only configured
servers. A discovered peer keeps the credentials of its first reporter. It can
gain TLS from a later report but cannot lose it.

For TCP connections that upgrade to TLS after INFO, the initial plaintext server
list is discarded. On nats-server 2.12 and later, connection setup waits for the
INFO after the first PONG, with its own timeout. Expiry of that wait still yields
a connected client. On older servers, configure multiple seeds or use TLS first
when initial failover targets are needed.

## TLS options

Certificate chain and name verification are enabled by default. For a name
mismatch, first check the certificate and set `tls_hostname` to the intended
server name. One trailing dot is removed from the verification/SNI name while
the address used to dial stays unchanged. Verification errors include the
expected name and OpenSSL's verification reason; Python may not expose the peer's
certificate names after a rejected handshake.

As a last resort, `tls_skip_verify=True` explicitly disables chain and name
verification. A supplied context that disables either check requires this flag.
Caller-owned contexts are never modified. With this flag, omit `tls` to create a
private unverified context, or supply a dedicated context already configured with
`check_hostname=False` and `verify_mode=ssl.CERT_NONE`. Other supplied contexts are
rejected before connecting.

Any TLS setting, or a secure URL in the configured list, requests TLS for the
whole list. `tls_optional=True` opts out of that whole-list requirement and lets
plain configured URLs stay plain. Secure URLs still require TLS. It cannot be
combined with `tls_required=True` or `tls_handshake_first=True`.

## Credentials, deadlines and reconnect

URL credentials take precedence over token, user/password and bare NKey options.
They are percent-decoded once. JWT and credentials-file handlers remain active.
Configured servers retain their own credentials; discovery cannot replace them.

The default connection timeout is 5 seconds. Transport setup, TLS, CONNECT and
the first PONG share that budget. The first reconnect starts immediately; later
attempts use bounded exponential backoff with jitter. `no_randomize=True` retains
configured order. The current reconnect limit is 60 failed attempts per server;
use zero for unlimited attempts. Removing or rediscovering an address does not
reset its failure count. Closing stops recovery and bounds transport shutdown.
