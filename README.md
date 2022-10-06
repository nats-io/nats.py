# NATS - Python3 Client for Asyncio

An [asyncio](https://docs.python.org/3/library/asyncio.html) Python client for the [NATS messaging system](https://nats.io).

[![docs](https://img.shields.io/static/v1?label=docs&message=docs&color=informational)](https://nats-io.github.io/nats.py/)
[![pypi](https://img.shields.io/pypi/v/nats-py.svg)](https://pypi.org/project/nats-py)
[![Build Status](https://travis-ci.com/nats-io/nats.py.svg?branch=main)](http://travis-ci.com/nats-io/nats.py)
[![Versions](https://img.shields.io/pypi/pyversions/nats-py.svg)](https://pypi.org/project/nats-py)
[![License Apache 2.0](https://img.shields.io/badge/License-Apache2-blue.svg)](https://www.apache.org/licenses/LICENSE-2.0)

## Supported platforms

Should be compatible with at least [Python +3.7](https://docs.python.org/3.7/library/asyncio.html).

## Installing

```bash
pip install nats-py
```

## Getting started

```python
import asyncio
import nats
from nats.errors import ConnectionClosedError, TimeoutError, NoServersError

async def main():
    # It is very likely that the demo server will see traffic from clients other than yours.
    # To avoid this, start your own locally and modify the example to use it.
    nc = await nats.connect("nats://demo.nats.io:4222")

    # You can also use the following for TLS against the demo server.
    #
    # nc = await nats.connect("tls://demo.nats.io:4443")

    async def message_handler(msg):
        subject = msg.subject
        reply = msg.reply
        data = msg.data.decode()
        print("Received a message on '{subject} {reply}': {data}".format(
            subject=subject, reply=reply, data=data))

    # Simple publisher and async subscriber via coroutine.
    sub = await nc.subscribe("foo", cb=message_handler)

    # Stop receiving after 2 messages.
    await sub.unsubscribe(limit=2)
    await nc.publish("foo", b'Hello')
    await nc.publish("foo", b'World')
    await nc.publish("foo", b'!!!!!')

    # Synchronous style with iterator also supported.
    sub = await nc.subscribe("bar")
    await nc.publish("bar", b'First')
    await nc.publish("bar", b'Second')

    try:
        async for msg in sub.messages:
            print(f"Received a message on '{msg.subject} {msg.reply}': {msg.data.decode()}")
            await sub.unsubscribe()
    except Exception as e:
        pass

    async def help_request(msg):
        print(f"Received a message on '{msg.subject} {msg.reply}': {msg.data.decode()}")
        await nc.publish(msg.reply, b'I can help')

    # Use queue named 'workers' for distributing requests
    # among subscribers.
    sub = await nc.subscribe("help", "workers", help_request)

    # Send a request and expect a single response
    # and trigger timeout if not faster than 500 ms.
    try:
        response = await nc.request("help", b'help me', timeout=0.5)
        print("Received response: {message}".format(
            message=response.data.decode()))
    except TimeoutError:
        print("Request timed out")

    # Remove interest in subscription.
    await sub.unsubscribe()

    # Terminate connection to NATS.
    await nc.drain()

if __name__ == '__main__':
    asyncio.run(main())
```

## JetStream

Starting v2.0.0 series, the client now has JetStream support:

```python
import asyncio
import nats
from nats.errors import TimeoutError

async def main():
    nc = await nats.connect("localhost")

    # Create JetStream context.
    js = nc.jetstream()

    # Persist messages on 'foo's subject.
    await js.add_stream(name="sample-stream", subjects=["foo"])

    for i in range(0, 10):
        ack = await js.publish("foo", f"hello world: {i}".encode())
        print(ack)

    # Create pull based consumer on 'foo'.
    psub = await js.pull_subscribe("foo", "psub")

    # Fetch and ack messagess from consumer.
    for i in range(0, 10):
        msgs = await psub.fetch(1)
        for msg in msgs:
            await msg.ack()
            print(msg)

    # Create single ephemeral push based subscriber.
    sub = await js.subscribe("foo")
    msg = await sub.next_msg()
    await msg.ack()

    # Create single push based subscriber that is durable across restarts.
    sub = await js.subscribe("foo", durable="myapp")
    msg = await sub.next_msg()
    await msg.ack()

    # Create deliver group that will be have load balanced messages.
    async def qsub_a(msg):
        print("QSUB A:", msg)
        await msg.ack()

    async def qsub_b(msg):
        print("QSUB B:", msg)
        await msg.ack()
    await js.subscribe("foo", "workers", cb=qsub_a)
    await js.subscribe("foo", "workers", cb=qsub_b)

    for i in range(0, 10):
        ack = await js.publish("foo", f"hello world: {i}".encode())
        print("\t", ack)

    # Create ordered consumer with flow control and heartbeats
    # that auto resumes on failures.
    osub = await js.subscribe("foo", ordered_consumer=True)
    data = bytearray()

    while True:
        try:
            msg = await osub.next_msg()
            data.extend(msg.data)
        except TimeoutError:
            break
    print("All data in stream:", len(data))

    await nc.close()

if __name__ == '__main__':
    asyncio.run(main())
```

## TLS

TLS connections can be configured with an [ssl context](https://docs.python.org/3/library/ssl.html#context-creation)

```python
ssl_ctx = ssl.create_default_context(purpose=ssl.Purpose.SERVER_AUTH)
ssl_ctx.load_verify_locations('ca.pem')
ssl_ctx.load_cert_chain(certfile='client-cert.pem',
                        keyfile='client-key.pem')
await nats.connect(servers=["tls://127.0.0.1:4443"], tls=ssl_ctx, tls_hostname="localhost")
```

Setting the scheme to `tls` in the connect URL will make the client create a [default ssl context](https://docs.python.org/3/library/ssl.html#ssl.create_default_context) automatically:

```python
import asyncio
import ssl
from nats.aio.client import Client as NATS

async def run():
    nc = NATS()
    await nc.connect("tls://demo.nats.io:4443")
```

*Note*: If getting SSL certificate errors in OS X, try first installing the `certifi` certificate bundle. If using Python 3.7 for example, then run:

```ps
$ /Applications/Python\ 3.7/Install\ Certificates.command
 -- pip install --upgrade certifi
Collecting certifi
...
 -- removing any existing file or link
 -- creating symlink to certifi certificate bundle
 -- setting permissions
 -- update complete
```

## NKEYS and JWT User Credentials

Since [v0.9.0](https://github.com/nats-io/nats.py/releases/tag/v0.9.0) release,
you can also optionally install [NKEYS](https://github.com/nats-io/nkeys.py) in order to use
the new NATS v2.0 auth features:

```sh
pip install nats-py[nkeys]
```

Usage:

```python
await nats.connect("tls://connect.ngs.global:4222", user_credentials="/path/to/secret.creds")
```

## Development

1. [Install nats server](https://docs.nats.io/running-a-nats-service/introduction/installation).
1. Make sure the server is available in your PATH: `nats-server -v`.
1. Install dependencies: `python3 -m pipenv install --dev`.
1. Run tests: `python3 -m pytest`.

## License

Unless otherwise noted, the NATS source files are distributed under
the Apache Version 2.0 license found in the LICENSE file.
