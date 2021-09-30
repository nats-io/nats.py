# NATS - Python3 Client for Asyncio

An [asyncio](https://docs.python.org/3/library/asyncio.html) Python client for the [NATS messaging system](https://nats.io).

[![pypi](https://img.shields.io/pypi/v/asyncio-nats-client.svg)](https://pypi.org/project/asyncio-nats-client)
[![Build Status](https://travis-ci.org/nats-io/nats.py.svg?branch=master)](http://travis-ci.org/nats-io/nats.py)
[![Versions](https://img.shields.io/pypi/pyversions/asyncio-nats-client.svg)](https://pypi.org/project/asyncio-nats-client)
[![License Apache 2.0](https://img.shields.io/badge/License-Apache2-blue.svg)](https://www.apache.org/licenses/LICENSE-2.0)

## Supported platforms

Should be compatible with at least [Python +3.7](https://docs.python.org/3.7/library/asyncio.html).

## Installing

```bash
pip install asyncio-nats-client
```

Starting from [v0.9.0](https://github.com/nats-io/nats.py/releases/tag/v0.9.0) release,
you can also optionally install [NKEYS](https://github.com/nats-io/nkeys.py) in order to use
the new NATS v2.0 auth features:

```
pip install asyncio-nats-client[nkeys]
```

## Basic Usage

```python
import asyncio
import nats
from nats.aio.errors import ErrConnectionClosed, ErrTimeout, ErrNoServers

async def run():
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
        headers = msg.headers
        print("Received a message on '{subject} {reply} {headers}': {data}".format(
            subject=subject, reply=reply, headers=headers, data=data))

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
    except ErrTimeout:
        print("Request timed out")

    # Remove interest in subscription.
    await sub.unsubscribe()

    # Terminate connection to NATS.
    await nc.drain()

if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.run_until_complete(run())
    loop.close()
```

## Wildcard Subscriptions

```python
import asyncio
import nats
from nats.aio.errors import ErrConnectionClosed, ErrTimeout, ErrNoServers

async def run():
    nc = await nats.connect("nats://127.0.0.1:4222")

    async def message_handler(msg):
        subject = msg.subject
        reply = msg.reply
        data = msg.data.decode()
        print("Received a message on '{subject} {reply}': {data}".format(
            subject=subject, reply=reply, data=data))

    # "*" matches any token, at any level of the subject.
    await nc.subscribe("foo.*.baz", cb=message_handler)
    await nc.subscribe("foo.bar.*", cb=message_handler)

    # ">" matches any length of the tail of a subject, and can only be the last token
    # E.g. 'foo.>' will match 'foo.bar', 'foo.bar.baz', 'foo.foo.bar.bax.22'
    await nc.subscribe("foo.>", cb=message_handler)

    # Matches all of the above.
    await nc.publish("foo.bar.baz", b'Hello World')

    # Gracefully close the connection.
    await nc.drain()

if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.run_until_complete(run())
    loop.close()
```

## Advanced Usage

```python
import asyncio
import nats
from nats.aio.errors import ErrTimeout, ErrNoServers

async def run():
    try:
        # Setting explicit list of servers in a cluster.
        nc = await nats.connect(servers=["nats://127.0.0.1:4222", "nats://127.0.0.1:4223", "nats://127.0.0.1:4224"])
    except ErrNoServers as e:
        print(e)
        return

    async def message_handler(msg):
        print("Request :", msg)
        await nc.publish(msg.reply, b"I can help!")

    await nc.subscribe("help.>", cb=message_handler)

    async def request_handler(msg):
        subject = msg.subject
        reply = msg.reply
        data = msg.data.decode()
        print("Received a message on '{subject} {reply}': {data}".format(
            subject=subject, reply=reply, data=data))

    # Signal the server to stop sending messages after we got 10 already.
    resp = await nc.request("help.please", b'help')
    print("Response:", resp)

    try:
        # Flush connection to server, returns when all messages have been processed.
        # It raises a timeout if roundtrip takes longer than 1 second.
        await nc.flush(1)
    except ErrTimeout:
        print("Flush timeout")

    await asyncio.sleep(1)

    # Drain gracefully closes the connection, allowing all subscribers to
    # handle any pending messages inflight that the server may have sent.
    await nc.drain()

if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.run_until_complete(run())
    loop.close()
```

## Clustered Usage

```python
import asyncio
import nats
from datetime import datetime
from nats.aio.client import Client as NATS
from nats.aio.errors import ErrConnectionClosed, ErrTimeout, ErrNoServers

async def run():

    # Setup pool of servers from a NATS cluster.
    options = {
        "servers": [
            "nats://user1:pass1@127.0.0.1:4222",
            "nats://user2:pass2@127.0.0.1:4223",
            "nats://user3:pass3@127.0.0.1:4224",
        ],
    }

    # Will try to connect to servers in order of configuration,
    # by defaults it connect to one in the pool randomly.
    options["dont_randomize"] = True

    # Optionally set reconnect wait and max reconnect attempts.
    # This example means 10 seconds total per backend.
    options["max_reconnect_attempts"] = 5
    options["reconnect_time_wait"] = 2

    async def disconnected_cb():
        print("Got disconnected!")

    async def reconnected_cb():
        # See who we are connected to on reconnect.
        print(f"Got reconnected to {nc.connected_url.netloc}")

    # Setup callbacks to be notified on disconnects and reconnects
    options["disconnected_cb"] = disconnected_cb
    options["reconnected_cb"] = reconnected_cb

    async def error_cb(e):
        print(f"There was an error: {e}")

    async def closed_cb():
        print("Connection is closed")

    async def subscribe_handler(msg):
        print("Got message: ", msg.subject, msg.reply, msg.data)

    # Setup callbacks to be notified when there is an error
    # or connection is closed.
    options["error_cb"] = error_cb
    options["closed_cb"] = closed_cb

    try:
        nc = await nats.connect(**options)
    except ErrNoServers as e:
        # Could not connect to any server in the cluster.
        print(e)
        return

    if nc.is_connected:
        await nc.subscribe("help.*", cb=subscribe_handler)

        max_messages = 1000
        start_time = datetime.now()
        print(f"Sending {max_messages} messages to NATS...")

        for i in range(0, max_messages):
            try:
                await nc.publish(f"help.{i}", b'A')
                await nc.flush(0.500)
            except ErrConnectionClosed as e:
                print("Connection closed prematurely.")
                break
            except ErrTimeout as e:
                print("Timeout occured when publishing msg i={}: {}".format(
                    i, e))

        end_time = datetime.now()
        await nc.drain()
        duration = end_time - start_time
        print(f"Duration: {duration}")

        try:
            await nc.publish("help", b"hello world")
        except ErrConnectionClosed:
            print("Can't publish since no longer connected.")

    err = nc.last_error
    if err is not None:
        print(f"Last Error: {err}")

if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.run_until_complete(run())
    loop.close()
```

## TLS

TLS connections can be configured with an [ssl context](https://docs.python.org/3/library/ssl.html#context-creation)

```python
ssl_ctx = ssl.create_default_context(purpose=ssl.Purpose.SERVER_AUTH)
ssl_ctx.load_verify_locations('ca.pem')
ssl_ctx.load_cert_chain(certfile='client-cert.pem',
                        keyfile='client-key.pem')
await nc.connect(servers=["tls://127.0.0.1:4443"], tls=ssl_ctx, tls_hostname="localhost")
```

Setting the scheme to `tls` in the connect URL will make the client create a [default ssl context](https://docs.python.org/3/library/ssl.html#ssl.create_default_context) automatically:

```python
import asyncio
from nats.aio.client import Client as NATS

async def run():
    nc = NATS()
    await nc.connect("tls://demo.nats.io:4443")
```

_Note_: If getting SSL certificate errors in OS X, try first installing the `certifi` certificate bundle. If using Python 3.7 for example, then run:

```
$ /Applications/Python\ 3.7/Install\ Certificates.command
 -- pip install --upgrade certifi
Collecting certifi
...
 -- removing any existing file or link
 -- creating symlink to certifi certificate bundle
 -- setting permissions
 -- update complete
```

## JetStream Usage

A JetStream client is available since [v2.0.0](https://github.com/nats-io/nats.py/releases/tag/v2.0.0) release, you can use it to interact with the JetStream API.

```python
import asyncio
from nats import connect

async def run():
    nc = await connect()
    js = nc.jetstream()

    # Request JetStream Account Info
    account_info = await js.account_info()
    # responses are dataclass and fields can be accessed as attributes
    print(account_info.limits)

    # Create a stream
    response = await js.stream.add("TEST", subjects=["test.>"])
    # Publish a message on the stream. You can use headers
    ack = await js.stream.publish("test.foo", b"bar", headers={"test": "1"})
    # An acknowledgment is returned with infos such as stream name, sequence, domain and duplicate status
    print(ack)

    # It's also possible to publish without waiting for ack
    await nc.publish("test.foo", b"bar", headers={"test": "2"})

    # Pull last message for a subject
    msg = await js.stream.msg_get("TEST", last_by_subj="test.foo")

    # Iterate over all messages received on a subject
    # Note: A stream must exist in order to pull last message but the
    # consumer is created on the fly if it does not exist yet
    async for msg in js.consumer.pull_msgs(
        "consumer_name",
        subject="test.foo",
        max_msgs=2,
        deliver_policy="all",
    ):
        print(msg)

    # Delete the durable consumer using stream name and consumer name
    await js.consumer.delete("TEST", "consumer_name")

    # Create a consumer manually
    await js.consumer.add("TEST", name="dur", deliver_policy="new")

    # Publish some messages and wait for ack
    await js.stream.publish("test.foo", b"1")
    await js.stream.publish("test.foo", b"2")
    # Get last message of consumer
    msg1 = await js.consumer.pull_next("dur", stream="TEST")
    # It also works with subject (but a stream listenning on this subject must exist)
    msg2 = await js.consumer.pull_next("dur", subject="test.foo")

    # Create an ephemeral consumer
    # Ephemeral consumer exist only when there is an interest in the deliver_subject
    # So we first need to create a subscription
    sub = await nc.subscribe("custom_deliver_subject", max_msgs=2)

    # Let's deliver all messages from stream
    consumer = await js.consumer.add(
        "TEST",
        deliver_subject="custom_deliver_subject",
        deliver_policy="all"
    )
    # And iterate on messages one by one
    async for msg in sub.messages:
        # Subscription will auto unsubscribe after having received 2 messages
        # and we'll break out of loop
        print(msg)
        # JetStream messages have metadata
        print(msg.metadata)
    # At this point 2 messages have been received
```

**Warning**: Both headers _keys_ and _values_ need to be string instances.

## Development

To run the tests:

```sh
python3 -m pipenv install
python3 -m pytest
```

## License

Unless otherwise noted, the NATS source files are distributed under
the Apache Version 2.0 license found in the LICENSE file.
