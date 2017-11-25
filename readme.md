# NATS - Python Client for Asyncio

An asyncio-based ([PEP 3156](https://www.python.org/dev/peps/pep-3156/)) Python client for the [NATS messaging system](https://nats.io).

[![pypi](https://img.shields.io/pypi/v/asyncio-nats-client.svg)](https://pypi.org/project/asyncio-nats-client)
[![Build Status](https://travis-ci.org/nats-io/asyncio-nats.svg?branch=master)](http://travis-ci.org/nats-io/asyncio-nats)
[![Versions](https://img.shields.io/pypi/pyversions/asyncio-nats-client.svg)](https://pypi.org/project/asyncio-nats-client)
[![License MIT](https://img.shields.io/npm/l/express.svg)](http://opensource.org/licenses/MIT)

## Supported platforms

Should be compatible with at least [Python 3.4](https://docs.python.org/3.4/library/asyncio.html)
and [Python +3.5.1](https://docs.python.org/3.5/library/asyncio.html) using [gnatsd](https://github.com/nats-io/gnatsd) as the server.

## Getting Started

```bash
git clone https://github.com/nats-io/asyncio-nats
cd asyncio-nats
python setup.py install
```

Or via `pip`:

```bash
pip install asyncio-nats-client
```

## Basic Usage

```python
import asyncio
from nats.aio.client import Client as NATS
from nats.aio.errors import ErrConnectionClosed, ErrTimeout, ErrNoServers

def run(loop):
  nc = NATS()

  yield from nc.connect(io_loop=loop)

  @asyncio.coroutine
  def message_handler(msg):
    subject = msg.subject
    reply = msg.reply
    data = msg.data.decode()
    print("Received a message on '{subject} {reply}': {data}".format(
      subject=subject, reply=reply, data=data))

  # Simple publisher and async subscriber via coroutine.
  sid = yield from nc.subscribe("foo", cb=message_handler)

  # Stop receiving after 2 messages.
  yield from nc.auto_unsubscribe(sid, 2)
  yield from nc.publish("foo", b'Hello')
  yield from nc.publish("foo", b'World')
  yield from nc.publish("foo", b'!!!!!')

  @asyncio.coroutine
  def help_request(msg):
    subject = msg.subject
    reply = msg.reply
    data = msg.data.decode()
    print("Received a message on '{subject} {reply}': {data}".format(
      subject=subject, reply=reply, data=data))
    yield from nc.publish(reply, b'I can help')

  # Use queue named 'workers' for distributing requests
  # among subscribers.
  yield from nc.subscribe("help", "workers", help_request)

  # Send a request and expect a single response
  # and trigger timeout if not faster than 50 ms.
  try:
    response = yield from nc.timed_request("help", b'help me', 0.050)
    print("Received response: {message}".format(message=response.data.decode()))
  except ErrTimeout:
    print("Request timed out")

  yield from asyncio.sleep(1, loop=loop)
  yield from nc.close()

if __name__ == '__main__':
  loop = asyncio.get_event_loop()
  loop.run_until_complete(run(loop))
  loop.close()
```

## Wildcard Subscriptions

```python
import asyncio
from nats.aio.client import Client as NATS
from nats.aio.errors import ErrConnectionClosed, ErrTimeout, ErrNoServers

def run(loop):
  nc = NATS()

  yield from nc.connect(io_loop=loop)

  @asyncio.coroutine
  def message_handler(msg):
    subject = msg.subject
    reply = msg.reply
    data = msg.data.decode()
    print("Received a message on '{subject} {reply}': {data}".format(
      subject=subject, reply=reply, data=data))

  # "*" matches any token, at any level of the subject.
  yield from nc.subscribe("foo.*.baz", cb=message_handler)
  yield from nc.subscribe("foo.bar.*", cb=message_handler)

  # ">" matches any length of the tail of a subject, and can only be the last token
  # E.g. 'foo.>' will match 'foo.bar', 'foo.bar.baz', 'foo.foo.bar.bax.22'
  yield from nc.subscribe("foo.>", cb=message_handler)

  # Matches all of the above.
  yield from nc.publish("foo.bar.baz", b'Hello World')

  yield from asyncio.sleep(1, loop=loop)
  yield from nc.close()

if __name__ == '__main__':
  loop = asyncio.get_event_loop()
  loop.run_until_complete(run(loop))
  loop.close()
```

## Advanced Usage

```python
import asyncio
from nats.aio.client import Client as NATS
from nats.aio.errors import ErrTimeout, ErrNoServers

def run(loop):
  nc = NATS()

  try:
    yield from nc.connect(io_loop=loop)
  except ErrNoServers as e:
    print(e)
    return

  @asyncio.coroutine
  def message_handler(msg):
    subject = msg.subject
    reply = msg.reply
    data = msg.data.decode()
    for i in range(0, 20):
      yield from nc.publish(reply, "i={i}".format(i=i).encode())

  yield from nc.subscribe("help.>", cb=message_handler)

  @asyncio.coroutine
  def request_handler(msg):
    subject = msg.subject
    reply = msg.reply
    data = msg.data.decode()
    print("Received a message on '{subject} {reply}': {data}".format(
      subject=subject, reply=reply, data=data))

  # Signal the server to stop sending messages after we got 10 already.
  yield from nc.request("help.please", b'help', expected=10, cb=request_handler)

  try:
    # Flush connection to server, returns when all messages have been processed.
    # It raises a timeout if roundtrip takes longer than 1 second.
    yield from nc.flush(1)
  except ErrTimeout:
    print("Flush timeout")

  yield from asyncio.sleep(1, loop=loop)
  yield from nc.close()

if __name__ == '__main__':
  loop = asyncio.get_event_loop()
  loop.run_until_complete(run(loop))
  loop.close()
```

## Clustered Usage

```python
import asyncio
from datetime import datetime
from nats.aio.client import Client as NATS
from nats.aio.errors import ErrConnectionClosed, ErrTimeout, ErrNoServers

def run(loop):

  nc = NATS()

  # Setup pool of servers from a NATS cluster.
  options = {
    "servers": [
      "nats://user1:pass1@127.0.0.1:4222",
      "nats://user2:pass2@127.0.0.1:4223",
      "nats://user3:pass3@127.0.0.1:4224",
      ],
    "io_loop": loop,
  }
  
  # Auth token can be used instead if setting
  # as part of uri string of a server: 
  # "nats://token@127.0.0.1:4222"

  # Will try to connect to servers in order of configuration,
  # by defaults it connect to one in the pool randomly.
  options["dont_randomize"] = True

  # Optionally set reconnect wait and max reconnect attempts.
  # This example means 10 seconds total per backend.
  options["max_reconnect_attempts"] = 5
  options["reconnect_time_wait"] = 2

  @asyncio.coroutine
  def disconnected_cb():
    print("Got disconnected!")

  @asyncio.coroutine
  def reconnected_cb():
    # See who we are connected to on reconnect.
    print("Got reconnected to {url}".format(url=nc.connected_url.netloc))

  # Setup callbacks to be notified on disconnects and reconnects
  options["disconnected_cb"] = disconnected_cb
  options["reconnected_cb"]  = reconnected_cb

  @asyncio.coroutine
  def error_cb(e):
    print("There was an error: {}".format(e))

  @asyncio.coroutine
  def closed_cb():
    print("Connection is closed")

  @asyncio.coroutine
  def subscribe_handler(msg):
    print("Got message: ", msg.subject, msg.reply, msg.data)

  # Setup callbacks to be notified when there is an error
  # or connection is closed.
  options["error_cb"] = error_cb
  options["closed_cb"] = closed_cb

  try:
    yield from nc.connect(**options)
  except ErrNoServers as e:
    # Could not connect to any server in the cluster.
    print(e)
    return

  if nc.is_connected:
    yield from nc.subscribe("help.*", cb=subscribe_handler)

    max_messages = 1000000
    start_time = datetime.now()
    print("Sending {} messages to NATS...".format(max_messages))

    for i in range(0, max_messages):
      try:
        yield from nc.publish("help.{}".format(i), b'A')
        yield from nc.flush(0.500)
      except ErrConnectionClosed as e:
        print("Connection closed prematurely.")
        break
      except ErrTimeout as e:
        print("Timeout occured when publishing msg i={}: {}".format(i, e))

    end_time = datetime.now()
    yield from nc.close()
    duration = end_time - start_time
    print("Duration: {}".format(duration))

    try:
      yield from nc.publish("help", b"hello world")
    except ErrConnectionClosed:
      print("Can't publish since no longer connected.")

  err = nc.last_error
  if err is not None:
    print("Last Error: {}".format(err))

if __name__ == '__main__':
  loop = asyncio.get_event_loop()
  loop.run_until_complete(run(loop))
  loop.close()
```

## Async/Await example (Python +3.5 only)

```python
import asyncio
from nats.aio.client import Client as NATS
from nats.aio.errors import ErrConnectionClosed, ErrTimeout, ErrNoServers

async def run(loop):
  nc = NATS()

  await nc.connect(io_loop=loop)

  async def message_handler(msg):
    subject = msg.subject
    reply = msg.reply
    data = msg.data.decode()
    print("Received a message on '{subject} {reply}': {data}".format(
      subject=subject, reply=reply, data=data))

  # Simple publisher and async subscriber via coroutine.
  sid = await nc.subscribe("foo", cb=message_handler)

  # Stop receiving after 2 messages.
  await nc.auto_unsubscribe(sid, 2)
  await nc.publish("foo", b'Hello')
  await nc.publish("foo", b'World')
  await nc.publish("foo", b'!!!!!')

  async def help_request(msg):
    subject = msg.subject
    reply = msg.reply
    data = msg.data.decode()
    print("Received a message on '{subject} {reply}': {data}".format(
      subject=subject, reply=reply, data=data))
    await nc.publish(reply, b'I can help')

  # Use queue named 'workers' for distributing requests
  # among subscribers.
  await nc.subscribe("help", "workers", help_request)

  # Send a request and expect a single response
  # and trigger timeout if not faster than 50 ms.
  try:
    response = await nc.timed_request("help", b'help me', 0.050)
    print("Received response: {message}".format(message=response.data.decode()))
  except ErrTimeout:
    print("Request timed out")

  await asyncio.sleep(1, loop=loop)
  await nc.close()

if __name__ == '__main__':
  loop = asyncio.get_event_loop()
  loop.run_until_complete(run(loop))
  loop.close()
```

## TLS

TLS connections can be configured with an [ssl context](https://docs.python.org/3/library/ssl.html#context-creation)

```python
ssl_ctx = ssl.create_default_context(purpose=ssl.Purpose.SERVER_AUTH)
ssl_ctx.protocol = ssl.PROTOCOL_TLSv1_2
ssl_ctx.load_verify_locations('ca.pem')
ssl_ctx.load_cert_chain(certfile='client-cert.pem',
                        keyfile='client-key.pem')
yield from nc.connect(io_loop=loop, tls=ssl_ctx)
```

## License

(The MIT License)

Copyright (c) 2015-2017 Apcera Inc.<br/>

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to
deal in the Software without restriction, including without limitation the
rights to use, copy, modify, merge, publish, distribute, sublicense, and/or
sell copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in
all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS
IN THE SOFTWARE.
