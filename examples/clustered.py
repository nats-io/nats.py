import asyncio
from datetime import datetime
from nats.io.client import Client as NATS
from nats.io.errors import ErrConnectionClosed, ErrTimeout, ErrNoServers

def error_cb(e):
  print("--- Error: {}".format(e))

def disconnected_cb():
  print("--- Disconnected")

def closed_cb():
  print("--- Connection is closed")

def reconnected_cb():
  print("--- Reconnected")

def go(loop):
  nc = NATS()

  options = {
    "servers": [
      "nats://user1:pass1@127.0.0.1:4222",
      "nats://user2:pass2@127.0.0.1:4223",
      "nats://user3:pass3@127.0.0.1:4224",
      ],
    "io_loop": loop,
    "error_cb": error_cb,
    "disconnected_cb": disconnected_cb,
    "closed_cb": closed_cb,
    "reconnected_cb": reconnected_cb,
    "verbose": True,
    "allow_reconnect": True,
    "ping_interval": 1,
  }

  try:
    yield from nc.connect(**options)
  except ErrNoServers:
    pass

  if nc.is_connected:
    yield from nc.subscribe("help.*")

    max_messages = 10000000
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
        # Can occur during while reconnecting for example...
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
  loop.run_until_complete(go(loop))
  loop.close()
