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
