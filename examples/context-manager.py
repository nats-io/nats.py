import asyncio
import os
import signal
import nats

@asyncio.coroutine
def run(loop):

  is_done = asyncio.Future(loop=loop)

  @asyncio.coroutine
  def closed_cb():
    print("Connection to NATS is closed.")
    is_done.set_result(True)

  opts = {
    "servers": ["nats://127.0.0.1:4222"],
    "io_loop": loop,
    "closed_cb": closed_cb
  }

  with (yield from nats.connect(**opts)) as nc:
    print("Connected to NATS at {}...".format(nc.connected_url.netloc))

    @asyncio.coroutine
    def subscribe_handler(msg):
      subject = msg.subject
      reply = msg.reply
      data = msg.data.decode()
      print("Received a message on '{subject} {reply}': {data}".format(
        subject=subject, reply=reply, data=data))

    yield from nc.subscribe("discover", cb=subscribe_handler)
    yield from nc.flush()

    for i in range(0, 10):
      yield from nc.publish("discover", b"hello world")
      yield from asyncio.sleep(0.1, loop=loop)

  yield from asyncio.wait_for(is_done, 60.0, loop=loop)
  loop.stop()

if __name__ == '__main__':
  loop = asyncio.get_event_loop()
  try:
    loop.run_until_complete(run(loop))
  finally:
    loop.close()
