import asyncio
from nats.io.client import Client as NATS

def error_cb(e):
  print("ERROR:", e)

@asyncio.coroutine
def go(loop):
  nc = NATS()
  print("Original Slow Callback duration: %d" % loop.slow_callback_duration)
  loop.slow_callback_duration = 0.001
  print("Modified Slow Callback duration: %d" % loop.slow_callback_duration)

  options = {
    "servers": ["nats://127.0.0.1:4222"],
    "io_loop": loop,
    "error_cb": error_cb,
    "verbose": True,
  }
  yield from nc.connect(**options)
  for i in range(0, 100000000000):
    if i % 10000 == 0:
      yield from asyncio.sleep(0.001)
    yield from nc.publish("help.%d".format(i), b'A')
  yield from nc.close()

if __name__ == '__main__':
  loop = asyncio.get_event_loop()
  loop.run_until_complete(go(loop))
  loop.close()
