import asyncio
from nats.aio.client import Client as NATS

def run(loop):
  nc = NATS()

  options = {
    "servers": ["nats://127.0.0.1:4222"],
    "io_loop": loop
  }

  yield from nc.connect(**options)
  print("Connected to NATS at {}...".format(nc.connected_url.netloc))

  @asyncio.coroutine
  def subscribe_handler(msg):
    subject = msg.subject
    reply = msg.reply
    data = msg.data.decode()
    print("Received a message on '{subject} {reply}': {data}".format(
      subject=subject, reply=reply, data=data))

  # Basic subscription to receive all published messages
  # which are being sent to a single topic 'discover'
  yield from nc.subscribe("discover", cb=subscribe_handler)

  # Subscription on queue named 'workers' so that
  # one subscriber handles message a request at a time.
  yield from nc.subscribe("help.*", "workers", subscribe_handler)

if __name__ == '__main__':
  loop = asyncio.get_event_loop()
  loop.run_until_complete(run(loop))
  try:
      loop.run_forever()
  finally:
      loop.close()
