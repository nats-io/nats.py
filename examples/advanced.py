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
