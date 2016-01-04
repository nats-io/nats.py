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
