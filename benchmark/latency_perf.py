import argparse, sys
import asyncio
import time
from random import randint
from nats.aio.client import Client as NATS

DEFAULT_ITERATIONS = 10000
HASH_MODULO = 1000

def show_usage():
  message = """
Usage: latency_perf [options]

options:
  -n ITERATIONS                    Iterations to spec (default: 1000)
  -S SUBJECT                       Send subject (default: (test)
  """
  print(message)

def show_usage_and_die():
  show_usage()
  sys.exit(1)

global received
received = 0

@asyncio.coroutine
def main(loop):
  parser = argparse.ArgumentParser()
  parser.add_argument('-n', '--iterations', default=DEFAULT_ITERATIONS, type=int)
  parser.add_argument('-S', '--subject', default='test')
  parser.add_argument('--servers', default=[], action='append')
  args = parser.parse_args()

  servers = args.servers
  if len(args.servers) < 1:
    servers = ["nats://127.0.0.1:4222"]
  opts = { "servers": servers }

  # Make sure we're connected to a server first...
  nc = NATS()
  try:
    yield from nc.connect(**opts)
  except Exception as e:
    sys.stderr.write("ERROR: {0}".format(e))
    show_usage_and_die()

  @asyncio.coroutine
  def handler(msg):
    yield from nc.publish(msg.reply, b'')
  yield from nc.subscribe(args.subject, cb=handler)

  # Start the benchmark
  start = time.monotonic()
  to_send = args.iterations

  print("Sending {0} request/responses on [{1}]".format(
      args.iterations, args.subject))
  while to_send > 0:
    to_send -= 1
    if to_send == 0:
      break

    yield from nc.timed_request(args.subject, b'')
    if (to_send % HASH_MODULO) == 0:
      sys.stdout.write("#")
      sys.stdout.flush()

  duration = time.monotonic() - start
  ms = "%.3f" % ((duration/args.iterations) * 1000)
  print("\nTest completed : {0} ms avg request/response latency".format(ms))
  yield from nc.close()

if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main(loop))
    loop.close()
