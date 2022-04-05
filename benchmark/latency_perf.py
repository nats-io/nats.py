import argparse
import asyncio
import sys
import time
from random import randint

import nats

DEFAULT_ITERATIONS = 10000
HASH_MODULO = 250

try:
  import uvloop
  asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
except:
  pass

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

async def main():
  parser = argparse.ArgumentParser()
  parser.add_argument('-n', '--iterations', default=DEFAULT_ITERATIONS, type=int)
  parser.add_argument('-S', '--subject', default='test')
  parser.add_argument('--servers', default=[], action='append')
  args = parser.parse_args()

  servers = args.servers
  if len(args.servers) < 1:
    servers = ["nats://127.0.0.1:4222"]

  try:
    nc = await nats.connect(servers)
  except Exception as e:
    sys.stderr.write(f"ERROR: {e}")
    show_usage_and_die()

  async def handler(msg):
    await nc.publish(msg.reply, b'')
  await nc.subscribe(args.subject, cb=handler)

  # Start the benchmark
  start = time.monotonic()
  to_send = args.iterations

  print("Sending {} request/responses on [{}]".format(
      args.iterations, args.subject))
  while to_send > 0:
    to_send -= 1
    if to_send == 0:
      break

    await nc.request(args.subject, b'')
    if (to_send % HASH_MODULO) == 0:
      sys.stdout.write("#")
      sys.stdout.flush()

  duration = time.monotonic() - start
  ms = "%.3f" % ((duration/args.iterations) * 1000)
  print(f"\nTest completed : {ms} ms avg request/response latency")
  await nc.close()

if __name__ == '__main__':
  asyncio.run(main())
