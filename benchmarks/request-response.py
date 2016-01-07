import asyncio
import json
import sys

import http.client
import time
from nats.aio.client import Client as NATS
from nats.aio.errors import ErrConnectionClosed, ErrTimeout

def run(loop):
    nc = NATS()

    errors = 0
    @asyncio.coroutine
    def error_cb(e):
        nonlocal errors
        errors += 1

    options = {
        "servers": ["nats://127.0.0.1:4225"],
        "io_loop": loop,
        "error_cb": error_cb,
    }

    try:
        yield from nc.connect(**options)
    except Exception as e:
        print("ERROR: Could not establish connection to NATS: {}".format(e), file=sys.stderr)

    try:
        max_messages = int(sys.argv[1])
    except:
        max_messages = 1000

    try:
        bytesize = int(sys.argv[2])
    except:
        bytesize = 1

    @asyncio.coroutine
    def subscribe_handler(msg):
        nonlocal nc
        yield from nc.publish(msg.reply, msg.data)

    yield from nc.subscribe("help.*", cb=subscribe_handler)
    yield from nc.flush()

    endpoint = '127.0.0.1:8225'
    httpclient = http.client.HTTPConnection(endpoint)
    httpclient.request('GET', '/varz')
    response = httpclient.getresponse()
    start_varz = json.loads((response.read()).decode())

    start_time = time.time()
    line = b'A' * int(bytesize)

    total_written = 0
    total_timeouts = 0
    for i in range(0, max_messages):
        try:
            response = yield from nc.timed_request("help.{}".format(i), line, timeout=0.5)
            total_written += 1
        except ErrConnectionClosed as e:
            break
        except ErrTimeout as e:
            total_timeouts += 1

    yield from nc.flush()
    yield from nc.close()
    end_time = time.time()
    duration = end_time - start_time
    rate = total_written / duration

    httpclient = http.client.HTTPConnection(endpoint)
    httpclient.request('GET', '/varz')
    response = httpclient.getresponse()
    end_varz = json.loads((response.read()).decode())

    delta_varz_in_msgs = end_varz["in_msgs"] - start_varz["in_msgs"]
    delta_varz_in_bytes = end_varz["in_bytes"] - start_varz["in_bytes"]
    print("|{0}|{1}|{2}|{3}|{4}|{5}|{6}|{7}|{8}".format(
        max_messages,
        bytesize,
        duration,
        rate,
        total_written,
        total_timeouts,
        errors,
        delta_varz_in_msgs,
        delta_varz_in_bytes,
        ))

if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.run_until_complete(run(loop))
    loop.close()
