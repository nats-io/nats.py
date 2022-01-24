# Copyright 2016-2019 The NATS Authors
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

import argparse
import asyncio
import os
import signal
import sys

import nats


def show_usage():
    usage = """
nats-req [-s SERVER] <subject> <data>

Example:

nats-req -s demo.nats.io echo 'ping'
"""
    print(usage)


def show_usage_and_die():
    show_usage()
    sys.exit(1)


async def run():
    parser = argparse.ArgumentParser()

    # e.g. nats-req -s demo.nats.io hello world
    parser.add_argument('subject', default='hello', nargs='?')
    parser.add_argument('-d', '--data', default="hello world")
    parser.add_argument('-s', '--servers', default="")
    parser.add_argument('--creds', default="")
    args, unknown = parser.parse_known_args()

    data = args.data
    if len(unknown) > 0:
        data = unknown[0]

    async def error_cb(e):
        print("Error:", e)

    async def reconnected_cb():
        print(f"Connected to NATS at {nc.connected_url.netloc}...")

    options = {"error_cb": error_cb, "reconnected_cb": reconnected_cb}

    if len(args.creds) > 0:
        options["user_credentials"] = args.creds

    nc = None
    try:
        if len(args.servers) > 0:
            options['servers'] = args.servers

        nc = await nats.connect(**options)
    except Exception as e:
        print(e)
        show_usage_and_die()

    async def req_callback(msg):
        await msg.respond(b'a response')

    await nc.subscribe(args.subject, cb=req_callback)

    try:
        future = nc.request(args.subject, data.encode())
        print(f"Published [{args.subject}] : '{data}'")
        msg = await future
        subject = msg.subject
        data = msg.data.decode()
        print(f"Received  [{subject}]: '{data}'")

    except nats.aio.errors.ErrTimeout:
        print("nats: request timed out!")
    await nc.drain()


if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    try:
        loop.run_until_complete(run())
    finally:
        loop.close()
