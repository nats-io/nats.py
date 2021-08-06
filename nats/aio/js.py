# Copyright 2016-2021 The NATS Authors
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

import asyncio
import json
import time
import nats.aio.client

DEFAULT_JS_API_PREFIX = "$JS.API"

class JetStream():
    """
    JetStream returns a context that can be used to produce and consume
    messages from NATS JetStream.
    """

    INBOX_PREFIX = bytearray(b'_INBOX.')

    ####################################
    #                                  #
    # JetStream Configuration Options  #
    #                                  #
    ####################################

    # AckPolicy
    AckExplicit = "explicit"
    AckAll = "all"
    AckNone = "none"

    def __init__(self, conn=None, prefix=DEFAULT_JS_API_PREFIX):
        self._prefix = prefix
        self._nc = conn
        self._jsm = JetStream._JSM(conn, prefix)

    class _Sub():
        def __init__(self, js=None):
            self._js = js
            self._rpre = None
            self._psub = None
            self._freqs = None
            self._stream = None
            self._consumer = None

    async def subscribe(
        self,
        subject: str,
        queue: str = "",
        cb = None,
        durable: str = None,  # nats.Durable
        stream: str = None,   # nats.BindStream
        consumer: str = None, # nats.Bind(stream, consumer)
        ack_policy: str = AckExplicit # nats.AckPolicy
        ):
        """
        subscribe returns a Subscription that will be delivered messages
        from a JetStream push based consumer.
        """
        if stream is None:
            stream = await self._jsm._lookup_stream_by_subject(subject)

        # Lookup for the consumer based on the durable name in case.
        found = False

        if durable is not None:
            cinfo = await self._jsm._consumer_info(stream, durable)
            if 'error' in cinfo and cinfo['error']['code'] == 404:
                found = False
            else:
                found = True

        # Create the consumer if not present.
        if not found:
            inbox = nats.aio.client.INBOX_PREFIX[:]
            inbox.extend(self._nc._nuid.next())

            sub = await self._nc.subscribe(inbox.decode(), queue=queue, cb=cb)

            await self._jsm._create_consumer(
                stream,
                durable=durable,
                ack_policy=ack_policy,
                deliver_subject=inbox.decode()
                # FIXME: Add more consumer options.
                )

            return sub

    async def pull_subscribe(
        self,
        subject: str,
        durable: str,       # nats.Durable
        stream: str = None, # nats.BindStream
        ):
        """
        pull_subscribe returns a Subscription that can be delivered messages
        from a JetStream pull based consumer by calling `sub.fetch`.

        In case 'stream' is passed, there will not be a lookup of the stream
        based on the subject.
        """

        if stream is None:
            stream = await self._jsm._lookup_stream_by_subject(subject)

        # Lookup for the consumer based on the durable name.
        cinfo = await self._jsm._consumer_info(stream, durable)
        found = False
        if 'error' in cinfo and cinfo['error']['code'] == 404:
            # {'code': 404, 'err_code': 10014, 'description': 'consumer not found'}
            found = False
        else:
            found = True

        if not found:
            # Create the consumer if not present.
            await self._jsm._create_consumer(
                stream,
                durable=durable,
                ack_policy=JetStream.AckExplicit,
                # FIXME: Add more consumer options.
                )

        # Create per pull subscriber wildcard mux for Fetch(1) use case.
        resp_sub_prefix = nats.aio.client.INBOX_PREFIX[:]
        resp_sub_prefix.extend(self._nc._nuid.next())
        resp_sub_prefix.extend(b'.')
        resp_mux_subject = resp_sub_prefix[:]
        resp_mux_subject.extend(b'*')

        jsub = JetStream._Sub(self)
        jsub._stream = stream
        jsub._consumer = durable
        jsub._rpre = resp_mux_subject[:len(resp_mux_subject)-2]
        jsub._freqs = asyncio.Queue()

        sub = nats.aio.client.Subscription(self._nc)
        sub._jsi = jsub

        async def handle_fetch(msg):
            future = await jsub._freqs.get()
            if not future.cancelled():
                future.set_result(msg)

        psub = await self._nc.subscribe(resp_mux_subject.decode(), cb=handle_fetch)
        sub._jsi.psub = psub

        return sub

    class _JSM():
        def __init__(self, conn=None, prefix=DEFAULT_JS_API_PREFIX):
            self._prefix = prefix
            self._nc = conn

        async def _account_info(self):
            msg = await self._nc.request(f"{self._prefix}.INFO")
            account_info = json.loads(msg.data)
            return account_info

        async def _lookup_stream_by_subject(self, subject: str):
            req_sub = f"{self._prefix}.STREAM.NAMES"
            req_data = json.dumps({"subject": subject})
            msg = await self._nc.request(req_sub, req_data.encode())
            info = json.loads(msg.data)
            return info['streams'][0]

        async def _add_stream(self, config={}):
            name = config["name"]
            data = json.dumps(config)
            msg = await self._nc.request(
                f"{self._prefix}.STREAM.CREATE.{name}", data.encode()
            )
            return msg

        async def _consumer_info(self, stream=None, consumer=None):
            msg = None
            msg = await self._nc.request(f"{self._prefix}.CONSUMER.INFO.{stream}.{consumer}")
            result = json.loads(msg.data)
            return result

        async def _create_consumer(
            self,
            stream: str = None,
            durable: str = None,
            deliver_subject: str = None,
            deliver_policy: str = None,
            opt_start_seq: str = None,
            opt_start_time: str = None,
            ack_policy: str = None,
            max_deliver: int = None,
            filter_subject: str = None,
            replay_policy: str = None,
            max_ack_pending: int = None,
            ack_wait: int = None,
            ):
            config = {
                "durable_name": durable,
                "deliver_subject": deliver_subject,
                "deliver_policy": deliver_policy,
                "opt_start_seq": opt_start_seq,
                "opt_start_time": opt_start_time,
                "ack_policy": ack_policy,
                "ack_wait": ack_wait,
                "max_deliver": max_deliver,
                "filter_subject": filter_subject,
                "replay_policy": replay_policy,
                "max_ack_pending": max_ack_pending
                }

            # Cleanup empty values.
            for k, v in dict(config).items():
                if v is None:
                    del config[k]
            req = {
                "stream_name": stream,
                "config": config
                }
            req_data = json.dumps(req).encode()

            msg = None
            if durable is not None:
                msg = await self._nc.request(f"{self._prefix}.CONSUMER.DURABLE.CREATE.{stream}.{durable}", req_data)
            else:
                msg = await self._nc.request(f"{self._prefix}.CONSUMER.CREATE.{stream}", req_data)

            result = json.loads(msg.data)
            return result
