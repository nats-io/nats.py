# Copyright 2021 The NATS Authors
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
import nats.aio.errors
from nats.js import api
from typing import Any, Dict, List, Optional

class JetStream:
    """
    JetStream returns a context that can be used to produce and consume
    messages from NATS JetStream.
    """    

    def __init__(
        self,
        conn=None,
        prefix=api.DefaultPrefix,
        domain=None,
        timeout=5,
        ):
        self._prefix = prefix
        if domain is not None:
            self._prefix = "$JS.f{domain}.API"
        self._nc = conn
        self._timeout = timeout

        # TODO: Change to proper JSM implementation.
        self._jsm = JetStream._JS(conn, prefix)

    async def publish(
        self,
        subject: str,
        payload: bytes = b'',
        timeout: float = None,
        stream: str = None,
        ) -> api.PubAck:
        """
        publish emits a new message to JetStream.
        """
        if timeout is None:
            timeout = self._timeout
        print("sending thing")
        
        msg = await self._nc.request(subject, payload, timeout=timeout)

        # TODO: Check possible failure responses.

        data = json.loads(msg.data)
        return api.PubAck(**data)

    async def pull_subscribe(
        self,
        subject: str,
        durable: str,
        stream: str = None,
    ):
        """
        pull_subscribe returns a Subscription that can be delivered messages
        from a JetStream pull based consumer by calling `sub.fetch`.

        In case 'stream' is passed, there will not be a lookup of the stream
        based on the subject.
        """
        if stream is None:
            stream = await self._jsm._lookup_stream_by_subject(subject)

        # FIXME: Make this inbox prefix customizable.
        deliver = api.InboxPrefix[:]
        deliver.extend(self._nc._nuid.next())

        # FIXME: Add options to customize subscription.
        consumer = durable
        sub = await self._nc.subscribe(deliver)
        return JetStream.PullSubscription(self, sub, stream, consumer, deliver)

    class PullSubscription:
        """
        PullSubscription is a subscription that can fetch messages.
        """
        def __init__(self, js, sub, stream, consumer, deliver):
            # JS/JSM context
            self._js = js
            self._nc = js._nc

            # NATS Subscription
            self._sub = sub
            self._stream = stream
            self._consumer = consumer
            prefix = self._js._prefix
            self._nms = f'{prefix}.CONSUMER.MSG.NEXT.{stream}.{consumer}'
            self._deliver = deliver

        async def fetch(self, batch: int = 1, timeout: int = 5):
            # TODO: Check connection is not closed.
            if batch < 1:
                raise ValueError("nats: invalid batch size")
            if timeout <= 0:
                raise ValueError("nats: invalid fetch timeout")

            msgs = []
            expires = (timeout * 1_000_000_000) - 100_000
            if batch == 1:
                msg = await self._fetch_one(batch, expires, timeout)
                msgs.append(msg)
            else:
                msgs = await self._fetch_n(batch, expires, timeout)
            return msgs

        async def _fetch_one(self, batch, expires, timeout):
            queue = self._sub._pending_queue

            # Check the next message in case there are any.
            if not queue.empty():
                try:
                    msg = queue.get_nowait()
                    return msg
                except:
                    # Fallthrough to make request in case this fails.
                    pass

            # Make lingering request with expiration.
            next_req = {}
            next_req['batch'] = 1
            next_req['expires'] = expires

            # Make publish request and wait for response.
            await self._nc.publish(
                self._nms,
                json.dumps(next_req).encode(),
                self._deliver,
                )

            # Wait for the response.
            fut = queue.get()
            msg = await asyncio.wait_for(fut, timeout=timeout)

            # Should have received at least a message at this point,
            # if that is not the case then error already.
            # TODO: Handle API errors and status messages.

            return msg

        async def _fetch_n(self, batch, expires, timeout):
            # FIXME: Implement fetching more than one.
            raise NotImplementedError

    class _JS():
        def is_status_msg(msg):
            print("STATUS?", msg)
        
        def __init__(
            self,
            conn=None,
            prefix=None,
            stream=None,
            consumer=None,
            nms=None,
            ):
            self._prefix = prefix
            self._nc = conn
            self._stream = stream
            self._consumer = consumer
            self._nms = nms

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
            msg = await self._nc.request(
                f"{self._prefix}.CONSUMER.INFO.{stream}.{consumer}"
            )
            result = json.loads(msg.data)
            return result

        async def _add_consumer(
            self,
            stream: str = None,
            durable: str = None,
            deliver_subject: str = None,
            deliver_policy: str = None,
            opt_start_seq: int = None,
            opt_start_time: int = None,
            ack_policy: str = None,
            max_deliver: int = None,
            filter_subject: str = None,
            replay_policy: str = None,
            max_ack_pending: int = None,
            ack_wait: int = None,
            num_waiting: int = None
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
                "max_ack_pending": max_ack_pending,
                "num_waiting": num_waiting
            }

            # Cleanup empty values.
            for k, v in dict(config).items():
                if v is None:
                    del config[k]
            req = {"stream_name": stream, "config": config}
            req_data = json.dumps(req).encode()

            msg = None
            if durable is not None:
                msg = await self._nc.request(
                    f"{self._prefix}.CONSUMER.DURABLE.CREATE.{stream}.{durable}",
                    req_data
                )
            else:
                msg = await self._nc.request(
                    f"{self._prefix}.CONSUMER.CREATE.{stream}", req_data
                )

            response = json.loads(msg.data)
            if 'error' in response and response['error']['code'] >= 400:
                err = response['error']
                code = err['code']
                err_code = err['err_code']
                description = err['description']
                raise nats.aio.errors.JetStreamAPIError(
                    code=code,
                    err_code=err_code,
                    description=description,
                    )
            return response
