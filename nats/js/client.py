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
import nats.js.errors
from nats.js.manager import JetStreamManager
from nats.js import api
from typing import Any, Dict, List, Optional

class JetStream:
    """
    JetStream returns a context that can be used to produce and consume
    messages from NATS JetStream.
    """

    def __init__(
            self,
            conn,
            prefix=api.DefaultPrefix,
            domain=None,
            timeout=5,
    ):
        self._prefix = prefix
        if domain is not None:
            self._prefix = f"$JS.{domain}.API"
        self._nc = conn
        self._timeout = timeout
        self._jsm = JetStreamManager(conn, prefix=prefix, domain=domain, timeout=timeout)

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
        hdr = None
        if timeout is None:
            timeout = self._timeout
        if stream is not None:
            hdr = {}
            hdr[nats.js.api.ExpectedStreamHdr] = stream

        try:
            msg = await self._nc.request(subject, payload, timeout=timeout, headers=hdr)
        except nats.aio.errors.ErrNoResponders:
            raise nats.js.errors.NoStreamResponseError

        resp = json.loads(msg.data)
        if 'error' in resp:
            raise nats.js.errors.APIError.from_error(resp['error'])

        return api.PubAck.loads(**resp)

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
            stream = await self._jsm.find_stream_name_by_subject(subject)

        # TODO: Lookup the pull subscription.
        # TODO: Auto create if not present.

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

class JetStreamContext(JetStream, JetStreamManager):
    """
    JetStreamContext includes both the JetStream and JetStream Manager.
    """
    def __init__(self, conn, **opts):
        super().__init__(conn, **opts)
