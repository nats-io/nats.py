# Copyright 2021-2022 The NATS Authors
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

from __future__ import annotations

import asyncio
import datetime
from dataclasses import dataclass
from typing import TYPE_CHECKING, List, Optional

import nats.errors
import nats.js.errors
from nats.js import api

if TYPE_CHECKING:
    from nats.js import JetStreamContext

KV_OP = "KV-Operation"
KV_DEL = "DEL"
KV_PURGE = "PURGE"
MSG_ROLLUP_SUBJECT = "sub"


class KeyValue:
    """
    KeyValue uses the JetStream KeyValue functionality.

    .. note::
       This functionality is EXPERIMENTAL and may be changed in later releases.

    ::

        import asyncio
        import nats

        async def main():
            nc = await nats.connect()
            js = nc.jetstream()

            # Create a KV
            kv = await js.create_key_value(bucket='MY_KV')

            # Set and retrieve a value
            await kv.put('hello', b'world')
            entry = await kv.get('hello')
            print(f'KeyValue.Entry: key={entry.key}, value={entry.value}')
            # KeyValue.Entry: key=hello, value=world

            await nc.close()

        if __name__ == '__main__':
            asyncio.run(main())

    """

    @dataclass
    class Entry:
        """
        An entry from a KeyValue store in JetStream.
        """
        bucket: str
        key: str
        value: Optional[bytes]
        revision: Optional[int]
        delta: Optional[int]
        created: Optional[int]
        operation: Optional[str]

    @dataclass(frozen=True)
    class BucketStatus:
        """
        BucketStatus is the status of a KeyValue bucket.
        """
        stream_info: api.StreamInfo
        bucket: str

        @property
        def values(self) -> int:
            """
            values returns the number of stored messages in the stream.
            """
            return self.stream_info.state.messages

        @property
        def history(self) -> int:
            """
            history returns the max msgs per subject.
            """
            return self.stream_info.config.max_msgs_per_subject

        @property
        def ttl(self) -> Optional[float]:
            """
            ttl returns the max age in seconds.
            """
            if self.stream_info.config.max_age is None:
                return None
            return self.stream_info.config.max_age

    def __init__(
        self,
        name: str,
        stream: str,
        pre: str,
        js: JetStreamContext,
        direct: bool,
    ) -> None:
        self._name = name
        self._stream = stream
        self._pre = pre
        self._js = js
        self._direct = direct

    async def get(self, key: str, revision: Optional[int] = None) -> Entry:
        """
        get returns the latest value for the key.
        """
        entry = None
        try:
            entry = await self._get(key, revision)
        except nats.js.errors.KeyDeletedError as err:
            raise nats.js.errors.KeyNotFoundError(err.entry, err.op)
        return entry

    async def _get(self, key: str, revision: Optional[int] = None) -> Entry:
        msg = None
        subject = f"{self._pre}{key}"
        try:
            if revision:
                msg = await self._js.get_msg(
                    self._stream,
                    seq=revision,
                    direct=self._direct,
                )
            else:
                msg = await self._js.get_msg(
                    self._stream,
                    subject=subject,
                    seq=revision,
                    direct=self._direct,
                )
        except nats.js.errors.NotFoundError:
            raise nats.js.errors.KeyNotFoundError

        # Check whether the revision from the stream does not match the key.
        if subject != msg.subject:
            raise nats.js.errors.KeyNotFoundError(
                message=f"expected '{subject}', but got '{msg.subject}'"
            )

        entry = KeyValue.Entry(
            bucket=self._name,
            key=key,
            value=msg.data,
            revision=msg.seq,
            delta=None,
            created=None,
            operation=None,
        )

        # Check headers to see if deleted or purged.
        if msg.headers:
            op = msg.headers.get(KV_OP, None)
            if op == KV_DEL or op == KV_PURGE:
                raise nats.js.errors.KeyDeletedError(entry, op)

        return entry

    async def put(self, key: str, value: bytes) -> int:
        """
        put will place the new value for the key into the store
        and return the revision number.
        """
        pa = await self._js.publish(f"{self._pre}{key}", value)
        return pa.seq

    async def create(self, key: str, value: bytes) -> int:
        """
        create will add the key/value pair iff it does not exist.
        """
        pa = None
        try:
            pa = await self.update(key, value, last=0)
        except nats.js.errors.KeyWrongLastSequenceError as err:
            # In case of attempting to recreate an already deleted key,
            # the client would get a KeyWrongLastSequenceError.  When this happens,
            # it is needed to fetch latest revision number and attempt to update.
            try:
                # NOTE: This reimplements the following behavior from Go client.
                #
                #   Since we have tombstones for DEL ops for watchers, this could be from that
                #   so we need to double check.
                #

                # Get latest revision to update in case it was deleted but if it was not
                await self._get(key)

                # No exception so not a deleted key, so reraise the original KeyWrongLastSequenceError.
                # If it was deleted then the error exception will contain metadata
                # to recreate using the last revision.
                raise err
            except nats.js.errors.KeyDeletedError as err:
                pa = await self.update(key, value, last=err.entry.revision)

        return pa

    async def update(
        self, key: str, value: bytes, last: Optional[int] = None
    ) -> int:
        """
        update will update the value iff the latest revision matches.
        """
        hdrs = {}
        if not last:
            last = 0
        hdrs[api.Header.EXPECTED_LAST_SUBJECT_SEQUENCE] = str(last)

        pa = None
        try:
            pa = await self._js.publish(
                f"{self._pre}{key}", value, headers=hdrs
            )
        except nats.js.errors.APIError as err:
            # Check for a BadRequest::KeyWrongLastSequenceError error code.
            if err.err_code == 10071:
                raise nats.js.errors.KeyWrongLastSequenceError(
                    description=err.description
                )
            else:
                raise err
        return pa.seq

    async def delete(self, key: str, last: Optional[int] = None) -> bool:
        """
        delete will place a delete marker and remove all previous revisions.
        """
        hdrs = {}
        hdrs[KV_OP] = KV_DEL

        if last and last > 0:
            hdrs[api.Header.EXPECTED_LAST_SUBJECT_SEQUENCE] = str(last)

        await self._js.publish(f"{self._pre}{key}", headers=hdrs)
        return True

    async def purge(self, key: str) -> bool:
        """
        purge will remove the key and all revisions.
        """
        hdrs = {}
        hdrs[KV_OP] = KV_PURGE
        hdrs[api.Header.ROLLUP] = MSG_ROLLUP_SUBJECT
        await self._js.publish(f"{self._pre}{key}", headers=hdrs)
        return True

    async def purge_deletes(self, olderthan: int = 30 * 60) -> bool:
        """
        purge will remove all current delete markers older.
        :param olderthan: time in seconds
        """

        watcher = await self.watchall()
        delete_markers = []
        async for update in watcher:
            if update.operation == KV_DEL or update.operation == KV_PURGE:
                delete_markers.append(update)

        for entry in delete_markers:
            keep = 0
            subject = f"{self._pre}{entry.key}"
            duration = datetime.datetime.now() - entry.created
            if olderthan > 0 and olderthan > duration.total_seconds():
                keep = 1
            await self._js.purge_stream(
                self._stream, subject=subject, keep=keep
            )
        return True

    async def status(self) -> BucketStatus:
        """
        status retrieves the status and configuration of a bucket.
        """
        info = await self._js.stream_info(self._stream)
        return KeyValue.BucketStatus(stream_info=info, bucket=self._name)

    class KeyWatcher:

        def __init__(self, js):
            self._js = js
            self._updates: asyncio.Queue[KeyValue.Entry | None] = asyncio.Queue(maxsize=256)
            self._sub = None
            self._pending: Optional[int] = None

            # init done means that the nil marker has been sent,
            # once this is sent it won't be sent anymore.
            self._init_done = False

        async def stop(self):
            """
            stop will stop this watcher.
            """
            await self._sub.unsubscribe()

        async def updates(self, timeout=5.0):
            """
            updates fetches the next update from a watcher.
            """
            try:
                return await asyncio.wait_for(self._updates.get(), timeout)
            except asyncio.TimeoutError:
                raise nats.errors.TimeoutError

        def __aiter__(self):
            return self

        async def __anext__(self):
            entry = await self._updates.get()
            if not entry:
                raise StopAsyncIteration
            else:
                return entry

    async def watchall(self, **kwargs) -> KeyWatcher:
        """
        watchall returns a KeyValue watcher that matches all the keys.
        """
        return await self.watch(">", **kwargs)

    async def keys(self, **kwargs) -> List[str]:
        """
        keys will return a list of the keys from a KeyValue store.
        """
        watcher = await self.watchall(
            ignore_deletes=True,
            meta_only=True,
        )
        keys = []

        async for key in watcher:
            # None entry is used to signal that there is no more info.
            if not key:
                break
            keys.append(key.key)
        await watcher.stop()

        if not keys:
            raise nats.js.errors.NoKeysError

        return keys

    async def history(self, key: str) -> List[Entry]:
        """
        history retrieves a list of the entries so far.
        """
        watcher = await self.watch(key, include_history=True)

        entries = []

        async for entry in watcher:
            # None entry is used to signal that there is no more info.
            if not entry:
                break
            entries.append(entry)

        await watcher.stop()

        if not entries:
            raise nats.js.errors.NoKeysError

        return entries

    async def watch(
        self,
        keys,
        headers_only=False,
        include_history=False,
        ignore_deletes=False,
        meta_only=False,
        inactive_threshold=None,
    ) -> KeyWatcher:
        """
        watch will fire a callback when a key that matches the keys
        pattern is updated.
        The first update after starting the watch is None in case
        there are no pending updates.
        """
        subject = f"{self._pre}{keys}"
        watcher = KeyValue.KeyWatcher(self)
        init_setup: asyncio.Future[bool] = asyncio.Future()

        async def watch_updates(msg):
            if not init_setup.done():
                await asyncio.wait_for(init_setup, timeout=self._js._timeout)

            meta = msg.metadata
            op = None
            if msg.header and KV_OP in msg.header:
                op = msg.header.get(KV_OP)

                # keys() uses this
                if ignore_deletes:
                    if (op == KV_PURGE or op == KV_DEL):
                        if meta.num_pending == 0 and not watcher._init_done:
                            await watcher._updates.put(None)
                            watcher._init_done = True
                        return

            entry = KeyValue.Entry(
                bucket=self._name,
                key=msg.subject[len(self._pre):],
                value=msg.data,
                revision=meta.sequence.stream,
                delta=meta.num_pending,
                created=meta.timestamp,
                operation=op,
            )
            await watcher._updates.put(entry)

            # When there are no more updates send an empty marker
            # to signal that it is done, this will unblock iterators
            if meta.num_pending == 0 and (not watcher._init_done):
                await watcher._updates.put(None)
                watcher._init_done = True

        deliver_policy = None
        if not include_history:
            deliver_policy = api.DeliverPolicy.LAST_PER_SUBJECT

        # Cleanup watchers after 5 minutes of inactivity by default.
        if not inactive_threshold:
            inactive_threshold = 5 * 60

        watcher._sub = await self._js.subscribe(
            subject,
            cb=watch_updates,
            ordered_consumer=True,
            deliver_policy=deliver_policy,
            headers_only=meta_only,
            inactive_threshold=inactive_threshold,
        )
        await asyncio.sleep(0)

        # Check from consumer info what is the number of messages
        # awaiting to be consumed to send the initial signal marker.
        try:
            cinfo = await watcher._sub.consumer_info()
            watcher._pending = cinfo.num_pending

            # If no delivered and/or pending messages, then signal
            # that this is the start.
            # The consumer subscription will start receiving messages
            # so need to check those that have already made it.
            received = watcher._sub.delivered
            init_setup.set_result(True)
            if cinfo.num_pending == 0 and received == 0:
                await watcher._updates.put(None)
                watcher._init_done = True
        except Exception as err:
            init_setup.cancel()
            await watcher._sub.unsubscribe()
            raise err

        return watcher
