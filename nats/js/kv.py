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

import base64
from dataclasses import dataclass
from typing import TYPE_CHECKING, Optional

from nats.js import api
import nats.js.errors

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
        js: "JetStreamContext",
    ) -> None:
        self._name = name
        self._stream = stream
        self._pre = pre
        self._js = js

    async def get(self, key: str, revision: Optional[int]=None) -> Entry:
        """
        get returns the latest value for the key.
        
        """
        entry = None
        try:
            entry = await self._get(key, revision)
        except nats.js.errors.KeyDeletedError as err:
            raise nats.js.errors.KeyNotFoundError(err.entry, err.op)
        return entry

    async def _get(self, key: str, revision: Optional[int]=None) -> Entry:
        msg = None
        data = None
        try:
            if revision:
                msg = await self._js.get_msg(self._stream, revision)
                subject = f"{self._pre}{key}"
            else:
                msg = await self._js.get_last_msg(
                    self._stream,
                    f"{self._pre}{key}",
                )
                data = None
                if msg.data:
                    data = base64.b64decode(msg.data)
        except nats.js.errors.NotFoundError as err:
            raise nats.js.errors.KeyNotFoundError

        entry = KeyValue.Entry(
            bucket=self._name,
            key=key,
            value=data,
            revision=msg.seq,
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
        pa = await self.update(key, value, last=0)
        return pa

    async def update(self, key: str, value: bytes, last: Optional[int]=None) -> int:
        """
        update will update the value iff the latest revision matches.
        """
        hdrs = {}
        if last:
            hdrs[api.Header.EXPECTED_LAST_SUBJECT_SEQUENCE] = str(last)
        pa = await self._js.publish(f"{self._pre}{key}", value, headers=hdrs)
        return pa.seq

    async def delete(self, key: str, last: Optional[int]=None) -> bool:
        """
        delete will place a delete marker and remove all previous revisions.
        """
        hdrs = {}
        hdrs[KV_OP] = KV_DEL

        if last:
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

    async def status(self) -> BucketStatus:
        """
        status retrieves the status and configuration of a bucket.
        """
        info = await self._js.stream_info(self._stream)
        return KeyValue.BucketStatus(stream_info=info, bucket=self._name)
