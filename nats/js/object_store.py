# Copyright 2021-2023 The NATS Authors
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
import re
import io
from datetime import datetime, timezone
from hashlib import sha256
from dataclasses import dataclass
import json
import asyncio
from typing import TYPE_CHECKING, Optional, Union, List

import nats.errors
from nats.js import api
from nats.js.errors import (
    BadObjectMetaError, DigestMismatchError, ObjectAlreadyExists,
    ObjectDeletedError, ObjectNotFoundError, NotFoundError, LinkIsABucketError
)
from nats.js.kv import MSG_ROLLUP_SUBJECT

VALID_BUCKET_RE = re.compile(r"^[a-zA-Z0-9_-]+$")
VALID_KEY_RE = re.compile(r"^[-/_=\.a-zA-Z0-9]+$")

if TYPE_CHECKING:
    from nats.js import JetStreamContext

OBJ_STREAM_TEMPLATE = "OBJ_{bucket}"
OBJ_ALL_CHUNKS_PRE_TEMPLATE = "$O.{bucket}.C.>"
OBJ_ALL_META_PRE_TEMPLATE = "$O.{bucket}.M.>"
OBJ_CHUNKS_PRE_TEMPLATE = "$O.{bucket}.C.{obj}"
OBJ_META_PRE_TEMPLATE = "$O.{bucket}.M.{obj}"
OBJ_NO_PENDING = "0"
OBJ_DEFAULT_CHUNK_SIZE = 128 * 1024  # 128k
OBJ_DIGEST_TYPE = "SHA-256="
OBJ_DIGEST_TEMPLATE = OBJ_DIGEST_TYPE + "{digest}"


class ObjectStore:
    """
    ObjectStore uses the JetStream ObjectStore functionality.

    .. note::
       This functionality is EXPERIMENTAL and may be changed in later releases.

    ::
    """

    @dataclass
    class ObjectResult:
        """
        ObjectResult is the result returned from the ObjectStore in JetStream.
        """

        info: api.ObjectInfo
        data: Optional[bytes] = bytes()

    @dataclass(frozen=True)
    class ObjectStoreStatus:
        """
        ObjectStoreStatus is the status of a ObjectStore bucket.
        """
        stream_info: api.StreamInfo
        bucket: str

        @property
        def description(self) -> Optional[str]:
            """
            description is the description supplied when creating the bucket.
            """
            return self.stream_info.config.description

        @property
        def ttl(self) -> Optional[float]:
            """
            ttl returns the max age in seconds.
            """
            if self.stream_info.config.max_age is None:
                return None
            return self.stream_info.config.max_age

        @property
        def storage_type(self) -> Optional[api.StorageType]:
            """
            storage indicates the underlying JetStream storage technology used to store data.
            """
            return self.stream_info.config.storage

        @property
        def replicas(self) -> Optional[int]:
            """
            replicas indicates how many storage replicas are kept for the data in the bucket.
            """
            return self.stream_info.config.num_replicas

        @property
        def sealed(self) -> bool:
            """
            sealed indicates the stream is sealed and cannot be modified in any way.
            """
            return self.stream_info.config.sealed

        @property
        def size(self) -> int:
            """
            size is the combined size of all data in the bucket including metadata, in bytes.
            """
            return self.stream_info.state.bytes

    def __init__(
        self,
        name: str,
        stream: str,
        js: "JetStreamContext",
    ) -> None:
        self._name = name
        self._stream = stream
        self._js = js

    async def get_info(
        self,
        name: str,
        show_deleted: Optional[bool] = False,
    ) -> api.ObjectInfo:
        """
        get_info will retrieve the current information for the object.
        """
        obj = name

        meta = OBJ_META_PRE_TEMPLATE.format(
            bucket=self._name,
            obj=base64.urlsafe_b64encode(bytes(obj, "utf-8")).decode()
        )
        stream = OBJ_STREAM_TEMPLATE.format(bucket=self._name)

        msg = None
        try:
            msg = await self._js.get_last_msg(stream, meta)
        except NotFoundError:
            raise ObjectNotFoundError

        data = None
        if msg.data:
            data = msg.data

        try:
            info = api.ObjectInfo.from_response(json.loads(data))
        except Exception as e:
            raise BadObjectMetaError from e

        if (not show_deleted) and info.deleted:
            raise ObjectNotFoundError

        # TODO: Uncomment below when time is added to RawStreamMsg.
        # info.mtime = m.time

        return info

    async def get(
        self,
        name: str,
        writeinto: Optional[io.BufferedIOBase] = None,
        show_deleted: Optional[bool] = False,
    ) -> ObjectResult:
        """
        get will pull the object from the underlying stream.
        """
        obj = name

        # Grab meta info.
        info = await self.get_info(obj, show_deleted)

        if info.nuid is None or info.nuid == "":
            raise BadObjectMetaError

        # Check for object links.If single objects we do a pass through.
        if info.is_link():
            if info.options.link.name is None or info.options.link.name == "":
                raise LinkIsABucketError
            lobs = await self._js.object_store(info.options.link.bucket)
            return await lobs.get(info.options.link.name)

        result = self.ObjectResult(info=info)

        if info.size == 0:
            return result

        chunk_subj = OBJ_CHUNKS_PRE_TEMPLATE.format(
            bucket=self._name, obj=info.nuid
        )
        sub = await self._js.subscribe(
            subject=chunk_subj, ordered_consumer=True
        )

        h = sha256()

        executor = None
        executor_fn = None
        if writeinto:
            executor = asyncio.get_running_loop().run_in_executor
            if hasattr(writeinto, 'buffer'):
                executor_fn = writeinto.buffer.write
            else:
                executor_fn = writeinto.write

        async for msg in sub._message_iterator:
            tokens = msg._get_metadata_fields(msg.reply)

            if executor:
                await executor(None, executor_fn, msg.data)
            else:
                result.data += msg.data
            h.update(msg.data)

            # Check if we are done.
            if tokens[8] == OBJ_NO_PENDING:
                await sub.unsubscribe()

                # Make sure the digest matches.
                sha = h.digest()
                digest_str = info.digest.replace(OBJ_DIGEST_TYPE, "").replace(
                    OBJ_DIGEST_TYPE.upper(), ""
                )
                rsha = base64.urlsafe_b64decode(digest_str)
                if not sha == rsha:
                    raise DigestMismatchError

        return result

    async def put(
        self,
        name: str,
        data: Union[str, bytes, io.BufferedIOBase],
        meta: Optional[api.ObjectMeta] = None,
    ) -> api.ObjectInfo:
        """
        put will place the contents from the reader into this object-store.
        """
        if meta is None:
            meta = api.ObjectMeta(name=name)
        elif len(name) > 0:
            meta.name = name

        if meta.options is None:
            meta.options = api.ObjectMetaOptions(
                max_chunk_size=OBJ_DEFAULT_CHUNK_SIZE,
            )

        obj = meta.name
        einfo = None

        # Create the new nuid so chunks go on a new subject if the name is re-used.
        newnuid = self._js._nc._nuid.next()

        # Create a random subject prefixed with the object stream name.
        chunk_subj = OBJ_CHUNKS_PRE_TEMPLATE.format(
            bucket=self._name, obj=newnuid.decode()
        )

        # Grab existing meta info (einfo). Ok to be found or not found, any other error is a problem.
        # Chunks on the old nuid can be cleaned up at the end.
        try:
            einfo = await self.get_info(meta.name)
        except NotFoundError:
            pass

        # Normalize based on type but treat all as readers.
        executor = None
        if isinstance(data, str):
            data = io.BytesIO(data.encode())
        elif isinstance(data, bytes):
            data = io.BytesIO(data)
        elif hasattr(data, 'readinto') or isinstance(data, io.BufferedIOBase):
            # Need to delegate to a threaded executor to avoid blocking.
            executor = asyncio.get_running_loop().run_in_executor
        elif hasattr(data, 'buffer') or isinstance(data, io.TextIOWrapper):
            data = data.buffer
        else:
            raise TypeError("nats: invalid type for object store")

        info = api.ObjectInfo(
            name=meta.name,
            description=meta.description,
            headers=meta.headers,
            options=meta.options,
            bucket=self._name,
            nuid=newnuid.decode(),
            size=0,
            chunks=0,
            mtime=datetime.now(timezone.utc).isoformat()
        )
        h = sha256()
        chunk = bytearray(meta.options.max_chunk_size)
        sent = 0
        total = 0

        while True:
            try:
                n = None
                if executor:
                    n = await executor(None, data.readinto, chunk)
                else:
                    n = data.readinto(chunk)

                if n == 0:
                    break
                payload = chunk[:n]
                h.update(payload)
                await self._js.publish(chunk_subj, payload)
                sent += 1
                total += n
            except Exception as err:
                await self._js.purge_stream(self._stream, subject=chunk_subj)
                raise err

        sha = h.digest()
        info.size = total
        info.chunks = sent
        info.digest = OBJ_DIGEST_TEMPLATE.format(
            digest=base64.urlsafe_b64encode(sha).decode()
        )

        # Prepare the meta message.
        meta_subj = OBJ_META_PRE_TEMPLATE.format(
            bucket=self._name,
            obj=base64.urlsafe_b64encode(bytes(obj, "utf-8")).decode()
        )
        # Publish the meta message.
        try:
            await self._js.publish(
                meta_subj,
                json.dumps(info.as_dict()).encode(),
                headers={api.Header.ROLLUP: MSG_ROLLUP_SUBJECT}
            )
        except Exception as err:
            await self._js.purge_stream(self._stream, subject=chunk_subj)
            raise err

        # NOTE: This time is not actually the correct time.
        info.mtime = datetime.now(timezone.utc).isoformat()

        # Delete any original chunks.
        if einfo is not None and not einfo.deleted:
            chunk_subj = OBJ_CHUNKS_PRE_TEMPLATE.format(
                bucket=self._name, obj=einfo.nuid
            )
            await self._js.purge_stream(self._stream, subject=chunk_subj)

        return info

    async def status(self) -> ObjectStoreStatus:
        """
        status retrieves runtime status about a bucket.
        """
        info = await self._js.stream_info(self._stream)
        status = self.ObjectStoreStatus(stream_info=info, bucket=self._name)
        return status

    async def seal(self):
        """
        seal will seal the object store, no further modifications will be allowed.
        """
        info = await self._js.stream_info(self._stream)
        config = info.config
        config.sealed = True
        await self._js.update_stream(config)

    async def update_meta(
        self,
        name: str,
        meta: api.ObjectMeta,
    ):
        """
        update_meta will place the contents from the reader into this object-store.
        """
        info = None
        try:
            info = await self.get_info(name)
        except ObjectNotFoundError:
            raise ObjectDeletedError

        # Can change it only if it has been deleted.
        if name != meta.name:
            einfo = await self.get_info(name, show_deleted=True)
            if not einfo.deleted:
                raise ObjectAlreadyExists

        info.name = meta.name
        info.description = meta.description
        info.headers = meta.headers

        # Prepare the meta message.
        meta_subj = OBJ_META_PRE_TEMPLATE.format(
            bucket=self._name,
            obj=base64.urlsafe_b64encode(bytes(name, "utf-8")).decode()
        )
        # Publish the meta message.
        try:
            await self._js.publish(
                meta_subj,
                json.dumps(info.as_dict()).encode(),
                headers={api.Header.ROLLUP: MSG_ROLLUP_SUBJECT}
            )
        except Exception as err:
            raise err

        # If the name changed, then need to store the meta under the new name.
        if name != meta.name:
            # TODO: purge the stream
            await self._js.purge_stream(self._stream, subject=meta_subj)

    class ObjectWatcher:

        def __init__(self, js):
            self._js = js
            self._updates = asyncio.Queue(maxsize=256)
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

    async def watch(
        self,
        ignore_deletes=False,
        include_history=False,
        meta_only=False,
    ) -> ObjectWatcher:
        """
        watch for changes in the underlying store and receive meta information updates.
        """
        all_meta = OBJ_ALL_META_PRE_TEMPLATE.format(bucket=self._name, )
        watcher = ObjectStore.ObjectWatcher(self)

        async def watch_updates(msg):
            meta = msg.metadata
            info = api.ObjectInfo.from_response(json.loads(msg.data))

            if (not ignore_deletes) or (not info.deleted):
                await watcher._updates.put(info)

            # When there are no more updates send an empty marker
            # to signal that it is done, this will unblock iterators
            if (not watcher._init_done) and meta.num_pending == 0:
                watcher._init_done = True
                await watcher._updates.put(None)

        try:
            await self._js.get_last_msg(self._stream, all_meta)
        except NotFoundError:
            watcher._init_done = True
            await watcher._updates.put(None)

        deliver_policy = None
        if not include_history:
            deliver_policy = api.DeliverPolicy.LAST_PER_SUBJECT

        watcher._sub = await self._js.subscribe(
            all_meta,
            cb=watch_updates,
            ordered_consumer=True,
            deliver_policy=deliver_policy,
        )
        await asyncio.sleep(0)

        return watcher

    async def delete(self, name: str) -> ObjectResult:
        """
        delete will delete the object.
        """
        obj = name

        # Grab meta info.
        info = await self.get_info(obj)

        if info.nuid is None or info.nuid == "":
            raise BadObjectMetaError

        # Purge chunks for the object.
        chunk_subj = OBJ_CHUNKS_PRE_TEMPLATE.format(
            bucket=self._name, obj=info.nuid
        )

        # Reset meta values.
        info.deleted = True
        info.size = 0
        info.chunks = 0
        info.digest = ''
        info.mtime = ''

        # Prepare the meta message.
        meta_subj = OBJ_META_PRE_TEMPLATE.format(
            bucket=self._name,
            obj=base64.urlsafe_b64encode(bytes(obj, "utf-8")).decode()
        )
        # Publish the meta message.
        try:
            await self._js.publish(
                meta_subj,
                json.dumps(info.as_dict()).encode(),
                headers={api.Header.ROLLUP: MSG_ROLLUP_SUBJECT}
            )
        finally:
            await self._js.purge_stream(self._stream, subject=chunk_subj)

    async def list(
        self,
        ignore_deletes=False,
    ) -> List[api.ObjectInfo]:
        """
        list will list all the objects in this store.
        """
        watcher = await self.watch(ignore_deletes=ignore_deletes)
        entries = []

        async for entry in watcher:
            # None entry is used to signal that there is no more info.
            if not entry:
                break
            entries.append(entry)

        await watcher.stop()

        if not entries:
            raise NotFoundError

        return entries
