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

import base64
import re
from datetime import datetime
from hashlib import sha256
from dataclasses import dataclass
import json
from typing import TYPE_CHECKING, Optional

from nats.js import api
from nats.js.errors import (
    BadObjectMetaError,
    DigestMismatchError,
    InvalidObjectNameError,
    NotFoundError,
    LinkIsABucketError
)
from nats.js.kv import MSG_ROLLUP_SUBJECT

VALID_BUCKET_RE = re.compile(r"^[a-zA-Z0-9_-]+$")
VALID_KEY_RE = re.compile(r"^[-/_=\.a-zA-Z0-9]+$")


def key_valid(key: str) -> bool:
    if len(key) == 0 or key[0] == '.' or key[-1] == '.':
        return False
    return VALID_KEY_RE.match(key) is not None


if TYPE_CHECKING:
    from nats.js import JetStreamContext

OBJ_STREAM_TEMPLATE = "OBJ_{bucket}"
OBJ_ALL_CHUNKS_PRE_TEMPLATE = "$O.{bucket}.C.>"
OBJ_ALL_META_PRE_TEMPLATE = "$O.{bucket}.M.>"
OBJ_CHUNKS_PRE_TEMPLATE = "$O.{bucket}.C.{obj}"
OBJ_META_PRE_TEMPLATE = "$O.{bucket}.M.{obj}"
OBJ_NO_PENDING = "0"
OBJ_DEFAULT_CHUNK_SIZE = 128 * 1024  # 128k
OBJ_DIGEST_TYPE = "sha-256="
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
        A result returned from the ObjectStore in JetStream.
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

    def __sanitize_name(self, name: str) -> str:
        name = name.replace(".", "_")
        return name.replace(" ", "_")

    async def get_info(self, name: str) -> api.ObjectInfo:
        """
        GetInfo will retrieve the current information for the object.
        """
        obj = self.__sanitize_name(name)

        if not key_valid(obj):
            raise InvalidObjectNameError

        meta = OBJ_META_PRE_TEMPLATE.format(bucket=self._name, obj=obj)
        stream = OBJ_STREAM_TEMPLATE.format(bucket=self._name)

        msg = await self._js.get_last_msg(stream, meta)

        data = None
        if msg.data:
            data = base64.b64decode(msg.data)

        try:
            info = api.ObjectInfo.from_response(json.loads(data))
        except Exception as e:
            raise BadObjectMetaError from e

        # TODO: Uncomment below when time is added to RawStreamMsg.
        # info.mtime = m.time

        return info

    async def get(self, name: str) -> ObjectResult:
        """
        Get will pull the object from the underlying stream.
        """
        obj = self.__sanitize_name(name)

        if not key_valid(obj):
            raise InvalidObjectNameError

        # Grab meta info.
        info = await self.get_info(obj)

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

        async for msg in sub._message_iterator:
            tokens = msg._get_metadata_fields(msg.reply)

            result.data += msg.data
            h.update(msg.data)

            # Check if we are done.
            if tokens[8] == OBJ_NO_PENDING:
                await sub.unsubscribe()

                # Make sure the digest matches.
                sha = h.digest()
                digest_str = info.digest.replace(OBJ_DIGEST_TYPE, "")
                rsha = base64.urlsafe_b64decode(digest_str)
                if not sha == rsha:
                    raise DigestMismatchError

        return result

    def __chunked(self, size, source):
        for i in range(0, len(source), size):
            yield source[i:i + size]

    async def put(self, meta: api.ObjectMeta, data: bytes) -> api.ObjectInfo:
        """
        Put will place the contents from the reader into this object-store.
        """
        if meta is None:
            raise BadObjectMetaError

        obj = self.__sanitize_name(meta.name)

        if not key_valid(obj):
            raise InvalidObjectNameError

        einfo = None

        # Grab existing meta info.
        try:
            einfo = await self.get_info(meta.name)
        except NotFoundError:
            pass

        # Create a random subject prefixed with the object stream name.
        id = self._js._nc._nuid.next()
        chunk_subj = OBJ_CHUNKS_PRE_TEMPLATE.format(
            bucket=self._name, obj=id.decode()
        )
        meta_subj = OBJ_META_PRE_TEMPLATE.format(bucket=self._name, obj=obj)

        try:
            h = sha256()
            sent = 0
            total = 0
            info = api.ObjectInfo(
                name=meta.name,
                description=meta.description,
                headers=meta.headers,
                options=meta.options,
                bucket=self._name,
                nuid=id.decode(),
                size=0,
                chunks=0,
                mtime=0
            )
            chunk_size = OBJ_DEFAULT_CHUNK_SIZE
            if meta.options is not None and meta.options.max_chunk_size is not None and meta.options.max_chunk_size > 0:
                chunk_size = meta.options.max_chunk_size

            for data_chunk in self.__chunked(chunk_size, data):
                h.update(data_chunk)
                await self._js.publish(chunk_subj, data_chunk)
                sent = sent + 1
                total = total + len(data_chunk)

            sha = h.digest()
            info.size = total
            info.chunks = sent
            info.digest = OBJ_DIGEST_TEMPLATE.format(
                digest=base64.urlsafe_b64encode(sha).decode()
            )

            await self._js.publish(
                meta_subj,
                json.dumps(info.as_dict()).encode(),
                headers={api.Header.ROLLUP: MSG_ROLLUP_SUBJECT}
            )

        except Exception as e:
            await self._js.purge_stream(
                self._stream, api.StreamPurgeRequest(filter=chunk_subj)
            )
            raise e

        info.mtime = datetime.utcnow().timestamp()

        if einfo is not None and not einfo.deleted:
            chunk_subj = OBJ_CHUNKS_PRE_TEMPLATE.format(
                bucket=self._name, obj=einfo.nuid
            )
            await self._js.purge_stream(
                self._stream, api.StreamPurgeRequest(filter=chunk_subj)
            )

        return info


# TODO: Functions left to implement
#

# // UpdateMeta will update the meta data for the object.
# UpdateMeta(name string, meta *ObjectMeta) error
# // Delete will delete the named object.
# Delete(name string) error
# // AddLink will add a link to another object into this object store.
# AddLink(name string, obj *ObjectInfo) (*ObjectInfo, error)
# // AddBucketLink will add a link to another object store.
# AddBucketLink(name string, bucket ObjectStore) (*ObjectInfo, error)
# // Seal will seal the object store, no further modifications will be allowed.
# Seal() error
# // Watch for changes in the underlying store and receive meta information updates.
# Watch(opts ...WatchOpt) (ObjectWatcher, error)
# // List will list all the objects in this store.
# List(opts ...WatchOpt) ([]*ObjectInfo, error)
# // Status retrieves run-time status about the backing store of the bucket.
# Status() (ObjectStoreStatus, error)
