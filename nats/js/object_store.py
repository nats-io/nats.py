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
import io
from datetime import datetime, timezone
from hashlib import sha256
from dataclasses import dataclass
import json
from typing import TYPE_CHECKING, Optional, Union

from nats.js import api
from nats.js.errors import (
    BadObjectMetaError, DigestMismatchError, InvalidObjectNameError,
    NotFoundError, LinkIsABucketError, Error
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

    def __sanitize_name(self, name: str) -> str:
        name = name.replace(".", "_")
        return name.replace(" ", "_")

    async def get_info(self, name: str) -> api.ObjectInfo:
        """
        get_info will retrieve the current information for the object.
        """
        obj = self.__sanitize_name(name)

        if not key_valid(obj):
            raise InvalidObjectNameError

        meta = OBJ_META_PRE_TEMPLATE.format(
            bucket=self._name,
            obj=base64.urlsafe_b64encode(bytes(obj, "utf-8")).decode()
        )
        stream = OBJ_STREAM_TEMPLATE.format(bucket=self._name)

        msg = await self._js.get_last_msg(stream, meta)

        data = None
        if msg.data:
            data = msg.data

        try:
            info = api.ObjectInfo.from_response(json.loads(data))
        except Exception as e:
            raise BadObjectMetaError from e

        # TODO: Uncomment below when time is added to RawStreamMsg.
        # info.mtime = m.time

        return info

    async def get(self, name: str) -> ObjectResult:
        """
        get will pull the object from the underlying stream.
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
                digest_str = info.digest.replace(OBJ_DIGEST_TYPE, "").replace(
                    OBJ_DIGEST_TYPE.upper(), ""
                )
                rsha = base64.urlsafe_b64decode(digest_str)
                if not sha == rsha:
                    raise DigestMismatchError

        return result

    async def put(self, name: str, data: Union[str, bytes, io.BufferedIOBase], meta: Optional[api.ObjectMeta] = None) -> api.ObjectInfo:
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

        obj = self.__sanitize_name(meta.name)

        if not key_valid(obj):
            raise InvalidObjectNameError

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
        if isinstance(data, str):
            data = io.BytesIO(data.encode())
        elif isinstance(data, bytes):
            data = io.BytesIO(data)
        elif (not isinstance(data, io.BufferedIOBase)):
            # Only allowing buffered readers at the moment.
            raise Error

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
            n = data.readinto(chunk)
            if n == 0:
                break
            payload = chunk[:n]
            h.update(payload)
            await self._js.publish(chunk_subj, payload)
            sent += 1
            total += n

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
        await self._js.publish(
            meta_subj,
            json.dumps(info.as_dict()).encode(),
            headers={api.Header.ROLLUP: MSG_ROLLUP_SUBJECT}
        )

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
