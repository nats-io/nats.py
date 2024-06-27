# Copyright 2016-2024 The NATS Authors
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

import re
from dataclasses import dataclass, asdict, field
from datetime import datetime, timedelta
from enum import Enum
from types import NotImplementedType
from typing import (
    Any,
    AsyncIterable,
    AsyncIterator,
    Dict,
    List,
    Optional,
    cast,
)

from nats import jetstream
from nats.jetstream.api import Client, Paged, Request, Response
from nats.jetstream.errors import *
from nats.jetstream.message import Header, Msg, Status


class RetentionPolicy(Enum):
    """
    Determines how messages in a stream are retained.
    """

    LIMITS = "limits"
    """LimitsPolicy means that messages are retained until any given limit is reached. This could be one of MaxMsgs, MaxBytes, or MaxAge."""

    INTEREST = "interest"
    """InterestPolicy specifies that when all known observables have acknowledged a message, it can be removed."""

    WORKQUEUE = "workqueue"
    """WorkQueuePolicy specifies that when the first worker or subscriber acknowledges the message, it can be removed."""


class DiscardPolicy(Enum):
    """
    Determines how to proceed when limits of messages or bytes are reached.
    """

    OLD = "old"
    """DiscardOld will remove older messages to return to the limits. This is the default."""

    NEW = "new"
    """DiscardNew will fail to store new messages once the limits are reached."""


class StorageType(Enum):
    """
    Determines how messages are stored for retention.
    """

    FILE = "file"
    """
    Specifies on disk storage.
    """

    MEMORY = "memory"
    """
    Specifies in-memory storage.
    """


class StoreCompression(Enum):
    """
    Determines how messages are compressed.
    """

    NONE = "none"
    """
    Disables compression on the stream.
    """

    S2 = "s2"
    """
    Enables S2 compression on the stream.
    """


@dataclass
class StreamInfo:
    """
    Provides configuration and current state for a stream.
    """

    config: StreamConfig = field(metadata={'json': 'config'})
    """Contains the configuration settings of the stream, set when creating or updating the stream."""

    timestamp: datetime = field(metadata={'json': 'ts'})
    """Indicates when the info was gathered by the server."""

    created: datetime = field(metadata={'json': 'created'})
    """The timestamp when the stream was created."""

    state: StreamState = field(metadata={'json': 'state'})
    """Provides the state of the stream at the time of request, including metrics like the number of messages in the stream, total bytes, etc."""

    cluster: Optional[ClusterInfo] = field(
        default=None, metadata={'json': 'cluster'}
    )
    """Contains information about the cluster to which this stream belongs (if applicable)."""

    mirror: Optional[StreamSourceInfo] = field(
        default=None, metadata={'json': 'mirror'}
    )
    """Contains information about another stream this one is mirroring. Mirroring is used to create replicas of another stream's data. This field is omitted if the stream is not mirroring another stream."""

    sources: List[StreamSourceInfo] = field(
        default_factory=list, metadata={'json': 'sources'}
    )
    """A list of source streams from which this stream collects data."""


@dataclass
class StreamConfig:
    """
    StreamConfig is the configuration of a JetStream stream.
    """

    name: str = field(metadata={'json': 'name'})
    """Name is the name of the stream. It is required and must be unique across the JetStream account. Names cannot contain whitespace, ., >, path separators (forward or backwards slash), and non-printable characters."""

    description: Optional[str] = field(
        default=None, metadata={'json': 'description'}
    )
    """Description is an optional description of the stream."""

    subjects: List[str] = field(
        default_factory=list, metadata={'json': 'subjects'}
    )
    """Subjects is a list of subjects that the stream is listening on. Wildcards are supported. Subjects cannot be set if the stream is created as a mirror."""

    retention: RetentionPolicy = field(
        default=RetentionPolicy.LIMITS, metadata={'json': 'retention'}
    )
    """Retention defines the message retention policy for the stream. Defaults to LimitsPolicy."""

    max_consumers: int = field(metadata={'json': 'max_consumers'})
    """MaxConsumers specifies the maximum number of consumers allowed for the stream."""

    max_msgs: int = field(metadata={'json': 'max_msgs'})
    """MaxMsgs is the maximum number of messages the stream will store. After reaching the limit, stream adheres to the discard policy. If not set, server default is -1 (unlimited)."""

    max_bytes: int = field(metadata={'json': 'max_bytes'})
    """MaxBytes is the maximum total size of messages the stream will store. After reaching the limit, stream adheres to the discard policy. If not set, server default is -1 (unlimited)."""

    discard: DiscardPolicy = field(metadata={'json': 'discard'})
    """Discard defines the policy for handling messages when the stream reaches its limits in terms of number of messages or total bytes."""

    discard_new_per_subject: Optional[bool] = field(
        default=None, metadata={'json': 'discard_new_per_subject'}
    )
    """DiscardNewPerSubject is a flag to enable discarding new messages per subject when limits are reached. Requires DiscardPolicy to be DiscardNew and the MaxMsgsPerSubject to be set."""

    max_age: timedelta = field(metadata={'json': 'max_age'})
    """MaxAge is the maximum age of messages that the stream will retain."""

    max_msgs_per_subject: int = field(
        metadata={'json': 'max_msgs_per_subject'}
    )
    """MaxMsgsPerSubject is the maximum number of messages per subject that the stream will retain."""

    max_msg_size: Optional[int] = field(
        default=None, metadata={'json': 'max_msg_size'}
    )
    """MaxMsgSize is the maximum size of any single message in the stream."""

    storage: StorageType = field(metadata={'json': 'storage'})
    """Storage specifies the type of storage backend used for the stream (file or memory)."""

    replicas: int = field(default=1, metadata={'json': 'num_replicas'})
    """Replicas is the number of stream replicas in clustered JetStream. Defaults to 1, maximum is 5."""

    no_ack: Optional[bool] = field(default=None, metadata={'json': 'no_ack'})
    """NoAck is a flag to disable acknowledging messages received by this stream. If set to true, publish methods from the JetStream client will not work as expected, since they rely on acknowledgements. Core NATS publish methods should be used instead. Note that this will make message delivery less reliable."""

    duplicates: Optional[timedelta] = field(
        default=None, metadata={'json': 'duplicate_window'}
    )
    """Duplicates is the window within which to track duplicate messages. If not set, server default is 2 minutes."""

    placement: Optional[Placement] = field(
        default=None, metadata={'json': 'placement'}
    )
    """Placement is used to declare where the stream should be placed via tags and/or an explicit cluster name."""

    mirror: Optional[StreamSource] = field(
        default=None, metadata={'json': 'mirror'}
    )
    """Mirror defines the configuration for mirroring another stream."""

    sources: List[StreamSource] = field(
        default_factory=list, metadata={'json': 'sources'}
    )
    """Sources is a list of other streams this stream sources messages from."""

    sealed: Optional[bool] = field(default=None, metadata={'json': 'sealed'})
    """Sealed streams do not allow messages to be published or deleted via limits or API, sealed streams cannot be unsealed via configuration update. Can only be set on already created streams via the Update API."""

    deny_delete: Optional[bool] = field(
        default=None, metadata={'json': 'deny_delete'}
    )
    """DenyDelete restricts the ability to delete messages from a stream via the API. Defaults to false."""

    deny_purge: Optional[bool] = field(
        default=None, metadata={'json': 'deny_purge'}
    )
    """DenyPurge restricts the ability to purge messages from a stream via the API. Defaults to false."""

    allow_rollup: Optional[bool] = field(
        default=None, metadata={'json': 'allow_rollup_hdrs'}
    )
    """AllowRollup allows the use of the Nats-Rollup header to replace all contents of a stream, or subject in a stream, with a single new message."""

    compression: StoreCompression = field(
        default=StoreCompression.NONE, metadata={'json': 'compression'}
    )
    """Compression specifies the message storage compression algorithm. Defaults to NoCompression."""

    first_sequence: Optional[int] = field(
        default=None, metadata={'json': 'first_seq'}
    )
    """FirstSeq is the initial sequence number of the first message in the stream."""

    subject_transform: Optional[SubjectTransformConfig] = field(
        default=None, metadata={'json': 'subject_transform'}
    )
    """SubjectTransform allows applying a transformation to matching messages' subjects."""

    republish: Optional[Republish] = field(
        default=None, metadata={'json': 'republish'}
    )
    """RePublish allows immediate republishing of a message to the configured subject after it's stored."""

    allow_direct: bool = field(
        default=False, metadata={'json': 'allow_direct'}
    )
    """AllowDirect enables direct access to individual messages using direct get API. Defaults to false."""

    mirror_direct: bool = field(
        default=False, metadata={'json': 'mirror_direct'}
    )
    """MirrorDirect enables direct access to individual messages from the origin stream using direct get API. Defaults to false."""

    consumer_limits: Optional[StreamConsumerLimits] = field(
        default=None, metadata={'json': 'consumer_limits'}
    )
    """ConsumerLimits defines limits of certain values that consumers can set, defaults for those who don't set these settings."""

    metadata: Dict[str, str] = field(
        default_factory=dict, metadata={'json': 'metadata'}
    )
    """Metadata is a set of application-defined key-value pairs for associating metadata on the stream. This feature requires nats-server v2.10.0 or later."""


@dataclass
class StreamSourceInfo:
    """
    StreamSourceInfo shows information about an upstream stream source/mirror.
    """

    name: str = field(metadata={'json': 'name'})
    """Name is the name of the stream that is being replicated."""

    lag: int = field(metadata={'json': 'lag'})
    """Lag informs how many messages behind the source/mirror operation is. This will only show correctly if there is active communication with stream/mirror."""

    active: timedelta = field(metadata={'json': 'active'})
    """Active informs when last the mirror or sourced stream had activity. Value will be -1 when there has been no activity."""

    filter_subject: Optional[str] = field(
        default=None, metadata={'json': 'filter_subject'}
    )
    """FilterSubject is the subject filter defined for this source/mirror."""

    subject_transforms: List[SubjectTransformConfig] = field(
        default_factory=list, metadata={'json': 'subject_transforms'}
    )
    """SubjectTransforms is a list of subject transforms defined for this source/mirror."""


@dataclass
class StreamState:
    """
    StreamState is the state of a JetStream stream at the time of request.
    """

    msgs: int = field(metadata={'json': 'messages'})
    """The number of messages stored in the stream."""

    bytes: int = field(metadata={'json': 'bytes'})
    """The number of bytes stored in the stream."""

    first_sequence: int = field(metadata={'json': 'first_seq'})
    """The the sequence number of the first message in the stream."""

    first_time: datetime = field(metadata={'json': 'first_ts'})
    """The timestamp of the first message in the stream."""

    last_sequence: int = field(metadata={'json': 'last_seq'})
    """The sequence number of the last message in the stream."""

    last_time: datetime = field(metadata={'json': 'last_ts'})
    """The timestamp of the last message in the stream."""

    consumers: int = field(metadata={'json': 'consumer_count'})
    """The number of consumers on the stream."""

    deleted: List[int] = field(
        default_factory=list, metadata={'json': 'deleted'}
    )
    """A list of sequence numbers that have been removed from the stream. This field will only be returned if the stream has been fetched with the DeletedDetails option."""

    num_deleted: int = field(default=0, metadata={'json': 'num_deleted'})
    """NumDeleted is the number of messages that have been removed from the stream. Only deleted messages causing a gap in stream sequence numbers are counted. Messages deleted at the beginning or end of the stream are not counted."""

    num_subjects: int = field(default=0, metadata={'json': 'num_subjects'})
    """NumSubjects is the number of unique subjects the stream has received messages on."""

    subjects: Dict[str, int] = field(
        default_factory=dict, metadata={'json': 'subjects'}
    )
    """Subjects is a map of subjects the stream has received messages on with message count per subject. This field will only be returned if the stream has been fetched with the SubjectFilter option."""


@dataclass
class ClusterInfo:
    """
    ClusterInfo shows information about the underlying set of servers that
    make up the stream or consumer.
    """

    name: Optional[str] = field(default=None, metadata={'json': 'name'})
    """Name is the name of the cluster."""

    leader: Optional[str] = field(default=None, metadata={'json': 'leader'})
    """Leader is the server name of the RAFT leader."""

    replicas: List[PeerInfo] = field(
        default_factory=list, metadata={'json': 'replicas'}
    )
    """Replicas is the list of members of the RAFT cluster."""


@dataclass
class PeerInfo:
    """
    PeerInfo shows information about the peers in the cluster that are
    supporting the stream or consumer.
    """

    name: str = field(metadata={'json': 'name'})
    """The server name of the peer."""

    current: bool = field(metadata={'json': 'current'})
    """Indicates if the peer is up to date and synchronized with the leader."""

    active: timedelta = field(metadata={'json': 'active'})
    """The duration since this peer was last seen."""

    offline: Optional[bool] = field(default=None, metadata={'json': 'offline'})
    """Indicates if the peer is considered offline by the group."""

    lag: Optional[int] = field(default=None, metadata={'json': 'lag'})
    """The number of uncommitted operations this peer is behind the leader."""


@dataclass
class SubjectTransformConfig:
    """
    SubjectTransformConfig is for applying a subject transform (to matching
    messages) before doing anything else when a new message is received.
    """

    source: str = field(metadata={'json': 'src'})
    """The subject pattern to match incoming messages against."""

    destination: str = field(metadata={'json': 'dest'})
    """The subject pattern to remap the subject to."""


@dataclass
class Republish:
    """
    RePublish is for republishing messages once committed to a stream. The
    original subject is remapped from the subject pattern to the destination
    pattern.
    """

    destination: str = field(metadata={'json': 'dest'})
    """The subject pattern to republish the subject to."""

    source: Optional[str] = field(default=None, metadata={'json': 'src'})
    """The subject pattern to match incoming messages against."""

    headers_only: Optional[bool] = field(
        default=None, metadata={'json': 'headers_only'}
    )
    """A flag to indicate that only the headers should be republished."""


@dataclass
class Placement:
    """
    Placement is used to guide placement of streams in clustered JetStream.
    """

    cluster: str = field(metadata={'json': 'cluster'})
    """The name of the cluster to which the stream should be assigned."""

    tags: List[str] = field(default_factory=list, metadata={'json': 'tags'})
    """Tags are used to match streams to servers in the cluster. A stream will be assigned to a server with a matching tag."""


@dataclass
class StreamSource:
    """
    StreamSource dictates how streams can source from other streams.
    """

    name: str = field(metadata={'json': 'name'})
    """The name of the stream to source from."""

    opt_start_seq: Optional[int] = field(
        default=None, metadata={'json': 'opt_start_seq'}
    )
    """The sequence number to start sourcing from."""

    opt_start_time: Optional[datetime] = field(
        default=None, metadata={'json': 'opt_start_time'}
    )
    """The timestamp of messages to start sourcing from."""

    filter_subject: Optional[str] = field(
        default=None, metadata={'json': 'filter_subject'}
    )
    """The subject filter used to only replicate messages with matching subjects."""

    subject_transforms: List[SubjectTransformConfig] = field(
        default_factory=list, metadata={'json': 'subject_transforms'}
    )
    """
    A list of subject transforms to apply to matching messages.

    Subject transforms on sources and mirrors are also used as subject filters with optional transformations.
    """

    external: Optional[ExternalStream] = field(
        default=None, metadata={'json': 'external'}
    )
    """A configuration referencing a stream source in another account or JetStream domain."""

    domain: Optional[str] = field(default=None, metadata={'json': '-'})
    """Used to configure a stream source in another JetStream domain. This setting will set the External field with the appropriate APIPrefix."""


@dataclass
class ExternalStream:
    """
    ExternalStream allows you to qualify access to a stream source in another
    account.
    """

    api_prefix: str = field(metadata={'json': 'api'})
    """The subject prefix that imports the other account/domain $JS.API.CONSUMER.> subjects."""

    deliver_prefix: str = field(metadata={'json': 'deliver'})
    """The delivery subject to use for the push consumer."""


@dataclass
class StreamConsumerLimits:
    """
    StreamConsumerLimits are the limits for a consumer on a stream. These can
    be overridden on a per consumer basis.
    """

    inactive_threshold: Optional[timedelta] = field(
        default=None, metadata={'json': 'inactive_threshold'}
    )
    """A duration which instructs the server to clean up the consumer if it has been inactive for the specified duration."""

    max_ack_pending: Optional[int] = field(
        default=None, metadata={'json': 'max_ack_pending'}
    )
    """A maximum number of outstanding unacknowledged messages for a consumer."""


@dataclass
class RawStreamMsg:
    subject: str = field(metadata={"json": "subject"})
    """ Subject of the message. """

    sequence: int = field(metadata={"json": "seq"})
    """ Sequence number of the message. """

    time: datetime = field(metadata={"json": "time"})
    """ Time of the message. """

    data: Optional[bytes] = field(default=None, metadata={"json": "data"})
    """ Data of the message."""

    headers: Optional[Dict[str, Any]] = field(
        default_factory=dict, metadata={"json": "hdrs"}
    )
    """ Headers of the message. """


@dataclass
class StoredMsg:
    subject: str = field(metadata={"json": "subject"})
    sequence: int = field(metadata={"json": "seq"})
    time: datetime = field(metadata={"json": "time"})
    headers: Optional[bytes] = field(default=None, metadata={"json": "hdrs"})
    data: Optional[bytes] = field(default=None, metadata={"json": "data"})


class Stream:
    """
    Stream contains operations on an existing stream. It allows fetching and removing
    messages from a stream, as well as purging a stream.
    """

    def __init__(self, client: Client, name: str, info: StreamInfo):
        self._client = client
        self._name = name
        self._info = info

    async def info(
        self,
        subject_filter: Optional[str] = None,
        deleted_details: Optional[bool] = None,
        timeout: Optional[float] = None
    ) -> StreamInfo:
        """Returns `StreamInfo` from the server."""
        # TODO(caspervonb): handle pagination
        info_subject = f"STREAM.INFO.{self._name}"
        info_request = StreamInfoRequest(
            subject_filter=subject_filter,
            deleted_details=deleted_details,
        )
        info_response = await self._client.request_json(
            info_subject, info_request, StreamInfoResponse, timeout=timeout
        )
        if info_response.error is not None:
            if info_response.error.error_code == ErrorCode.STREAM_NOT_FOUND:
                raise StreamNotFoundError(*info_response.error)

            raise Error(*info_response.error)

        return cast(StreamInfo, info_response)

    @property
    def cached_info(self) -> StreamInfo:
        """Returns the `StreamInfo` currently cached on this stream."""
        return self._info

    # TODO(caspervonb): Go does not return anything for this operation, should we?
    async def purge(
        self,
        sequence: Optional[int] = None,
        keep: Optional[int] = None,
        subject: Optional[str] = None,
        timeout: Optional[float] = None
    ) -> int:
        """
        Removes messages from a stream.
        This is a destructive operation.
        """

        # TODO(caspervonb): enforce types with overloads
        if keep is not None and sequence is not None:
            raise ValueError(
                "both 'keep' and 'sequence' cannot be provided in purge request"
            )

        purge_subject = f"STREAM.PURGE.{self._name}"
        purge_request = StreamPurgeRequest(
            sequence=sequence,
            keep=keep,
            subject=subject,
        )

        purge_response = await self._client.request_json(
            purge_subject, purge_request, StreamPurgeResponse, timeout=timeout
        )
        if purge_response.error is not None:
            raise Error(*purge_response.error)

        return purge_response.purged

    async def _get_msg(
        self,
        sequence: Optional[int] = None,
        next_by_subject: Optional[str] = None,
        last_by_subject: Optional[str] = None,
        timeout: Optional[float] = None,
    ) -> RawStreamMsg:
        msg_get_request = MsgGetRequest(
            sequence=sequence,
            last_by_subject=last_by_subject,
            next_by_subject=next_by_subject,
        )

        if self._info.config.allow_direct:
            if last_by_subject is not None:
                direct_get_subject = f"DIRECT.GET.{self._name}.{last_by_subject}"
                direct_get_request = b""
            else:
                direct_get_subject = f"DIRECT.GET.{sequence}"
                direct_get_request = msg_get_request.as_json().encode()

            direct_get_response = await self._client.request_msg(
                direct_get_subject, direct_get_request, timeout=timeout
            )

            headers = direct_get_response.headers
            if headers is None:
                raise Error('response should have headers')

            data = direct_get_response.data
            if len(data) == 0:
                status = headers.get("Status")
                if status == Status.NO_MESSAGES:
                    raise MsgNotFoundError()
                else:
                    description = headers.get(
                        "Description", "unable to get message"
                    )
                    raise Error(description=description)

            subject = headers.get(Header.SUBJECT)
            if subject is None:
                raise Error('missing subject header')

            sequence = headers.get(Header.SEQUENCE)
            if sequence is None:
                raise Error('missing sequence header')

            try:
                sequence = int(sequence)
            except ValueError as error:
                raise Error(f'invalid sequence header: {error}')

            time = headers.get(Header.TIMESTAMP)
            if time is None:
                raise Error(f'missing timestamp header')

            try:
                time = datetime.strptime(time, "%Y-%m-%dT%H:%M:%S.%fZ")
            except ValueError as error:
                raise ValueError(f'invalid timestamp header: {error}')

                return RawStreamMsg(
                    subject=subject,
                    sequence=sequence,
                    headers=headers,
                    data=data,
                    time=time,
                )

        msg_get_subject = "MSG.GET.{self._name}"
        msg_get_response = await self._client.request_json(
            msg_get_subject, msg_get_request, MsgGetResponse, timeout=timeout
        )

        if msg_get_response.error is not None:
            if msg_get_response.error.error_code == ErrorCode.MESSAGE_NOT_FOUND:
                raise MsgNotFoundError()

            raise Error(*msg_get_response.error)

        headers = None
        raw_headers = msg_get_response.msg.headers
        if raw_headers:
            # TODO(caspervonb): parse headers
            pass

        return RawStreamMsg(
            subject=msg_get_response.msg.subject,
            sequence=msg_get_response.msg.sequence,
            time=msg_get_response.msg.time,
            headers=headers,
        )

    async def get_msg(
        self,
        sequence: int,
        timeout: Optional[float] = None,
    ) -> RawStreamMsg:
        """
        Retrieves a raw stream message stored in JetStream by sequence number.
        """
        return await self._get_msg(sequence=sequence, timeout=timeout)

    async def get_last_msg_for_subject(
        self, subject: str, timeout: Optional[float] = None
    ) -> RawStreamMsg:
        """
        Retrieves the last raw stream message stored in JetStream on a given subject.
        """
        return await self._get_msg(last_by_subject=subject, timeout=timeout)

    async def _delete_msg(
        self, sequence: int, no_erase: bool, timeout: Optional[float]
    ):
        msg_delete_subject = f"STREAM.MSG.DELETE.{sequence}"
        msg_delete_request = MsgDeleteRequest(
            sequence=sequence,
            no_erase=no_erase,
        )

        msg_delete_response = await self._client.request_json(
            msg_delete_subject,
            msg_delete_request,
            MsgDeleteResponse,
            timeout=timeout
        )

        if msg_delete_response.error is not None:
            raise Error(*msg_delete_response.error)

    async def delete_msg(
        self, sequence: int, timeout: Optional[float] = None
    ) -> None:
        """
        Deletes a message from a stream.
        """
        await self._delete_msg(
            sequence=sequence, no_erase=True, timeout=timeout
        )

    async def secure_delete_msg(
        self, sequence: int, timeout: Optional[float] = None
    ) -> None:
        """
        Deletes a message from a stream.
        """
        await self._delete_msg(
            sequence=sequence, no_erase=False, timeout=timeout
        )


class StreamManager:
    """
    Provides methods for managing streams.
    """

    def __init__(self, client: Client) -> None:
        self._client = client

    async def create_stream(
        self, config: StreamConfig, timeout: Optional[float] = None
    ) -> Stream:
        """
        Creates a new stream with given config.
        """

        stream_create_subject = f"STREAM.CREATE"
        stream_create_request = StreamCreateRequest(**asdict(config))
        stream_create_response = await self._client.request_json(
            stream_create_subject,
            stream_create_request,
            StreamCreateResponse,
            timeout=timeout
        )

        if stream_create_response.error:
            if stream_create_response.error.error_code == ErrorCode.STREAM_NAME_IN_USE:
                raise StreamNameAlreadyInUseError(
                ) from stream_create_response.error

            raise Error(*stream_create_response.error)

        # Check if subject transforms are supported
        if config.subject_transform and not stream_create_response.config.subject_transform:
            raise StreamSubjectTransformNotSupportedError()

        # Check if sources and subject transforms are supported
        if config.sources:
            if not stream_create_response.config.sources:
                raise StreamSourceNotSupportedError()

            for i in range(len(config.sources)):
                source = config.sources[i]
                response_source = stream_create_response.config.sources[i]

                if source.subject_transforms and not response_source.subject_transforms:
                    raise StreamSourceMultipleFilterSubjectsNotSupported()

        return Stream(
            client=self._client,
            name=stream_create_response.config.name,
            info=cast(StreamInfo, stream_create_response),
        )


    async def update_stream(
        self, config: StreamConfig, timeout: Optional[float] = None
    ) -> Stream:
        """
        Updates an existing stream with the given config.
        """
        raise NotImplementedError

    async def create_or_update_stream(
        self, config: StreamConfig, timeout: Optional[float] = None
    ) -> Stream:
        """CreateOrUpdateStream creates a stream with given config or updates it if it already exists."""
        try:
            return await self.update_stream(config, timeout=timeout)
        except StreamNotFoundError:
            return await self.create_stream(config, timeout=timeout)

    async def stream(
        self, name: str, timeout: Optional[float] = None
    ) -> Stream:
        """Stream fetches StreamInfo and returns a Stream interface for a given stream name."""
        validate_stream_name(name)

        stream_info_subject = f"STREAM.INFO.{name}"
        stream_info_request = StreamInfoRequest()
        stream_info_response = await self._client.request_json(
            stream_info_subject,
            stream_info_request,
            StreamInfoResponse,
            timeout=timeout
        )

        if stream_info_response.error:
            if stream_info_response.error.error_code == ErrorCode.STREAM_NOT_FOUND:
                raise StreamNotFoundError()

            raise Error(*stream_info_response.error)

        return Stream(
            client=self._client,
            name=name,
            info=cast(StreamInfo, stream_info_response)
        )

    async def stream_name_by_subject(
        self, subject: str, timeout: Optional[float] = None
    ) -> str:
        """StreamNameBySubject returns a stream name listening on a given subject."""
        raise NotImplementedError

    async def delete_stream(
        self, stream: str, timeout: Optional[float] = None
    ) -> None:
        """DeleteStream removes a stream with given name."""
        validate_stream_name(stream)

        stream_delete_subject = f"STREAM.DELETE.{stream}"
        stream_delete_request = StreamDeleteRequest()
        stream_delete_response = await self._client.request_json(
            stream_delete_subject,
            stream_delete_request,
            StreamDeleteResponse,
            timeout=timeout
        )

        if stream_delete_response.error:
            if stream_delete_response.error.error_code == ErrorCode.STREAM_NOT_FOUND:
                raise StreamNotFoundError() from stream_delete_response.error

            raise Error(*stream_delete_response.error)

            return Stream(
                client=self._client,
                name=name,
                info=cast(StreamInfo, stream_delete_response)
            )

    def list_streams(self,
                     timeout: Optional[float] = None
                     ) -> AsyncIterator[StreamInfo]:
        """ListStreams returns a StreamInfoLister for iterating over stream infos."""
        raise NotImplementedError

    def stream_names(self,
                     timeout: Optional[float] = None) -> AsyncIterator[str]:
        """StreamNames returns a StreamNameLister for iterating over stream names."""
        raise NotImplementedError


class StreamInfoAsyncIterator:
    pass


class StreamInfoLister(AsyncIterable):
    "Provides asyncronous iteration over `StreamInfo`"
    pass


class StreamNameLister:
    pass


@dataclass
class StreamCreateRequest(Request, StreamConfig):
    pass

@dataclass
class StreamCreateResponse(Response, StreamInfo):
    pass

@dataclass
class StreamUpdateRequest(Request, StreamConfig):
    pass

@dataclass
class StreamUpdateResponse(Response, StreamInfo):
    pass

@dataclass
class StreamDeleteRequest(Request):
    pass


@dataclass
class StreamDeleteResponse(Response):
    pass


@dataclass
class StreamInfoRequest(Request, Paged):
    deleted_details: Optional[bool] = field(
        default=False, metadata={'json': 'deleted_details'}
    )
    subject_filter: Optional[str] = field(
        default=None, metadata={'json': 'subjects_filter'}
    )


@dataclass
class StreamInfoResponse(Response, Paged, StreamInfo):
    pass


@dataclass
class StreamPurgeRequest(Request):
    subject: Optional[str] = field(default=None, metadata={'json': 'filter'})
    sequence: Optional[int] = field(default=None, metadata={'json': 'seq'})
    keep: Optional[int] = field(default=None, metadata={'json': 'keep'})


@dataclass
class StreamPurgeResponse(Response):
    success: bool = field(default=False, metadata={'json': 'success'})
    purged: int = field(default=0, metadata={'json': 'purged'})


@dataclass
class MsgGetRequest(Request):
    sequence: Optional[int] = field(metadata={'json': 'seq'})
    last_by_subject: Optional[str] = field(metadata={'json': 'last_by_subj'})
    next_by_subject: Optional[str] = field(metadata={'json': 'next_by_subj'})


@dataclass
class MsgGetResponse(Response):
    msg: StoredMsg = field(init=False, metadata={'json': 'seq'})


@dataclass
class MsgDeleteRequest(Request):
    sequence: int = field(metadata={'json': 'seq'})
    no_erase: bool = field(metadata={'json': 'no_erase'})


@dataclass
class MsgDeleteResponse(Response):
    success: bool = field(default=False, metadata={'json': 'success'})


def validate_stream_name(stream_name: Optional[str]):
    if stream_name is None:
        raise ValueError("Stream name is required.")

    if stream_name == "":
        raise ValueError("Stream name cannot be empty.")

    if re.search(r'[>\*\./\\]', stream_name):
        raise ValueError(f"Invalid stream name: '{stream_name}'")


__all__ = [
    'RetentionPolicy',
    'DiscardPolicy',
    'StorageType',
    'StoreCompression',
    'StreamInfo',
    'StreamConfig',
    'StreamSourceInfo',
    'ClusterInfo',
    'PeerInfo',
    'SubjectTransformConfig',
    'Republish',
    'Placement',
    'StreamSource',
    'ExternalStream',
    'StreamConsumerLimits',
    'Stream',
    'StreamManager',
]
