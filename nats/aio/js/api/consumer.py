import json
from dataclasses import asdict
from typing import TYPE_CHECKING, Any, AsyncGenerator, Optional

from nats.aio.errors import JetStreamAPIError
from nats.aio.js.models.consumers import (
    AckPolicy, ConsumerCreateRequest, ConsumerCreateResponse,
    ConsumerDeleteResponse, ConsumerGetNextRequest, ConsumerInfoResponse,
    ConsumerListRequest, ConsumerListResponse, ConsumerNamesRequest,
    ConsumerNamesResponse, DeliverPolicy, ReplayPolicy
)

from .utils import check_js_msg

if TYPE_CHECKING:
    from nats.aio.js.client import JetStream  # pragma: no cover
    from nats.aio.messages import Msg  # pragma: no cover


class ConsumerAPI:
    def __init__(self, js: "JetStream") -> None:
        self._js = js

    async def info(
        self,
        stream: str,
        name: str,
        timeout: float = 1,
    ) -> ConsumerInfoResponse:
        """Get consumer info.

        Args:
            * `stream`: Name of the stream
            * `name`: Name of the durable consumer
            * `timeout`: timeout to wait before raising a TimeoutError.
    
        Returns:
            A `ConsumerInfoResponse`.
        """
        return await self._js._request(
            f"CONSUMER.INFO.{stream}.{name}",
            timeout=timeout,
            response_dc=ConsumerInfoResponse,
        )

    async def list(
        self,
        stream: str,
        offset: int = 0,
        timeout: float = 1,
    ) -> ConsumerListResponse:
        """List existing consumers for a stream.

        Args:
            * `stream`: Name of the stream.
            * `offset`: Number of consumers to skip.
            * `timeout`: timeout to wait before raising a TimeoutError.
    
        Returns:
            A `ConsumerListResponse`. List of consumer info is available under `.consumers` attribute.
        """
        return await self._js._request(
            f"CONSUMER.LIST.{stream}",
            {"offset": offset},
            timeout=timeout,
            request_dc=ConsumerListRequest,
            response_dc=ConsumerListResponse,
        )

    async def names(
        self,
        stream: str,
        offset: int = 0,
        subject: Optional[str] = None,
        timeout: float = 1,
    ) -> ConsumerNamesResponse:
        """List existing consumers for a stream.

        It's possible to list consumers listenning on a specific subject.

        Args:
            * `stream`: Name of the stream
            * `offset`: Number of consumers to skip
            * `subject`: Return names of consumers listenning on given subjet. Can be a wildcard.
            * `timeout`: timeout to wait before raising a TimeoutError.

        Returns:
            A `ConsumerNamesResponse`. List of consumer names is available under `.consumers` attribute.
        """
        return await self._js._request(
            f"CONSUMER.NAMES.{stream}",
            {
                "offset": offset,
                "subject": subject
            },
            timeout=timeout,
            request_dc=ConsumerNamesRequest,
            response_dc=ConsumerNamesResponse,
        )

    async def add(
        self,
        stream: str,
        name: Optional[str] = None,
        deliver_subject: Optional[str] = None,
        deliver_group: Optional[str] = None,
        deliver_policy: DeliverPolicy = DeliverPolicy.last,
        replay_policy: ReplayPolicy = ReplayPolicy.instant,
        ack_policy: AckPolicy = AckPolicy.explicit,
        ack_wait: Optional[int] = None,
        max_deliver: int = -1,
        filter_subject: Optional[str] = None,
        sample_freq: Optional[str] = None,
        rate_limit_bps: Optional[int] = None,
        max_ack_pending: Optional[int] = None,
        idle_heartbeat: Optional[int] = None,
        flow_control: Optional[bool] = None,
        max_waiting: Optional[int] = None,
        timeout: float = 1,
    ) -> ConsumerCreateResponse:
        """Create a new consumer.

        If a name is given, a durable consumer will be created,
        else an ephemeral consumer will be created.

        Args:
            * `stream`: Name of the stream
            * `name`: Name of durable consumer. Must be empty for an epheral consumer.
            * `deliver_subject`: Subject on which messages will be delivered. Use None for pull consumers. Value cannot be None when consumer is ephemeral.
            * `deliver_group`:
            * `deliver_policy`: One of "all", "last", "new", "last_per_subject", "by_start_sequence", "by_start_time".
            * `replay_policy`: One of "instant", "original". Applies when deliver_policy is one of "all", "by_start_sequence", "by_start_time".
            * `ack_policy`: One of None, "all", "explicit". Defines how messages should be acknowledged

        Returns:
            A `ConsumerCreateResponse`
        """
        config = dict(
            durable_name=name,
            deliver_subject=deliver_subject,
            deliver_group=deliver_group,
            deliver_policy=deliver_policy,
            ack_policy=ack_policy,
            ack_wait=ack_wait,
            max_deliver=max_deliver,
            filter_subject=filter_subject,
            replay_policy=replay_policy,
            sample_freq=sample_freq,
            rate_limit_bps=rate_limit_bps,
            max_ack_pending=max_ack_pending,
            idle_heartbeat=idle_heartbeat,
            flow_control=flow_control,
            max_waiting=max_waiting,
        )
        subject = f"CONSUMER.DURABLE.CREATE.{stream}.{name}" if name else f"CONSUMER.CREATE.{stream}"
        return await self._js._request(
            subject,
            {
                "stream_name": stream,
                "config": config
            },
            timeout=timeout,
            request_dc=ConsumerCreateRequest,
            response_dc=ConsumerCreateResponse,
        )

    async def delete(
        self,
        stream: str,
        name: str,
        timeout: float = 1,
    ) -> ConsumerDeleteResponse:
        """Delete an existing durable consumer.

        Args:
            * `stream`: Name of the stream.
            * `name`: Name of durable consumer.
            * `timeout`: timeout to wait before raising a TimeoutError.
        """
        return await self._js._request(
            f"CONSUMER.DELETE.{stream}.{name}",
            timeout=timeout,
            response_dc=ConsumerDeleteResponse,
        )

    async def pull_msgs(
        self,
        name: str,
        stream: Optional[str] = None,
        subject: Optional[str] = None,
        no_wait: bool = False,
        timeout: Optional[float] = None,
        auto_ack: bool = True,
        max_msgs: Optional[int] = None,
        **kwargs: Any,
    ) -> AsyncGenerator["Msg", None]:
        """Asynchronously iterate over messages from a pull consumer.

        * When `stream` is set, `subject` is ignored.
        * When `stream` is None and `subject` is set, first stream found listening on subject will be used.
        * If stream is not found, a `JetStreamAPIError` with code 404 is raised.
        * If `no_wait` is True and no messages are found in stream, a `JetStreamAPIError` with code 404 is raised.
        * If `no_wait` is False, iterator waits for next message for `timeout` seconds.
        * If `timeout` is None, iterator waits for next message forever.
        * If `auto_ack` is True, messages will be acknowledged before being yielded by iterator. If False, messages might need to be acked manually.
        * If `max_msgs` is set, iterator will break after having received `max_msgs` messages

        Example usage:
            >>> async for msg in js.pull_msgs("STREAM", subject="foo", auto_ack=False):
            >>>     await msg.ack_sync()
        """
        if stream is None:
            stream_names = await self._js.stream.names(subject=subject)
            try:
                stream = stream_names.streams[0]
            except IndexError:
                raise JetStreamAPIError(
                    code="404", description="stream not found"
                )
        try:
            con_info = await self.info(stream, name)
        except JetStreamAPIError as err:

            if err.code and (int(err.code) != 404
                             or err.description != "consumer not found"):
                raise
            await self.add(
                stream,
                name,
                deliver_subject=None,
                filter_subject=subject,
                **kwargs
            )
        else:
            if con_info.config.deliver_subject is not None:
                raise JetStreamAPIError(
                    code=500,
                    description="cannot pull messages from push consumer"
                )
        # At this point we're sure consumer exists
        inbox: str = self._js._nc._nuid.next().decode("utf-8")
        total: int = 0
        subscription = await self._js._nc.subscribe(inbox)
        # Stop subscription on error
        try:
            while True:
                # Generate payload
                payload = (
                    json.dumps(
                        asdict(
                            ConsumerGetNextRequest(
                                batch=1,
                                expires=int(timeout *
                                            1e9) if timeout else None,
                                no_wait=no_wait if no_wait else None,
                            )
                        )
                    ).encode()
                )
                # Request next message to be published on inbox subject
                # await self._nc.publish_request(
                await self._js._nc.publish(
                    f"$JS.API.CONSUMER.MSG.NEXT.{stream}.{name}",
                    payload=payload,
                    reply=inbox,
                )
                # Wait for next message on inbox subscription
                msg = await subscription.next_msg(timeout=timeout)
                # Check headers
                check_js_msg(msg)
                # Increment message counter
                total += 1
                # Optionally acknowledge the message
                if auto_ack:
                    await msg.ack_sync()
                # Yield the message
                yield msg
                # Stop subscription if maximum number of message has been received
                if max_msgs and (max_msgs <= total):
                    break
        # Always stop the subscription on exit
        finally:
            await subscription.unsubscribe()

    async def pull_next(
        self,
        name: str,
        stream: Optional[str] = None,
        subject: Optional[str] = None,
        no_wait: bool = False,
        timeout: Optional[float] = None,
        auto_ack: bool = True,
        **kwargs,
    ) -> "Msg":
        """Get next consumer message.

        If no consumer exists with name `name` but a stream is found, consumer is created
        automatically with `**kwargs` used as consumer config.

        By default function waits until returning next message.

        * When `stream` is set, `subject` is ignored.
        * When `stream` is None and `subject` is set, first stream found listening on subject will be used.
        * If stream is not found, a `JetStreamAPIError` with code 404 is raised.
        * If `no_wait` is True and no message is available, a `JetStreamAPIError` with code 404 is raised.
        * If `timeout` is set and no message is received before `timeout` seconds, a `TimeoutError` is raised.
        * If `auto_ack` is True, messages are acknowledged automatically. If False messages might need to be acknowledged manually.

        Example usage:
            >>> msg = await js.consumer.next_msg("CON", subject="foo")
        """
        # Wait for consumer next message
        async for msg in self.pull_msgs(
            name,
            stream=stream,
            subject=subject,
            timeout=timeout,
            auto_ack=auto_ack,
            no_wait=no_wait,
            **kwargs,
        ):
            # Return on first message
            return msg
        # This code should never be reached, because either an error is raised or a message is yielded by pull_msgs method.
        raise JetStreamAPIError(  # pragma: no cover
            code="500", description="Something's wrong"
        )
