import json
from dataclasses import asdict
from typing import TYPE_CHECKING, Any, AsyncGenerator, Optional

from nats.aio.errors import JetStreamAPIError
from nats.aio.js.models.consumers import (
    AckPolicy, ConsumerConfig, ConsumerCreateRequest, ConsumerCreateResponse,
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
        /,
        timeout: float = 1,
    ) -> ConsumerInfoResponse:
        return await self._js._request(
            f"CONSUMER.INFO.{stream}.{name}",
            None,
            ConsumerInfoResponse,
            timeout=timeout,
        )

    async def list(
        self,
        stream: str,
        /,
        offset: int = 0,
        timeout: float = 1,
    ) -> ConsumerListResponse:
        options = ConsumerListRequest(offset=offset)
        return await self._js._request(
            f"CONSUMER.LIST.{stream}",
            asdict(options),
            ConsumerListResponse,
            timeout=timeout,
        )

    async def names(
        self,
        stream: str,
        /,
        offset: int = 0,
        subject: Optional[str] = None,
        timeout: float = 1,
    ) -> ConsumerNamesResponse:
        options = ConsumerNamesRequest(offset=offset, subject=subject)
        return await self._js._request(
            f"CONSUMER.NAMES.{stream}",
            asdict(options),
            ConsumerNamesResponse,
            timeout=timeout,
        )

    async def add(
        self,
        stream: str,
        name: Optional[str] = None,
        /,
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
        config = ConsumerConfig(
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
        options = ConsumerCreateRequest(stream_name=stream, config=config)
        subject = f"CONSUMER.DURABLE.CREATE.{stream}.{name}" if name else f"CONSUMER.CREATE.{stream}"
        return await self._js._request(
            subject,
            asdict(options),
            ConsumerCreateResponse,
            timeout=timeout,
        )

    async def delete(
        self,
        stream: str,
        name: str,
        timeout: float = 1,
    ) -> ConsumerDeleteResponse:
        return await self._js._request(
            f"CONSUMER.DELETE.{stream}.{name}",
            None,
            ConsumerDeleteResponse,
            timeout=timeout,
        )

    async def pull_msgs(
        self,
        name: str,
        /,
        stream: Optional[str] = None,
        subject: Optional[str] = None,
        no_wait: bool = False,
        timeout: Optional[float] = None,
        auto_ack: bool = True,
        max_msgs: Optional[int] = None,
        **kwargs: Any,
    ) -> AsyncGenerator["Msg", None]:
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
        /,
        stream: Optional[str] = None,
        subject: Optional[str] = None,
        no_wait: bool = False,
        timeout: Optional[float] = None,
        auto_ack: bool = True,
        **kwargs,
    ) -> "Msg":
        """Wait and return next message by default. If no_wait is True and no message is available, None is returned."""
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
