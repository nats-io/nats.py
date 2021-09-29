import asyncio
from typing import TYPE_CHECKING, AsyncIterator, Awaitable, Callable, Optional

from nats.aio import defaults
from nats.aio.errors import (
    ErrConnectionClosed, ErrConnectionDraining, ErrTimeout, NatsError
)

if TYPE_CHECKING:
    from nats.aio.client import Client
    from nats.aio.messages import Msg


class Subscription:
    """
    A subscription represents interest in a particular subject.

    A subscription should not be constructed directly, rather
    `connection.subscribe()` should be used to get a subscription.
    """
    def __init__(
        self,
        conn: "Client",
        id: int = 0,
        subject: str = '',
        queue: str = '',
        cb: Optional[Callable[["Msg"], Awaitable[None]]] = None,
        future: Optional['asyncio.Future[Msg]'] = None,
        max_msgs: int = 0,
        pending_msgs_limit: int = defaults.SUB_PENDING_MSGS_LIMIT,
        pending_bytes_limit: int = defaults.SUB_PENDING_BYTES_LIMIT,
    ) -> None:
        self._conn = conn
        self._id = id
        self._subject = subject
        self._queue = queue
        self._max_msgs = max_msgs
        self._received = 0
        self._cb = cb
        self._future = future

        # Per subscription message processor.
        self._pending_msgs_limit = pending_msgs_limit
        self._pending_bytes_limit = pending_bytes_limit
        self._pending_queue: asyncio.Queue["Msg"] = asyncio.Queue(
            maxsize=pending_msgs_limit
        )
        self._pending_size = 0
        self._wait_for_msgs_task: Optional[asyncio.Task[None]] = None
        self._message_iterator: Optional[_SubscriptionMessageIterator] = None

    @property
    def messages(self) -> AsyncIterator['Msg']:
        """
        Retrieves an async iterator for the messages from the subscription.

        This is only available if a callback isn't provided when creating a
        subscription.
        """
        if not self._message_iterator:
            raise NatsError(
                "cannot iterate over messages with a non iteration subscription type"
            )

        return self._message_iterator

    async def next_msg(self, timeout: Optional[float] = 1.0) -> "Msg":
        """
        next_msg can be used to retrieve the next message
        from a stream of messages using await syntax.
        """
        future: asyncio.Future["Msg"] = asyncio.Future()

        async def _next_msg() -> None:
            msg = await self._pending_queue.get()
            future.set_result(msg)

        task = asyncio.create_task(_next_msg())
        try:
            msg = await asyncio.wait_for(future, timeout)
            return msg
        except asyncio.TimeoutError:
            future.cancel()
            task.cancel()
            raise ErrTimeout

    def _start(self, error_cb: Callable[[Exception], Awaitable[None]]) -> None:
        """
        Creates the resources for the subscription to start processing messages.
        """
        if self._cb:
            if not asyncio.iscoroutinefunction(self._cb) and \
                not (hasattr(self._cb, "func") and \
                    asyncio.iscoroutinefunction(self._cb.func)): # type: ignore[attr-defined]
                raise NatsError("nats: must use coroutine for subscriptions")

            self._wait_for_msgs_task = asyncio.create_task(
                self._wait_for_msgs(error_cb)
            )

        elif self._future:
            # Used to handle the single response from a request.
            pass
        else:
            self._message_iterator = _SubscriptionMessageIterator(
                self._pending_queue
            )

    async def drain(self) -> None:
        """
        Removes interest in a subject, but will process remaining messages.
        """
        try:
            # Announce server that no longer want to receive more
            # messages in this sub and just process the ones remaining.
            await self._conn._send_unsubscribe(self._id)

            # Roundtrip to ensure that the server has sent all messages.
            await self._conn.flush()

            if self._pending_queue:
                # Wait until no more messages are left,
                # then cancel the subscription task.
                await self._pending_queue.join()

            # stop waiting for messages
            self._stop_processing()

            # Subscription is done and won't be receiving further
            # messages so can throw it away now.
            self._conn._remove_sub(self._id)
        except asyncio.CancelledError:
            # In case draining of a connection times out then
            # the sub per task will be canceled as well.
            pass

    async def unsubscribe(self, limit: int = 0) -> None:
        """
        Removes interest in a subject, remaining messages will be discarded.

        If `limit` is greater than zero, interest is not immediately removed,
        rather, interest will be automatically removed after `limit` messages
        are received.
        """
        if self._conn.is_closed:
            raise ErrConnectionClosed
        if self._conn.is_draining:
            raise ErrConnectionDraining

        self._max_msgs = limit
        if limit == 0 or self._received >= limit:
            self._stop_processing()
            self._conn._remove_sub(self._id)

        if not self._conn.is_reconnecting:
            await self._conn._send_unsubscribe(self._id, limit=limit)

    def _stop_processing(self):
        """
        Stops the subscription from processing new messages.
        """
        if self._wait_for_msgs_task and not self._wait_for_msgs_task.done():
            self._wait_for_msgs_task.cancel()
        if self._message_iterator:
            self._message_iterator._cancel()

    async def _wait_for_msgs(
        self, error_cb: Callable[[Exception], Awaitable[None]]
    ):
        """
        A coroutine to read and process messages if a callback is provided.

        Should be called as a task.
        """
        while True:
            try:
                msg = await self._pending_queue.get()
                self._pending_size -= len(msg.data)

                try:
                    # Invoke depending of type of handler.
                    await self._cb(msg)  # type: ignore[misc]
                except asyncio.CancelledError:
                    # In case the coroutine handler gets cancelled
                    # then stop task loop and return.
                    break
                except Exception as e:
                    # All errors from calling a handler
                    # are async errors.
                    if error_cb:
                        await error_cb(e)
                finally:
                    # indicate the message finished processing so drain can continue
                    self._pending_queue.task_done()

            except asyncio.CancelledError:
                break


class _SubscriptionMessageIterator:
    def __init__(self, queue) -> None:
        self._queue: asyncio.Queue["Msg"] = queue
        self._unsubscribed_future: asyncio.Future[bool] = asyncio.Future()

    def _cancel(self) -> None:
        if not self._unsubscribed_future.done():
            self._unsubscribed_future.set_result(True)

    def __aiter__(self) -> "_SubscriptionMessageIterator":
        return self

    async def __anext__(self) -> "Msg":
        get_task = asyncio.create_task(self._queue.get())
        finished, _ = await asyncio.wait(
            [get_task, self._unsubscribed_future],  # type: ignore[type-var]
            return_when=asyncio.FIRST_COMPLETED
        )
        if get_task in finished:
            self._queue.task_done()
            return get_task.result()
        elif self._unsubscribed_future.done():
            get_task.cancel()

        raise StopAsyncIteration
