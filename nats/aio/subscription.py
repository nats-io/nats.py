# Copyright 2016-2021 The NATS Authors
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

# Default Pending Limits of Subscriptions
DEFAULT_SUB_PENDING_MSGS_LIMIT = 512 * 1024
DEFAULT_SUB_PENDING_BYTES_LIMIT = 128 * 1024 * 1024

import asyncio
from typing import AsyncIterator, Awaitable, Callable, List, Optional, Union, Tuple
from nats.aio.errors import *
from nats.errors import *
from nats.aio.msg import Msg


class Subscription:
    """
    A Subscription represents interest in a particular subject.

    A Subscription should not be constructed directly, rather
    `connection.subscribe()` should be used to get a subscription.

    ::

        nc = await nats.connect()

        # Async Subscription
        async def cb(msg):
          print('Received', msg)
        await nc.subscribe('foo', cb=cb)

        # Sync Subscription
        sub = nc.subscribe('foo')
        msg = await sub.next_msg()
        print('Received', msg)

    """
    def __init__(
        self,
        conn,
        id: int = 0,
        subject: str = '',
        queue: str = '',
        cb: Optional[Callable[['Msg'], None]] = None,
        future: Optional[asyncio.Future] = None,
        max_msgs: int = 0,
        pending_msgs_limit: int = DEFAULT_SUB_PENDING_MSGS_LIMIT,
        pending_bytes_limit: int = DEFAULT_SUB_PENDING_BYTES_LIMIT,
    ):
        self._conn = conn
        self._id = id
        self._subject = subject
        self._queue = queue
        self._max_msgs = max_msgs
        self._received = 0
        self._cb = cb
        self._future = future
        self._closed = False

        # Per subscription message processor.
        self._pending_msgs_limit = pending_msgs_limit
        self._pending_bytes_limit = pending_bytes_limit
        self._pending_queue = asyncio.Queue(maxsize=pending_msgs_limit)
        self._pending_size = 0
        self._wait_for_msgs_task = None
        self._message_iterator = None

        # For JetStream enabled subscriptions.
        self._jsi = None

    @property
    def subject(self) -> str:
        """
        Returns the subject of the `Subscription`.
        """
        return self._subject

    @property
    def queue(self) -> str:
        """
        Returns the queue name of the `Subscription` if part of a queue group.
        """
        return self._queue

    @property
    def messages(self) -> AsyncIterator['Msg']:
        """
        Retrieves an async iterator for the messages from the subscription.

        This is only available if a callback isn't provided when creating a
        subscription.
        """
        if not self._message_iterator:
            raise Error(
                "cannot iterate over messages with a non iteration subscription type"
            )

        return self._message_iterator

    @property
    def pending_msgs(self) -> int:
        """
        Number of delivered messages by the NATS Server that are being buffered
        in the pending queue.
        """
        return self._pending_queue.qsize()

    @property
    def delivered(self) -> int:
        """
        Number of delivered messages to this subscription so far.
        """
        return self._received

    async def next_msg(self, timeout: float = 1.0):
        """
        :params timeout: Time to wait for next message before
        :raises nats.errors.TimeoutError:

        next_msg can be used to retrieve the next message
        from a stream of messages using await syntax, this
        only works when not passing a callback on `subscribe`::

            sub = await nc.subscribe('hello')
            msg = await sub.next_msg(timeout=1)

        """
        future = asyncio.Future()

        async def _next_msg():
            msg = await self._pending_queue.get()
            future.set_result(msg)

        task = asyncio.get_running_loop().create_task(_next_msg())
        try:
            msg = await asyncio.wait_for(future, timeout)
            return msg
        except asyncio.TimeoutError:
            future.cancel()
            task.cancel()
            raise TimeoutError

    def _start(self, error_cb):
        """
        Creates the resources for the subscription to start processing messages.
        """
        if self._cb:
            if not asyncio.iscoroutinefunction(self._cb) and \
                not (hasattr(self._cb, "func") and asyncio.iscoroutinefunction(self._cb.func)):
                raise Error("nats: must use coroutine for subscriptions")

            self._wait_for_msgs_task = asyncio.get_running_loop().create_task(
                self._wait_for_msgs(error_cb)
            )

        elif self._future:
            # Used to handle the single response from a request.
            pass
        else:
            self._message_iterator = _SubscriptionMessageIterator(
                self._pending_queue
            )

    async def drain(self):
        """
        Removes interest in a subject, but will process remaining messages.
        """
        if self._conn.is_closed:
            raise ConnectionClosedError
        if self._conn.is_draining:
            raise ConnectionDrainingError
        if self._closed:
            raise BadSubscriptionError
        await self._drain()

    async def _drain(self):
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
        finally:
            self._closed = True

    async def unsubscribe(self, limit: int = 0):
        """
        :param limit: Max number of messages to receive before unsubscribing.

        Removes interest in a subject, remaining messages will be discarded.

        If `limit` is greater than zero, interest is not immediately removed,
        rather, interest will be automatically removed after `limit` messages
        are received.
        """
        if self._conn.is_closed:
            raise ConnectionClosedError
        if self._conn.is_draining:
            raise ConnectionDrainingError
        if self._closed:
            raise BadSubscriptionError

        self._max_msgs = limit
        if limit == 0 or self._received >= limit:
            self._closed = True
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

    async def _wait_for_msgs(self, error_cb):
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
                    await self._cb(msg)
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
    def __init__(self, queue):
        self._queue = queue
        self._unsubscribed_future = asyncio.Future()

    def _cancel(self):
        if not self._unsubscribed_future.done():
            self._unsubscribed_future.set_result(True)

    def __aiter__(self):
        return self

    async def __anext__(self):
        get_task = asyncio.get_running_loop().create_task(self._queue.get())
        finished, _ = await asyncio.wait([get_task, self._unsubscribed_future],
                                         return_when=asyncio.FIRST_COMPLETED)
        if get_task in finished:
            self._queue.task_done()
            return get_task.result()
        elif self._unsubscribed_future.done():
            get_task.cancel()

        raise StopAsyncIteration
