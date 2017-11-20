import sys
import time
import json
import ssl
import asyncio
import unittest
import http.client

from nats.aio.client import __version__
from nats.aio.client import Client as NATS
from nats.aio.utils import new_inbox, INBOX_PREFIX
from nats.aio.errors import ErrConnectionClosed, ErrNoServers, ErrTimeout, \
    ErrBadSubject, NatsError
from tests.utils import async_test, start_gnatsd, NatsTestCase, \
    SingleServerTestCase, MultiServerAuthTestCase, MultiServerAuthTokenTestCase, TLSServerTestCase, \
    MultiTLSServerAuthTestCase, ClusteringTestCase

class ClientUtilsTest(NatsTestCase):

    def test_default_connect_command(self):
        nc = NATS()
        nc.options["verbose"] = False
        nc.options["pedantic"] = False
        nc.options["auth_required"] = False
        nc.options["name"] = None
        got = nc._connect_command()
        expected = 'CONNECT {"lang": "python3", "pedantic": false, "protocol": 1, "verbose": false, "version": "%s"}\r\n' % __version__
        self.assertEqual(expected.encode(), got)

    def test_default_connect_command_with_name(self):
        nc = NATS()
        nc.options["verbose"] = False
        nc.options["pedantic"] = False
        nc.options["auth_required"] = False
        nc.options["name"] = "secret"
        got = nc._connect_command()
        expected = 'CONNECT {"lang": "python3", "name": "secret", "pedantic": false, "protocol": 1, "verbose": false, "version": "%s"}\r\n' % __version__
        self.assertEqual(expected.encode(), got)

    def tests_generate_new_inbox(self):
        inbox = new_inbox()
        self.assertTrue(inbox.startswith(INBOX_PREFIX))
        min_expected_len = len(INBOX_PREFIX)
        self.assertTrue(len(inbox) > min_expected_len)


class ClientTest(SingleServerTestCase):

    @async_test
    def test_default_connect(self):
        nc = NATS()
        yield from nc.connect(io_loop=self.loop)
        self.assertIn('auth_required', nc._server_info)
        self.assertIn('max_payload', nc._server_info)
        self.assertEqual(nc._server_info['max_payload'], nc.max_payload)
        self.assertTrue(nc.max_payload > 0)
        self.assertTrue(nc.is_connected)
        yield from nc.close()
        self.assertTrue(nc.is_closed)
        self.assertFalse(nc.is_connected)

    @async_test
    def test_connect_no_servers_on_connect_init(self):
        nc = NATS()
        with self.assertRaises(ErrNoServers):
            yield from nc.connect(io_loop=self.loop, servers=["nats://127.0.0.1:4221"])

    @async_test
    def test_publish(self):
        nc = NATS()
        yield from nc.connect(io_loop=self.loop)
        for i in range(0, 100):
            yield from nc.publish("hello.%d" % i, b'A')

        with self.assertRaises(ErrBadSubject):
            yield from nc.publish("", b'')

        yield from nc.flush()
        yield from nc.close()
        yield from asyncio.sleep(1, loop=self.loop)
        self.assertEqual(100, nc.stats['out_msgs'])
        self.assertEqual(100, nc.stats['out_bytes'])

        endpoint = '127.0.0.1:{port}'.format(
            port=self.server_pool[0].http_port)
        httpclient = http.client.HTTPConnection(endpoint, timeout=5)
        httpclient.request('GET', '/varz')
        response = httpclient.getresponse()
        varz = json.loads((response.read()).decode())
        self.assertEqual(100, varz['in_msgs'])
        self.assertEqual(100, varz['in_bytes'])

    @async_test
    def test_flush(self):
        nc = NATS()
        yield from nc.connect(io_loop=self.loop)
        for i in range(0, 10):
            yield from nc.publish("flush.%d" % i, b'AA')
            yield from nc.flush()
        self.assertEqual(10, nc.stats['out_msgs'])
        self.assertEqual(20, nc.stats['out_bytes'])
        yield from nc.close()

    @async_test
    def test_subscribe(self):
        nc = NATS()
        msgs = []

        @asyncio.coroutine
        def subscription_handler(msg):
            msgs.append(msg)

        payload = b'hello world'
        yield from nc.connect(io_loop=self.loop)
        sid = yield from nc.subscribe("foo", cb=subscription_handler)
        yield from nc.publish("foo", payload)
        yield from nc.publish("bar", payload)

        with self.assertRaises(ErrBadSubject):
            yield from nc.publish("", b'')

        # Wait a bit for message to be received.
        yield from asyncio.sleep(0.2, loop=self.loop)

        self.assertEqual(1, len(msgs))
        msg = msgs[0]
        self.assertEqual('foo', msg.subject)
        self.assertEqual('', msg.reply)
        self.assertEqual(payload, msg.data)
        self.assertEqual(1, nc._subs[sid].received)
        yield from nc.close()

        # After close, the subscription is gone
        with self.assertRaises(KeyError):
            nc._subs[sid]

        self.assertEqual(1,  nc.stats['in_msgs'])
        self.assertEqual(11, nc.stats['in_bytes'])
        self.assertEqual(2,  nc.stats['out_msgs'])
        self.assertEqual(22, nc.stats['out_bytes'])

        endpoint = '127.0.0.1:{port}'.format(
            port=self.server_pool[0].http_port)
        httpclient = http.client.HTTPConnection(endpoint, timeout=5)
        httpclient.request('GET', '/connz')
        response = httpclient.getresponse()
        connz = json.loads((response.read()).decode())
        self.assertEqual(1, len(connz['connections']))
        self.assertEqual(2,  connz['connections'][0]['in_msgs'])
        self.assertEqual(22, connz['connections'][0]['in_bytes'])
        self.assertEqual(1,  connz['connections'][0]['out_msgs'])
        self.assertEqual(11, connz['connections'][0]['out_bytes'])

    @async_test
    def test_invalid_subscribe_error(self):
        nc = NATS()
        msgs = []
        future_error = asyncio.Future(loop=self.loop)

        @asyncio.coroutine
        def subscription_handler(msg):
            msgs.append(msg)

        @asyncio.coroutine
        def closed_cb():
            nonlocal future_error
            future_error.set_result(nc.last_error)

        yield from nc.connect(io_loop=self.loop, closed_cb=closed_cb)
        yield from nc.subscribe("foo.", cb=subscription_handler)
        yield from asyncio.wait_for(future_error, 1.0, loop=self.loop)
        nats_error = future_error.result()
        self.assertEqual(type(nats_error), NatsError)
        self.assertEqual(str(nats_error), "nats: 'Invalid Subject'")

    @async_test
    def test_subscribe_async(self):
        nc = NATS()
        msgs = []

        @asyncio.coroutine
        def subscription_handler(msg):
            if msg.subject == "tests.1":
                yield from asyncio.sleep(0.5, loop=self.loop)
            if msg.subject == "tests.3":
                yield from asyncio.sleep(0.2, loop=self.loop)
            msgs.append(msg)

        yield from nc.connect(io_loop=self.loop)
        sid = yield from nc.subscribe_async("tests.>", cb=subscription_handler)

        for i in range(0, 5):
            yield from nc.publish("tests.{}".format(i), b'bar')

        # Wait a bit for messages to be received.
        yield from asyncio.sleep(1, loop=self.loop)
        self.assertEqual(5, len(msgs))
        self.assertEqual("tests.1", msgs[4].subject)
        self.assertEqual("tests.3", msgs[3].subject)
        yield from nc.close()

    @async_test
    def test_subscribe_sync(self):
        nc = NATS()
        msgs = []

        @asyncio.coroutine
        def subscription_handler(msg):
            if msg.subject == "tests.1":
                yield from asyncio.sleep(0.5, loop=self.loop)
            if msg.subject == "tests.3":
                yield from asyncio.sleep(0.2, loop=self.loop)
            msgs.append(msg)

        yield from nc.connect(io_loop=self.loop)
        sid = yield from nc.subscribe("tests.>", cb=subscription_handler)

        for i in range(0, 5):
            yield from nc.publish("tests.{}".format(i), b'bar')

        # Wait a bit for messages to be received.
        yield from asyncio.sleep(1, loop=self.loop)
        self.assertEqual(5, len(msgs))
        self.assertEqual("tests.1", msgs[1].subject)
        self.assertEqual("tests.3", msgs[3].subject)
        yield from nc.close()

    @async_test
    def test_subscribe_sync_call_soon(self):
        nc = NATS()
        msgs = []

        def subscription_handler(msg):
            msgs.append(msg)

        yield from nc.connect(io_loop=self.loop)
        sid = yield from nc.subscribe("tests.>", cb=subscription_handler)

        for i in range(0, 5):
            yield from nc.publish("tests.{}".format(i), b'bar')

        # Wait a bit for messages to be received.
        yield from asyncio.sleep(1, loop=self.loop)
        self.assertEqual(5, len(msgs))

        # Check that they were received sequentially.
        self.assertEqual("tests.1", msgs[1].subject)
        self.assertEqual("tests.3", msgs[3].subject)
        yield from nc.close()

    @async_test
    def test_subscribe_async_without_coroutine_unsupported(self):
        nc = NATS()
        msgs = []

        def subscription_handler(msg):
            if msg.subject == "tests.1":
                time.sleep(0.5)
            if msg.subject == "tests.3":
                time.sleep(0.2)
            msgs.append(msg)

        yield from nc.connect(io_loop=self.loop)

        with self.assertRaises(NatsError):
            sid = yield from nc.subscribe_async("tests.>", cb=subscription_handler)
        yield from nc.close()

    @async_test
    def test_invalid_subscription_type(self):
        nc = NATS()

        with self.assertRaises(NatsError):
            yield from nc.subscribe("hello", cb=None, future=None)

        with self.assertRaises(NatsError):
            yield from nc.subscribe_async("hello", cb=None)

    @async_test
    def test_unsubscribe(self):
        nc = NATS()
        msgs = []

        @asyncio.coroutine
        def subscription_handler(msg):
            msgs.append(msg)

        yield from nc.connect(io_loop=self.loop)
        sid = yield from nc.subscribe("foo", cb=subscription_handler)
        yield from nc.publish("foo", b'A')
        yield from nc.publish("foo", b'B')

        # Wait a bit to receive the messages
        yield from asyncio.sleep(0.5, loop=self.loop)
        self.assertEqual(2, len(msgs))
        yield from nc.unsubscribe(sid)
        yield from nc.publish("foo", b'C')
        yield from nc.publish("foo", b'D')

        # Ordering should be preserverd in these at least
        self.assertEqual(b'A', msgs[0].data)
        self.assertEqual(b'B', msgs[1].data)

        # Should not exist by now
        with self.assertRaises(KeyError):
            nc._subs[sid].received

        yield from asyncio.sleep(1, loop=self.loop)
        endpoint = '127.0.0.1:{port}'.format(
            port=self.server_pool[0].http_port)
        httpclient = http.client.HTTPConnection(endpoint, timeout=5)
        httpclient.request('GET', '/connz')
        response = httpclient.getresponse()
        connz = json.loads((response.read()).decode())
        self.assertEqual(1, len(connz['connections']))
        self.assertEqual(0,  connz['connections'][0]['subscriptions'])
        self.assertEqual(4,  connz['connections'][0]['in_msgs'])
        self.assertEqual(4,  connz['connections'][0]['in_bytes'])
        self.assertEqual(2,  connz['connections'][0]['out_msgs'])
        self.assertEqual(2,  connz['connections'][0]['out_bytes'])

        yield from nc.close()
        self.assertEqual(2, nc.stats['in_msgs'])
        self.assertEqual(2, nc.stats['in_bytes'])
        self.assertEqual(4, nc.stats['out_msgs'])
        self.assertEqual(4, nc.stats['out_bytes'])

    @async_test
    def test_timed_request(self):
        nc = NATS()
        msgs = []
        counter = 0

        @asyncio.coroutine
        def worker_handler(msg):
            nonlocal counter
            counter += 1
            msgs.append(msg)
            yield from nc.publish(msg.reply, 'Reply:{}'.format(counter).encode())

        @asyncio.coroutine
        def slow_worker_handler(msg):
            yield from asyncio.sleep(0.5, loop=self.loop)
            yield from nc.publish(msg.reply, b'timeout by now...')

        yield from nc.connect(io_loop=self.loop)
        yield from nc.subscribe("help", cb=worker_handler)
        yield from nc.subscribe("slow.help", cb=slow_worker_handler)

        response = yield from nc.timed_request("help", b'please', timeout=1)
        self.assertEqual(b'Reply:1', response.data)
        response = yield from nc.timed_request("help", b'please', timeout=1)
        self.assertEqual(b'Reply:2', response.data)

        with self.assertRaises(ErrTimeout):
            yield from nc.timed_request("slow.help", b'please', timeout=0.1)
        yield from asyncio.sleep(1, loop=self.loop)
        yield from nc.close()

    @async_test
    def test_pending_data_size_tracking(self):
        nc = NATS()
        yield from nc.connect(io_loop=self.loop)
        largest_pending_data_size = 0
        for i in range(0, 100):
            yield from nc.publish("example", b'A' * 100000)
            if nc.pending_data_size > 0:
                largest_pending_data_size = nc.pending_data_size
        self.assertTrue(largest_pending_data_size > 0)
        yield from nc.close()

    @async_test
    def test_close(self):
        nc = NATS()

        disconnected_count = 0
        reconnected_count = 0
        closed_count = 0
        err_count = 0

        @asyncio.coroutine
        def disconnected_cb():
            nonlocal disconnected_count
            disconnected_count += 1

        @asyncio.coroutine
        def reconnected_cb():
            nonlocal reconnected_count
            reconnected_count += 1

        @asyncio.coroutine
        def closed_cb():
            nonlocal closed_count
            closed_count += 1

        @asyncio.coroutine
        def err_cb(e):
            nonlocal err_count
            err_count += 1

        options = {
            'io_loop': self.loop,
            'disconnected_cb': disconnected_cb,
            'closed_cb': closed_cb,
            'reconnected_cb': reconnected_cb,
            'error_cb': err_cb,
        }

        yield from nc.connect(**options)
        yield from nc.close()

        with self.assertRaises(ErrConnectionClosed):
            yield from nc.publish("foo", b'A')

        with self.assertRaises(ErrConnectionClosed):
            yield from nc.subscribe("bar", "workers")

        with self.assertRaises(ErrConnectionClosed):
            yield from nc.publish_request("bar", "inbox", b'B')

        with self.assertRaises(ErrConnectionClosed):
            yield from nc.flush()

        self.assertEqual(1, closed_count)
        self.assertEqual(1, disconnected_count)
        self.assertEqual(0, reconnected_count)
        self.assertEqual(0, err_count)

    @async_test
    def test_pending_data_size_flush_on_close(self):
        nc = NATS()

        disconnected_count = 0
        reconnected_count = 0
        closed_count = 0
        err_count = 0

        @asyncio.coroutine
        def disconnected_cb():
            nonlocal disconnected_count
            disconnected_count += 1

        @asyncio.coroutine
        def reconnected_cb():
            nonlocal reconnected_count
            reconnected_count += 1

        @asyncio.coroutine
        def closed_cb():
            nonlocal closed_count
            closed_count += 1

        options = {
            'dont_randomize': True,
            'io_loop': self.loop,
            'disconnected_cb': disconnected_cb,
            'closed_cb': closed_cb,
            'reconnected_cb': reconnected_cb,
            'reconnect_time_wait': 0.01
        }
        yield from nc.connect(**options)

        total_received = 0
        future = asyncio.Future(loop=self.loop)

        @asyncio.coroutine
        def receiver_cb(msg):
            nonlocal total_received
            total_received += 1
            if total_received == 200:
                future.set_result(True)

        # Extra connection which should be receiving all the messages
        nc2 = NATS()
        yield from nc2.connect(**options)
        yield from nc2.subscribe("example.*", cb=receiver_cb)
        yield from nc2.flush()

        for i in range(0, 200):
            yield from nc.publish("example.{}".format(i), b'A' * 20)

        # All pending messages should have been emitted to the server
        # by the first connection at this point.
        yield from nc.close()

        # Wait for the server to flush all the messages back to the receiving client
        yield from asyncio.wait_for(future, 1, loop=self.loop)
        yield from nc2.close()
        self.assertEqual(total_received, 200)


class ClientReconnectTest(MultiServerAuthTestCase):

    @async_test
    def test_connect_with_auth(self):
        nc = NATS()

        options = {
            'servers': [
                "nats://foo:bar@127.0.0.1:4223",
                "nats://hoge:fuga@127.0.0.1:4224"
            ],
            'io_loop': self.loop
        }
        yield from nc.connect(**options)
        self.assertIn('auth_required', nc._server_info)
        self.assertIn('max_payload', nc._server_info)
        self.assertEqual(nc._server_info['max_payload'], nc._max_payload)
        self.assertTrue(nc.is_connected)
        yield from nc.close()
        self.assertTrue(nc.is_closed)
        self.assertFalse(nc.is_connected)

    @async_test
    def test_connect_with_failed_auth(self):
        nc = NATS()

        options = {
            'reconnect_time_wait': 0.2,
            'servers': [
                "nats://hello:world@127.0.0.1:4223",
            ],
            'io_loop': self.loop
        }
        with self.assertRaises(ErrNoServers):
            yield from nc.connect(**options)

        self.assertIn('auth_required', nc._server_info)
        self.assertTrue(nc._server_info['auth_required'])
        self.assertFalse(nc.is_connected)
        yield from nc.close()
        self.assertTrue(nc.is_closed)
        self.assertEqual(ErrNoServers, type(nc.last_error))
        self.assertEqual(0, nc.stats['reconnects'])

    @async_test
    def test_infinite_reconnect(self):
        nc = NATS()

        disconnected_count = 0
        errors = []

        @asyncio.coroutine
        def disconnected_cb():
            nonlocal disconnected_count
            disconnected_count += 1

        @asyncio.coroutine
        def err_cb(e):
            nonlocal errors
            errors.append(e)

        options = {
            'dont_randomize': True,
            'reconnect_time_wait': 0.5,
            'disconnected_cb': disconnected_cb,
            'error_cb': err_cb,
            'servers': [
                "nats://foo:bar@127.0.0.1:4223",
                "nats://hoge:fuga@127.0.0.1:4224"
            ],
            'max_reconnect_attempts': -1,
            'io_loop': self.loop
        }

        yield from nc.connect(**options)
        self.assertIn('auth_required', nc._server_info)
        self.assertTrue(nc._server_info['auth_required'])
        self.assertTrue(nc.is_connected)

        # Stop all servers so that there aren't any available to reconnect
        yield from self.loop.run_in_executor(None, self.server_pool[0].stop)
        yield from self.loop.run_in_executor(None, self.server_pool[1].stop)
        for i in range(0, 10):
            yield from asyncio.sleep(0, loop=self.loop)
            yield from asyncio.sleep(0.2, loop=self.loop)
            yield from asyncio.sleep(0, loop=self.loop)

        self.assertTrue(len(errors) > 0)
        self.assertFalse(nc.is_connected)
        self.assertEqual(ConnectionRefusedError, type(nc.last_error))

        # Restart one of the servers and confirm we are reconnected
        # even after many tries from small reconnect_time_wait.
        yield from self.loop.run_in_executor(None, self.server_pool[1].start)
        for i in range(0, 10):
            yield from asyncio.sleep(0, loop=self.loop)
            yield from asyncio.sleep(0.2, loop=self.loop)
            yield from asyncio.sleep(0, loop=self.loop)

        # Many attempts but only at most 2 reconnects would have occured,
        # in case it was able to reconnect to another server while it was
        # shutting down.
        self.assertTrue(nc.stats['reconnects'] >= 1)

        # Wrap off and disconnect
        yield from nc.close()
        self.assertTrue(nc.is_closed)
        self.assertEqual(ConnectionRefusedError, type(nc.last_error))

    @async_test
    def test_pending_data_size_flush_reconnect(self):
        nc = NATS()

        disconnected_count = 0
        reconnected_count = 0
        closed_count = 0
        err_count = 0

        @asyncio.coroutine
        def disconnected_cb():
            nonlocal disconnected_count
            disconnected_count += 1

        @asyncio.coroutine
        def reconnected_cb():
            nonlocal reconnected_count
            reconnected_count += 1

        @asyncio.coroutine
        def closed_cb():
            nonlocal closed_count
            closed_count += 1

        options = {
            'servers': [
                "nats://foo:bar@127.0.0.1:4223",
                "nats://hoge:fuga@127.0.0.1:4224"
            ],
            'dont_randomize': True,
            'io_loop': self.loop,
            'disconnected_cb': disconnected_cb,
            'closed_cb': closed_cb,
            'reconnected_cb': reconnected_cb,
            'reconnect_time_wait': 0.01
        }
        yield from nc.connect(**options)
        largest_pending_data_size = 0
        post_flush_pending_data = None
        done_once = False

        @asyncio.coroutine
        def cb(msg):
            pass

        yield from nc.subscribe("example.*", cb=cb)

        for i in range(0, 200):
            yield from nc.publish("example.{}".format(i), b'A' * 20)
            if nc.pending_data_size > 0:
                largest_pending_data_size = nc.pending_data_size
            if nc.pending_data_size > 100:
                # Stop the first server and connect to another one asap.
                if not done_once:
                    yield from nc.flush(2)
                    post_flush_pending_data = nc.pending_data_size
                    yield from self.loop.run_in_executor(None, self.server_pool[0].stop)
                    done_once = True

        self.assertTrue(largest_pending_data_size > 0)
        self.assertTrue(post_flush_pending_data == 0)

        # Confirm we have reconnected eventually
        for i in range(0, 10):
            yield from asyncio.sleep(0, loop=self.loop)
            yield from asyncio.sleep(0.2, loop=self.loop)
            yield from asyncio.sleep(0, loop=self.loop)
        self.assertEqual(1, nc.stats['reconnects'])
        try:
            yield from nc.flush(2)
        except ErrTimeout:
            # If disconnect occurs during this flush, then we will have a timeout here
            pass
        finally:
            yield from nc.close()

        self.assertTrue(disconnected_count >= 1)
        self.assertTrue(closed_count >= 1)

    @async_test
    def test_custom_flush_queue_reconnect(self):
        nc = NATS()

        disconnected_count = 0
        reconnected_count = 0
        closed_count = 0
        err_count = 0

        @asyncio.coroutine
        def disconnected_cb():
            nonlocal disconnected_count
            disconnected_count += 1

        @asyncio.coroutine
        def reconnected_cb():
            nonlocal reconnected_count
            reconnected_count += 1

        @asyncio.coroutine
        def closed_cb():
            nonlocal closed_count
            closed_count += 1

        options = {
            'servers': [
                "nats://foo:bar@127.0.0.1:4223",
                "nats://hoge:fuga@127.0.0.1:4224"
            ],
            'dont_randomize': True,
            'io_loop': self.loop,
            'disconnected_cb': disconnected_cb,
            'closed_cb': closed_cb,
            'reconnected_cb': reconnected_cb,
            'flusher_queue_size': 100,
            'reconnect_time_wait': 0.01
        }
        yield from nc.connect(**options)
        largest_pending_data_size = 0
        post_flush_pending_data = None
        done_once = False

        @asyncio.coroutine
        def cb(msg):
            pass

        yield from nc.subscribe("example.*", cb=cb)

        for i in range(0, 500):
            yield from nc.publish("example.{}".format(i), b'A' * 20)
            if nc.pending_data_size > 0:
                largest_pending_data_size = nc.pending_data_size
            if nc.pending_data_size > 100:
                # Stop the first server and connect to another one asap.
                if not done_once:
                    yield from nc.flush(2)
                    post_flush_pending_data = nc.pending_data_size
                    yield from self.loop.run_in_executor(None, self.server_pool[0].stop)
                    done_once = True

        self.assertTrue(largest_pending_data_size > 0)
        self.assertTrue(post_flush_pending_data == 0)

        # Confirm we have reconnected eventually
        for i in range(0, 10):
            yield from asyncio.sleep(0, loop=self.loop)
            yield from asyncio.sleep(0.2, loop=self.loop)
            yield from asyncio.sleep(0, loop=self.loop)
        self.assertEqual(1, nc.stats['reconnects'])
        try:
            yield from nc.flush(2)
        except ErrTimeout:
            # If disconnect occurs during this flush, then we will have a timeout here
            pass
        finally:
            yield from nc.close()

        self.assertTrue(disconnected_count >= 1)
        self.assertTrue(closed_count >= 1)

    @async_test
    def test_auth_reconnect(self):
        nc = NATS()
        disconnected_count = 0
        reconnected_count = 0
        closed_count = 0
        err_count = 0

        @asyncio.coroutine
        def disconnected_cb():
            nonlocal disconnected_count
            disconnected_count += 1

        @asyncio.coroutine
        def reconnected_cb():
            nonlocal reconnected_count
            reconnected_count += 1

        @asyncio.coroutine
        def closed_cb():
            nonlocal closed_count
            closed_count += 1

        @asyncio.coroutine
        def err_cb(e):
            nonlocal err_count
            err_count += 1

        counter = 0

        @asyncio.coroutine
        def worker_handler(msg):
            nonlocal counter
            counter += 1
            if msg.reply != "":
                yield from nc.publish(msg.reply, 'Reply:{}'.format(counter).encode())

        options = {
            'servers': [
                "nats://foo:bar@127.0.0.1:4223",
                "nats://hoge:fuga@127.0.0.1:4224"
            ],
            'io_loop': self.loop,
            'disconnected_cb': disconnected_cb,
            'closed_cb': closed_cb,
            'reconnected_cb': reconnected_cb,
            'error_cb': err_cb,
            'dont_randomize': True,
        }
        yield from nc.connect(**options)
        self.assertTrue(nc.is_connected)

        yield from nc.subscribe("one", cb=worker_handler)
        yield from nc.subscribe("two", cb=worker_handler)
        yield from nc.subscribe("three", cb=worker_handler)

        response = yield from nc.timed_request("one", b'Help!', timeout=1)
        self.assertEqual(b'Reply:1', response.data)

        # Stop the first server and connect to another one asap.
        yield from self.loop.run_in_executor(None, self.server_pool[0].stop)

        # FIXME: Find better way to wait for the server to be stopped.
        yield from asyncio.sleep(0.5, loop=self.loop)

        response = yield from nc.timed_request("three", b'Help!', timeout=1)
        self.assertEqual('Reply:2'.encode(), response.data)
        yield from asyncio.sleep(0.5, loop=self.loop)
        yield from nc.close()
        self.assertEqual(1, nc.stats['reconnects'])
        self.assertEqual(1, closed_count)
        self.assertEqual(2, disconnected_count)
        self.assertEqual(1, reconnected_count)
        self.assertEqual(1, err_count)


class ClientAuthTokenTest(MultiServerAuthTokenTestCase):

    @async_test
    def test_connect_with_auth_token(self):
        nc = NATS()

        options = {
            'servers': [
                "nats://token@127.0.0.1:4223",
            ],
            'io_loop': self.loop
        }
        yield from nc.connect(**options)
        self.assertIn('auth_required', nc._server_info)
        self.assertTrue(nc.is_connected)
        yield from nc.close()
        self.assertTrue(nc.is_closed)
        self.assertFalse(nc.is_connected)

    @async_test
    def test_connect_with_bad_auth_token(self):
        nc = NATS()

        options = {
            'servers': [
                "nats://token@127.0.0.1:4225",
            ],
            'allow_reconnect': False,
            'reconnect_time_wait': 0.1,
            'max_reconnect_attempts': 1,
            'io_loop': self.loop
        }
        # Authorization Violation
        with self.assertRaises(NatsError):
            yield from nc.connect(**options)

        self.assertIn('auth_required', nc._server_info)
        self.assertFalse(nc.is_connected)

    @async_test
    def test_reconnect_with_auth_token(self):
        nc = NATS()

        disconnected_count = 0
        reconnected_count = 0
        closed_count = 0
        err_count = 0

        @asyncio.coroutine
        def disconnected_cb():
            nonlocal disconnected_count
            disconnected_count += 1

        @asyncio.coroutine
        def reconnected_cb():
            nonlocal reconnected_count
            reconnected_count += 1

        @asyncio.coroutine
        def closed_cb():
            nonlocal closed_count
            closed_count += 1

        counter = 0

        @asyncio.coroutine
        def worker_handler(msg):
            nonlocal counter
            counter += 1
            if msg.reply != "":
                yield from nc.publish(msg.reply, 'Reply:{}'.format(counter).encode())

        options = {
            'servers': [
                "nats://token@127.0.0.1:4223",
                "nats://token@127.0.0.1:4224",
            ],
            'disconnected_cb': disconnected_cb,
            'closed_cb': closed_cb,
            'reconnected_cb': reconnected_cb,
            'dont_randomize': True,
            'io_loop': self.loop
        }
        yield from nc.connect(**options)
        yield from nc.subscribe("test", cb=worker_handler)
        self.assertIn('auth_required', nc._server_info)
        self.assertTrue(nc.is_connected)

        # Trigger a reconnnect
        yield from self.loop.run_in_executor(None, self.server_pool[0].stop)
        yield from asyncio.sleep(1, loop=self.loop)

        yield from nc.subscribe("test", cb=worker_handler)
        response = yield from nc.timed_request("test", b'data', timeout=1)
        self.assertEqual(b'Reply:1', response.data)

        yield from nc.close()
        self.assertTrue(nc.is_closed)
        self.assertFalse(nc.is_connected)
        self.assertEqual(1, closed_count)
        self.assertEqual(2, disconnected_count)
        self.assertEqual(1, reconnected_count)


class ClientTLSTest(TLSServerTestCase):

    @async_test
    def test_connect(self):
        nc = NATS()
        yield from nc.connect(io_loop=self.loop, servers=['nats://localhost:4224'],
                              tls=self.ssl_ctx)
        self.assertEqual(nc._server_info['max_payload'], nc.max_payload)
        self.assertTrue(nc._server_info['tls_required'])
        self.assertTrue(nc._server_info['tls_verify'])
        self.assertTrue(nc.max_payload > 0)
        self.assertTrue(nc.is_connected)
        yield from nc.close()
        self.assertTrue(nc.is_closed)
        self.assertFalse(nc.is_connected)

    @async_test
    def test_subscribe(self):
        nc = NATS()
        msgs = []

        @asyncio.coroutine
        def subscription_handler(msg):
            msgs.append(msg)

        payload = b'hello world'
        yield from nc.connect(io_loop=self.loop, servers=['nats://localhost:4224'],
                              tls=self.ssl_ctx)
        sid = yield from nc.subscribe("foo", cb=subscription_handler)
        yield from nc.publish("foo", payload)
        yield from nc.publish("bar", payload)

        with self.assertRaises(ErrBadSubject):
            yield from nc.publish("", b'')

        # Wait a bit for message to be received.
        yield from asyncio.sleep(0.2, loop=self.loop)

        self.assertEqual(1, len(msgs))
        msg = msgs[0]
        self.assertEqual('foo', msg.subject)
        self.assertEqual('', msg.reply)
        self.assertEqual(payload, msg.data)
        self.assertEqual(1, nc._subs[sid].received)
        yield from nc.close()


class ClientTLSReconnectTest(MultiTLSServerAuthTestCase):

    @async_test
    def test_tls_reconnect(self):

        nc = NATS()
        disconnected_count = 0
        reconnected_count = 0
        closed_count = 0
        err_count = 0

        @asyncio.coroutine
        def disconnected_cb():
            nonlocal disconnected_count
            disconnected_count += 1

        @asyncio.coroutine
        def reconnected_cb():
            nonlocal reconnected_count
            reconnected_count += 1

        @asyncio.coroutine
        def closed_cb():
            nonlocal closed_count
            closed_count += 1

        @asyncio.coroutine
        def err_cb(e):
            nonlocal err_count
            err_count += 1

        counter = 0

        @asyncio.coroutine
        def worker_handler(msg):
            nonlocal counter
            counter += 1
            if msg.reply != "":
                yield from nc.publish(msg.reply, 'Reply:{}'.format(counter).encode())

        options = {
            'servers': [
                "nats://foo:bar@localhost:4223",
                "nats://hoge:fuga@localhost:4224"
            ],
            'io_loop': self.loop,
            'disconnected_cb': disconnected_cb,
            'closed_cb': closed_cb,
            'reconnected_cb': reconnected_cb,
            'error_cb': err_cb,
            'dont_randomize': True,
            'tls': self.ssl_ctx
        }
        yield from nc.connect(**options)
        self.assertTrue(nc.is_connected)

        yield from nc.subscribe("example", cb=worker_handler)
        response = yield from nc.timed_request("example", b'Help!', timeout=1)
        self.assertEqual(b'Reply:1', response.data)

        # Trigger a reconnnect and should be fine
        yield from self.loop.run_in_executor(None, self.server_pool[0].stop)
        yield from asyncio.sleep(1, loop=self.loop)

        yield from nc.subscribe("example", cb=worker_handler)
        response = yield from nc.timed_request("example", b'Help!', timeout=1)
        self.assertEqual(b'Reply:2', response.data)

        yield from nc.close()
        self.assertTrue(nc.is_closed)
        self.assertFalse(nc.is_connected)
        self.assertEqual(1, nc.stats['reconnects'])
        self.assertEqual(1, closed_count)
        self.assertEqual(2, disconnected_count)
        self.assertEqual(1, reconnected_count)
        self.assertEqual(1, err_count)

class ClusterDiscoveryTest(ClusteringTestCase):

    @async_test
    def test_discover_servers_on_first_connect(self):
        nc = NATS()

        # Start rest of cluster members so that we receive them
        # connect_urls on the first connect.
        yield from self.loop.run_in_executor(None, self.server_pool[1].start)
        yield from asyncio.sleep(1, loop=self.loop)
        yield from self.loop.run_in_executor(None, self.server_pool[2].start)
        yield from asyncio.sleep(1, loop=self.loop)

        options = {
            'servers': [
                "nats://127.0.0.1:4223",
                ],
            'io_loop': self.loop
            }
        yield from nc.connect(**options)
        self.assertTrue(nc.is_connected)
        yield from nc.close()
        self.assertTrue(nc.is_closed)
        self.assertEqual(len(nc.servers), 3)
        self.assertEqual(len(nc.discovered_servers), 2)

    @async_test
    def test_discover_servers_after_first_connect(self):
        nc = NATS()

        options = {
            'servers': [
                "nats://127.0.0.1:4223",
                ],
            'io_loop': self.loop
            }
        yield from nc.connect(**options)

        # Start rest of cluster members so that we receive them
        # connect_urls on the first connect.
        yield from self.loop.run_in_executor(None, self.server_pool[1].start)
        yield from asyncio.sleep(1, loop=self.loop)
        yield from self.loop.run_in_executor(None, self.server_pool[2].start)
        yield from asyncio.sleep(1, loop=self.loop)

        yield from nc.close()
        self.assertTrue(nc.is_closed)
        self.assertEqual(len(nc.servers), 3)
        self.assertEqual(len(nc.discovered_servers), 2)

class ConnectFailuresTest(SingleServerTestCase):

    @async_test
    def test_empty_info_op_uses_defaults(self):

        @asyncio.coroutine
        def bad_server(reader, writer):
            writer.write(b'INFO {}\r\n')
            yield from writer.drain()

            data = yield from reader.readline()
            yield from asyncio.sleep(0.2, loop=self.loop)
            writer.close()

        yield from asyncio.start_server(
            bad_server,
            '127.0.0.1',
            4555,
            loop=self.loop
            )

        disconnected_count = 0

        @asyncio.coroutine
        def disconnected_cb():
            nonlocal disconnected_count
            disconnected_count += 1

        nc = NATS()
        options = {
            'servers': [
                "nats://127.0.0.1:4555",
                ],
            'disconnected_cb': disconnected_cb,
            'io_loop': self.loop
            }
        yield from nc.connect(**options)
        self.assertEqual(nc.max_payload, 1048576)

        yield from nc.close()
        self.assertEqual(1, disconnected_count)

    @async_test
    def test_empty_response_from_server(self):

        @asyncio.coroutine
        def bad_server(reader, writer):
            writer.write(b'')
            yield from asyncio.sleep(0.2, loop=self.loop)
            writer.close()

        yield from asyncio.start_server(
            bad_server,
            '127.0.0.1',
            4555,
            loop=self.loop
            )

        errors = []

        @asyncio.coroutine
        def error_cb(e):
            nonlocal errors
            errors.append(e)

        nc = NATS()
        options = {
            'servers': [
                "nats://127.0.0.1:4555",
                ],
            'error_cb': error_cb,
            'io_loop': self.loop,
            'allow_reconnect': False,
            }

        with self.assertRaises(NatsError):
            yield from nc.connect(**options)
        self.assertEqual(1, len(errors))            
        self.assertEqual(errors[0], nc.last_error)

    @async_test
    def test_malformed_info_response_from_server(self):

        @asyncio.coroutine
        def bad_server(reader, writer):
            writer.write(b'INF')
            yield from asyncio.sleep(0.2, loop=self.loop)
            writer.close()

        yield from asyncio.start_server(
            bad_server,
            '127.0.0.1',
            4555,
            loop=self.loop
            )

        errors = []

        @asyncio.coroutine
        def error_cb(e):
            nonlocal errors
            errors.append(e)

        nc = NATS()
        options = {
            'servers': [
                "nats://127.0.0.1:4555",
                ],
            'error_cb': error_cb,
            'io_loop': self.loop,
            'allow_reconnect': False,
            }

        with self.assertRaises(NatsError):
            yield from nc.connect(**options)
        self.assertEqual(1, len(errors))            
        self.assertEqual(errors[0], nc.last_error)

    @async_test
    def test_malformed_info_json_response_from_server(self):

        @asyncio.coroutine
        def bad_server(reader, writer):
            writer.write(b'INFO {\r\n')
            yield from asyncio.sleep(0.2, loop=self.loop)
            writer.close()

        yield from asyncio.start_server(
            bad_server,
            '127.0.0.1',
            4555,
            loop=self.loop
            )

        errors = []

        @asyncio.coroutine
        def error_cb(e):
            nonlocal errors
            errors.append(e)

        nc = NATS()
        options = {
            'servers': [
                "nats://127.0.0.1:4555",
                ],
            'error_cb': error_cb,
            'io_loop': self.loop,
            'allow_reconnect': False,
            }

        with self.assertRaises(NatsError):
            yield from nc.connect(**options)
        self.assertEqual(1, len(errors))            
        self.assertEqual(errors[0], nc.last_error)
        yield from asyncio.sleep(0.5, loop=self.loop)

if __name__ == '__main__':
    runner = unittest.TextTestRunner(stream=sys.stdout)
    unittest.main(verbosity=2, exit=False, testRunner=runner)
