import sys
import time
import json
import ssl
import asyncio
import unittest
import http.client
from unittest import mock

import nats
from nats.aio.client import __version__
from nats.aio.client import Client as NATS
from nats.aio.utils import new_inbox, INBOX_PREFIX
from nats.aio.errors import ErrConnectionClosed, ErrNoServers, ErrTimeout, \
     ErrBadSubject, ErrBadSubscription, ErrConnectionDraining, ErrDrainTimeout, NatsError, ErrInvalidCallbackType
from tests.utils import async_test, start_gnatsd, Gnatsd, NatsTestCase, \
    SingleServerTestCase, MultiServerAuthTestCase, MultiServerAuthTokenTestCase, TLSServerTestCase, \
    MultiTLSServerAuthTestCase, ClusteringTestCase, ClusteringDiscoveryAuthTestCase


class ClientUtilsTest(NatsTestCase):
    def test_default_connect_command(self):
        nc = NATS()
        nc.options["verbose"] = False
        nc.options["pedantic"] = False
        nc.options["auth_required"] = False
        nc.options["name"] = None
        nc.options["no_echo"] = False
        got = nc._connect_command()
        expected = 'CONNECT {"echo": true, "lang": "python3", "pedantic": false, "protocol": 1, "verbose": false, "version": "%s"}\r\n' % __version__
        self.assertEqual(expected.encode(), got)

    def test_default_connect_command_with_name(self):
        nc = NATS()
        nc.options["verbose"] = False
        nc.options["pedantic"] = False
        nc.options["auth_required"] = False
        nc.options["name"] = "secret"
        nc.options["no_echo"] = False
        got = nc._connect_command()
        expected = 'CONNECT {"echo": true, "lang": "python3", "name": "secret", "pedantic": false, "protocol": 1, "verbose": false, "version": "%s"}\r\n' % __version__
        self.assertEqual(expected.encode(), got)

    def tests_generate_new_inbox(self):
        inbox = new_inbox()
        self.assertTrue(inbox.startswith(INBOX_PREFIX))
        min_expected_len = len(INBOX_PREFIX)
        self.assertTrue(len(inbox) > min_expected_len)


class ClientTest(SingleServerTestCase):
    @async_test
    async def test_default_connect(self):
        nc = NATS()
        await nc.connect()
        self.assertIn('server_id', nc._server_info)
        self.assertIn('client_id', nc._server_info)
        self.assertIn('max_payload', nc._server_info)
        self.assertEqual(nc._server_info['max_payload'], nc.max_payload)
        self.assertTrue(nc.max_payload > 0)
        self.assertTrue(nc.is_connected)
        self.assertTrue(nc.client_id > 0)
        await nc.close()
        self.assertTrue(nc.is_closed)
        self.assertFalse(nc.is_connected)

    def test_connect_syntax_sugar(self):
        nc = NATS()
        nc._setup_server_pool([
            "nats://127.0.0.1:4222", "nats://127.0.0.1:4223",
            "nats://127.0.0.1:4224"
        ])
        self.assertEqual(3, len(nc._server_pool))

        nc = NATS()
        nc._setup_server_pool("nats://127.0.0.1:4222")
        self.assertEqual(1, len(nc._server_pool))

        nc = NATS()
        nc._setup_server_pool("127.0.0.1:4222")
        self.assertEqual(1, len(nc._server_pool))

        nc = NATS()
        nc._setup_server_pool("nats://127.0.0.1:")
        self.assertEqual(1, len(nc._server_pool))

        nc = NATS()
        nc._setup_server_pool("127.0.0.1")
        self.assertEqual(1, len(nc._server_pool))
        self.assertEqual(4222, nc._server_pool[0].uri.port)

        nc = NATS()
        nc._setup_server_pool("demo.nats.io")
        self.assertEqual(1, len(nc._server_pool))
        self.assertEqual("demo.nats.io", nc._server_pool[0].uri.hostname)
        self.assertEqual(4222, nc._server_pool[0].uri.port)

        nc = NATS()
        nc._setup_server_pool("localhost:")
        self.assertEqual(1, len(nc._server_pool))
        self.assertEqual(4222, nc._server_pool[0].uri.port)

        nc = NATS()
        with self.assertRaises(NatsError):
            nc._setup_server_pool("::")
        self.assertEqual(0, len(nc._server_pool))

        nc = NATS()
        with self.assertRaises(NatsError):
            nc._setup_server_pool("nats://")

        nc = NATS()
        with self.assertRaises(NatsError):
            nc._setup_server_pool("://")
        self.assertEqual(0, len(nc._server_pool))

        nc = NATS()
        with self.assertRaises(NatsError):
            nc._setup_server_pool("")
        self.assertEqual(0, len(nc._server_pool))

        # Auth examples
        nc = NATS()
        nc._setup_server_pool("hello:world@demo.nats.io:4222")
        self.assertEqual(1, len(nc._server_pool))
        uri = nc._server_pool[0].uri
        self.assertEqual("demo.nats.io", uri.hostname)
        self.assertEqual(4222, uri.port)
        self.assertEqual("hello", uri.username)
        self.assertEqual("world", uri.password)

        nc = NATS()
        nc._setup_server_pool("hello:@demo.nats.io:4222")
        self.assertEqual(1, len(nc._server_pool))
        uri = nc._server_pool[0].uri
        self.assertEqual("demo.nats.io", uri.hostname)
        self.assertEqual(4222, uri.port)
        self.assertEqual("hello", uri.username)
        self.assertEqual("", uri.password)

        nc = NATS()
        nc._setup_server_pool(":@demo.nats.io:4222")
        self.assertEqual(1, len(nc._server_pool))
        uri = nc._server_pool[0].uri
        self.assertEqual("demo.nats.io", uri.hostname)
        self.assertEqual(4222, uri.port)
        self.assertEqual("", uri.username)
        self.assertEqual("", uri.password)

        nc = NATS()
        nc._setup_server_pool("@demo.nats.io:4222")
        self.assertEqual(1, len(nc._server_pool))
        uri = nc._server_pool[0].uri
        self.assertEqual("demo.nats.io", uri.hostname)
        self.assertEqual(4222, uri.port)
        self.assertEqual("", uri.username)
        self.assertEqual(None, uri.password)

        nc = NATS()
        nc._setup_server_pool("@demo.nats.io:")
        self.assertEqual(1, len(nc._server_pool))
        uri = nc._server_pool[0].uri
        self.assertEqual("demo.nats.io", uri.hostname)
        self.assertEqual(4222, uri.port)
        self.assertEqual(None, uri.username)
        self.assertEqual(None, uri.password)

        nc = NATS()
        nc._setup_server_pool("@demo.nats.io")
        self.assertEqual(1, len(nc._server_pool))
        uri = nc._server_pool[0].uri
        self.assertEqual("demo.nats.io", uri.hostname)
        self.assertEqual(4222, uri.port)
        self.assertEqual("", uri.username)
        self.assertEqual(None, uri.password)

    @async_test
    async def test_connect_no_servers_on_connect_init(self):
        nc = NATS()
        with self.assertRaises(ErrNoServers):
            await nc.connect(
                servers=["nats://127.0.0.1:4221"],
                max_reconnect_attempts=2,
                reconnect_time_wait=0.2,
            )

    @async_test
    async def test_publish(self):
        nc = NATS()
        await nc.connect()
        for i in range(0, 100):
            await nc.publish("hello.%d" % i, b'A')

        with self.assertRaises(ErrBadSubject):
            await nc.publish("", b'')

        await nc.flush()
        await nc.close()
        await asyncio.sleep(1)
        self.assertEqual(100, nc.stats['out_msgs'])
        self.assertEqual(100, nc.stats['out_bytes'])

        endpoint = '127.0.0.1:{port}'.format(
            port=self.server_pool[0].http_port
        )
        httpclient = http.client.HTTPConnection(endpoint, timeout=5)
        httpclient.request('GET', '/varz')
        response = httpclient.getresponse()
        varz = json.loads((response.read()).decode())
        self.assertEqual(100, varz['in_msgs'])
        self.assertEqual(100, varz['in_bytes'])

    @async_test
    async def test_flush(self):
        nc = NATS()
        await nc.connect()
        for i in range(0, 10):
            await nc.publish("flush.%d" % i, b'AA')
            await nc.flush()
        self.assertEqual(10, nc.stats['out_msgs'])
        self.assertEqual(20, nc.stats['out_bytes'])
        await nc.close()

    @async_test
    async def test_subscribe(self):
        nc = NATS()
        msgs = []

        async def subscription_handler(msg):
            msgs.append(msg)

        payload = b'hello world'
        await nc.connect()
        sub = await nc.subscribe("foo", cb=subscription_handler)
        await nc.publish("foo", payload)
        await nc.publish("bar", payload)

        with self.assertRaises(ErrBadSubject):
            await nc.publish("", b'')

        # Wait a bit for message to be received.
        await asyncio.sleep(0.2)

        self.assertEqual(1, len(msgs))
        msg = msgs[0]
        self.assertEqual('foo', msg.subject)
        self.assertEqual('', msg.reply)
        self.assertEqual(payload, msg.data)
        self.assertEqual(1, sub._received)
        await nc.close()

        # After close, the subscription is gone
        with self.assertRaises(KeyError):
            nc._subs[sub._id]

        self.assertEqual(1, nc.stats['in_msgs'])
        self.assertEqual(11, nc.stats['in_bytes'])
        self.assertEqual(2, nc.stats['out_msgs'])
        self.assertEqual(22, nc.stats['out_bytes'])

        endpoint = '127.0.0.1:{port}'.format(
            port=self.server_pool[0].http_port
        )
        httpclient = http.client.HTTPConnection(endpoint, timeout=5)
        httpclient.request('GET', '/connz')
        response = httpclient.getresponse()
        connz = json.loads((response.read()).decode())
        self.assertEqual(1, len(connz['connections']))
        self.assertEqual(2, connz['connections'][0]['in_msgs'])
        self.assertEqual(22, connz['connections'][0]['in_bytes'])
        self.assertEqual(1, connz['connections'][0]['out_msgs'])
        self.assertEqual(11, connz['connections'][0]['out_bytes'])

    @async_test
    async def test_subscribe_functools_partial(self):
        import functools

        nc = NATS()
        msgs = []
        partial_arg = None

        async def subscription_handler(arg1, msg):
            nonlocal partial_arg
            partial_arg = arg1
            msgs.append(msg)

        partial_sub_handler = functools.partial(
            subscription_handler, "example"
        )

        payload = b'hello world'
        await nc.connect()

        sid = await nc.subscribe("foo", cb=partial_sub_handler)

        await nc.publish("foo", payload)
        await nc.drain()

        self.assertEqual(1, len(msgs))
        self.assertEqual(partial_arg, "example")
        msg = msgs[0]
        self.assertEqual('foo', msg.subject)
        self.assertEqual('', msg.reply)
        self.assertEqual(payload, msg.data)

    @async_test
    async def test_subscribe_no_echo(self):
        nc = NATS()
        msgs = []

        nc2 = NATS()
        msgs2 = []

        async def subscription_handler(msg):
            msgs.append(msg)

        async def subscription_handler2(msg):
            msgs2.append(msg)

        await nc.connect(no_echo=True)
        await nc2.connect(no_echo=False)

        sub = await nc.subscribe("foo", cb=subscription_handler)
        sub2 = await nc2.subscribe("foo", cb=subscription_handler2)

        payload = b'hello world'
        for i in range(0, 10):
            await nc.publish("foo", payload)
            await asyncio.sleep(0)
        await nc.flush()

        # Wait a bit for message to be received.
        await asyncio.sleep(1)

        self.assertEqual(0, len(msgs))
        self.assertEqual(10, len(msgs2))
        self.assertEqual(0, sub._received)
        self.assertEqual(10, sub2._received)
        await nc.close()
        await nc2.close()

        self.assertEqual(0, nc.stats['in_msgs'])
        self.assertEqual(0, nc.stats['in_bytes'])
        self.assertEqual(10, nc.stats['out_msgs'])
        self.assertEqual(110, nc.stats['out_bytes'])

        self.assertEqual(10, nc2.stats['in_msgs'])
        self.assertEqual(110, nc2.stats['in_bytes'])
        self.assertEqual(0, nc2.stats['out_msgs'])
        self.assertEqual(0, nc2.stats['out_bytes'])

    @async_test
    async def test_invalid_subscribe_error(self):
        nc = NATS()
        msgs = []
        future_error = asyncio.Future()

        async def subscription_handler(msg):
            msgs.append(msg)

        async def closed_cb():
            nonlocal future_error
            future_error.set_result(nc.last_error)

        await nc.connect(closed_cb=closed_cb)
        await nc.subscribe("foo.", cb=subscription_handler)
        await asyncio.wait_for(future_error, 1.0)
        nats_error = future_error.result()
        self.assertEqual(type(nats_error), NatsError)
        self.assertEqual(str(nats_error), "nats: 'Invalid Subject'")

    @async_test
    async def test_subscribe_callback(self):
        nc = NATS()
        msgs = []

        async def subscription_handler(msg):
            if msg.subject == "tests.1":
                await asyncio.sleep(0.5)
            if msg.subject == "tests.3":
                await asyncio.sleep(0.2)
            msgs.append(msg)

        await nc.connect()
        await nc.subscribe("tests.>", cb=subscription_handler)

        for i in range(0, 5):
            await nc.publish(f"tests.{i}", b'bar')

        # Wait a bit for messages to be received.
        await asyncio.sleep(1)
        self.assertEqual(5, len(msgs))
        self.assertEqual("tests.1", msgs[1].subject)
        self.assertEqual("tests.3", msgs[3].subject)
        await nc.close()

    @async_test
    async def test_subscribe_iterate(self):
        nc = NATS()
        msgs = []
        fut = asyncio.Future()

        async def iterator_func(sub):
            async for msg in sub.messages:
                msgs.append(msg)
            fut.set_result(None)

        await nc.connect()
        sub = await nc.subscribe('tests.>')

        asyncio.ensure_future(iterator_func(sub))

        for i in range(0, 5):
            await nc.publish(f"tests.{i}", b'bar')

        await asyncio.sleep(0)
        await asyncio.wait_for(sub.drain(), 1)

        await asyncio.wait_for(fut, 1)
        self.assertEqual(5, len(msgs))
        self.assertEqual("tests.1", msgs[1].subject)
        self.assertEqual("tests.3", msgs[3].subject)
        await nc.close()

    @async_test
    async def test_subscribe_without_coroutine_unsupported(self):
        nc = NATS()
        msgs = []

        def subscription_handler(msg):
            if msg.subject == "tests.1":
                time.sleep(0.5)
            if msg.subject == "tests.3":
                time.sleep(0.2)
            msgs.append(msg)

        await nc.connect()

        with self.assertRaises(NatsError):
            await nc.subscribe("tests.>", cb=subscription_handler)
        await nc.close()

    @async_test
    async def test_unsubscribe(self):
        nc = NATS()
        msgs = []

        async def subscription_handler(msg):
            msgs.append(msg)

        await nc.connect()
        sub = await nc.subscribe("foo", cb=subscription_handler)
        await nc.publish("foo", b'A')
        await nc.publish("foo", b'B')

        # Wait a bit to receive the messages
        await asyncio.sleep(0.5)
        self.assertEqual(2, len(msgs))
        await sub.unsubscribe()
        await nc.publish("foo", b'C')
        await nc.publish("foo", b'D')

        # Ordering should be preserverd in these at least
        self.assertEqual(b'A', msgs[0].data)
        self.assertEqual(b'B', msgs[1].data)

        # Should not exist by now
        with self.assertRaises(KeyError):
            nc._subs[sub._id].received

        await asyncio.sleep(1)
        endpoint = '127.0.0.1:{port}'.format(
            port=self.server_pool[0].http_port
        )
        httpclient = http.client.HTTPConnection(endpoint, timeout=5)
        httpclient.request('GET', '/connz')
        response = httpclient.getresponse()
        connz = json.loads((response.read()).decode())
        self.assertEqual(1, len(connz['connections']))
        self.assertEqual(0, connz['connections'][0]['subscriptions'])
        self.assertEqual(4, connz['connections'][0]['in_msgs'])
        self.assertEqual(4, connz['connections'][0]['in_bytes'])
        self.assertEqual(2, connz['connections'][0]['out_msgs'])
        self.assertEqual(2, connz['connections'][0]['out_bytes'])

        await nc.close()
        self.assertEqual(2, nc.stats['in_msgs'])
        self.assertEqual(2, nc.stats['in_bytes'])
        self.assertEqual(4, nc.stats['out_msgs'])
        self.assertEqual(4, nc.stats['out_bytes'])

    @async_test
    async def test_old_style_request(self):
        nc = NATS()
        msgs = []
        counter = 0

        async def worker_handler(msg):
            nonlocal counter
            counter += 1
            msgs.append(msg)
            await nc.publish(msg.reply, f'Reply:{counter}'.encode())

        async def slow_worker_handler(msg):
            await asyncio.sleep(0.5)
            await nc.publish(msg.reply, b'timeout by now...')

        await nc.connect()
        await nc.subscribe("help", cb=worker_handler)
        await nc.subscribe("slow.help", cb=slow_worker_handler)

        response = await nc.request(
            "help", b'please', timeout=1, old_style=True
        )
        self.assertEqual(b'Reply:1', response.data)
        response = await nc.request(
            "help", b'please', timeout=1, old_style=True
        )
        self.assertEqual(b'Reply:2', response.data)

        with self.assertRaises(ErrTimeout):
            await nc.request(
                "slow.help", b'please', timeout=0.1, old_style=True
            )
        await asyncio.sleep(1)
        await nc.close()

    @async_test
    async def test_new_style_request(self):
        nc = NATS()
        msgs = []
        counter = 0

        async def worker_handler(msg):
            nonlocal counter
            counter += 1
            msgs.append(msg)
            await nc.publish(msg.reply, f'Reply:{counter}'.encode())

        async def slow_worker_handler(msg):
            await asyncio.sleep(0.5)
            await nc.publish(msg.reply, b'timeout by now...')

        await nc.connect()
        await nc.subscribe("help", cb=worker_handler)
        await nc.subscribe("slow.help", cb=slow_worker_handler)

        response = await nc.request("help", b'please', timeout=1)
        self.assertEqual(b'Reply:1', response.data)
        response = await nc.request("help", b'please', timeout=1)
        self.assertEqual(b'Reply:2', response.data)

        with self.assertRaises(ErrTimeout):
            await nc.request("slow.help", b'please', timeout=0.1)
        await asyncio.sleep(1)
        await nc.close()

    @async_test
    async def test_pending_data_size_tracking(self):
        nc = NATS()
        await nc.connect()
        largest_pending_data_size = 0
        for i in range(0, 100):
            await nc.publish("example", b'A' * 100000)
            if nc.pending_data_size > 0:
                largest_pending_data_size = nc.pending_data_size
        self.assertTrue(largest_pending_data_size > 0)
        await nc.close()

    @async_test
    async def test_close(self):
        nc = NATS()

        disconnected_count = 0
        reconnected_count = 0
        closed_count = 0
        err_count = 0

        async def disconnected_cb():
            nonlocal disconnected_count
            disconnected_count += 1

        async def reconnected_cb():
            nonlocal reconnected_count
            reconnected_count += 1

        async def closed_cb():
            nonlocal closed_count
            closed_count += 1

        async def err_cb(e):
            nonlocal err_count
            err_count += 1

        options = {
            'disconnected_cb': disconnected_cb,
            'closed_cb': closed_cb,
            'reconnected_cb': reconnected_cb,
            'error_cb': err_cb,
        }

        await nc.connect(**options)
        await nc.close()

        with self.assertRaises(ErrConnectionClosed):
            await nc.publish("foo", b'A')

        with self.assertRaises(ErrConnectionClosed):
            await nc.subscribe("bar", "workers")

        with self.assertRaises(ErrConnectionClosed):
            await nc.publish("bar", b'B', reply="inbox")

        with self.assertRaises(ErrConnectionClosed):
            await nc.flush()

        self.assertEqual(1, closed_count)
        self.assertEqual(1, disconnected_count)
        self.assertEqual(0, reconnected_count)
        self.assertEqual(0, err_count)

    @async_test
    async def test_pending_data_size_flush_on_close(self):
        nc = NATS()

        disconnected_count = 0
        reconnected_count = 0
        closed_count = 0
        err_count = 0

        async def disconnected_cb():
            nonlocal disconnected_count
            disconnected_count += 1

        async def reconnected_cb():
            nonlocal reconnected_count
            reconnected_count += 1

        async def closed_cb():
            nonlocal closed_count
            closed_count += 1

        options = {
            'dont_randomize': True,
            'disconnected_cb': disconnected_cb,
            'closed_cb': closed_cb,
            'reconnected_cb': reconnected_cb,
            'reconnect_time_wait': 0.01
        }
        await nc.connect(**options)

        total_received = 0
        future = asyncio.Future()

        async def receiver_cb(msg):
            nonlocal total_received
            total_received += 1
            if total_received == 200:
                future.set_result(True)

        # Extra connection which should be receiving all the messages
        nc2 = NATS()
        await nc2.connect(**options)
        await nc2.subscribe("example.*", cb=receiver_cb)
        await nc2.flush()

        for i in range(0, 200):
            await nc.publish(f"example.{i}", b'A' * 20)

        # All pending messages should have been emitted to the server
        # by the first connection at this point.
        await nc.close()

        # Wait for the server to flush all the messages back to the receiving client
        await asyncio.wait_for(future, 1)
        await nc2.close()
        self.assertEqual(total_received, 200)


class ClientReconnectTest(MultiServerAuthTestCase):
    @async_test
    async def test_connect_with_auth(self):
        nc = NATS()

        options = {
            'servers': [
                "nats://foo:bar@127.0.0.1:4223",
                "nats://hoge:fuga@127.0.0.1:4224"
            ]
        }
        await nc.connect(**options)
        self.assertIn('auth_required', nc._server_info)
        self.assertIn('max_payload', nc._server_info)
        self.assertEqual(nc._server_info['max_payload'], nc._max_payload)
        self.assertTrue(nc.is_connected)
        await nc.close()
        self.assertTrue(nc.is_closed)
        self.assertFalse(nc.is_connected)

    @async_test
    async def test_connect_with_failed_auth(self):
        nc = NATS()

        options = {
            'reconnect_time_wait': 0.2,
            'servers': ["nats://hello:world@127.0.0.1:4223", ],
            'max_reconnect_attempts': 3
        }
        with self.assertRaises(ErrNoServers):
            await nc.connect(**options)

        self.assertIn('auth_required', nc._server_info)
        self.assertTrue(nc._server_info['auth_required'])
        self.assertFalse(nc.is_connected)
        await nc.close()
        self.assertTrue(nc.is_closed)
        self.assertEqual(ErrNoServers, type(nc.last_error))
        self.assertEqual(0, nc.stats['reconnects'])

    @async_test
    async def test_infinite_reconnect(self):
        nc = NATS()

        disconnected_count = 0
        errors = []

        async def disconnected_cb():
            nonlocal disconnected_count
            disconnected_count += 1

        async def err_cb(e):
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
            'max_reconnect_attempts': -1
        }

        await nc.connect(**options)
        self.assertIn('auth_required', nc._server_info)
        self.assertTrue(nc._server_info['auth_required'])
        self.assertTrue(nc.is_connected)

        # Stop all servers so that there aren't any available to reconnect
        await self.loop.run_in_executor(None, self.server_pool[0].stop)
        await self.loop.run_in_executor(None, self.server_pool[1].stop)
        for i in range(0, 10):
            await asyncio.sleep(0)
            await asyncio.sleep(0.2)
            await asyncio.sleep(0)

        self.assertTrue(len(errors) > 0)
        self.assertFalse(nc.is_connected)
        self.assertEqual(ConnectionRefusedError, type(nc.last_error))

        # Restart one of the servers and confirm we are reconnected
        # even after many tries from small reconnect_time_wait.
        await self.loop.run_in_executor(None, self.server_pool[1].start)
        for i in range(0, 10):
            await asyncio.sleep(0)
            await asyncio.sleep(0.2)
            await asyncio.sleep(0)

        # Many attempts but only at most 2 reconnects would have occured,
        # in case it was able to reconnect to another server while it was
        # shutting down.
        self.assertTrue(nc.stats['reconnects'] >= 1)

        # Wrap off and disconnect
        await nc.close()
        self.assertTrue(nc.is_closed)
        self.assertEqual(ConnectionRefusedError, type(nc.last_error))

    @async_test
    async def test_failed_reconnect_removes_servers(self):
        nc = NATS()

        disconnected_count = 0
        errors = []
        closed_future = asyncio.Future()

        async def disconnected_cb():
            nonlocal disconnected_count
            disconnected_count += 1

        async def closed_cb():
            nonlocal closed_future
            closed_future.set_result(True)

        async def err_cb(e):
            nonlocal errors
            errors.append(e)

        options = {
            'dont_randomize': True,
            'reconnect_time_wait': 0.5,
            'disconnected_cb': disconnected_cb,
            'error_cb': err_cb,
            'closed_cb': closed_cb,
            'servers': [
                "nats://foo:bar@127.0.0.1:4223",
                "nats://hoge:fuga@127.0.0.1:4224",
                "nats://hello:world@127.0.0.1:4225"
            ],
            'max_reconnect_attempts': 3,
            'dont_randomize': True
        }

        await nc.connect(**options)
        self.assertIn('auth_required', nc._server_info)
        self.assertTrue(nc._server_info['auth_required'])
        self.assertTrue(nc.is_connected)

        # Check number of nodes in the server pool.
        self.assertEqual(3, len(nc._server_pool))

        # Stop all servers so that there aren't any available to reconnect
        # then start one of them again.
        await self.loop.run_in_executor(None, self.server_pool[1].stop)
        await self.loop.run_in_executor(None, self.server_pool[0].stop)
        for i in range(0, 10):
            await asyncio.sleep(0)
            await asyncio.sleep(0.1)
            await asyncio.sleep(0)

        self.assertTrue(len(errors) > 0)
        self.assertFalse(nc.is_connected)
        self.assertEqual(3, len(nc._server_pool))
        self.assertEqual(ConnectionRefusedError, type(nc.last_error))

        # Restart one of the servers and confirm we are reconnected
        # even after many tries from small reconnect_time_wait.
        await self.loop.run_in_executor(None, self.server_pool[1].start)
        for i in range(0, 10):
            await asyncio.sleep(0)
            await asyncio.sleep(0.1)
            await asyncio.sleep(0)

        # Stop the server once again
        await self.loop.run_in_executor(None, self.server_pool[1].stop)
        for i in range(0, 10):
            await asyncio.sleep(0)
            await asyncio.sleep(0.1)
            await asyncio.sleep(0)

        # Only reconnected succesfully once to the same server.
        self.assertTrue(nc.stats['reconnects'] == 1)
        self.assertEqual(1, len(nc._server_pool))

        # await nc.close()
        if not closed_future.done():
            await asyncio.wait_for(closed_future, 2)

        self.assertEqual(0, len(nc._server_pool))
        self.assertTrue(nc.is_closed)
        self.assertEqual(ErrNoServers, type(nc.last_error))

    @async_test
    async def test_closing_tasks(self):
        nc = NATS()

        disconnected_count = 0
        errors = []
        closed_future = asyncio.Future()

        async def disconnected_cb():
            nonlocal disconnected_count
            disconnected_count += 1

        async def closed_cb():
            nonlocal closed_future
            closed_future.set_result(True)

        async def err_cb(e):
            nonlocal errors
            errors.append(e)

        options = {
            'dont_randomize': True,
            'reconnect_time_wait': 0.5,
            'disconnected_cb': disconnected_cb,
            'error_cb': err_cb,
            'closed_cb': closed_cb,
            'servers': ["nats://foo:bar@127.0.0.1:4223"],
            'max_reconnect_attempts': 3,
            'dont_randomize': True,
        }

        await nc.connect(**options)
        self.assertTrue(nc.is_connected)

        # Do a sudden close and wrap up test.
        await nc.close()

        # There should be only a couple of tasks remaining related
        # to the handling of the currently running test.
        expected_tasks = 2
        pending_tasks_count = 0
        for task in asyncio.all_tasks():
            if not task.done():
                pending_tasks_count += 1
        self.assertEqual(expected_tasks, pending_tasks_count)

    @async_test
    async def test_pending_data_size_flush_reconnect(self):
        nc = NATS()

        disconnected_count = 0
        reconnected_count = 0
        closed_count = 0
        err_count = 0

        async def disconnected_cb():
            nonlocal disconnected_count
            disconnected_count += 1

        async def reconnected_cb():
            nonlocal reconnected_count
            reconnected_count += 1

        async def closed_cb():
            nonlocal closed_count
            closed_count += 1

        options = {
            'servers': [
                "nats://foo:bar@127.0.0.1:4223",
                "nats://hoge:fuga@127.0.0.1:4224"
            ],
            'dont_randomize': True,
            'disconnected_cb': disconnected_cb,
            'closed_cb': closed_cb,
            'reconnected_cb': reconnected_cb,
            'reconnect_time_wait': 0.01
        }
        await nc.connect(**options)
        largest_pending_data_size = 0
        post_flush_pending_data = None
        done_once = False

        async def cb(msg):
            pass

        await nc.subscribe("example.*", cb=cb)

        for i in range(0, 200):
            await nc.publish(f"example.{i}", b'A' * 20)
            if nc.pending_data_size > 0:
                largest_pending_data_size = nc.pending_data_size
            if nc.pending_data_size > 100:
                # Stop the first server and connect to another one asap.
                if not done_once:
                    await nc.flush(2)
                    post_flush_pending_data = nc.pending_data_size
                    await self.loop.run_in_executor(
                        None, self.server_pool[0].stop
                    )
                    done_once = True

        self.assertTrue(largest_pending_data_size > 0)
        self.assertTrue(post_flush_pending_data == 0)

        # Confirm we have reconnected eventually
        for i in range(0, 10):
            await asyncio.sleep(0)
            await asyncio.sleep(0.2)
            await asyncio.sleep(0)
        self.assertEqual(1, nc.stats['reconnects'])
        try:
            await nc.flush(2)
        except ErrTimeout:
            # If disconnect occurs during this flush, then we will have a timeout here
            pass
        finally:
            await nc.close()

        self.assertTrue(disconnected_count >= 1)
        self.assertTrue(closed_count >= 1)

    @async_test
    async def test_custom_flush_queue_reconnect(self):
        nc = NATS()

        disconnected_count = 0
        reconnected_count = 0
        closed_count = 0
        err_count = 0

        async def disconnected_cb():
            nonlocal disconnected_count
            disconnected_count += 1

        async def reconnected_cb():
            nonlocal reconnected_count
            reconnected_count += 1

        async def closed_cb():
            nonlocal closed_count
            closed_count += 1

        options = {
            'servers': [
                "nats://foo:bar@127.0.0.1:4223",
                "nats://hoge:fuga@127.0.0.1:4224"
            ],
            'dont_randomize': True,
            'disconnected_cb': disconnected_cb,
            'closed_cb': closed_cb,
            'reconnected_cb': reconnected_cb,
            'flusher_queue_size': 100,
            'reconnect_time_wait': 0.01
        }
        await nc.connect(**options)
        largest_pending_data_size = 0
        post_flush_pending_data = None
        done_once = False

        async def cb(msg):
            pass

        await nc.subscribe("example.*", cb=cb)

        for i in range(0, 500):
            await nc.publish(f"example.{i}", b'A' * 20)
            if nc.pending_data_size > 0:
                largest_pending_data_size = nc.pending_data_size
            if nc.pending_data_size > 100:
                # Stop the first server and connect to another one asap.
                if not done_once:
                    await nc.flush(2)
                    post_flush_pending_data = nc.pending_data_size
                    await self.loop.run_in_executor(
                        None, self.server_pool[0].stop
                    )
                    done_once = True

        self.assertTrue(largest_pending_data_size > 0)
        self.assertTrue(post_flush_pending_data == 0)

        # Confirm we have reconnected eventually
        for i in range(0, 10):
            await asyncio.sleep(0)
            await asyncio.sleep(0.2)
            await asyncio.sleep(0)
        self.assertEqual(1, nc.stats['reconnects'])
        try:
            await nc.flush(2)
        except ErrTimeout:
            # If disconnect occurs during this flush, then we will have a timeout here
            pass
        finally:
            await nc.close()

        self.assertTrue(disconnected_count >= 1)
        self.assertTrue(closed_count >= 1)

    @async_test
    async def test_auth_reconnect(self):
        nc = NATS()
        disconnected_count = 0
        reconnected_count = 0
        closed_count = 0
        err_count = 0

        async def disconnected_cb():
            nonlocal disconnected_count
            disconnected_count += 1

        async def reconnected_cb():
            nonlocal reconnected_count
            reconnected_count += 1

        async def closed_cb():
            nonlocal closed_count
            closed_count += 1

        async def err_cb(e):
            nonlocal err_count
            err_count += 1

        counter = 0

        async def worker_handler(msg):
            nonlocal counter
            counter += 1
            if msg.reply != "":
                await nc.publish(msg.reply, f'Reply:{counter}'.encode())

        options = {
            'servers': [
                "nats://foo:bar@127.0.0.1:4223",
                "nats://hoge:fuga@127.0.0.1:4224"
            ],
            'disconnected_cb': disconnected_cb,
            'closed_cb': closed_cb,
            'reconnected_cb': reconnected_cb,
            'error_cb': err_cb,
            'dont_randomize': True,
        }
        await nc.connect(**options)
        self.assertTrue(nc.is_connected)

        await nc.subscribe("one", cb=worker_handler)
        await nc.subscribe("two", cb=worker_handler)
        await nc.subscribe("three", cb=worker_handler)

        response = await nc.request("one", b'Help!', timeout=1)
        self.assertEqual(b'Reply:1', response.data)

        # Stop the first server and connect to another one asap.
        await self.loop.run_in_executor(None, self.server_pool[0].stop)

        # FIXME: Find better way to wait for the server to be stopped.
        await asyncio.sleep(0.5)

        response = await nc.request("three", b'Help!', timeout=1)
        self.assertEqual(b'Reply:2', response.data)
        await asyncio.sleep(0.5)
        await nc.close()
        self.assertEqual(1, nc.stats['reconnects'])
        self.assertEqual(1, closed_count)
        self.assertEqual(2, disconnected_count)
        self.assertEqual(1, reconnected_count)
        self.assertEqual(1, err_count)


class ClientAuthTokenTest(MultiServerAuthTokenTestCase):
    @async_test
    async def test_connect_with_auth_token(self):
        nc = NATS()

        options = {'servers': ["nats://token@127.0.0.1:4223", ]}
        await nc.connect(**options)
        self.assertIn('auth_required', nc._server_info)
        self.assertTrue(nc.is_connected)
        await nc.close()
        self.assertTrue(nc.is_closed)
        self.assertFalse(nc.is_connected)

    @async_test
    async def test_connect_with_auth_token_option(self):
        nc = NATS()

        options = {
            'servers': ["nats://127.0.0.1:4223", ],
            'token': "token",
        }
        await nc.connect(**options)
        self.assertIn('auth_required', nc._server_info)
        self.assertTrue(nc.is_connected)
        await nc.close()
        self.assertTrue(nc.is_closed)
        self.assertFalse(nc.is_connected)

    @async_test
    async def test_connect_with_bad_auth_token(self):
        nc = NATS()

        options = {
            'servers': ["nats://token@127.0.0.1:4225", ],
            'allow_reconnect': False,
            'reconnect_time_wait': 0.1,
            'max_reconnect_attempts': 1,
        }
        # Authorization Violation
        with self.assertRaises(NatsError):
            await nc.connect(**options)

        self.assertIn('auth_required', nc._server_info)
        self.assertFalse(nc.is_connected)

    @async_test
    async def test_reconnect_with_auth_token(self):
        nc = NATS()

        disconnected_count = 0
        reconnected_count = 0
        closed_count = 0
        err_count = 0

        async def disconnected_cb():
            nonlocal disconnected_count
            disconnected_count += 1

        async def reconnected_cb():
            nonlocal reconnected_count
            reconnected_count += 1

        async def closed_cb():
            nonlocal closed_count
            closed_count += 1

        counter = 0

        async def worker_handler(msg):
            nonlocal counter
            counter += 1
            if msg.reply != "":
                await nc.publish(msg.reply, f'Reply:{counter}'.encode())

        options = {
            'servers': [
                "nats://token@127.0.0.1:4223",
                "nats://token@127.0.0.1:4224",
            ],
            'disconnected_cb': disconnected_cb,
            'closed_cb': closed_cb,
            'reconnected_cb': reconnected_cb,
            'dont_randomize': True
        }
        await nc.connect(**options)
        await nc.subscribe("test", cb=worker_handler)
        self.assertIn('auth_required', nc._server_info)
        self.assertTrue(nc.is_connected)

        # Trigger a reconnnect
        await self.loop.run_in_executor(None, self.server_pool[0].stop)
        await asyncio.sleep(1)

        await nc.subscribe("test", cb=worker_handler)
        response = await nc.request("test", b'data', timeout=1)
        self.assertEqual(b'Reply:1', response.data)

        await nc.close()
        self.assertTrue(nc.is_closed)
        self.assertFalse(nc.is_connected)
        self.assertEqual(1, closed_count)
        self.assertEqual(2, disconnected_count)
        self.assertEqual(1, reconnected_count)


class ClientTLSTest(TLSServerTestCase):
    @async_test
    async def test_connect(self):
        nc = NATS()
        await nc.connect(servers=['nats://127.0.0.1:4224'], tls=self.ssl_ctx)
        self.assertEqual(nc._server_info['max_payload'], nc.max_payload)
        self.assertTrue(nc._server_info['tls_required'])
        self.assertTrue(nc._server_info['tls_verify'])
        self.assertTrue(nc.max_payload > 0)
        self.assertTrue(nc.is_connected)
        await nc.close()
        self.assertTrue(nc.is_closed)
        self.assertFalse(nc.is_connected)

    @async_test
    async def test_default_connect_using_tls_scheme(self):
        nc = NATS()

        # Will attempt to connect using TLS with default certs.
        with self.assertRaises(ssl.SSLError):
            await nc.connect(
                servers=['tls://127.0.0.1:4224'], allow_reconnect=False
            )

    @async_test
    async def test_default_connect_using_tls_scheme_in_url(self):
        nc = NATS()

        # Will attempt to connect using TLS with default certs.
        with self.assertRaises(ssl.SSLError):
            await nc.connect('tls://127.0.0.1:4224', allow_reconnect=False)

    @async_test
    async def test_connect_tls_with_custom_hostname(self):
        nc = NATS()

        # Will attempt to connect using TLS with an invalid hostname.
        with self.assertRaises(ssl.SSLError):
            await nc.connect(
                servers=['nats://127.0.0.1:4224'],
                tls=self.ssl_ctx,
                tls_hostname="nats.example",
                allow_reconnect=False,
            )

    @async_test
    async def test_subscribe(self):
        nc = NATS()
        msgs = []

        async def subscription_handler(msg):
            msgs.append(msg)

        payload = b'hello world'
        await nc.connect(servers=['nats://127.0.0.1:4224'], tls=self.ssl_ctx)
        sub = await nc.subscribe("foo", cb=subscription_handler)
        await nc.publish("foo", payload)
        await nc.publish("bar", payload)

        with self.assertRaises(ErrBadSubject):
            await nc.publish("", b'')

        # Wait a bit for message to be received.
        await asyncio.sleep(0.2)

        self.assertEqual(1, len(msgs))
        msg = msgs[0]
        self.assertEqual('foo', msg.subject)
        self.assertEqual('', msg.reply)
        self.assertEqual(payload, msg.data)
        self.assertEqual(1, sub._received)
        await nc.close()


class ClientTLSReconnectTest(MultiTLSServerAuthTestCase):
    @async_test
    async def test_tls_reconnect(self):

        nc = NATS()
        disconnected_count = 0
        reconnected_count = 0
        closed_count = 0
        err_count = 0

        async def disconnected_cb():
            nonlocal disconnected_count
            disconnected_count += 1

        async def reconnected_cb():
            nonlocal reconnected_count
            reconnected_count += 1

        async def closed_cb():
            nonlocal closed_count
            closed_count += 1

        async def err_cb(e):
            nonlocal err_count
            err_count += 1

        counter = 0

        async def worker_handler(msg):
            nonlocal counter
            counter += 1
            if msg.reply != "":
                await nc.publish(msg.reply, f'Reply:{counter}'.encode())

        options = {
            'servers': [
                "nats://foo:bar@127.0.0.1:4223",
                "nats://hoge:fuga@127.0.0.1:4224"
            ],
            'disconnected_cb': disconnected_cb,
            'closed_cb': closed_cb,
            'reconnected_cb': reconnected_cb,
            'error_cb': err_cb,
            'dont_randomize': True,
            'tls': self.ssl_ctx
        }
        await nc.connect(**options)
        self.assertTrue(nc.is_connected)

        await nc.subscribe("example", cb=worker_handler)
        response = await nc.request("example", b'Help!', timeout=1)
        self.assertEqual(b'Reply:1', response.data)

        # Trigger a reconnnect and should be fine
        await self.loop.run_in_executor(None, self.server_pool[0].stop)
        await asyncio.sleep(1)

        await nc.subscribe("example", cb=worker_handler)
        response = await nc.request("example", b'Help!', timeout=1)
        self.assertEqual(b'Reply:2', response.data)

        await nc.close()
        self.assertTrue(nc.is_closed)
        self.assertFalse(nc.is_connected)
        self.assertEqual(1, nc.stats['reconnects'])
        self.assertEqual(1, closed_count)
        self.assertEqual(2, disconnected_count)
        self.assertEqual(1, reconnected_count)
        self.assertEqual(1, err_count)


class ClusterDiscoveryTest(ClusteringTestCase):
    @async_test
    async def test_discover_servers_on_first_connect(self):
        nc = NATS()

        # Start rest of cluster members so that we receive them
        # connect_urls on the first connect.
        await self.loop.run_in_executor(None, self.server_pool[1].start)
        await asyncio.sleep(1)
        await self.loop.run_in_executor(None, self.server_pool[2].start)
        await asyncio.sleep(1)

        options = {'servers': ["nats://127.0.0.1:4223", ]}

        discovered_server_cb = mock.Mock()

        with mock.patch('asyncio.iscoroutinefunction', return_value=True):
            await nc.connect(
                **options, discovered_server_cb=discovered_server_cb
            )
        self.assertTrue(nc.is_connected)
        await nc.close()
        self.assertTrue(nc.is_closed)
        self.assertEqual(len(nc.servers), 3)
        self.assertEqual(len(nc.discovered_servers), 2)
        self.assertEqual(discovered_server_cb.call_count, 0)

    @async_test
    async def test_discover_servers_after_first_connect(self):
        nc = NATS()

        options = {'servers': ["nats://127.0.0.1:4223", ]}
        discovered_server_cb = mock.Mock()
        with mock.patch('asyncio.iscoroutinefunction', return_value=True):
            await nc.connect(
                **options, discovered_server_cb=discovered_server_cb
            )

        # Start rest of cluster members so that we receive them
        # connect_urls on the first connect.
        await self.loop.run_in_executor(None, self.server_pool[1].start)
        await asyncio.sleep(1)
        await self.loop.run_in_executor(None, self.server_pool[2].start)
        await asyncio.sleep(1)

        await nc.close()
        self.assertTrue(nc.is_closed)
        self.assertEqual(len(nc.servers), 3)
        self.assertEqual(len(nc.discovered_servers), 2)
        self.assertEqual(discovered_server_cb.call_count, 2)


class ClusterDiscoveryReconnectTest(ClusteringDiscoveryAuthTestCase):
    @async_test
    async def test_reconnect_to_new_server_with_auth(self):
        nc = NATS()
        errors = []
        reconnected = asyncio.Future()

        async def reconnected_cb():
            nonlocal reconnected
            reconnected.set_result(True)

        async def err_cb(e):
            print(e)
            nonlocal errors
            errors.append(e)

        options = {
            'servers': ["nats://127.0.0.1:4223", ],
            'reconnected_cb': reconnected_cb,
            'error_cb': err_cb,
            'reconnect_time_wait': 0.1,
            'user': "foo",
            'password': "bar",
        }
        await nc.connect(**options)

        # Wait for cluster to assemble...
        await asyncio.sleep(1)

        async def handler(msg):
            await nc.publish(msg.reply, b'ok')

        await nc.subscribe("foo", cb=handler)

        # Remove first member and try to reconnect
        await self.loop.run_in_executor(None, self.server_pool[0].stop)
        await asyncio.wait_for(reconnected, 2)

        msg = await nc.request("foo", b'hi')
        self.assertEqual(b'ok', msg.data)

        await nc.close()
        self.assertTrue(nc.is_closed)
        self.assertEqual(len(nc.servers), 3)
        self.assertEqual(len(nc.discovered_servers), 2)


class ConnectFailuresTest(SingleServerTestCase):
    @async_test
    async def test_empty_info_op_uses_defaults(self):
        async def bad_server(reader, writer):
            writer.write(b'INFO {}\r\n')
            await writer.drain()

            data = await reader.readline()
            await asyncio.sleep(0.2)
            writer.close()

        await asyncio.start_server(bad_server, '127.0.0.1', 4555)

        disconnected_count = 0

        async def disconnected_cb():
            nonlocal disconnected_count
            disconnected_count += 1

        nc = NATS()
        options = {
            'servers': ["nats://127.0.0.1:4555", ],
            'disconnected_cb': disconnected_cb
        }
        await nc.connect(**options)
        self.assertEqual(nc.max_payload, 1048576)

        await nc.close()
        self.assertEqual(1, disconnected_count)

    @async_test
    async def test_empty_response_from_server(self):
        async def bad_server(reader, writer):
            writer.write(b'')
            await asyncio.sleep(0.2)
            writer.close()

        await asyncio.start_server(bad_server, '127.0.0.1', 4555)

        errors = []

        async def error_cb(e):
            nonlocal errors
            errors.append(e)

        nc = NATS()
        options = {
            'servers': ["nats://127.0.0.1:4555", ],
            'error_cb': error_cb,
            'allow_reconnect': False,
        }

        with self.assertRaises(NatsError):
            await nc.connect(**options)
        self.assertEqual(1, len(errors))
        self.assertEqual(errors[0], nc.last_error)

    @async_test
    async def test_malformed_info_response_from_server(self):
        async def bad_server(reader, writer):
            writer.write(b'INF')
            await asyncio.sleep(0.2)
            writer.close()

        await asyncio.start_server(bad_server, '127.0.0.1', 4555)

        errors = []

        async def error_cb(e):
            nonlocal errors
            errors.append(e)

        nc = NATS()
        options = {
            'servers': ["nats://127.0.0.1:4555", ],
            'error_cb': error_cb,
            'allow_reconnect': False,
        }

        with self.assertRaises(NatsError):
            await nc.connect(**options)
        self.assertEqual(1, len(errors))
        self.assertEqual(errors[0], nc.last_error)

    @async_test
    async def test_malformed_info_json_response_from_server(self):
        async def bad_server(reader, writer):
            writer.write(b'INFO {\r\n')
            await asyncio.sleep(0.2)
            writer.close()

        await asyncio.start_server(bad_server, '127.0.0.1', 4555)

        errors = []

        async def error_cb(e):
            nonlocal errors
            errors.append(e)

        nc = NATS()
        options = {
            'servers': ["nats://127.0.0.1:4555", ],
            'error_cb': error_cb,
            'allow_reconnect': False,
        }

        with self.assertRaises(NatsError):
            await nc.connect(**options)
        self.assertEqual(1, len(errors))
        self.assertEqual(errors[0], nc.last_error)
        await asyncio.sleep(0.5)

    @async_test
    async def test_connect_timeout(self):
        async def slow_server(reader, writer):
            await asyncio.sleep(1)
            writer.close()

        await asyncio.start_server(slow_server, '127.0.0.1', 4555)

        disconnected_count = 0
        reconnected = asyncio.Future()

        async def disconnected_cb():
            nonlocal disconnected_count
            disconnected_count += 1

        async def reconnected_cb():
            nonlocal reconnected
            reconnected.set_result(True)

        nc = NATS()
        options = {
            'servers': ["nats://127.0.0.1:4555", ],
            'disconnected_cb': disconnected_cb,
            'reconnected_cb': reconnected_cb,
            'connect_timeout': 0.5,
            'dont_randomize': True,
            'allow_reconnect': False,
        }

        with self.assertRaises(asyncio.TimeoutError):
            await nc.connect(**options)

        await nc.close()
        await asyncio.sleep(0.5)
        self.assertEqual(1, disconnected_count)

    @async_test
    async def test_connect_timeout_then_connect_to_healthy_server(self):
        async def slow_server(reader, writer):
            await asyncio.sleep(1)
            writer.close()

        await asyncio.start_server(slow_server, '127.0.0.1', 4555)

        disconnected_count = 0
        reconnected = asyncio.Future()

        async def disconnected_cb():
            nonlocal disconnected_count
            disconnected_count += 1

        async def reconnected_cb():
            nonlocal reconnected
            reconnected.set_result(True)

        errors = []

        async def error_cb(e):
            nonlocal errors
            errors.append(e)

        nc = NATS()
        options = {
            'servers': [
                "nats://127.0.0.1:4555",
                "nats://127.0.0.1:4222",
            ],
            'disconnected_cb': disconnected_cb,
            'reconnected_cb': reconnected_cb,
            'error_cb': error_cb,
            'connect_timeout': 0.5,
            'dont_randomize': True,
        }

        await nc.connect(**options)

        # Should have reconnected to healthy server.
        self.assertTrue(nc.is_connected)

        for i in range(0, 10):
            await nc.publish("foo", b'ok ok')
        await nc.flush()
        await nc.close()

        self.assertEqual(1, len(errors))
        self.assertTrue(type(errors[0]) is asyncio.TimeoutError)
        await asyncio.sleep(0.5)
        self.assertEqual(1, disconnected_count)


class ClientDrainTest(SingleServerTestCase):
    @async_test
    async def test_drain_subscription(self):
        nc = NATS()

        future = asyncio.Future()

        async def closed_cb():
            nonlocal future
            future.set_result(True)

        await nc.connect(closed_cb=closed_cb)

        await nc.drain()

        # Should be closed after draining
        await asyncio.wait_for(future, 1)

        self.assertTrue(nc.is_closed)
        self.assertFalse(nc.is_connected)

    @async_test
    async def test_drain_single_subscription(self):
        nc = NATS()
        await nc.connect()

        msgs = []

        # Should be replying the request response...
        async def handler(msg):
            nonlocal msgs
            msgs.append(msg)
            if len(msgs) == 10:
                await asyncio.sleep(0.5)

        sub = await nc.subscribe("foo", cb=handler)

        for i in range(0, 200):
            await nc.publish("foo", b'hi')

            # Relinquish control so that messages are processed.
            await asyncio.sleep(0)
        await nc.flush()

        before_drain = sub._pending_queue.qsize()
        self.assertTrue(before_drain > 0)

        # TODO: Calling double drain on the same sub should be prevented?
        drain_task = sub.drain()
        await asyncio.wait_for(drain_task, 1)

        for i in range(0, 200):
            await nc.publish("foo", b'hi')

            # Relinquish control so that messages are processed.
            await asyncio.sleep(0)

        # No more messages should have been processed.
        after_drain = sub._pending_queue.qsize()
        self.assertEqual(0, after_drain)
        self.assertEqual(200, len(msgs))

        await nc.close()
        self.assertTrue(nc.is_closed)
        self.assertFalse(nc.is_connected)

    @async_test
    async def test_drain_subscription_with_future(self):
        nc = NATS()
        await nc.connect()

        fut = asyncio.Future()
        sub = await nc.subscribe("foo", future=fut)

        await nc.publish("foo", b'hi')
        await nc.flush()

        drain_task = sub.drain()
        await asyncio.wait_for(drain_task, 1)
        await asyncio.wait_for(fut, 1)
        msg = fut.result()
        self.assertEqual(msg.subject, "foo")
        await nc.close()

    @async_test
    async def test_drain_connection(self):
        drain_done = asyncio.Future()

        nc = NATS()
        errors = []

        async def error_cb(e):
            nonlocal errors
            errors.append(e)

        async def closed_cb():
            nonlocal drain_done
            drain_done.set_result(True)

        await nc.connect(closed_cb=closed_cb, error_cb=error_cb)

        nc2 = NATS()
        await nc2.connect()

        msgs = []

        async def handler(msg):
            if len(msgs) % 20 == 1:
                await asyncio.sleep(0.2)
            await nc.publish(msg.reply, b'OK!', reply=msg.subject)
            await nc2.flush()

        sub_foo = await nc.subscribe("foo", cb=handler)
        sub_bar = await nc.subscribe("bar", cb=handler)
        sub_quux = await nc.subscribe("quux", cb=handler)

        async def replies(msg):
            nonlocal msgs
            msgs.append(msg)

        await nc2.subscribe("my-replies.*", cb=replies)
        for i in range(0, 201):
            await nc2.publish(
                "foo",
                b'help',
                reply="my-replies.%s" % nc._nuid.next().decode()
            )
            await nc2.publish(
                "bar",
                b'help',
                reply="my-replies.%s" % nc._nuid.next().decode()
            )
            await nc2.publish(
                "quux",
                b'help',
                reply="my-replies.%s" % nc._nuid.next().decode()
            )

            # Relinquish control so that messages are processed.
            await asyncio.sleep(0)
        await nc2.flush()

        self.assertTrue(sub_foo._pending_queue.qsize() > 0)
        self.assertTrue(sub_bar._pending_queue.qsize() > 0)
        self.assertTrue(sub_quux._pending_queue.qsize() > 0)

        # Drain and close the connection. In case of timeout then
        # an async error will be emitted via the error callback.
        task = self.loop.create_task(nc.drain())

        # Let the draining task a bit of time to run...
        await asyncio.sleep(0.1)

        with self.assertRaises(ErrConnectionDraining):
            await nc.subscribe("hello", cb=handler)

        # Should be no-op or bail if connection closed.
        await nc.drain()

        # State should be closed here already,
        await asyncio.wait_for(task, 5)
        await asyncio.wait_for(drain_done, 5)

        self.assertEqual(sub_foo._pending_queue.qsize(), 0)
        self.assertEqual(sub_bar._pending_queue.qsize(), 0)
        self.assertEqual(sub_quux._pending_queue.qsize(), 0)
        self.assertEqual(0, len(nc._subs.items()))
        self.assertEqual(1, len(nc2._subs.items()))
        self.assertTrue(len(msgs) > 599)

        # No need to close since drain reaches the closed state.
        # await nc.close()
        await nc2.close()
        self.assertTrue(nc.is_closed)
        self.assertFalse(nc.is_connected)
        self.assertTrue(nc2.is_closed)
        self.assertFalse(nc2.is_connected)

    @async_test
    async def test_drain_connection_timeout(self):
        drain_done = asyncio.Future()

        nc = NATS()
        errors = []

        async def error_cb(e):
            nonlocal errors
            errors.append(e)

        async def closed_cb():
            nonlocal drain_done
            drain_done.set_result(True)

        await nc.connect(
            closed_cb=closed_cb, error_cb=error_cb, drain_timeout=0.1
        )

        nc2 = NATS()
        await nc2.connect()

        msgs = []

        async def handler(msg):
            if len(msgs) % 20 == 1:
                await asyncio.sleep(0.2)
            if len(msgs) % 50 == 1:
                await asyncio.sleep(0.5)
            await nc.publish(msg.reply, b'OK!', reply=msg.subject)
            await nc2.flush()

        await nc.subscribe("foo", cb=handler)
        await nc.subscribe("bar", cb=handler)
        await nc.subscribe("quux", cb=handler)

        async def replies(msg):
            nonlocal msgs
            msgs.append(msg)

        await nc2.subscribe("my-replies.*", cb=replies)
        for i in range(0, 201):
            await nc2.publish(
                "foo",
                b'help',
                reply="my-replies.%s" % nc._nuid.next().decode()
            )
            await nc2.publish(
                "bar",
                b'help',
                reply="my-replies.%s" % nc._nuid.next().decode()
            )
            await nc2.publish(
                "quux",
                b'help',
                reply="my-replies.%s" % nc._nuid.next().decode()
            )

            # Relinquish control so that messages are processed.
            await asyncio.sleep(0)
        await nc2.flush()

        # Drain and close the connection. In case of timeout then
        # an async error will be emitted via the error callback.
        await nc.drain()
        self.assertTrue(errors[0] is ErrDrainTimeout)

        # No need to close since drain reaches the closed state.
        # await nc.close()
        await nc2.close()
        self.assertTrue(nc.is_closed)
        self.assertFalse(nc.is_connected)
        self.assertTrue(nc2.is_closed)
        self.assertFalse(nc2.is_connected)

    @async_test
    async def test_non_async_callbacks_raise_error(self):
        nc = NATS()

        def f():
            pass

        for cb in ['error_cb', 'disconnected_cb', 'discovered_server_cb',
                   'closed_cb', 'reconnected_cb']:

            with self.assertRaises(ErrInvalidCallbackType):
                await nc.connect(
                    servers=["nats://127.0.0.1:4222"],
                    max_reconnect_attempts=2,
                    reconnect_time_wait=0.2,
                    **{cb: f}
                )


if __name__ == '__main__':
    runner = unittest.TextTestRunner(stream=sys.stdout)
    unittest.main(verbosity=2, exit=False, testRunner=runner)
