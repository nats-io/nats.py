import sys
import os
import time
import json
import asyncio
import subprocess
import unittest
import http.client

from tests.utils import Gnatsd, NatsTestCase, SingleServerTestCase, async_test
from nats.aio.client import __version__
from nats.aio.client import Client as NATS
from nats.aio.utils  import new_inbox, INBOX_PREFIX
from nats.aio.errors import (ErrConnectionClosed, ErrNoServers)

class ClientUtilsTest(NatsTestCase):

  def test_default_connect_command(self):
    nc = NATS()
    nc.options["verbose"] = False
    nc.options["pedantic"] = False
    nc.options["auth_required"] = False
    nc.options["name"] = None
    got = nc._connect_command()
    expected = 'CONNECT {"lang": "python3", "pedantic": false, "verbose": false, "version": "%s"}\r\n' % __version__
    self.assertEqual(expected.encode(), got)

  def test_default_connect_command_with_name(self):
    nc = NATS()
    nc.options["verbose"] = False
    nc.options["pedantic"] = False
    nc.options["auth_required"] = False
    nc.options["name"] = "secret"
    got = nc._connect_command()
    expected = 'CONNECT {"lang": "python3", "name": "secret", "pedantic": false, "verbose": false, "version": "%s"}\r\n' % __version__
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
    self.assertEqual(nc._server_info['max_payload'], nc._max_payload)
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
  def test_publish_1B_messages(self):
    nc = NATS()
    yield from nc.connect(io_loop=self.loop)
    for i in range(0, 100):
      yield from nc.publish("hello.%d" % i, b'A')
    yield from nc.close()
    self.assertEqual(100, nc.stats['out_msgs'])
    self.assertEqual(100, nc.stats['out_bytes'])

    endpoint = '127.0.0.1:{port}'.format(port=self.server_pool[0].http_port)
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

    # Wait a bit for message to be received.
    yield from asyncio.sleep(0.2, loop=self.loop)
    yield from nc.close()

    self.assertEqual(1, len(msgs))
    msg = msgs[0]
    self.assertEqual('foo', msg.subject)
    self.assertEqual('', msg.reply)
    self.assertEqual(payload, msg.data)
    self.assertEqual(1, nc._subs[sid].received)

    self.assertEqual(1,  nc.stats['in_msgs'])
    self.assertEqual(11, nc.stats['in_bytes'])
    self.assertEqual(2,  nc.stats['out_msgs'])
    self.assertEqual(22, nc.stats['out_bytes'])

    endpoint = '127.0.0.1:{port}'.format(port=self.server_pool[0].http_port)
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

    yield from nc.close()
    self.assertEqual(2, nc.stats['in_msgs'])
    self.assertEqual(2, nc.stats['in_bytes'])
    self.assertEqual(4, nc.stats['out_msgs'])
    self.assertEqual(4, nc.stats['out_bytes'])

    endpoint = '127.0.0.1:{port}'.format(port=self.server_pool[0].http_port)
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

  @async_test
  def test_closed_status(self):
    nc = NATS()
    yield from nc.connect(io_loop=self.loop)
    yield from nc.close()

    with self.assertRaises(ErrConnectionClosed):
      yield from nc.publish("foo", b'A')

    with self.assertRaises(ErrConnectionClosed):
      yield from nc.subscribe("bar", "workers")

    with self.assertRaises(ErrConnectionClosed):
      yield from nc.publish_request("bar", "inbox", b'B')

    with self.assertRaises(ErrConnectionClosed):
      yield from nc.flush()

if __name__ == '__main__':
  runner = unittest.TextTestRunner(stream=sys.stdout)
  unittest.main(verbosity=2, exit=False, testRunner=runner)
