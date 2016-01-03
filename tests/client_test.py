import sys
import os
import time
import json
import asyncio
import subprocess
import unittest
import http.client

from tests.utils import Gnatsd, SingleServerTestCase, async_test
from nats.aio.client import Client as NATS

class ClientTest(SingleServerTestCase):

  @async_test
  def test_default_connect(self):
    nc = NATS()
    yield from nc.connect(io_loop=self.loop)
    self.assertIn('auth_required', nc._server_info)
    self.assertIn('max_payload', nc._server_info)

    self.assertEqual(nc._server_info['max_payload'],
                     nc._max_payload)

    self.assertTrue(nc.is_connected)
    yield from nc.close()
    self.assertFalse(nc.is_connected)

  @async_test
  def test_publish_1B_messages(self):
    nc = NATS()
    yield from nc.connect(io_loop=self.loop)
    for i in range(0, 100):
      yield from nc.publish("hello.%d" % i, b'A')
    yield from nc.close()
    self.assertEqual(100, nc.stats['out_msgs'])
    self.assertEqual(100, nc.stats['out_bytes'])

  @async_test
  def test_flush(self):
    nc = NATS()
    yield from nc.connect(io_loop=self.loop)
    for i in range(0, 10):
      yield from nc.publish("flush.%d" % i, b'A')
      yield from nc.flush()
    self.assertEqual(10, nc.stats['out_msgs'])
    self.assertEqual(10, nc.stats['out_bytes'])
    yield from nc.close()

if __name__ == '__main__':
  runner = unittest.TextTestRunner(stream=sys.stdout)
  unittest.main(verbosity=2, exit=False, testRunner=runner)
