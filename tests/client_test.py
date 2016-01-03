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
    yield from nc.close()

if __name__ == '__main__':
  runner = unittest.TextTestRunner(stream=sys.stdout)
  unittest.main(verbosity=2, exit=False, testRunner=runner)
