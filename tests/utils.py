import sys
import os
import time
import json
import signal
import asyncio
import subprocess
import unittest
import http.client
from functools import wraps

class Gnatsd(object):

  def __init__(self,
               port=4222,
               user="",
               password="",
               timeout=0,
               http_port=8222,
               debug=False
            ):
    self.port = port
    self.user = user
    self.password = password
    self.timeout = timeout
    self.http_port = http_port
    self.proc = None
    self.debug = debug

    env_debug_flag = os.environ.get("DEBUG_NATS_TEST")
    if env_debug_flag == "true":
      self.debug = True

  def start(self):
    cmd = ["gnatsd", "-p", "%d" % self.port, "-m", "%d" % self.http_port]
    if self.user != "":
      cmd.append("--user")
      cmd.append(self.user)
    if self.password != "":
      cmd.append("--pass")
      cmd.append(self.password)

    if self.debug:
      cmd.append("-DV")

    if self.debug:
      self.proc = subprocess.Popen(cmd)
    else:
      self.proc = subprocess.Popen(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

    if self.debug:
      if self.proc is None:
        print("[\031[0;33mDEBUG\033[0;0m] Failed to start server listening on port %d started." % self.port)
      else:
        print("[\033[0;33mDEBUG\033[0;0m] Server listening on port %d started." % self.port)
    return self.proc

  def stop(self):
    if self.debug:
      print("[\033[0;33mDEBUG\033[0;0m] Server listening on %d will stop." % self.port)

    if self.debug:
      if self.proc is None:
        print("[\033[0;31mDEBUG\033[0;0m] Failed terminating server listening on port %d" % self.port)

    if self.proc.returncode is not None:
      if self.debug:
        print("[\033[0;31mDEBUG\033[0;0m] Server listening on port {port} finished running already with exit {ret}".format(port=self.port, ret=self.proc.returncode))
    else:
      os.kill(self.proc.pid, signal.SIGKILL)
      self.proc.wait()
      if self.debug:
        print("[\033[0;33mDEBUG\033[0;0m] Server listening on %d was stopped." % self.port)

class NatsTestCase(unittest.TestCase):

  def setUp(self):
    print("\n=== RUN {0}.{1}".format(self.__class__.__name__, self._testMethodName))

class SingleServerTestCase(NatsTestCase):

  def setUp(self):
    super(SingleServerTestCase, self).setUp()
    self.server_pool = []
    self.loop = asyncio.new_event_loop()

    # Make sure that we are setting which loop we are using explicitly.
    asyncio.set_event_loop(None)

    server = Gnatsd(port=4222)
    self.server_pool.append(server)
    for gnatsd in self.server_pool:
      gnatsd.start()

      endpoint = '127.0.0.1:{port}'.format(port=gnatsd.http_port)
      retries = 0
      while True:
        if retries > 100:
          break

        try:
          httpclient = http.client.HTTPConnection(endpoint, timeout=5)
          httpclient.request('GET', '/varz')
          response = httpclient.getresponse()
          if response.code == 200:
            break
        except:
          retries += 1
          time.sleep(0.1)

  def tearDown(self):
    for gnatsd in self.server_pool:
      gnatsd.stop()
    self.loop.close()

class MultiServerAuthTestCase(NatsTestCase):

  def setUp(self):
    super(MultiServerAuthTestCase, self).setUp()
    self.server_pool = []
    self.loop = asyncio.new_event_loop()
    asyncio.set_event_loop(None)

    server1 = Gnatsd(port=4223, user="foo", password="bar", http_port=8223)
    self.server_pool.append(server1)
    server2 = Gnatsd(port=4224, user="hoge", password="fuga", http_port=8224)
    self.server_pool.append(server2)
    for gnatsd in self.server_pool:
      gnatsd.start()

      endpoint = '127.0.0.1:{port}'.format(port=gnatsd.http_port)
      retries = 0
      while True:
        if retries > 100:
          break

        try:
          httpclient = http.client.HTTPConnection(endpoint, timeout=5)
          httpclient.request('GET', '/varz')
          response = httpclient.getresponse()
          if response.code == 200:
            break
        except:
          retries += 1
          time.sleep(0.1)

  def tearDown(self):
    for gnatsd in self.server_pool:
      gnatsd.stop()
    self.loop.close()

def async_test(test_case_fun, timeout=5):
  @wraps(test_case_fun)
  def wrapper(test_case, *args, **kw):
    return test_case.loop.run_until_complete(
      asyncio.wait_for(test_case_fun(test_case, *args, **kw),
                       timeout,
                       loop=test_case.loop))
  return wrapper
