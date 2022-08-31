import asyncio
import http.client
import os
import shutil
import signal
import ssl
import subprocess
import tempfile
import time
import unittest
from functools import wraps
from pathlib import Path

try:
    import uvloop
    asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
except:
    pass

THIS_DIR = os.path.dirname(os.path.abspath(__file__))
SERVER_BIN_DIR_NAME = "nats-server"


class NATSD:

    def __init__(
        self,
        port=4222,
        user="",
        password="",
        token="",
        timeout=0,
        http_port=8222,
        debug=False,
        tls=False,
        cluster_listen=None,
        routes=None,
        config_file=None,
        with_jetstream=None,
    ):
        self.port = port
        self.user = user
        self.password = password
        self.timeout = timeout
        self.http_port = http_port
        self.proc = None
        self.tls = tls
        self.token = token
        self.cluster_listen = cluster_listen
        self.routes = routes or []
        self.bin_name = "nats-server"
        self.config_file = config_file
        self.debug = debug or os.environ.get("DEBUG_NATS_TEST") == "true"
        self.with_jetstream = with_jetstream
        self.store_dir = None

        if with_jetstream:
            self.store_dir = tempfile.mkdtemp()

    def start(self):
        # Default path
        if Path(self.bin_name).is_file():
            self.bin_name = Path(self.bin_name).absolute()
        # Path in `../scripts/install_nats.sh`
        elif Path.home().joinpath(SERVER_BIN_DIR_NAME,
                                  self.bin_name).is_file():
            self.bin_name = str(
                Path.home().joinpath(SERVER_BIN_DIR_NAME, self.bin_name)
            )
        # This directory contains binary
        elif Path(THIS_DIR).joinpath(self.bin_name).is_file():
            self.bin_name = str(Path(THIS_DIR).joinpath(self.bin_name))

        cmd = [
            self.bin_name, "-p",
            "%d" % self.port, "-m",
            "%d" % self.http_port, "-a", "127.0.0.1"
        ]
        if self.user:
            cmd.append("--user")
            cmd.append(self.user)
        if self.password:
            cmd.append("--pass")
            cmd.append(self.password)

        if self.token:
            cmd.append("--auth")
            cmd.append(self.token)

        if self.debug:
            cmd.append("-DV")

        if self.with_jetstream:
            cmd.append("-js")
            cmd.append(f"-sd={self.store_dir}")

        if self.tls:
            cmd.append('--tls')
            cmd.append('--tlscert')
            cmd.append(get_config_file('certs/server-cert.pem'))
            cmd.append('--tlskey')
            cmd.append(get_config_file('certs/server-key.pem'))
            cmd.append('--tlsverify')
            cmd.append('--tlscacert')
            cmd.append(get_config_file('certs/ca.pem'))

        if self.cluster_listen is not None:
            cmd.append('--cluster_listen')
            cmd.append(self.cluster_listen)

        if len(self.routes) > 0:
            cmd.append('--routes')
            cmd.append(','.join(self.routes))
            cmd.append('--cluster_name')
            cmd.append('CLUSTER')

        if self.config_file is not None:
            cmd.append("--config")
            cmd.append(self.config_file)

        if self.debug:
            self.proc = subprocess.Popen(cmd)
        else:
            self.proc = subprocess.Popen(
                cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL
            )

        if self.debug:
            if self.proc is None:
                print(
                    "[\031[0;33mDEBUG\033[0;0m] Failed to start server listening on port %d started."
                    % self.port
                )
            else:
                print(
                    "[\033[0;33mDEBUG\033[0;0m] Server listening on port %d started."
                    % self.port
                )
        return self.proc

    def stop(self):
        if self.debug:
            print(
                "[\033[0;33mDEBUG\033[0;0m] Server listening on %d will stop."
                % self.port
            )

        if self.debug:
            if self.proc is None:
                print(
                    "[\033[0;31mDEBUG\033[0;0m] Failed terminating server listening on port %d"
                    % self.port
                )

        if self.proc.returncode is not None:
            if self.debug:
                print(
                    "[\033[0;31mDEBUG\033[0;0m] Server listening on port {port} finished running already with exit {ret}"
                    .format(port=self.port, ret=self.proc.returncode)
                )
        else:
            os.kill(self.proc.pid, signal.SIGKILL)
            self.proc.wait()
            if self.debug:
                print(
                    "[\033[0;33mDEBUG\033[0;0m] Server listening on %d was stopped."
                    % self.port
                )


class SingleServerTestCase(unittest.TestCase):

    def setUp(self):
        self.server_pool = []
        self.loop = asyncio.new_event_loop()

        server = NATSD(port=4222)
        self.server_pool.append(server)
        for natsd in self.server_pool:
            start_natsd(natsd)

    def tearDown(self):
        for natsd in self.server_pool:
            natsd.stop()
        self.loop.close()


class MultiServerAuthTestCase(unittest.TestCase):

    def setUp(self):
        super().setUp()
        self.server_pool = []
        self.loop = asyncio.new_event_loop()

        server1 = NATSD(port=4223, user="foo", password="bar", http_port=8223)
        self.server_pool.append(server1)
        server2 = NATSD(
            port=4224, user="hoge", password="fuga", http_port=8224
        )
        self.server_pool.append(server2)
        for natsd in self.server_pool:
            start_natsd(natsd)

    def tearDown(self):
        for natsd in self.server_pool:
            natsd.stop()
        self.loop.close()


class MultiServerAuthTokenTestCase(unittest.TestCase):

    def setUp(self):
        super().setUp()
        self.server_pool = []
        self.loop = asyncio.new_event_loop()

        server1 = NATSD(port=4223, token="token", http_port=8223)
        self.server_pool.append(server1)
        server2 = NATSD(port=4224, token="token", http_port=8224)
        self.server_pool.append(server2)
        server3 = NATSD(port=4225, token="secret", http_port=8225)
        self.server_pool.append(server3)
        for natsd in self.server_pool:
            start_natsd(natsd)

    def tearDown(self):
        for natsd in self.server_pool:
            natsd.stop()
        self.loop.close()


class TLSServerTestCase(unittest.TestCase):

    def setUp(self):
        super().setUp()
        self.loop = asyncio.new_event_loop()

        self.natsd = NATSD(port=4224, tls=True)
        start_natsd(self.natsd)

        self.ssl_ctx = ssl.create_default_context(
            purpose=ssl.Purpose.SERVER_AUTH
        )
        # self.ssl_ctx.protocol = ssl.PROTOCOL_TLSv1_2
        self.ssl_ctx.load_verify_locations(get_config_file('certs/ca.pem'))
        self.ssl_ctx.load_cert_chain(
            certfile=get_config_file('certs/client-cert.pem'),
            keyfile=get_config_file('certs/client-key.pem')
        )

    def tearDown(self):
        self.natsd.stop()
        self.loop.close()


class MultiTLSServerAuthTestCase(unittest.TestCase):

    def setUp(self):
        super().setUp()
        self.server_pool = []
        self.loop = asyncio.new_event_loop()

        server1 = NATSD(
            port=4223, user="foo", password="bar", http_port=8223, tls=True
        )
        self.server_pool.append(server1)
        server2 = NATSD(
            port=4224, user="hoge", password="fuga", http_port=8224, tls=True
        )
        self.server_pool.append(server2)
        for natsd in self.server_pool:
            start_natsd(natsd)

        self.ssl_ctx = ssl.create_default_context(
            purpose=ssl.Purpose.SERVER_AUTH
        )
        # self.ssl_ctx.protocol = ssl.PROTOCOL_TLSv1_2
        self.ssl_ctx.load_verify_locations(get_config_file('certs/ca.pem'))
        self.ssl_ctx.load_cert_chain(
            certfile=get_config_file('certs/client-cert.pem'),
            keyfile=get_config_file('certs/client-key.pem')
        )

    def tearDown(self):
        for natsd in self.server_pool:
            natsd.stop()
        self.loop.close()


class ClusteringTestCase(unittest.TestCase):

    def setUp(self):
        super().setUp()
        self.server_pool = []
        self.loop = asyncio.new_event_loop()

        routes = [
            "nats://127.0.0.1:6223", "nats://127.0.0.1:6224",
            "nats://127.0.0.1:6225"
        ]
        server1 = NATSD(
            port=4223,
            http_port=8223,
            cluster_listen="nats://127.0.0.1:6223",
            routes=routes
        )
        self.server_pool.append(server1)

        server2 = NATSD(
            port=4224,
            http_port=8224,
            cluster_listen="nats://127.0.0.1:6224",
            routes=routes
        )
        self.server_pool.append(server2)

        server3 = NATSD(
            port=4225,
            http_port=8225,
            cluster_listen="nats://127.0.0.1:6225",
            routes=routes
        )
        self.server_pool.append(server3)

        # We start with the first one only
        for natsd in self.server_pool:
            start_natsd(natsd)
            break

    def tearDown(self):
        for natsd in self.server_pool:
            try:
                natsd.stop()
            except:
                pass
        self.loop.close()


class ClusteringDiscoveryAuthTestCase(unittest.TestCase):

    def setUp(self):
        super().setUp()
        self.server_pool = []
        self.loop = asyncio.new_event_loop()

        routes = ["nats://127.0.0.1:6223"]
        server1 = NATSD(
            port=4223,
            user="foo",
            password="bar",
            http_port=8223,
            cluster_listen="nats://127.0.0.1:6223",
            routes=routes
        )
        self.server_pool.append(server1)

        server2 = NATSD(
            port=4224,
            user="foo",
            password="bar",
            http_port=8224,
            cluster_listen="nats://127.0.0.1:6224",
            routes=routes
        )
        self.server_pool.append(server2)

        server3 = NATSD(
            port=4225,
            user="foo",
            password="bar",
            http_port=8225,
            cluster_listen="nats://127.0.0.1:6225",
            routes=routes
        )
        self.server_pool.append(server3)

        for natsd in self.server_pool:
            start_natsd(natsd)

    def tearDown(self):
        for natsd in self.server_pool:
            try:
                natsd.stop()
            except:
                pass
        self.loop.close()


class NkeysServerTestCase(unittest.TestCase):

    def setUp(self):
        self.server_pool = []
        self.loop = asyncio.new_event_loop()

        server = NATSD(
            port=4222, config_file=get_config_file("nkeys/nkeys_server.conf")
        )
        self.server_pool.append(server)
        for natsd in self.server_pool:
            start_natsd(natsd)

    def tearDown(self):
        for natsd in self.server_pool:
            natsd.stop()
        self.loop.close()


class TrustedServerTestCase(unittest.TestCase):

    def setUp(self):
        super().setUp()
        self.server_pool = []
        self.loop = asyncio.new_event_loop()

        server = NATSD(
            port=4222,
            config_file=(get_config_file("nkeys/resolver_preload.conf"))
        )
        self.server_pool.append(server)
        for natsd in self.server_pool:
            start_natsd(natsd)

    def tearDown(self):
        for natsd in self.server_pool:
            natsd.stop()
        self.loop.close()


class SingleJetStreamServerTestCase(unittest.TestCase):

    def setUp(self):
        self.server_pool = []
        self.loop = asyncio.new_event_loop()

        server = NATSD(port=4222, with_jetstream=True)
        self.server_pool.append(server)
        for natsd in self.server_pool:
            start_natsd(natsd)

    def tearDown(self):
        for natsd in self.server_pool:
            natsd.stop()
            shutil.rmtree(natsd.store_dir)
        self.loop.close()


def start_natsd(natsd: NATSD):
    natsd.start()

    endpoint = f'127.0.0.1:{natsd.http_port}'
    retries = 0
    while True:
        if retries > 100:
            break

        try:
            httpclient = http.client.HTTPConnection(endpoint, timeout=5)
            httpclient.request('GET', '/varz')
            response = httpclient.getresponse()
            if response.status == 200:
                break
        except:
            retries += 1
            time.sleep(0.1)


def get_config_file(file_path):
    return os.path.join(THIS_DIR, file_path)


def async_test(test_case_fun, timeout=5):

    @wraps(test_case_fun)
    def wrapper(test_case, *args, **kw):
        asyncio.set_event_loop(test_case.loop)
        return asyncio.run(
            asyncio.wait_for(test_case_fun(test_case, *args, **kw), timeout)
        )

    return wrapper


def async_long_test(test_case_fun, timeout=10):

    @wraps(test_case_fun)
    def wrapper(test_case, *args, **kw):
        asyncio.set_event_loop(test_case.loop)
        return asyncio.run(
            asyncio.wait_for(test_case_fun(test_case, *args, **kw), timeout)
        )

    return wrapper
