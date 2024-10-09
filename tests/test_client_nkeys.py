import asyncio

import pytest

nkeys_installed = None

try:
    import nkeys

    nkeys_installed = True
except ModuleNotFoundError:
    nkeys_installed = False

from nats.aio.client import Client as NATS, RawCredentials
from nats.aio.errors import *
from nats.errors import *
from tests.utils import (
    NkeysServerTestCase,
    TrustedServerTestCase,
    async_test,
    get_config_file,
)


class ClientNkeysAuthTest(NkeysServerTestCase):

    @async_test
    async def test_nkeys_connect(self):
        import os

        config_file = get_config_file("nkeys/foo-user.nk")
        seed = None
        with open(config_file, "rb") as f:
            seed = bytearray(os.fstat(f.fileno()).st_size)
            f.readinto(seed)
        args_list = [
            {
                "nkeys_seed": config_file
            },
            {
                "nkeys_seed_str": seed.decode()
            },
        ]
        for nkeys_args in args_list:
            if not nkeys_installed:
                pytest.skip("nkeys not installed")

            nc = NATS()

            future = asyncio.Future()

            async def error_cb(e):
                nonlocal future
                future.set_result(True)

            await nc.connect(
                ["tls://127.0.0.1:4222"],
                error_cb=error_cb,
                connect_timeout=10,
                allow_reconnect=False,
                **nkeys_args,
            )

            async def help_handler(msg):
                await nc.publish(msg.reply, b"OK!")

            await nc.subscribe("help", cb=help_handler)
            await nc.flush()
            msg = await nc.request("help", b"I need help")
            self.assertEqual(msg.data, b"OK!")

            await nc.subscribe("bar", cb=help_handler)
            await nc.flush()

            await asyncio.wait_for(future, 1)

            msg = await nc.request("help", b"I need help")
            self.assertEqual(msg.data, b"OK!")

            await nc.close()


class ClientJWTAuthTest(TrustedServerTestCase):

    @async_test
    async def test_nkeys_jwt_creds_user_connect(self):
        if not nkeys_installed:
            pytest.skip("nkeys not installed")

        nc = NATS()

        async def error_cb(e):
            print("Async Error:", e, type(e))

        await nc.connect(
            ["tls://127.0.0.1:4222"],
            error_cb=error_cb,
            connect_timeout=5,
            user_credentials=get_config_file("nkeys/foo-user.creds"),
            allow_reconnect=False,
        )

        async def help_handler(msg):
            await nc.publish(msg.reply, b"OK!")

        await nc.subscribe("help", cb=help_handler)
        await nc.flush()
        msg = await nc.request("help", b"I need help")
        self.assertEqual(msg.data, b"OK!")
        await nc.close()

    @async_test
    async def test_nkeys_jwt_creds_user_connect_tuple(self):
        if not nkeys_installed:
            pytest.skip("nkeys not installed")

        nc = NATS()

        async def error_cb(e):
            print("Async Error:", e, type(e))

        await nc.connect(
            ["tls://127.0.0.1:4222"],
            error_cb=error_cb,
            connect_timeout=5,
            user_credentials=(
                get_config_file("nkeys/foo-user.jwt"),
                get_config_file("nkeys/foo-user.nk"),
            ),
            allow_reconnect=False,
        )

        async def help_handler(msg):
            await nc.publish(msg.reply, b"OK!")

        await nc.subscribe("help", cb=help_handler)
        await nc.flush()
        msg = await nc.request("help", b"I need help")
        self.assertEqual(msg.data, b"OK!")
        await nc.close()

    @async_test
    async def test_nkeys_jwt_creds_bad_nkeys_connect(self):
        if not nkeys_installed:
            pytest.skip("nkeys not installed")

        with self.assertRaises(InvalidUserCredentialsError):
            nc = NATS()
            await nc.connect(
                ["tls://127.0.0.1:4222"],
                connect_timeout=5,
                user_credentials=get_config_file("nkeys/bad-user.creds"),
                allow_reconnect=False,
            )

        with self.assertRaises(nkeys.ErrInvalidSeed):
            nc = NATS()
            await nc.connect(
                ["tls://127.0.0.1:4222"],
                connect_timeout=5,
                user_credentials=get_config_file("nkeys/bad-user2.creds"),
                allow_reconnect=False,
            )

    @async_test
    async def test_nkeys_jwt_creds_user_connect_raw_credentials(self):
        if not nkeys_installed:
            pytest.skip("nkeys not installed")

        nc = NATS()

        async def error_cb(e):
            print("Async Error:", e, type(e))

        creds = RawCredentials(
            """
-----BEGIN NATS USER JWT-----
eyJ0eXAiOiJqd3QiLCJhbGciOiJlZDI1NTE5In0.eyJqdGkiOiJXTURGT1dHV1JGWkRGRFVSM0dPUkdESEtUTTdDUlZBVDQ1RkRFMllNRUY1N0VOQ0JBVFFRIiwiaWF0IjoxNTUzODQwOTQ0LCJpc3MiOiJBRDdTRUFOUzZCQ0JGNkZISUI3U1EzVUdKVlBXNTNCWE9BTFA3NVlYSkJCWFFMN0VBRkI2TkpOQSIsIm5hbWUiOiJmb28tdXNlciIsInN1YiI6IlVDSzVON042Nk9CT0lORlhBWUMyQUNKUVlGU09ENFZZTlU2QVBFSlRBVkZaQjJTVkhMS0dFVzdMIiwidHlwZSI6InVzZXIiLCJuYXRzIjp7InB1YiI6e30sInN1YiI6e319fQ.Vri09BN561m37GvuSWoGN9L9TSkwQbjC_jIv1BCJcoxZqNc_Pa7WbR12b3SAS4_Ip2D9-2HCwyYib1JUEIO8Bg
------END NATS USER JWT------

************************* IMPORTANT *************************
NKEY Seed printed below can be used to sign and prove identity.
NKEYs are sensitive and should be treated as secrets.

-----BEGIN USER NKEY SEED-----
SUAMLK2ZNL35WSMW37E7UD4VZ7ELPKW7DHC3BWBSD2GCZ7IUQQXZIORRBU
------END USER NKEY SEED------

*************************************************************
"""
        )

        await nc.connect(
            ["tls://127.0.0.1:4222"],
            error_cb=error_cb,
            connect_timeout=5,
            user_credentials=creds,
            allow_reconnect=False,
        )

        async def help_handler(msg):
            await nc.publish(msg.reply, b"OK!")

        await nc.subscribe("help", cb=help_handler)
        await nc.flush()
        msg = await nc.request("help", b"I need help")
        self.assertEqual(msg.data, b"OK!")
        await nc.close()
