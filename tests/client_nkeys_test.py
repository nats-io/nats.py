import sys
import asyncio
import unittest
import json
import base64
import re
import nkeys

from nats.aio.client import Client as NATS
from nats.aio.errors import ErrTimeout, ErrInvalidUserCredentials
from tests.utils import (async_test, TrustedServerTestCase)

class ClientNkeysTest(TrustedServerTestCase):

    @async_test
    async def test_nkeys_jwt_creds_user_connect(self):
        nc = NATS()

        async def error_cb(e):
            print("Async Error:", e, type(e))

        await nc.connect("tls://127.0.0.1:4222",
                         loop=self.loop,
                         error_cb=error_cb,
                         connect_timeout=10,
                         user_credentials="./tests/nkeys/foo-user.creds",
                         allow_reconnect=False,
                         )

        async def help_handler(msg):
            await nc.publish(msg.reply, b'OK!')

        await nc.subscribe("help", cb=help_handler)
        await nc.flush()
        msg = await nc.request("help", b'I need help')
        self.assertEqual(msg.data, b'OK!')
        await nc.close()

    @async_test
    async def test_nkeys_jwt_creds_user_connect_tuple(self):
        nc = NATS()

        async def error_cb(e):
            print("Async Error:", e, type(e))

        await nc.connect("tls://127.0.0.1:4222",
                         loop=self.loop,
                         error_cb=error_cb,
                         connect_timeout=10,
                         user_credentials=("./tests/nkeys/foo-user.jwt", "./tests/nkeys/foo-user.nk"),
                         allow_reconnect=False,
                         )

        async def help_handler(msg):
            await nc.publish(msg.reply, b'OK!')

        await nc.subscribe("help", cb=help_handler)
        await nc.flush()
        msg = await nc.request("help", b'I need help')
        self.assertEqual(msg.data, b'OK!')
        await nc.close()

    @async_test
    async def test_nkeys_jwt_creds_bad_nkeys_connect(self):
        with self.assertRaises(ErrInvalidUserCredentials):
            nc = NATS()
            await nc.connect("tls://127.0.0.1:4222",
                             loop=self.loop,
                             connect_timeout=10,
                             user_credentials="./tests/nkeys/bad-user.creds",
                             allow_reconnect=False,
                             )

        with self.assertRaises(nkeys.ErrInvalidSeed):
            nc = NATS()
            await nc.connect("tls://127.0.0.1:4222",
                             loop=self.loop,
                             connect_timeout=10,
                             user_credentials="./tests/nkeys/bad-user2.creds",
                             allow_reconnect=False,
                             )

if __name__ == '__main__':
    runner = unittest.TextTestRunner(stream=sys.stdout)
    unittest.main(verbosity=2, exit=False, testRunner=runner)
