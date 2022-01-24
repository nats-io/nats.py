import asyncio
import sys
import unittest

from nats.aio.client import Subscription
from nats.errors import ProtocolError
from nats.protocol.parser import *
from tests.utils import async_test


class MockNatsClient:

    def __init__(self):
        self._subs = {}
        self._pongs = []
        self._pings_outstanding = 0
        self._pongs_received = 0
        self._server_info = {"max_payload": 1048576, "auth_required": False}
        self._server_pool = []

    async def _send_command(self, cmd):
        pass

    async def _process_pong(self):
        pass

    async def _process_ping(self):
        pass

    async def _process_msg(self, sid, subject, reply, payload, headers=None):
        sub = self._subs[sid]
        await sub._cb(sid, subject, reply, payload)

    async def _process_err(self, err=None):
        pass

    def _process_info(self, info):
        self._server_info = info


class ProtocolParserTest(unittest.TestCase):

    def setUp(self):
        self.loop = asyncio.new_event_loop()

    @async_test
    async def test_parse_ping(self):
        ps = Parser(MockNatsClient())
        data = b'PING\r\n'
        await ps.parse(data)
        self.assertEqual(len(ps.buf), 0)
        self.assertEqual(ps.state, AWAITING_CONTROL_LINE)

    @async_test
    async def test_parse_pong(self):
        ps = Parser(MockNatsClient())
        data = b'PONG\r\n'
        await ps.parse(data)
        self.assertEqual(len(ps.buf), 0)
        self.assertEqual(ps.state, AWAITING_CONTROL_LINE)

    @async_test
    async def test_parse_ok(self):
        ps = Parser()
        data = b'+OK\r\n'
        await ps.parse(data)
        self.assertEqual(len(ps.buf), 0)
        self.assertEqual(ps.state, AWAITING_CONTROL_LINE)

    @async_test
    async def test_parse_msg(self):
        nc = MockNatsClient()
        expected = b'hello world!'

        async def payload_test(sid, subject, reply, payload):
            self.assertEqual(payload, expected)

        params = {
            "subject": "hello",
            "queue": None,
            "cb": payload_test,
            "future": None,
        }
        sub = Subscription(nc, 1, **params)
        nc._subs[1] = sub
        ps = Parser(nc)
        data = b'MSG hello 1 world 12\r\n'

        await ps.parse(data)
        self.assertEqual(len(ps.buf), 0)
        self.assertEqual(len(ps.msg_arg.keys()), 3)
        self.assertEqual(ps.msg_arg["subject"], b'hello')
        self.assertEqual(ps.msg_arg["reply"], b'world')
        self.assertEqual(ps.msg_arg["sid"], 1)
        self.assertEqual(ps.needed, 12)
        self.assertEqual(ps.state, AWAITING_MSG_PAYLOAD)

        await ps.parse(b'hello world!')
        self.assertEqual(len(ps.buf), 12)
        self.assertEqual(ps.state, AWAITING_MSG_PAYLOAD)

        data = b'\r\n'
        await ps.parse(data)
        self.assertEqual(len(ps.buf), 0)
        self.assertEqual(ps.state, AWAITING_CONTROL_LINE)

    @async_test
    async def test_parse_msg_op(self):
        ps = Parser()
        data = b'MSG hello'
        await ps.parse(data)
        self.assertEqual(len(ps.buf), 9)
        self.assertEqual(ps.state, AWAITING_CONTROL_LINE)

    @async_test
    async def test_parse_split_msg_op(self):
        ps = Parser()
        data = b'MSG'
        await ps.parse(data)
        self.assertEqual(len(ps.buf), 3)
        self.assertEqual(ps.state, AWAITING_CONTROL_LINE)

    @async_test
    async def test_parse_split_msg_op_space(self):
        ps = Parser()
        data = b'MSG '
        await ps.parse(data)
        self.assertEqual(len(ps.buf), 4)
        self.assertEqual(ps.state, AWAITING_CONTROL_LINE)

    @async_test
    async def test_parse_split_msg_op_wrong_args(self):
        ps = Parser(MockNatsClient())
        data = b'MSG PONG\r\n'
        with self.assertRaises(ProtocolError):
            await ps.parse(data)

    @async_test
    async def test_parse_err_op(self):
        ps = Parser(MockNatsClient())
        data = b"-ERR 'Slow "
        await ps.parse(data)
        self.assertEqual(len(ps.buf), 11)
        self.assertEqual(ps.state, AWAITING_CONTROL_LINE)

        data = b"Consumer'\r\n"
        await ps.parse(data)
        self.assertEqual(len(ps.buf), 0)
        self.assertEqual(ps.state, AWAITING_CONTROL_LINE)

    @async_test
    async def test_parse_err(self):
        ps = Parser(MockNatsClient())
        data = b"-ERR 'Slow Consumer'\r\n"
        await ps.parse(data)
        self.assertEqual(len(ps.buf), 0)
        self.assertEqual(ps.state, AWAITING_CONTROL_LINE)

    @async_test
    async def test_parse_info(self):
        nc = MockNatsClient()
        ps = Parser(nc)
        server_id = 'A' * 2048
        data = f'INFO {{"server_id": "{server_id}", "max_payload": 100, "auth_required": false, "connect_urls":["127.0.0.0.1:4223"]}}\r\n'
        await ps.parse(data.encode())
        self.assertEqual(len(ps.buf), 0)
        self.assertEqual(ps.state, AWAITING_CONTROL_LINE)
        self.assertEqual(len(nc._server_info['server_id']), 2048)

    @async_test
    async def test_parse_msg_long_subject_reply(self):
        nc = MockNatsClient()

        msgs = 0

        async def payload_test(sid, subject, reply, payload):
            nonlocal msgs
            msgs += 1

        params = {
            "subject": "hello",
            "queue": None,
            "cb": payload_test,
            "future": None,
        }
        sub = Subscription(nc, 1, **params)
        nc._subs[1] = sub

        ps = Parser(nc)
        reply = 'A' * 2043
        data = f'PING\r\nMSG hello 1 {reply}'
        await ps.parse(data.encode())
        await ps.parse(b'AAAAA 0\r\n\r\nMSG hello 1 world 0')
        self.assertEqual(msgs, 1)
        self.assertEqual(len(ps.buf), 19)
        self.assertEqual(ps.state, AWAITING_CONTROL_LINE)
        await ps.parse(b'\r\n\r\n')
        self.assertEqual(msgs, 2)

    @async_test
    async def test_parse_unknown_long_protocol_line(self):
        nc = MockNatsClient()

        msgs = 0

        async def payload_test(sid, subject, reply, payload):
            nonlocal msgs
            msgs += 1

        params = {
            "subject": "hello",
            "queue": None,
            "cb": payload_test,
            "future": None,
        }
        sub = Subscription(nc, 1, **params)
        nc._subs[1] = sub

        ps = Parser(nc)
        reply = 'A' * 2043

        # FIXME: Malformed long protocol lines will not be detected
        # by the client, so we rely on the ping/pong interval
        # from the client to give up instead.
        data = f'PING\r\nWRONG hello 1 {reply}'
        await ps.parse(data.encode())
        await ps.parse(b'AAAAA 0')
        self.assertEqual(ps.state, AWAITING_CONTROL_LINE)
        await ps.parse(b'\r\n\r\n')
        await ps.parse(b'\r\n\r\n')
