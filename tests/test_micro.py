import asyncio
import json
import random

import nats
from nats import micro

import unittest
from unittest import TestCase
from nats.micro import Verb
from tests.utils import SingleServerTestCase, async_test


class ServiceTest(SingleServerTestCase):

    @async_test
    async def test_service_basics(self):
        nc = await nats.connect()

        async def do_add(req):
            await req.respond(b"42")

        svcs = []

        # Create 5 service responders.
        for i in range(5):
            svc = micro.add_service(
                nc,
                name="CoolAddService",
                version="0.1.0",
                description="Add things together",
                metadata={"basic": "metadata"},
            )

            await svc.add_endpoint(
                name="svc.add",
                subject="svc.add",
                handler=do_add,
            )

            await svc.start()

            svcs.append(svc)

        # Now send 50 requests.
        for i in range(50):
            msg = await nc.request(
                "svc.add", b'{ "x": 22, "y": 11 }', timeout=5
            )

            self.assertIsNotNone(msg)

        for svc in svcs:
            info = svc.info()
            self.assertEqual(info.name, "CoolAddService")
            self.assertEqual(info.description, "Add things together")
            self.assertEqual(info.version, "0.1.0")
            self.assertDictEqual(info.metadata, {"basic": "metadata"})

        # Make sure we can request info, 1 response.
        subj = micro.control_subject(
            micro.Verb.INFO,
            "CoolAddService",
            None,
        )

        inf = await nc.request(subj, b'')
        info = json.loads(inf.data)

        self.assertEqual(info["name"], "CoolAddService")
        self.assertEqual(info["description"], "Add things together")

        # Ping all services. Multiple responses.
        inbox = nc.new_inbox()
        sub = await nc.subscribe(inbox)
        info_subject = micro.control_subject(
            micro.Verb.PING, "CoolAddService", ""
        )

        await nc.publish(info_subject, reply=inbox)

        ping_count = 0
        while True:
            try:
                msg = await sub.next_msg()
                ping_count += 1
            except Exception:
                break

        self.assertEqual(ping_count, len(svcs))

        # Get stats from all services
        stats_inbox = nc.new_inbox()
        sub = await nc.subscribe(stats_inbox)
        stats_subject = micro.control_subject(
            micro.Verb.STATS, "CoolAddService", ""
        )

        await nc.publish(stats_subject, reply=stats_inbox)

        stats = []
        requests_num = 0

        while True:
            try:
                msg = await sub.next_msg()
                srv_stats = json.loads(msg.data)
                requests_num += srv_stats['endpoints'][0]['num_requests']
                stats.append(srv_stats)
            except Exception:
                break

        self.assertEqual(len(stats), 5)
        self.assertEqual(requests_num, 50)

        # Reset stats for a service
        svcs[0].reset()
        self.assertEqual(svcs[0].stats().endpoints[0].num_requests, 0)

        for svc in svcs:
            await svc.stop()

    @async_test
    async def test_groups(self):
        tests = [
            {
                "name": "no groups",
                "endpoint_name": "foo",
                "groups": None,
                "expected_endpoint":
                    micro.api.EndpointInfo(
                        name="foo",
                        subject="foo",
                        queue_group="q",
                        metadata={}
                    )
            },
            {
                "name": "single group",
                "endpoint_name": "foo",
                "groups": ["g1"],
                "expected_endpoint":
                    micro.api.EndpointInfo(
                        name="foo",
                        subject="g1.foo",
                        queue_group="q",
                        metadata={}
                    )
            },
            # {
            #     "name": "single empty group",
            #     "endpoint_name": "foo",
            #     "groups": [""],
            #     "expected_endpoint": micro.api.EndpointInfo(
            #         name="foo",
            #         subject="foo",
            #         queue_group="q",
            #         metadata={}
            #     )
            # },
            # {
            #     "name": "empty groups",
            #     "endpoint_name": "foo",
            #     "groups": ["", "g1", ""],
            #     "expected_endpoint": micro.api.EndpointInfo(
            #         name="foo",
            #         subject="g1.foo",
            #         queue_group="q",
            #         metadata={}
            #     )
            # },
            {
                "name": "multiple groups",
                "endpoint_name": "foo",
                "groups": ["g1", "g2", "g3"],
                "expected_endpoint":
                    micro.api.EndpointInfo(
                        name="foo",
                        subject="g1.g2.g3.foo",
                        queue_group="q",
                        metadata={}
                    )
            },
        ]

        # TODO(capervonb): empty groups are not supported yet
        async def handler(req):
            await req.respond(b"")

        for test in tests:
            with self.subTest(test['name']):
                nc = await nats.connect()
                srv = micro.add_service(
                    nc, name="test_service", version="0.0.1"
                )
                await srv.start()

                if test['groups']:
                    group = srv.add_group(test['groups'][0])
                    for g in test['groups'][1:]:
                        group = group.add_group(g)
                    await group.add_endpoint(test['endpoint_name'], handler)
                else:
                    await srv.add_endpoint(test['endpoint_name'], handler)

                info = srv.info()
                self.assertEqual(len(info.endpoints), 1)
                self.assertEqual(info.endpoints[0], test['expected_endpoint'])

                await srv.stop()

    @async_test
    async def test_monitoring_handlers(self):
        nc = await nats.connect()

        async def handler(req):
            await req.respond(b"")

        scv = micro.add_service(nc, name="test_service", version="0.0.1")
        await scv.add_endpoint("test.fn", handler)
        await scv.start()

        info = scv.info()

        subjects = {
            "$SRV.PING": {
                'id': info.id,
                'name': 'test_service',
                'metadata': {},
                'type': 'io.nats.micro.v1.ping_response',
                'version': '0.0.1',
            },
            f"$SRV.PING.{info.name}": {
                'id': info.id,
                'name': 'test_service',
                'metadata': {},
                'type': 'io.nats.micro.v1.ping_response',
                'version': '0.0.1',
            },
            f"$SRV.PING.test_service.{info.id}": {
                'id': info.id,
                'name': 'test_service',
                'metadata': {},
                'type': 'io.nats.micro.v1.ping_response',
                'version': '0.0.1',
            },
            "$SRV.INFO": {
                'id': info.id,
                'description': '',
                'endpoints': [{
                    'metadata': {},
                    'name': 'test.fn',
                    'queue_group': 'q',
                    'subject': 'test.fn'
                }],
                'name': 'test_service',
                'metadata': {},
                'type': 'io.nats.micro.v1.info_response',
                'version': '0.0.1',
            },
            f"$SRV.INFO.{info.name}": {
                'id': info.id,
                'description': '',
                'endpoints': [{
                    'metadata': {},
                    'name': 'test.fn',
                    'queue_group': 'q',
                    'subject': 'test.fn'
                }],
                'name': 'test_service',
                'metadata': {},
                'type': 'io.nats.micro.v1.info_response',
                'version': '0.0.1',
            },
            f"$SRV.INFO.test_service.{info.id}": {
                'id': info.id,
                'description': '',
                'endpoints': [{
                    'metadata': {},
                    'name': 'test.fn',
                    'queue_group': 'q',
                    'subject': 'test.fn'
                }],
                'name': 'test_service',
                'metadata': {},
                'type': 'io.nats.micro.v1.info_response',
                'version': '0.0.1',
            },
        }

        for subject, expected_response in subjects.items():
            with self.subTest(subject):
                msg = await nc.request(subject, b"")
                response = json.loads(msg.data)
                assertEqual = self.assertEqual(response, expected_response)

        await scv.stop()

    @async_test
    async def test_service_stats(self):
        nc = await nats.connect()
        scv = micro.add_service(nc, name="test_service", version="0.0.1")

        def handler(req: micro.Request):
            return req.respond(b"ok")

        await scv.add_endpoint(
            name="test.func", subject="test.func", handler=handler
        )
        await scv.start()

        # Send 10 requests to the endpoint
        for _ in range(10):
            await nc.request("test.func", b"msg", timeout=1)

        # Send a malformed request (no reply subject)
        await nc.publish("test.func", b"err")

        # Allow some time for the error to be processed
        await asyncio.sleep(1)

        info = scv.info()

        resp = await nc.request(
            micro.control_subject(micro.Verb.STATS, "test_service", info.id)
        )
        stats = micro.api.ServiceStats.from_dict(json.loads(resp.data))

        self.assertEqual(len(stats.endpoints), 1)
        self.assertEqual(stats.name, info.name)
        self.assertEqual(stats.id, info.id)

        self.assertEqual(stats.endpoints[0].name, "test.func")
        self.assertEqual(stats.endpoints[0].subject, "test.func")
        self.assertEqual(stats.endpoints[0].num_requests, 11)
        self.assertEqual(stats.endpoints[0].num_errors, 1)

        self.assertGreater(stats.endpoints[0].average_processing_time, 0)
        self.assertGreater(stats.endpoints[0].processing_time, 0)

        self.assertIsNotNone(stats.started)

        await scv.stop()


class TestControlSubject(unittest.TestCase):

    def test_ping_all(self):
        result = micro.control_subject(micro.Verb.PING, None, None)
        self.assertEqual(result, "$SRV.PING")

    def test_ping_name(self):
        result = micro.control_subject(micro.Verb.PING, "test", None)
        self.assertEqual(result, "$SRV.PING.test")

    def test_ping_id(self):
        result = micro.control_subject(micro.Verb.PING, "test", "123")
        self.assertEqual(result, "$SRV.PING.test.123")

    def test_invalid_verb(self):
        with self.assertRaises(ValueError):
            micro.control_subject(micro.Verb("bar"), None, None)
