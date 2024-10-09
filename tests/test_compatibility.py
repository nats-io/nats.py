from __future__ import annotations

import asyncio
import json
import os
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional
from unittest import TestCase, skipIf

import nats
from nats.aio.subscription import Subscription
from nats.micro.request import ServiceError
from nats.micro.service import (
    EndpointConfig,
    EndpointStats,
    Request,
    Service,
    ServiceConfig,
)

from .utils import *

try:
    import uvloop

    asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
except:
    pass


@skipIf("NATS_URL" not in os.environ, "NATS_URL not set in environment")
class CompatibilityTest(TestCase):

    def setUp(self):
        self.loop = asyncio.new_event_loop()

    def tearDown(self):
        self.loop.close()

    async def validate_test_result(self, sub: Subscription):
        try:
            msg = await sub.next_msg(timeout=5)
            self.assertNotIn(
                "fail", msg.subject, f"Test step failed: {msg.subject}"
            )
        except asyncio.TimeoutError:
            self.fail("Timeout waiting for test result")

    @async_long_test
    async def test_service_compatibility(self):

        @dataclass
        class TestGroupConfig:
            name: str
            queue_group: Optional[str] = None

            @classmethod
            def from_dict(cls, data: Dict[str, Any]) -> TestGroupConfig:
                return cls(
                    name=data["name"], queue_group=data.get("queue_group")
                )

        @dataclass
        class TestEndpointConfig:
            name: str
            group: Optional[str] = None
            queue_group: Optional[str] = None
            subject: Optional[str] = None
            metadata: Optional[Dict[str, str]] = None

            @classmethod
            def from_dict(cls, data: Dict[str, Any]) -> TestEndpointConfig:
                return cls(
                    name=data["name"],
                    group=data.get("group"),
                    queue_group=data.get("queue_group"),
                    subject=data.get("subject"),
                    metadata=data.get("metadata"),
                )

        @dataclass
        class TestServiceConfig:
            name: str
            version: str
            description: str
            queue_group: Optional[str] = None
            metadata: Dict[str, str] = field(default_factory=dict)
            groups: List[TestGroupConfig] = field(default_factory=list)
            endpoints: List[TestEndpointConfig] = field(default_factory=list)

            @classmethod
            def from_dict(cls, data: Dict[str, Any]) -> TestServiceConfig:
                return cls(
                    name=data["name"],
                    version=data["version"],
                    description=data["description"],
                    queue_group=data.get("queue_group"),
                    metadata=data["metadata"],
                    groups=[
                        TestGroupConfig.from_dict(group)
                        for group in data.get("groups", [])
                    ],
                    endpoints=[
                        TestEndpointConfig.from_dict(endpoint)
                        for endpoint in data.get("endpoints", [])
                    ],
                )

        @dataclass
        class TestStepConfig:
            suite: str
            test: str
            command: str
            config: TestServiceConfig

            @classmethod
            def from_dict(cls, data: Dict[str, Any]) -> TestStepConfig:
                return cls(
                    suite=data["suite"],
                    test=data["test"],
                    command=data["command"],
                    config=TestServiceConfig.from_dict(data["config"]),
                )

        async def echo_handler(request: Request):
            await request.respond(request.data)

        async def faulty_handler(request: Request):
            raise ServiceError("500", "handler error")

        def stats_handler(endpoint: EndpointStats) -> Dict[str, str]:
            return {"endpoint": endpoint.name}

        nc = await nats.connect(os.environ["NATS_URL"])
        sub = await nc.subscribe("tests.service.core.>")

        # 1. Get service and endpoint configs
        msg = await sub.next_msg(timeout=60000)
        test_step = TestStepConfig.from_dict(json.loads(msg.data))

        services = []
        service_config = ServiceConfig(
            name=test_step.config.name,
            version=test_step.config.version,
            description=test_step.config.description,
            queue_group=test_step.config.queue_group,
            metadata=test_step.config.metadata,
            stats_handler=stats_handler,
        )

        svc = Service(nc, service_config)
        await svc.start()

        groups = {}
        for group_config in test_step.config.groups:
            group = svc.add_group(
                name=group_config.name, queue_group=group_config.queue_group
            )
            groups[group_config.name] = group

        for step_endpoint_config in test_step.config.endpoints:
            handler = echo_handler
            if step_endpoint_config.name == "faulty":
                handler = faulty_handler

            endpoint_config = EndpointConfig(
                name=step_endpoint_config.name,
                subject=step_endpoint_config.subject,
                queue_group=step_endpoint_config.queue_group,
                metadata=step_endpoint_config.metadata,
                handler=handler,
            )

            group = svc
            if step_endpoint_config.group:
                group = groups[step_endpoint_config.group]

            await group.add_endpoint(endpoint_config)

        services.append(svc)

        await msg.respond(b"")

        # 2. Stop services
        msg = await sub.next_msg(timeout=3600)
        for svc in services:
            await svc.stop()

        await msg.respond(b"")

        await self.validate_test_result(sub)
