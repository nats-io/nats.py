"""Tests for nats-service."""

from __future__ import annotations

import asyncio
import json

import pytest
from nats.client import Client
from nats.service import (
    DEFAULT_PREFIX,
    DEFAULT_QUEUE_GROUP,
    ERROR_CODE_HEADER,
    ERROR_HEADER,
    INFO_RESPONSE_TYPE,
    NO_QUEUE_GROUP,
    PING_RESPONSE_TYPE,
    STATS_RESPONSE_TYPE,
    Request,
    ServiceError,
    add_service,
    control_subject,
)


async def _echo(request: Request) -> None:
    await request.respond(request.data)


async def test_invalid_name(client: Client) -> None:
    with pytest.raises(ValueError):
        await add_service(client, name="", version="0.1.0")
    with pytest.raises(ValueError):
        await add_service(client, name="bad name!", version="0.1.0")


async def test_invalid_subject(client: Client) -> None:
    async with await add_service(client, name="svc", version="0.1.0") as service:
        with pytest.raises(ValueError):
            await service.add_endpoint(name="echo", handler=_echo, subject="")
        with pytest.raises(ValueError):
            await service.add_endpoint(name="echo", handler=_echo, subject="has space")
        with pytest.raises(ValueError):
            await service.add_endpoint(name="echo", handler=_echo, subject="a.>.b")


async def test_invalid_version(client: Client) -> None:
    with pytest.raises(ValueError):
        await add_service(client, name="svc", version="")
    with pytest.raises(ValueError):
        await add_service(client, name="svc", version="not-a-version")


def test_control_subject_levels() -> None:
    assert control_subject("PING") == f"{DEFAULT_PREFIX}.PING"
    assert control_subject("PING", name="svc") == f"{DEFAULT_PREFIX}.PING.svc"
    assert control_subject("PING", name="svc", id="abc") == f"{DEFAULT_PREFIX}.PING.svc.abc"


async def test_add_service_and_ping(client: Client) -> None:
    async with await add_service(client, name="svc", version="0.1.0") as service:
        response = await client.request(control_subject("PING"), b"", timeout=1.0)

    payload = json.loads(response.data)
    assert payload["type"] == PING_RESPONSE_TYPE
    assert payload["name"] == "svc"
    assert payload["id"] == service.id
    assert payload["version"] == "0.1.0"
    assert payload["metadata"] == {}


async def test_ping_responds_on_all_three_subjects(client: Client) -> None:
    async with await add_service(client, name="svc", version="0.1.0") as service:
        for subject in (
            control_subject("PING"),
            control_subject("PING", name="svc"),
            control_subject("PING", name="svc", id=service.id),
        ):
            response = await client.request(subject, b"", timeout=1.0)
            payload = json.loads(response.data)
            assert payload["id"] == service.id


async def test_info_lists_endpoints(client: Client) -> None:
    async with await add_service(client, name="svc", version="0.1.0", description="d") as service:
        await service.add_endpoint(name="echo", handler=_echo)
        response = await client.request(control_subject("INFO", name="svc"), b"", timeout=1.0)

    payload = json.loads(response.data)
    assert payload["type"] == INFO_RESPONSE_TYPE
    assert payload["description"] == "d"
    assert payload["endpoints"] == [
        {
            "name": "echo",
            "subject": "echo",
            "queue_group": DEFAULT_QUEUE_GROUP,
            "metadata": {},
        }
    ]


async def test_endpoint_handles_requests(client: Client) -> None:
    async with await add_service(client, name="svc", version="0.1.0") as service:
        await service.add_endpoint(name="echo", handler=_echo)
        response = await client.request("echo", b"hello", timeout=1.0)
        assert response.data == b"hello"

        stats = service.stats()
        assert stats.endpoints[0].num_requests == 1
        assert stats.endpoints[0].num_errors == 0
        assert stats.endpoints[0].processing_time > 0
        assert stats.endpoints[0].average_processing_time > 0


async def test_endpoint_uses_explicit_subject(client: Client) -> None:
    async with await add_service(client, name="svc", version="0.1.0") as service:
        await service.add_endpoint(name="echo", handler=_echo, subject="api.echo")
        response = await client.request("api.echo", b"hi", timeout=1.0)
        assert response.data == b"hi"


async def test_group_prefixes_subject(client: Client) -> None:
    async with await add_service(client, name="svc", version="0.1.0") as service:
        group = service.add_group("v1")
        await group.add_endpoint(name="echo", handler=_echo)
        response = await client.request("v1.echo", b"hi", timeout=1.0)
        assert response.data == b"hi"

        nested = group.add_group("admin")
        await nested.add_endpoint(name="ping", handler=_echo, subject="ping")
        response = await client.request("v1.admin.ping", b"yo", timeout=1.0)
        assert response.data == b"yo"


async def test_service_error_translates_to_headers(client: Client) -> None:
    async def boom(request: Request) -> None:
        raise ServiceError(418, "i'm a teapot")

    async with await add_service(client, name="svc", version="0.1.0") as service:
        await service.add_endpoint(name="boom", handler=boom)
        response = await client.request("boom", b"", timeout=1.0)

    assert response.headers is not None
    assert response.headers.get(ERROR_HEADER) == "i'm a teapot"
    assert response.headers.get(ERROR_CODE_HEADER) == "418"
    assert service.stats().endpoints[0].num_errors == 1


async def test_uncaught_exception_responds_with_500(client: Client) -> None:
    async def boom(request: Request) -> None:
        raise RuntimeError("oops")

    async with await add_service(client, name="svc", version="0.1.0") as service:
        await service.add_endpoint(name="boom", handler=boom)
        response = await client.request("boom", b"", timeout=1.0)

    assert response.headers is not None
    assert response.headers.get(ERROR_CODE_HEADER) == "500"
    assert response.headers.get(ERROR_HEADER) == "internal error"
    stats = service.stats()
    assert stats.endpoints[0].num_errors == 1
    assert "oops" in stats.endpoints[0].last_error


async def test_stats_payload(client: Client) -> None:
    async with await add_service(client, name="svc", version="0.1.0") as service:
        await service.add_endpoint(name="echo", handler=_echo)
        await client.request("echo", b"a", timeout=1.0)
        await client.request("echo", b"b", timeout=1.0)
        response = await client.request(control_subject("STATS"), b"", timeout=1.0)

    payload = json.loads(response.data)
    assert payload["type"] == STATS_RESPONSE_TYPE
    assert payload["started"].endswith("Z")
    assert payload["endpoints"][0]["num_requests"] == 2
    assert payload["endpoints"][0]["queue_group"] == DEFAULT_QUEUE_GROUP


async def test_stats_handler_attaches_custom_data(client: Client) -> None:
    def stats_handler(stat):  # noqa: ANN001 — exercising public typing
        return {"label": stat.name}

    async with await add_service(client, name="svc", version="0.1.0", stats_handler=stats_handler) as service:
        await service.add_endpoint(name="echo", handler=_echo)
        await client.request("echo", b"x", timeout=1.0)
        response = await client.request(control_subject("STATS"), b"", timeout=1.0)

    payload = json.loads(response.data)
    assert payload["endpoints"][0]["data"] == {"label": "echo"}


async def test_reset_clears_stats(client: Client) -> None:
    async with await add_service(client, name="svc", version="0.1.0") as service:
        await service.add_endpoint(name="echo", handler=_echo)
        await client.request("echo", b"x", timeout=1.0)
        assert service.stats().endpoints[0].num_requests == 1
        service.reset()
        assert service.stats().endpoints[0].num_requests == 0


async def test_no_queue_group_uses_normal_subscribe(client: Client) -> None:
    async with await add_service(client, name="svc", version="0.1.0", queue_group=NO_QUEUE_GROUP) as service:
        await service.add_endpoint(name="echo", handler=_echo)
        assert service.info().endpoints[0].queue_group == ""


async def test_stop_is_idempotent(client: Client) -> None:
    service = await add_service(client, name="svc", version="0.1.0")
    await service.stop()
    await service.stop()
    assert service.stopped.is_set()


async def test_stop_drains_in_flight_requests(client: Client) -> None:
    started = asyncio.Event()
    proceed = asyncio.Event()

    async def slow(request: Request) -> None:
        started.set()
        await proceed.wait()
        await request.respond(b"done")

    service = await add_service(client, name="svc", version="0.1.0")
    await service.add_endpoint(name="slow", handler=slow)

    response_task = asyncio.create_task(client.request("slow", b"", timeout=5.0))
    await started.wait()
    stop_task = asyncio.create_task(service.stop())
    await asyncio.sleep(0.05)
    proceed.set()

    response = await response_task
    await stop_task
    assert response.data == b"done"
