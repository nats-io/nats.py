"""PullSubscribe constructor/lifecycle regressions against an independent Go probe."""

import asyncio
import shutil
import socket
from copy import deepcopy
from unittest.mock import AsyncMock

import pytest

import nats
from nats.aio.client import Client
from nats.js import api
from nats.js.errors import BadRequestError, NotFoundError, ServiceUnavailableError
from nats.js.manager import JetStreamManager
from tests.utils import NATSD, start_natsd


@pytest.fixture(scope="module")
def server():
    with socket.socket() as listener, socket.socket() as monitor:
        listener.bind(("127.0.0.1", 0))
        monitor.bind(("127.0.0.1", 0))
        port, http_port = listener.getsockname()[1], monitor.getsockname()[1]
    server = NATSD(port=port, http_port=http_port, with_jetstream=True)
    try:
        start_natsd(server)
        assert server.proc.poll() is None, "test server failed to start"
        yield server
    finally:
        server.stop()
        shutil.rmtree(server.store_dir)


@pytest.fixture
async def js(server):
    nc = await nats.connect(f"nats://127.0.0.1:{server.port}")
    js = nc.jetstream()
    await js.add_stream(name="PULL", subjects=["events", "other"])
    try:
        yield js
    finally:
        await js.delete_stream("PULL")
        await nc.close()


# Measured with nats.go f666d4365aaf7b1d9e598bd04f8a5048790ba2ec;
# see oracles/pull_subscribe.go. Enum names use their Python wire values here.
@pytest.mark.parametrize(
    "label, expected",
    [
        pytest.param(
            "ack_mismatch",
            {
                "consumer_not_found": False,
                "error": "nats: configuration requests ack policy to be none, but consumer's value is explicit",
            },
            id="ack_mismatch",
        ),
        pytest.param(
            "all_subjects",
            {"ack_policy": "explicit", "durable": "", "filter_subject": "", "retained": False},
            id="all_subjects",
        ),
        pytest.param(
            "bound",
            {"ack_policy": "explicit", "durable": "worker", "filter_subject": "events", "retained": True},
            id="bound",
        ),
        pytest.param(
            "config_durable",
            {"ack_policy": "explicit", "durable": "worker", "filter_subject": "events", "retained": False},
            id="config_durable",
        ),
        pytest.param(
            "durable",
            {"ack_policy": "explicit", "durable": "worker", "filter_subject": "events", "retained": False},
            id="durable",
        ),
        pytest.param(
            "ephemeral",
            {"ack_policy": "explicit", "durable": "", "filter_subject": "events", "retained": False},
            id="ephemeral",
        ),
        pytest.param(
            "existing",
            {"ack_policy": "explicit", "durable": "worker", "filter_subject": "events", "retained": True},
            id="existing",
        ),
        pytest.param(
            "max_waiting_mismatch",
            {
                "consumer_not_found": False,
                "error": "nats: configuration requests max waiting to be 10, but consumer's value is 20",
            },
            id="max_waiting_mismatch",
        ),
        pytest.param(
            "missing_bound", {"consumer_not_found": True, "error": "nats: consumer not found"}, id="missing_bound"
        ),
        pytest.param(
            "named",
            {"ack_policy": "explicit", "durable": "", "filter_subject": "events", "name": "named", "retained": False},
            id="named",
        ),
        pytest.param(
            "push_config",
            {"consumer_not_found": False, "error": "nats: cannot pull subscribe to push based consumer"},
            id="push_config",
        ),
        pytest.param(
            "push_existing",
            {"consumer_not_found": False, "error": "nats: cannot pull subscribe to push based consumer"},
            id="push_existing",
        ),
        pytest.param(
            "subject_mismatch",
            {"consumer_not_found": False, "error": "nats: subject does not match consumer"},
            id="subject_mismatch",
        ),
        pytest.param(
            "subject_required", {"consumer_not_found": False, "error": "nats: subject required"}, id="subject_required"
        ),
    ],
)
async def test_go_observations(js, label, expected):
    subject, durable, config = "events", None, None
    stream = "PULL"
    if label in ("existing", "bound", "subject_mismatch", "push_existing", "ack_mismatch", "max_waiting_mismatch"):
        await js.add_consumer(
            stream,
            config=api.ConsumerConfig(
                durable_name="worker",
                filter_subject="events",
                deliver_subject="delivery" if label == "push_existing" else None,
                max_waiting=None if label == "push_existing" else 20,
            ),
        )
        durable = "worker"
    if label == "durable":
        durable = "worker"
    elif label == "config_durable":
        config = api.ConsumerConfig(durable_name="worker")
    elif label == "named":
        config = api.ConsumerConfig(name="named")
    elif label == "all_subjects":
        subject = ""
    elif label == "subject_required":
        subject, stream = "", None
    elif label == "subject_mismatch":
        subject = "other"
    elif label == "push_config":
        config = api.ConsumerConfig(deliver_subject="delivery")
    elif label == "ack_mismatch":
        config = api.ConsumerConfig(ack_policy=api.AckPolicy.NONE)
    elif label == "max_waiting_mismatch":
        config = api.ConsumerConfig(max_waiting=10)
    observation = {}
    try:
        if label in ("bound", "missing_bound"):
            sub = await js.pull_subscribe_bind("missing" if label == "missing_bound" else "worker", stream=stream)
        else:
            sub = await js.pull_subscribe(subject, durable, stream=stream, config=config)
    except (ValueError, NotFoundError) as err:
        # API exception classes remain Python-native; compare the Go sentinel separately.
        missing = isinstance(err, NotFoundError) and err.err_code == 10014
        observation = {"error": "nats: consumer not found" if missing else str(err), "consumer_not_found": missing}
    else:
        info = await sub.consumer_info()
        observation = {
            "durable": info.config.durable_name or "",
            "ack_policy": info.config.ack_policy,
            "filter_subject": info.config.filter_subject or "",
        }
        if label == "named":
            observation["name"] = info.name
        await sub.unsubscribe()
        try:
            await js.consumer_info("PULL", info.name)
        except NotFoundError as err:
            assert err.err_code == 10014
            observation["retained"] = False
        else:
            observation["retained"] = True
    assert observation == expected


@pytest.mark.parametrize(
    "kwargs, message",
    [
        ({"subject": "events", "durable": "bad.name"}, "invalid consumer name"),
        ({"subject": "events", "config": api.ConsumerConfig(name="bad name")}, "invalid consumer name"),
        ({"subject": "events", "stream": "bad.stream"}, "invalid stream name"),
        (
            {"subject": "events", "durable": "a", "config": api.ConsumerConfig(durable_name="b")},
            "duplicate consumer names",
        ),
        ({"subject": ""}, "subject required"),
        ({"subject": "events", "config": api.ConsumerConfig(deliver_subject="delivery")}, "push based consumer"),
    ],
)
async def test_preflight_without_io(kwargs, message, monkeypatch):
    nc = Client()
    js = nc.jetstream()
    request = AsyncMock(side_effect=AssertionError("unexpected API request"))
    monkeypatch.setattr(JetStreamManager, "_api_request", request)
    nc.subscribe = AsyncMock(side_effect=AssertionError("unexpected subscription"))
    with pytest.raises(ValueError, match=message):
        await js.pull_subscribe(**kwargs)
    request.assert_not_awaited()
    nc.subscribe.assert_not_awaited()


@pytest.mark.parametrize(
    "kwargs",
    [
        {"stream": "PULL"},
        {"consumer": "worker"},
        {"stream": "PULL", "consumer": "bad.name"},
        {"stream": "PULL", "consumer": "a", "durable": "b"},
        {"stream": "PULL", "name": "a", "durable": "b"},
    ],
)
async def test_bind_preflight_without_io(kwargs, monkeypatch):
    nc = Client()
    js = nc.jetstream()
    lookup = AsyncMock(side_effect=AssertionError("unexpected lookup"))
    monkeypatch.setattr(JetStreamManager, "consumer_info", lookup)
    nc.subscribe = AsyncMock(side_effect=AssertionError("unexpected subscription"))
    with pytest.raises(ValueError):
        await js.pull_subscribe_bind(**kwargs)
    lookup.assert_not_awaited()
    nc.subscribe.assert_not_awaited()


async def test_config_reuse_and_fetch_lifecycle(js):
    config = api.ConsumerConfig(max_waiting=10, metadata={"owner": "test"})
    original = deepcopy(config)
    first = await js.pull_subscribe("events", "worker", config=config)
    second = await js.pull_subscribe("events", "otherworker", config=config)
    assert config == original
    await js.publish("events", b"payload")
    for sub in (first, second):
        msgs = await sub.fetch(timeout=1)
        assert [msg.data for msg in msgs] == [b"payload"]
        await msgs[0].ack_sync()
        with pytest.raises(nats.errors.TimeoutError):
            await sub.fetch(timeout=0.05)
        await sub.unsubscribe()
        with pytest.raises(nats.errors.BadSubscriptionError):
            await sub.fetch(timeout=1)
        with pytest.raises(nats.errors.BadSubscriptionError):
            await sub.unsubscribe()
    assert (await js.stream_info("PULL")).state.consumer_count == 0


async def test_matching_durable_config_and_existing_owner(js):
    owner = await js.pull_subscribe("events", "worker", config=api.ConsumerConfig(durable_name="worker"))
    attached = await js.pull_subscribe("events", "worker")
    await attached.unsubscribe()
    await owner.consumer_info()
    await owner.unsubscribe()
    with pytest.raises(NotFoundError):
        await js.consumer_info("PULL", "worker")


async def test_bound_aliases_and_push_rejection(js):
    await js.add_consumer("PULL", durable_name="worker", filter_subject="events")
    for kwargs in (
        {"consumer": "worker"},
        {"name": "worker"},
        {"durable": "worker"},
        {"consumer": "worker", "name": "worker", "durable": "worker"},
    ):
        sub = await js.pull_subscribe_bind(stream="PULL", **kwargs)
        await sub.unsubscribe()
        await js.consumer_info("PULL", "worker")
    await js.add_consumer("PULL", durable_name="push", deliver_subject="delivery")
    before = len(js._nc._subs)
    with pytest.raises(ValueError, match="push based consumer"):
        await js.pull_subscribe_bind("push", stream="PULL")
    assert len(js._nc._subs) == before


async def test_failed_creation_cleans_inbox_and_config(js):
    config = api.ConsumerConfig(flow_control=True)
    original = deepcopy(config)
    before = len(js._nc._subs)
    with pytest.raises(BadRequestError):
        await js.pull_subscribe("events", "invalid", config=config)
    assert len(js._nc._subs) == before
    assert config == original
    assert (await js.stream_info("PULL")).state.consumer_count == 0


async def test_missing_stream_does_not_create(js, monkeypatch):
    create = AsyncMock(side_effect=AssertionError("unexpected create"))
    monkeypatch.setattr(JetStreamManager, "add_consumer", create)
    with pytest.raises(NotFoundError) as error:
        await js.pull_subscribe("events", "worker", stream="MISSING")
    assert error.value.err_code == 10059
    create.assert_not_awaited()


@pytest.mark.parametrize("error", [nats.errors.TimeoutError(), ServiceUnavailableError()])
async def test_binding_unavailable_lookup_retains_consumer(js, monkeypatch, error):
    await js.add_consumer("PULL", durable_name="worker", filter_subject="events")
    with monkeypatch.context() as patch:
        patch.setattr(JetStreamManager, "consumer_info", AsyncMock(side_effect=error))
        sub = await js.pull_subscribe_bind("worker", stream="PULL")
    await js.publish("events", b"message")
    assert (await sub.fetch(timeout=1))[0].data == b"message"
    await sub.unsubscribe()
    await js.consumer_info("PULL", "worker")


async def test_connection_close_retains_created_consumer(js, server):
    nc = await nats.connect(f"nats://127.0.0.1:{server.port}")
    await nc.jetstream().pull_subscribe("events", "worker", stream="PULL")
    await nc.close()
    await js.consumer_info("PULL", "worker")


async def test_creation_uses_returned_consumer_name(js, monkeypatch):
    add_consumer = JetStreamManager.add_consumer

    async def assign_name(manager, stream, config):
        config = deepcopy(config)
        config.name = "assigned"
        return await add_consumer(manager, stream, config=config)

    monkeypatch.setattr(JetStreamManager, "add_consumer", assign_name)
    sub = await js.pull_subscribe("events", stream="PULL")
    assert (await sub.consumer_info()).name == "assigned"
    await js.publish("events", b"payload")
    assert (await sub.fetch(timeout=1))[0].data == b"payload"
    await sub.unsubscribe()
    with pytest.raises(NotFoundError):
        await js.consumer_info("PULL", "assigned")


async def test_cancelled_setup_cleans_inbox(js, monkeypatch):
    monkeypatch.setattr(JetStreamManager, "add_consumer", AsyncMock(side_effect=asyncio.CancelledError))
    before = len(js._nc._subs)
    with pytest.raises(asyncio.CancelledError):
        await js.pull_subscribe("events", stream="PULL")
    assert len(js._nc._subs) == before


async def test_omitted_and_explicit_policy_defaults(js):
    await js.add_consumer("PULL", durable_name="worker", ack_policy=api.AckPolicy.NONE, filter_subject="events")
    sub = await js.pull_subscribe("events", "worker")
    await sub.unsubscribe()
    with pytest.raises(ValueError, match="configuration requests ack policy"):
        await js.pull_subscribe("events", "worker", config=api.ConsumerConfig())
    sub = await js.pull_subscribe("events", "worker", config=api.ConsumerConfig(ack_policy=None))
    await sub.unsubscribe()
    await js.consumer_info("PULL", "worker")
