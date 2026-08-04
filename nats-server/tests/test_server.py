"""
Tests for the nats.server module.
"""

import asyncio
import json
import os
from pathlib import Path
from typing import NotRequired, TypedDict
from urllib.parse import urlparse

import pytest
from nats.server import Server, ServerCluster, ServerError, run, run_cluster


class ServerInfo(TypedDict):
    """Information about a NATS server as reported in the INFO protocol message.

    See: https://docs.nats.io/reference/reference-protocols/nats-protocol#info
    """

    # Required fields
    server_id: str
    server_name: str
    version: str
    go: str
    host: str
    port: int
    headers: bool
    max_payload: int
    proto: int

    # Optional fields
    client_id: NotRequired[int]  # uint64 in protocol
    auth_required: NotRequired[bool]
    tls_required: NotRequired[bool]
    tls_verify: NotRequired[bool]
    tls_available: NotRequired[bool]
    connect_urls: NotRequired[list[str]]
    ws_connect_urls: NotRequired[list[str]]
    ldm: NotRequired[bool]  # Lame Duck Mode
    git_commit: NotRequired[str]
    jetstream: NotRequired[bool]
    ip: NotRequired[str]
    client_ip: NotRequired[str]
    nonce: NotRequired[str]
    cluster: NotRequired[str]
    domain: NotRequired[str]


async def fetch_server_info(server: Server) -> ServerInfo:
    """Fetch server info from a running NATS server.

    Args:
        server: The server instance to fetch info from.

    Returns:
        dict: The server info as a dictionary.
    """
    parsed = urlparse(server.client_url)

    reader, writer = await asyncio.open_connection(
        host=parsed.hostname,
        port=parsed.port,
    )

    data = await reader.readline()
    info = json.loads(data[5:])

    writer.close()
    await writer.wait_closed()

    return info


async def test_run_with_default_port():
    """Test the run function with the default port (4222)."""
    # Start with default port (no port specified)
    server = await run()

    # Verify the server is using the default port
    assert server.port == 4222
    assert server.host == "0.0.0.0"
    assert server.is_running is True

    # Verify server is actually listening
    info = await fetch_server_info(server)
    assert info["port"] == server.port
    assert info["host"] == server.host

    await server.shutdown()


async def test_run_with_available_port(available_port_for_localhost):
    """Test the basic usage of the run function with a specific port."""
    # Use the run function to start a server
    server = await run(port=available_port_for_localhost)

    # Verify the server is running
    assert isinstance(server, Server)
    assert server.is_running is True
    assert server.port == available_port_for_localhost
    assert server.host == "0.0.0.0"

    # Verify server is actually listening
    info = await fetch_server_info(server)
    assert info["port"] == server.port
    assert info["host"] == server.host

    await server.shutdown()


async def test_run_with_dynamic_port():
    """Test the run function with dynamic port assignment."""
    # Use port=0 for dynamic port assignment
    server = await run(port=0)

    # Verify a port was assigned
    assert server.port > 0
    assert server.host == "0.0.0.0"
    assert server.is_running is True

    # Verify server is actually listening
    info = await fetch_server_info(server)
    assert info["port"] == server.port
    assert info["host"] == server.host

    await server.shutdown()


async def test_run_with_debug_flag():
    """Test the run function with debug logging enabled."""
    server = await run(port=0, debug=True)

    try:
        assert server.is_running is True
        assert server.port > 0
        info = await fetch_server_info(server)
        assert info["port"] == server.port
        assert info["host"] == server.host
    finally:
        await server.shutdown()


async def test_run_with_conflicting_port():
    """Test the run function when the requested port is already in use."""
    # Start first server on the available port
    first_server = await run(port=0)
    assert first_server.is_running

    # Try to start second server on the same port
    with pytest.raises(ServerError) as exc_info:
        await run(port=first_server.port, timeout=2.0)

    assert "port" in str(exc_info.value)

    await first_server.shutdown()


@pytest.mark.parametrize("jetstream", [True, False])
async def test_run_with_jetstream(jetstream, tmp_path):
    """Test the run function with JetStream enabled and disabled."""
    # Start server with specified JetStream setting
    store_dir = str(tmp_path / "jetstream") if jetstream else None
    server = await run(port=0, jetstream=jetstream, store_dir=store_dir)

    # Verify JetStream setting matches what was requested
    info = await fetch_server_info(server)
    assert info.get("jetstream", False) is jetstream
    assert server.is_running is True
    assert server.port > 0
    assert server.host == "0.0.0.0"

    await server.shutdown()


async def test_run_with_port_from_config():
    """Test the run function with port specified in config file."""
    # Start server with config file that sets port to 4567
    server = await run(config_path="tests/configs/port.conf")

    # Verify the server is using the port from config
    assert server.port == 4567
    assert server.host == "0.0.0.0"
    assert server.is_running is True

    # Verify server is actually listening
    info = await fetch_server_info(server)
    assert info["port"] == server.port
    assert info["host"] == server.host

    await server.shutdown()


async def test_run_with_jetstream_config():
    """Test the run function with JetStream enabled via config file."""
    # Start server with config file that enables jetstream
    server = await run(port=0, config_path="tests/configs/jetstream.conf")

    # Verify JetStream is enabled from config
    info = await fetch_server_info(server)
    assert info.get("jetstream", False) is True
    assert server.is_running is True
    assert server.port > 0
    assert server.host == "0.0.0.0"

    await server.shutdown()


async def test_run_with_invalid_config():
    """Test the run function with an invalid config file."""
    # Try to start server with invalid config
    with pytest.raises(ServerError) as exc_info:
        await run(config_path="tests/configs/invalid.conf")

    # Verify error message matches the actual nats-server output
    assert "Parse error" in str(exc_info.value)


async def test_run_with_jetstream_and_store_dir(tmp_path):
    """Test the run function with a custom store_dir."""

    # Create a directory path for testing
    store_dir = str(tmp_path / "jetstream_data")

    # Start server with JetStream enabled and custom store_dir
    server = await run(port=0, jetstream=True, store_dir=store_dir)

    try:
        # Verify server is running
        assert server.is_running is True
        info = await fetch_server_info(server)
        assert info.get("jetstream", False) is True
        assert server.port > 0
        assert server.host == "0.0.0.0"

        # Verify the store directory was created
        assert os.path.exists(store_dir)
        assert os.path.isdir(store_dir)

        # Give JetStream a moment to initialize the directory structure
        # This can take a little time on slower systems or CI environments
        max_attempts = 10
        wait_time = 0.5  # seconds per attempt

        for _ in range(max_attempts):
            dir_contents = list(Path(store_dir).iterdir())
            if len(dir_contents) > 0:
                break
            await asyncio.sleep(wait_time)

        # JetStream should have created some files or subdirectories in the store_dir
        assert len(list(Path(store_dir).iterdir())) > 0

    finally:
        # Always shutdown the server
        await server.shutdown()


async def test_run_with_store_dir_as_file(tmp_path):
    """Test the run function fails when store_dir points to a file instead of a directory."""
    # Create a file instead of a directory
    store_file = tmp_path / "not_a_directory"
    store_file.write_text("this is a file, not a directory")

    # Try to start server with JetStream using a file as store_dir
    with pytest.raises(ServerError) as exc_info:
        await run(port=0, jetstream=True, store_dir=str(store_file), timeout=2.0)

    # Verify the error message indicates the storage directory issue
    error_msg = str(exc_info.value).lower()
    assert (
        "storage directory is not a directory" in error_msg
        or "could not create storage directory" in error_msg
    )


async def test_run_with_ipv6_host():
    """Test the run function with IPv6 localhost address."""
    # Start server with IPv6 localhost address
    server = await run(host="::1", port=0)

    # Verify the server is using IPv6 localhost
    assert server.host == "::1"
    assert server.port > 0
    assert server.is_running is True

    # Verify server is actually listening on IPv6
    info = await fetch_server_info(server)
    assert info["port"] == server.port
    assert info["host"] == server.host

    await server.shutdown()


async def test_run_with_ipv6_any_address():
    """Test the run function with IPv6 any address (::)."""
    # Start server listening on all IPv6 addresses
    server = await run(host="::", port=0)

    # Verify the server is using IPv6 any address
    assert server.host == "::"
    assert server.port > 0
    assert server.is_running is True

    # Verify client_url is converted to loopback and brackets are added
    assert server.client_url == f"nats://[::1]:{server.port}"

    # Verify server is actually listening (connect to localhost)
    info = await fetch_server_info(server)
    assert info["port"] == server.port
    assert info["host"] == server.host

    await server.shutdown()


async def test_run_cluster():
    """Test the run_cluster function creates a 3-node cluster."""
    cluster = await run_cluster()

    try:
        # Verify cluster has 3 servers
        assert isinstance(cluster, ServerCluster)
        assert len(cluster.servers) == 3

        # Verify all servers are running
        for server in cluster.servers:
            assert isinstance(server, Server)
            assert server.is_running is True
            assert server.port > 0

            # Verify server is actually listening and get info
            info = await fetch_server_info(server)
            assert info["port"] == server.port
            assert info["host"] == server.host

            # Verify connect_urls is a list (may be empty if cluster not fully formed yet)
            connect_urls = info.get("connect_urls", [])
            assert isinstance(connect_urls, list)

        # Verify client_url property works
        assert cluster.client_url() == cluster.servers[0].client_url

        # Verify all servers have unique ports
        ports = [server.port for server in cluster.servers]
        assert len(ports) == len(set(ports))

    finally:
        await cluster.shutdown()


async def test_run_cluster_with_jetstream_config():
    """Test the run_cluster function with a JetStream config file."""
    cluster = await run_cluster("tests/configs/jetstream.conf")

    try:
        # Verify cluster has 3 servers
        assert len(cluster.servers) == 3

        # Verify all servers have JetStream enabled
        for server in cluster.servers:
            info = await fetch_server_info(server)
            assert info["port"] == server.port
            assert info["host"] == server.host
            assert info.get("jetstream", False) is True
            assert server.is_running is True

            # Verify connect_urls is a list (may be empty if cluster not fully formed yet)
            connect_urls = info.get("connect_urls", [])
            assert isinstance(connect_urls, list)

    finally:
        await cluster.shutdown()


async def test_run_cluster_with_jetstream(tmp_path):
    """Test the run_cluster function with JetStream enabled via CLI flag."""
    cluster = await run_cluster(jetstream=True, store_dir=str(tmp_path))

    try:
        # Verify cluster has 3 servers
        assert len(cluster.servers) == 3

        # Verify all servers have JetStream enabled
        for server in cluster.servers:
            info = await fetch_server_info(server)
            assert info["port"] == server.port
            assert info["host"] == server.host
            assert info.get("jetstream", False) is True
            assert server.is_running is True

            # Verify connect_urls is a list (may be empty if cluster not fully formed yet)
            connect_urls = info.get("connect_urls", [])
            assert isinstance(connect_urls, list)

    finally:
        await cluster.shutdown()


async def test_server_context_manager():
    """Test the Server as a context manager."""
    async with await run(port=0) as server:
        assert server.is_running is True
        assert server.port > 0
        info = await fetch_server_info(server)
        assert info["port"] == server.port
        assert info["host"] == server.host

    # Server should be shutdown after context exit
    assert server.is_running is False


async def test_cluster_context_manager():
    """Test the ServerCluster as a context manager."""
    async with await run_cluster() as cluster:
        assert len(cluster.servers) == 3
        for server in cluster.servers:
            assert server.is_running is True

    # All servers should be shutdown after context exit
    for server in cluster.servers:
        assert server.is_running is False


async def test_server_double_shutdown():
    """Test that calling shutdown twice doesn't cause errors."""
    server = await run(port=0)
    assert server.is_running is True

    await server.shutdown()
    assert server.is_running is False

    # Second shutdown should be a no-op
    await server.shutdown()
    assert server.is_running is False


async def test_server_shutdown_with_timeout():
    """Test shutdown with very short timeout triggers kill path."""
    server = await run(port=0)
    assert server.is_running is True

    # Use a very short timeout - server likely won't terminate in time
    # This should trigger the kill path
    await server.shutdown(timeout=0.0001)

    assert server.is_running is False


async def test_server_connect_urls():
    """Test the connect_urls from server info."""
    server = await run(port=0)

    try:
        # connect_urls from info returns a list (may be empty for single server)
        info = await fetch_server_info(server)
        urls = info.get("connect_urls", [])
        assert isinstance(urls, list)

    finally:
        await server.shutdown()


async def test_run_with_startup_timeout():
    """Test that run raises TimeoutError on timeout."""
    # Use an impossibly short timeout that should always fail
    with pytest.raises(asyncio.TimeoutError):
        await run(port=0, timeout=0.0)


async def test_run_with_specific_ipv4_address():
    """Test the run function with a specific IPv4 address (127.0.0.1)."""
    server = await run(host="127.0.0.1", port=0)

    try:
        assert server.host == "127.0.0.1"
        assert server.port > 0
        assert server.is_running is True
        info = await fetch_server_info(server)
        assert info["port"] == server.port
        assert info["host"] == server.host

    finally:
        await server.shutdown()


async def test_run_with_config_and_cli_args():
    """Test that CLI arguments take precedence over config file."""
    # Config file sets port to 4567, but we override with port=0
    server = await run(port=0, config_path="tests/configs/port.conf")

    try:
        # CLI arg should win, so port should not be 4567
        assert server.port != 4567
        assert server.port > 0
        assert server.is_running is True

    finally:
        await server.shutdown()


async def test_run_store_dir_without_jetstream(tmp_path):
    """Test that store_dir without jetstream is ignored."""
    store_dir = str(tmp_path / "unused_store")

    server = await run(port=0, jetstream=False, store_dir=store_dir)

    try:
        assert server.is_running is True
        info = await fetch_server_info(server)
        assert info.get("jetstream", False) is False

        # Store dir should not be created when jetstream is disabled
        # (nats-server ignores the flag)

    finally:
        await server.shutdown()


async def test_cluster_with_conflicting_config(tmp_path):
    """Test run_cluster with config that includes cluster settings."""
    # The function should still work, merging config with generated cluster setup
    cluster = await run_cluster(
        "tests/configs/jetstream.conf", jetstream=True, store_dir=str(tmp_path)
    )

    try:
        assert len(cluster.servers) == 3
        for server in cluster.servers:
            assert server.is_running is True
            info = await fetch_server_info(server)
            assert info["port"] == server.port
            assert info["host"] == server.host
            assert info.get("jetstream", False) is True

            # Verify connect_urls is a list (may be empty if cluster not fully formed yet)
            connect_urls = info.get("connect_urls", [])
            assert isinstance(connect_urls, list)

    finally:
        await cluster.shutdown()


async def test_run_with_invalid_host():
    """Test the run function with an invalid host address."""
    with pytest.raises(ServerError) as exc_info:
        await run(host="999.999.999.999", port=0, timeout=2.0)

    assert (
        "exited" in str(exc_info.value).lower()
        or "error" in str(exc_info.value).lower()
    )


async def test_cluster_client_url():
    """Test the cluster client_url method returns first server's URL."""
    cluster = await run_cluster()

    try:
        # client_url should return the first server's client_url
        assert cluster.client_url() == cluster.servers[0].client_url
        assert cluster.client_url().startswith("nats://")

    finally:
        await cluster.shutdown()


@pytest.mark.parametrize("size", [1, 2, 5])
async def test_run_cluster_with_custom_size(size):
    """Test run_cluster with different cluster sizes."""
    cluster = await run_cluster(size=size)

    try:
        assert len(cluster.servers) == size
        for server in cluster.servers:
            assert server.is_running is True

    finally:
        await cluster.shutdown()


async def test_run_cluster_with_invalid_size():
    """Test run_cluster raises ValueError for invalid size."""
    with pytest.raises(ValueError) as exc_info:
        await run_cluster(size=0)

    assert "at least 1" in str(exc_info.value)


async def test_run_cluster_with_invalid_config():
    """Test run_cluster with invalid config triggers cleanup path."""
    with pytest.raises(ServerError) as exc_info:
        await run_cluster(config_path="tests/configs/invalid.conf", size=1)

    assert "Failed to start cluster" in str(exc_info.value)
