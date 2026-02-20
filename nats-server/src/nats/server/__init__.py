"""Tools for managing NATS server instances programmatically.

This module provides a high-level interface for controlling NATS servers
in Python code. It handles process management, automatic port assignment,
and server readiness detection.

Example:
    >>> import asyncio
    >>> import nats.server
    >>>
    >>> async def main():
    ...     server = await nats.server.run(port=0)  # Auto port assignment
    ...     print(f"Server running on {server.host}:{server.port}")
    ...     await server.shutdown()
    >>> asyncio.run(main())
"""

from __future__ import annotations

import asyncio
import asyncio.subprocess
import contextlib
import logging
import os
import re
import socket
import tempfile
from typing import Self

# Set up logging
logger = logging.getLogger(__name__)

# Regex patterns for parsing server output
FATAL_PATTERN = re.compile(r"\[FTL\]\s+(.*)")
INFO_PATTERN = re.compile(r"\[INF\]\s+(.*)")
READY_PATTERN = re.compile(r"Server is ready")
LISTENING_PATTERN = re.compile(r"Listening for client connections on (.+):(\d+)")


class ServerError(Exception):
    """Raised when a NATS server encounters an error during startup or operation."""


class ServerCluster:
    """A cluster of NATS server instances."""

    servers: list["Server"]

    def __init__(self, servers: list["Server"]):
        """Initialize a new NATS server cluster.

        Args:
            servers: List of Server instances that form the cluster.
        """
        self.servers = servers

    def client_url(self) -> str:
        """Get the client URL of the first server in the cluster.

        Returns:
            The connect URL of the first server as a string.
        """
        return self.servers[0].client_url

    async def shutdown(self) -> None:
        """Shutdown all servers in the cluster."""
        await asyncio.gather(
            *[server.shutdown() for server in self.servers],
            return_exceptions=True,
        )

    async def __aenter__(self) -> Self:
        """Context manager entry."""
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        """Context manager exit, ensures all servers are stopped."""
        await self.shutdown()


class Server:
    """Process controller for a NATS server instance."""

    _process: asyncio.subprocess.Process | None
    _host: str
    _port: int

    def __init__(
        self,
        process: asyncio.subprocess.Process,
        host: str,
        port: int,
    ):
        """Initialize a new NATS server instance.

        Args:
            process: The subprocess running the NATS server.
            host: The host the server is listening on.
            port: The port the server is listening on.
        """
        self._process = process
        self._host = host
        self._port = port

    @property
    def host(self) -> str:
        """Get the host the server is running on."""
        return self._host

    @property
    def port(self) -> int:
        """Get the port the server is listening on."""
        return self._port

    @property
    def client_url(self) -> str:
        """Get the client url for the server, this can be used to connect to the server.

        Returns:
            The connect URL as a string.
        """
        # Replace bind-all addresses with loopback addresses
        match self._host:
            case "0.0.0.0":
                host = "127.0.0.1"
            case "::":
                host = "::1"
            case _:
                host = self._host

        # Wrap IPv6 addresses in brackets for URL formatting
        if ":" in host:
            host = f"[{host}]"

        return f"nats://{host}:{self._port}"

    @property
    def is_running(self) -> bool:
        """Get whether the server is currently running."""
        return self._process is not None and self._process.returncode is None

    async def shutdown(self, timeout: float = 5.0) -> None:
        """Shutdown the NATS server.

        Args:
            timeout: Maximum time in seconds to wait for graceful termination
                    before forcing the process to exit. Defaults to 5.0 seconds.
        """
        if not self._process:
            return

        try:
            self._process.terminate()
            await asyncio.wait_for(self._process.wait(), timeout=timeout)
        except asyncio.TimeoutError:
            # Process didn't terminate in time, force kill
            try:
                self._process.kill()
                await self._process.wait()
            except ProcessLookupError:
                # Process already terminated, ignore
                pass
        finally:
            self._process = None

    async def __aenter__(self) -> Self:
        """Context manager entry."""
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        """Context manager exit, ensures server is stopped."""
        await self.shutdown()


async def _create_server_process(
    *,
    host: str | None = None,
    port: int | None = None,
    debug: bool = False,
    jetstream: bool = False,
    store_dir: str | None = None,
    config_path: str | None = None,
    cluster: str | None = None,
    cluster_name: str | None = None,
    routes: str | None = None,
    name: str | None = None,
) -> asyncio.subprocess.Process:
    """Create a NATS server subprocess with the given configuration.

    Args:
        host: Host to listen on (--addr).
        port: Port to listen on (--port).
        debug: Whether to enable debug logging (--debug).
        jetstream: Whether to enable JetStream (--jetstream).
        store_dir: Directory to use for JetStream storage (--store_dir).
        config_path: Path to server config file (--config).
        cluster: Cluster URL for solicited routes (--cluster).
        cluster_name: Name of the cluster (--cluster_name).
        routes: Routes to solicit and connect (--routes).
        name: Server name (-n).

    Returns:
        The created subprocess.
    """
    cmd = ["nats-server"]

    # Direct parameter to CLI flag mapping
    if host is not None:
        cmd.extend(["--addr", host])

    if port is not None:
        # Port 0 means random assignment, nats-server uses -1
        cmd.extend(["--port", str(-1 if port == 0 else port)])

    if debug:
        cmd.append("--debug")

    if jetstream:
        cmd.append("--jetstream")

    if store_dir is not None:
        cmd.extend(["--store_dir", store_dir])

    if config_path is not None:
        cmd.extend(["--config", config_path])

    if cluster is not None:
        cmd.extend(["--cluster", cluster])

    if cluster_name is not None:
        cmd.extend(["--cluster_name", cluster_name])

    if routes is not None:
        cmd.extend(["--routes", routes])

    if name is not None:
        cmd.extend(["-n", name])

    return await asyncio.create_subprocess_exec(
        *cmd,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )


async def _wait_for_server_ready(
    process: asyncio.subprocess.Process,
    timeout: float = 10.0,
) -> tuple[str, int]:
    """Wait for a NATS server to become ready.

    Args:
        process: The server subprocess.
        timeout: Maximum time to wait in seconds.

    Returns:
        Tuple of (host, port) the server is listening on.

    Raises:
        ServerError: If the server fails to start or become ready.
        asyncio.TimeoutError: If the server doesn't become ready within timeout.
    """

    async def wait_ready() -> tuple[str, int]:
        host: str | None = None
        port: int | None = None
        error_lines = []

        assert process.stderr is not None
        async for stderr_line_bytes in process.stderr:
            stderr_line: str = stderr_line_bytes.decode().strip()

            if READY_PATTERN.search(stderr_line):
                assert host is not None
                assert port is not None
                return host, port

            if match := LISTENING_PATTERN.search(stderr_line):
                host_part = match.group(1)
                if host_part.startswith("[") and host_part.endswith("]"):
                    host = host_part[1:-1]
                else:
                    host = host_part
                port = int(match.group(2))

            if INFO_PATTERN.search(stderr_line):
                continue

            if match := FATAL_PATTERN.search(stderr_line):
                error_lines.append(match.group(1))

            error_lines.append(stderr_line)

        returncode = await process.wait()

        if returncode != 0:
            msg = f"Server exited with code {returncode}"
            if error_lines:
                errors = "\n".join(error_lines)
                msg += f"\nErrors:\n{errors}"
            raise ServerError(msg)

        raise ServerError("Server ended without becoming ready")  # pragma: no cover

    return await asyncio.wait_for(wait_ready(), timeout=timeout)


async def run(
    *,
    host: str | None = None,
    port: int | None = None,
    debug: bool = False,
    jetstream: bool = False,
    store_dir: str | None = None,
    config_path: str | None = None,
    timeout: float = 10.0,
) -> Server:
    """Start a NATS server and wait for it to be ready.

    Args:
        host: Host to listen on. If None, use CLI default.
        port: Port to listen on. If 0, a random port will be chosen. If None, use CLI default.
        debug: Whether to enable debug logging.
        jetstream: Whether to enable JetStream.
        store_dir: Directory to use for JetStream storage. Only used when jetstream=True.
        config_path: Path to server config file.
        timeout: Maximum time to wait for server to start in seconds.

    Returns:
        A Server instance representing the running server.

    Raises:
        ServerError: If the server fails to start or become ready within the timeout.
    """
    process = await _create_server_process(
        host=host,
        port=port,
        debug=debug,
        jetstream=jetstream,
        store_dir=store_dir,
        config_path=config_path,
    )

    assigned_host, assigned_port = await _wait_for_server_ready(
        process, timeout=timeout
    )

    return Server(process, assigned_host, assigned_port)


async def run_cluster(
    config_path: str = "",
    jetstream: bool = False,
    size: int = 3,
    store_dir: str | None = None,
) -> ServerCluster:
    """Start a NATS cluster.

    Creates a cluster of interconnected NATS servers with automatically
    allocated ports. The cluster nodes are configured with routes to each
    other for full mesh connectivity.

    Args:
        config_path: Optional path to a config file to use for all nodes.
        jetstream: Whether to enable JetStream on all nodes.
        size: Number of nodes in the cluster. Defaults to 3.
        store_dir: Base directory for JetStream storage. Each node gets a subdirectory (node1/, node2/, etc.).
                   If None, a temporary directory is used for each node to avoid conflicts.

    Returns:
        A ServerCluster instance containing the running servers.

    Raises:
        ServerError: If any server fails to start.
        ValueError: If size is less than 1.
    """
    if size < 1:
        raise ValueError("Cluster size must be at least 1")

    # Use OS to allocate available ports for each node (client + cluster port)
    available_ports = []
    cluster_ports = []
    sockets = []

    try:
        # Create socket pairs for each node to reserve both client and cluster ports
        for _ in range(size):
            client_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            client_sock.bind(("127.0.0.1", 0))
            client_port = client_sock.getsockname()[1]

            cluster_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            cluster_sock.bind(("127.0.0.1", 0))
            cluster_port = cluster_sock.getsockname()[1]

            available_ports.append(client_port)
            cluster_ports.append(cluster_port)
            sockets.extend([client_sock, cluster_sock])
    finally:
        for sock in sockets:
            sock.close()

    servers = []
    try:
        for i in range(size):
            routes = [cluster_ports[j] for j in range(size) if j != i]

            # Always create unique store directory for each cluster node
            # This prevents conflicts when JetStream is enabled (via flag or config)
            # If JetStream is disabled, the server will ignore this parameter
            if store_dir:
                # Use provided base directory and create subdirectory for each node
                node_store_dir = os.path.join(store_dir, f"node{i + 1}")
                os.makedirs(node_store_dir, exist_ok=True)
            else:
                # Create unique temp directory for each node to avoid conflicts
                node_store_dir = tempfile.mkdtemp(prefix=f"nats-node{i + 1}-")

            server = await _run_cluster_node(
                config_path=config_path,
                port=available_ports[i],
                routes=routes,
                name=f"node{i + 1}",
                cluster_name="cluster",
                cluster_port=cluster_ports[i],
                jetstream=jetstream,
                store_dir=node_store_dir,
            )
            servers.append(server)

    except Exception as e:
        for server in servers:
            with contextlib.suppress(Exception):
                await server.shutdown()
        raise ServerError(f"Failed to start cluster: {e}") from e

    return ServerCluster(servers)


async def _run_cluster_node(
    *,
    config_path: str,
    port: int,
    routes: list[int],
    name: str,
    cluster_name: str,
    cluster_port: int,
    jetstream: bool = False,
    store_dir: str | None = None,
) -> Server:
    """Start a single node of a NATS cluster.

    Args:
        config_path: Optional path to config file.
        port: Client port for this node.
        routes: List of cluster ports for other nodes.
        name: Name for this node.
        cluster_name: Name of the cluster.
        cluster_port: Cluster port for this node.
        jetstream: Whether to enable JetStream.
        store_dir: Directory for JetStream storage.

    Returns:
        A Server instance for the cluster node.
    """
    # Build cluster URL and routes string for CLI
    cluster_url = f"nats://127.0.0.1:{cluster_port}"
    routes_str = ",".join(f"nats://127.0.0.1:{r}" for r in routes) if routes else None

    process = await _create_server_process(
        port=port,
        cluster=cluster_url,
        cluster_name=cluster_name,
        routes=routes_str,
        name=name,
        jetstream=jetstream,
        store_dir=store_dir,
        config_path=config_path if config_path else None,
    )

    assigned_host, assigned_port = await _wait_for_server_ready(process, timeout=10.0)

    return Server(process, assigned_host, assigned_port)
