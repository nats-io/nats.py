"""
Pytest configuration and fixtures for the nats.server test suite.

This file contains shared fixtures used across multiple test files,
particularly for verifying that the required nats-server executable is available.
"""

import shutil
import socket
import subprocess

import pytest


def get_nats_server_version():
    """Get the nats-server version or fail if not installed."""
    nats_server_path = shutil.which("nats-server")
    if not nats_server_path:
        pytest.fail("nats-server is not installed or not in PATH")
        return None

    try:
        result = subprocess.run([nats_server_path, "--version"],
                                capture_output=True,
                                check=True,
                                text=True)
        return result.stdout.strip() or result.stderr.strip()
    except subprocess.SubprocessError as e:
        pytest.fail(f"Failed to run nats-server: {e}")
        return None


@pytest.fixture(scope="session", autouse=True)
def ensure_nats_server():
    """
    Verify nats-server is installed and available before running any tests.

    This fixture runs automatically at the start of the test session and
    fails fast if the nats-server executable cannot be found.
    """
    get_nats_server_version()


@pytest.fixture
def available_port_for_localhost() -> int:
    """
    Get a free TCP port that can be used for testing.

    This fixture finds an available port by letting the OS assign one,
    then returns that port number. The port is released immediately after
    finding it, so there's a small chance it could be taken before the test uses it.

    Returns:
        An available TCP port number
    """
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.bind(("localhost", 0))
        return sock.getsockname()[1]
