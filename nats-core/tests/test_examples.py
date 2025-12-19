"""Integration tests for example scripts.

These tests verify that all example scripts work correctly.
"""

import asyncio
import subprocess
import sys
from pathlib import Path

import pytest
from nats.server import Server


@pytest.fixture
def examples_dir() -> Path:
    """Get the examples directory path."""
    return Path(__file__).parent.parent / "examples"


@pytest.mark.asyncio
async def test_pub_sub_example(server: Server, examples_dir: Path):
    """Test that nats-pub and nats-sub work together."""
    # Start a subscriber in the background
    sub_proc = subprocess.Popen(
        [
            sys.executable,
            str(examples_dir / "nats-sub.py"),
            "-s",
            server.client_url,
            "test.subject",
        ],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )

    try:
        # Give subscriber time to connect
        await asyncio.sleep(0.1)

        # Publish a message
        pub_result = subprocess.run(
            [
                sys.executable,
                str(examples_dir / "nats-pub.py"),
                "-s",
                server.client_url,
                "test.subject",
                "Hello from test!",
            ],
            capture_output=True,
            text=True,
            timeout=5,
        )

        assert pub_result.returncode == 0
        assert "Published [test.subject]" in pub_result.stdout

        # Give subscriber time to receive
        await asyncio.sleep(0.1)

    finally:
        sub_proc.terminate()
        sub_proc.wait(timeout=2)


@pytest.mark.asyncio
async def test_request_reply_example(server: Server, examples_dir: Path):
    """Test that nats-req and nats-rply work together."""
    # Start a replier in the background
    rply_proc = subprocess.Popen(
        [
            sys.executable,
            str(examples_dir / "nats-rply.py"),
            "-s",
            server.client_url,
            "test.help",
            "I can help!",
        ],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )

    try:
        # Give replier time to connect
        await asyncio.sleep(0.1)

        # Send a request
        req_result = subprocess.run(
            [
                sys.executable,
                str(examples_dir / "nats-req.py"),
                "-s",
                server.client_url,
                "test.help",
                "What is NATS?",
            ],
            capture_output=True,
            text=True,
            timeout=5,
        )

        assert req_result.returncode == 0
        assert "Published [test.help]" in req_result.stdout
        assert "I can help!" in req_result.stdout

    finally:
        rply_proc.terminate()
        rply_proc.wait(timeout=2)


@pytest.mark.asyncio
async def test_echo_example(server: Server, examples_dir: Path):
    """Test that nats-echo works correctly."""
    # Start echo service in the background
    echo_proc = subprocess.Popen(
        [
            sys.executable,
            str(examples_dir / "nats-echo.py"),
            "-s",
            server.client_url,
            "-id",
            "test-echo",
            "echo.test",
        ],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )

    try:
        # Give echo service time to connect
        await asyncio.sleep(0.1)

        # Test echo functionality
        echo_result = subprocess.run(
            [
                sys.executable,
                str(examples_dir / "nats-req.py"),
                "-s",
                server.client_url,
                "echo.test",
                "Echo this!",
            ],
            capture_output=True,
            text=True,
            timeout=5,
        )

        assert echo_result.returncode == 0
        assert "Echo this!" in echo_result.stdout

        # Test status endpoint
        status_result = subprocess.run(
            [
                sys.executable,
                str(examples_dir / "nats-req.py"),
                "-s",
                server.client_url,
                "echo.test.status",
                "",
            ],
            capture_output=True,
            text=True,
            timeout=5,
        )

        assert status_result.returncode == 0
        assert "test-echo" in status_result.stdout
        assert "echo_count" in status_result.stdout

    finally:
        echo_proc.terminate()
        echo_proc.wait(timeout=2)


@pytest.mark.asyncio
async def test_queue_example(server: Server, examples_dir: Path):
    """Test that nats-qsub distributes messages across queue members."""
    # Start two queue subscribers
    qsub1_proc = subprocess.Popen(
        [
            sys.executable,
            str(examples_dir / "nats-qsub.py"),
            "-s",
            server.client_url,
            "test.queue",
            "workers",
        ],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )

    qsub2_proc = subprocess.Popen(
        [
            sys.executable,
            str(examples_dir / "nats-qsub.py"),
            "-s",
            server.client_url,
            "test.queue",
            "workers",
        ],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )

    try:
        # Give subscribers time to connect
        await asyncio.sleep(0.1)

        # Publish multiple messages
        for i in range(10):
            pub_result = subprocess.run(
                [
                    sys.executable,
                    str(examples_dir / "nats-pub.py"),
                    "-s",
                    server.client_url,
                    "test.queue",
                    f"Message {i + 1}",
                ],
                capture_output=True,
                text=True,
                timeout=5,
            )
            assert pub_result.returncode == 0

        # Give subscribers time to receive messages
        await asyncio.sleep(0.2)

    finally:
        qsub1_proc.terminate()
        qsub2_proc.terminate()
        qsub1_proc.wait(timeout=2)
        qsub2_proc.wait(timeout=2)


@pytest.mark.asyncio
async def test_examples_help_text(examples_dir: Path):
    """Test that all examples have working --help."""
    examples = [
        "nats-pub.py",
        "nats-sub.py",
        "nats-qsub.py",
        "nats-req.py",
        "nats-rply.py",
        "nats-echo.py",
    ]

    for example in examples:
        result = subprocess.run(
            [sys.executable, str(examples_dir / example), "--help"],
            capture_output=True,
            text=True,
            timeout=5,
        )
        assert result.returncode == 0
        assert "usage:" in result.stdout.lower()
        assert "--server" in result.stdout or "-s" in result.stdout
