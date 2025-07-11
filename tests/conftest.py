import eventlet.wsgi

eventlet.monkey_patch()  # MUST BE THERE FOR THE TESTS TO WORK

import random
import socket
import subprocess
import time

import pytest
import redis

from znsocket import Client


@pytest.fixture
def eventlet_memory_server():
    port = random.randint(10000, 20000)

    # Start znsocket subprocess
    proc = subprocess.Popen(
        ["znsocket", "--port", str(port)],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )

    # Wait for the server to be ready
    for _ in range(100):
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
                sock.settimeout(0.2)
                sock.connect(("127.0.0.1", port))
                break
        except (ConnectionRefusedError, OSError):
            time.sleep(0.1)
    else:
        proc.kill()
        raise TimeoutError("Server did not start in time")

    yield f"znsocket://127.0.0.1:{port}"

    # Clean up: kill the subprocess
    proc.terminate()
    try:
        proc.wait(timeout=5)
    except subprocess.TimeoutExpired:
        proc.kill()


@pytest.fixture
def eventlet_memory_server_redis():
    port = random.randint(10000, 20000)

    proc = subprocess.Popen(
        ["znsocket", "--port", str(port), "--storage", "redis://localhost:6379/0"],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )

    for _ in range(100):
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
                sock.settimeout(0.2)
                sock.connect(("127.0.0.1", port))
                break
        except (ConnectionRefusedError, OSError):
            time.sleep(0.1)
    else:
        proc.kill()
        raise TimeoutError("Server did not start in time")

    yield f"znsocket://127.0.0.1:{port}"

    proc.terminate()
    try:
        proc.wait(timeout=5)
    except subprocess.TimeoutExpired:
        proc.kill()


@pytest.fixture
def znsclient(eventlet_memory_server):
    r = Client.from_url(eventlet_memory_server)
    yield r
    r.flushall()


@pytest.fixture
def znsclient_w_redis(eventlet_memory_server_redis):
    r = Client.from_url(eventlet_memory_server_redis)
    yield r
    r.flushall()


@pytest.fixture
def redisclient():
    r = redis.Redis.from_url("redis://localhost:6379/0", decode_responses=True)
    yield r
    r.flushdb()


@pytest.fixture
def empty() -> None:
    """Test against Python list implementation"""
    return None
