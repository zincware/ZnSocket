import eventlet.wsgi

eventlet.monkey_patch()  # MUST BE THERE FOR THE TESTS TO WORK

import random

import pytest
import redis
import socketio
import socketio.exceptions

from znsocket import Client, Server, attach_events


@pytest.fixture
def eventlet_memory_server():
    port = random.randint(10000, 20000)

    def start_server():
        server = Server(port=port)
        server.run()

    thread = eventlet.spawn(start_server)

    # wait for the server to be ready
    for _ in range(100):
        try:
            with socketio.SimpleClient() as client:
                client.connect(f"http://localhost:{port}")
                break
        except socketio.exceptions.ConnectionError:
            eventlet.sleep(0.1)
    else:
        raise TimeoutError("Server did not start in time")

    yield f"znsocket://127.0.0.1:{port}"

    thread.kill()


@pytest.fixture
def eventlet_memory_server_redis():
    port = random.randint(10000, 20000)

    def start_server():
        sio = socketio.Server()
        r = redis.Redis.from_url("redis://localhost:6379/0", decode_responses=True)
        attach_events(sio, storage=r)
        server_app = socketio.WSGIApp(sio)
        eventlet.wsgi.server(eventlet.listen(("localhost", port)), server_app)

    thread = eventlet.spawn(start_server)

    # wait for the server to be ready
    for _ in range(100):
        try:
            with socketio.SimpleClient() as client:
                client.connect(f"http://localhost:{port}")
                break
        except socketio.exceptions.ConnectionError:
            eventlet.sleep(0.1)
    else:
        raise TimeoutError("Server did not start in time")

    yield f"znsocket://127.0.0.1:{port}"

    thread.kill()


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
