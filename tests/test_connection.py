import eventlet.wsgi

eventlet.monkey_patch()  # MUST BE THERE FOR THE TESTS TO WORK

import random

import pytest
import socketio

import znsocket.client
from znsocket.server import get_sio

sio = get_sio()


@pytest.fixture
def eventlet_server():
    port = random.randint(10000, 20000)

    def start_server():
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

    yield f"http://localhost:{port}"

    thread.kill()


def test_example(eventlet_server):
    c1 = znsocket.client.Client(eventlet_server, room="tmp")
    c2 = znsocket.client.Client(eventlet_server, room="tmp")
    c3 = znsocket.client.Client(eventlet_server)

    c1.content = "Hello, World!"
    c3.content = "Hello, there!"
    eventlet.sleep(0.1)

    assert c1.content == "Hello, World!"
    assert c2.content == "Hello, World!"
    assert c3.content == "Hello, there!"

    c2.content = "Lorem Ipsum"
    eventlet.sleep(0.1)

    assert c1.content == "Lorem Ipsum"
    assert c2.content == "Lorem Ipsum"
    assert c3.content == "Hello, there!"


def test_attribute_error(eventlet_server):
    c1 = znsocket.client.Client(eventlet_server, room="tmp")
    with pytest.raises(AttributeError):
        _ = c1.non_existent_attribute
