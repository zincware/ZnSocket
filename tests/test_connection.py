import eventlet.wsgi

eventlet.monkey_patch()  # MUST BE THERE FOR THE TESTS TO WORK

import os
import random

import pytest
import socketio

import znsocket.client
from znsocket.server import SqlDatabase, get_sio


@pytest.fixture
def eventlet_memory_server():
    sio = get_sio()
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


@pytest.fixture
def eventlet_sql_server(tmp_path):
    db_path = tmp_path / "znsocket.db"

    db = SqlDatabase(engine=f"sqlite:///{db_path}")

    sio = get_sio(db=db)

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


@pytest.mark.parametrize("server", ["eventlet_memory_server", "eventlet_sql_server"])
def test_example(server, request):
    eventlet_server = request.getfixturevalue(server)
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


@pytest.mark.parametrize("server", ["eventlet_memory_server", "eventlet_sql_server"])
def test_attribute_error(server, request):
    eventlet_server = request.getfixturevalue(server)
    c1 = znsocket.client.Client(eventlet_server, room="tmp")
    with pytest.raises(AttributeError):
        _ = c1.non_existent_attribute


@pytest.mark.parametrize("server", ["eventlet_memory_server", "eventlet_sql_server"])
def test_multiple_attributes(server, request):
    eventlet_server = request.getfixturevalue(server)
    c1 = znsocket.client.Client(eventlet_server, room="tmp")
    c2 = znsocket.client.Client(eventlet_server, room="tmp")

    c1.a = "1"
    c1.b = "2"
    eventlet.sleep(0.1)

    assert c1.a == "1"
    assert c1.b == "2"
    assert c1.a == c2.a
    assert c1.b == c2.b

    c2.a = "3"
    eventlet.sleep(0.1)

    assert c1.a == "3"
    assert c1.b == "2"
    assert c1.a == c2.a
    assert c1.b == c2.b
