import pytest
import socketio.exceptions

from znsocket import Client, exceptions


def test_client_from_url(eventlet_memory_server):
    r = Client.from_url(eventlet_memory_server)
    r.set("name", "Alice")
    assert r.get("name") == "Alice"


def test_client_decode_responses(eventlet_memory_server):
    with pytest.raises(NotImplementedError):
        _ = Client.from_url(eventlet_memory_server, decode_responses=False)


def test_client_connection_error():
    with pytest.raises(
        exceptions.ConnectionError, match="Could not connect to http://127.0.0.1:5000"
    ):
        Client.from_url("znsocket://127.0.0.1:5000")

    with pytest.raises(
        socketio.exceptions.ConnectionError,
        match="Could not connect to http://127.0.0.1:5000",
    ):
        Client.from_url("znsocket://127.0.0.1:5000")
