import random

import eventlet.wsgi
import pytest
import socketio
from flask import Flask

from znsocket import Client, Storage, attach_events


@pytest.fixture
def flask_server_with_znsocket():
    """Create a Flask server with znsocket.Storage attached."""
    port = random.randint(10000, 20000)

    # Create Flask app
    app = Flask(__name__)
    app.config['SECRET_KEY'] = 'test-secret'

    # Create custom storage instance
    storage = Storage()

    # Create a regular socketio server and attach znsocket events
    sio = socketio.Server(cors_allowed_origins="*")
    attach_events(sio, namespace="/znsocket", storage=storage)

    # Create WSGI app
    server_app = socketio.WSGIApp(sio, app)

    def start_server():
        eventlet.wsgi.server(eventlet.listen(("127.0.0.1", port)), server_app)

    # Start server in background thread
    thread = eventlet.spawn(start_server)

    # Wait for server to be ready
    for _ in range(100):
        try:
            with socketio.SimpleClient() as client:
                client.connect(f"http://127.0.0.1:{port}")
                break
        except socketio.exceptions.ConnectionError:
            eventlet.sleep(0.1)
    else:
        raise TimeoutError("Flask server did not start in time")

    yield f"znsocket://127.0.0.1:{port}", storage

    thread.kill()


@pytest.fixture
def flask_client(flask_server_with_znsocket):
    """Create a znsocket.Client connected to the Flask server."""
    url, storage = flask_server_with_znsocket
    client = Client.from_url(url)
    yield client, storage
    client.flushall()


def test_flask_server_basic_connection(flask_client):
    """Test basic connection to Flask server with znsocket.Storage."""
    client, storage = flask_client

    # Test basic set/get operations
    client.set("test_key", "test_value")
    assert client.get("test_key") == "test_value"

    # Verify storage is shared between client and server
    assert storage.get("test_key") == "test_value"


def test_flask_server_redis_api_compatibility(flask_client):
    """Test Redis API compatibility with Flask server."""
    client, _ = flask_client

    # Test hash operations
    client.hset("user:1", "name", "John")
    client.hset("user:1", "age", "30")
    assert client.hget("user:1", "name") == "John"
    assert client.hget("user:1", "age") == "30"

    # Test list operations
    client.rpush("tasks", "task1")
    client.rpush("tasks", "task2")
    assert client.llen("tasks") == 2
    assert client.lrange("tasks", 0, -1) == ["task1", "task2"]

    # Test set operations
    client.sadd("tags", "python")
    client.sadd("tags", "flask")
    members = client.smembers("tags")
    assert "python" in members
    assert "flask" in members


def test_flask_server_pipeline_operations(flask_client):
    """Test pipeline operations with Flask server."""
    client, _ = flask_client

    # Create a pipeline
    pipe = client.pipeline()
    pipe.set("key1", "value1")
    pipe.set("key2", "value2")
    pipe.get("key1")
    pipe.get("key2")

    # Execute pipeline
    results = pipe.execute()

    # Verify results
    assert results[0] == True  # set returns True
    assert results[1] == True  # set returns True
    assert results[2] == "value1"  # get returns value
    assert results[3] == "value2"  # get returns value


def test_flask_server_storage_persistence(flask_client):
    """Test that storage persists data correctly."""
    client, storage = flask_client

    # Set data through client
    client.hset("config", "debug", "true")
    client.hset("config", "port", "5000")

    # Verify data exists in storage directly
    assert storage.hget("config", "debug") == "true"
    assert storage.hget("config", "port") == "5000"

    # Modify data directly in storage
    storage.hset("config", "version", "1.0.0")

    # Verify client can read the modified data
    assert client.hget("config", "version") == "1.0.0"


def test_flask_server_error_handling(flask_client):
    """Test error handling in Flask server integration."""
    client, _ = flask_client

    # Test non-existent key
    assert client.get("nonexistent") is None

    # Test invalid operations - string key with hash operations
    client.set("string_key", "value")

    # Trying to use string key as hash should raise TypeError
    try:
        client.hset("string_key", "field", "value")
        assert False, "Should have raised TypeError"
    except TypeError as e:
        assert "does not support item assignment" in str(e)


def test_flask_server_multiple_clients(flask_server_with_znsocket):
    """Test multiple clients connecting to the same Flask server."""
    url, _ = flask_server_with_znsocket

    # Create two clients
    client1 = Client.from_url(url)
    client2 = Client.from_url(url)

    try:
        # Client 1 sets data
        client1.set("shared_key", "from_client1")

        # Client 2 should see the same data
        assert client2.get("shared_key") == "from_client1"

        # Client 2 modifies data
        client2.set("shared_key", "from_client2")

        # Client 1 should see the modification
        assert client1.get("shared_key") == "from_client2"

    finally:
        # Cleanup
        client1.flushall()
        client2.flushall()


def test_flask_server_custom_storage():
    """Test Flask server with custom storage instance."""
    port = random.randint(10000, 20000)

    # Create custom storage with pre-populated data
    custom_storage = Storage()
    custom_storage.set("initial_key", "initial_value")
    custom_storage.hset("initial_hash", "field1", "value1")

    # Create Flask app
    app = Flask(__name__)
    app.config['SECRET_KEY'] = 'test-secret'

    # Create socketio server and attach znsocket events with custom storage
    sio = socketio.Server(cors_allowed_origins="*")
    attach_events(sio, namespace="/znsocket", storage=custom_storage)

    # Create WSGI app
    server_app = socketio.WSGIApp(sio, app)

    def start_server():
        eventlet.wsgi.server(eventlet.listen(("127.0.0.1", port)), server_app)

    thread = eventlet.spawn(start_server)

    # Wait for server to be ready
    for _ in range(100):
        try:
            with socketio.SimpleClient() as client:
                client.connect(f"http://127.0.0.1:{port}")
                break
        except socketio.exceptions.ConnectionError:
            eventlet.sleep(0.1)
    else:
        thread.kill()
        raise TimeoutError("Flask server did not start in time")

    try:
        # Connect client and verify pre-populated data
        client = Client.from_url(f"znsocket://127.0.0.1:{port}")

        # Should see pre-populated data
        assert client.get("initial_key") == "initial_value"
        assert client.hget("initial_hash", "field1") == "value1"

        # Client operations should work normally
        client.set("new_key", "new_value")
        assert client.get("new_key") == "new_value"

    finally:
        thread.kill()
