import json
import random
import subprocess
import sys
import tempfile
import textwrap

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
    app.config["SECRET_KEY"] = "test-secret"

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


def test_flask_server_cross_process_communication(flask_server_with_znsocket):
    """Test Flask server with znsocket.Client in separate process."""
    url, storage = flask_server_with_znsocket

    # Set initial data in storage
    storage.set("process_test", "initial_value")
    storage.hset("process_hash", "field1", "value1")
    storage.rpush("process_list", "item1")
    storage.rpush("process_list", "item2")

    # Create a Python script to run in subprocess
    client_script = textwrap.dedent(f"""
    import json

    from znsocket import Client

    def main():
        try:
            client = Client.from_url('{url}')

            # Test reading existing data
            result = {{}}
            result['get_test'] = client.get('process_test')
            result['hash_test'] = client.hget('process_hash', 'field1')
            result['list_test'] = client.lrange('process_list', 0, -1)

            # Test writing new data
            client.set('subprocess_key', 'subprocess_value')
            client.hset('subprocess_hash', 'sub_field', 'sub_value')
            client.rpush('subprocess_list', 'sub_item')

            # Verify writes
            result['write_test'] = client.get('subprocess_key')
            result['hash_write_test'] = client.hget('subprocess_hash', 'sub_field')
            result['list_write_test'] = client.lrange('subprocess_list', 0, -1)

            print(json.dumps(result))
            return 0
        except Exception as e:
            print(f"Error: {{e}}", file=sys.stderr)
            return 1

    if __name__ == '__main__':
        exit(main())
    """)

    # Write script to temporary file
    with tempfile.NamedTemporaryFile(mode="w", suffix=".py", delete=False) as f:
        f.write(client_script)
        script_path = f.name

    try:
        # Run the subprocess
        result = subprocess.run(
            [sys.executable, script_path], capture_output=True, text=True, timeout=30
        )

        # Check if subprocess succeeded
        assert result.returncode == 0

        # Parse JSON output
        output = json.loads(result.stdout.strip())

        # Verify subprocess could read existing data
        assert output["get_test"] == "initial_value"
        assert output["hash_test"] == "value1"
        assert output["list_test"] == ["item1", "item2"]

        # Verify subprocess could write data
        assert output["write_test"] == "subprocess_value"
        assert output["hash_write_test"] == "sub_value"
        assert output["list_write_test"] == ["sub_item"]

        # Verify main process can see subprocess writes
        assert storage.get("subprocess_key") == "subprocess_value"
        assert storage.hget("subprocess_hash", "sub_field") == "sub_value"
        assert storage.lrange("subprocess_list", 0, -1) == ["sub_item"]

        # Test bidirectional communication
        # Main process writes, subprocess should see it
        storage.set("main_to_sub", "hello_subprocess")

        # Create another subprocess to verify real-time communication
        verify_script = textwrap.dedent(f"""
        from znsocket import Client

        client = Client.from_url('{url}')
        print(client.get('main_to_sub'))
        """)

        with tempfile.NamedTemporaryFile(mode="w", suffix=".py", delete=False) as f:
            f.write(verify_script)
            verify_path = f.name

        try:
            verify_result = subprocess.run(
                [sys.executable, verify_path],
                capture_output=True,
                text=True,
                timeout=10,
            )

            assert verify_result.returncode == 0
            assert verify_result.stdout.strip() == "hello_subprocess"

        finally:
            import os

            os.unlink(verify_path)

    finally:
        # Cleanup
        import os

        os.unlink(script_path)
