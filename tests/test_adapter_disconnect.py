import time

import pytest

import znsocket


@pytest.fixture
def list_data():
    return [{"name": "John", "age": 30}, {"name": "Jane", "age": 25}]


@pytest.mark.parametrize(
    "server", ["eventlet_memory_server", "eventlet_memory_server_redis"]
)
def test_adapter_disconnect_cleanup(server, request, list_data):
    """Test that adapters are properly cleaned up when clients disconnect."""
    server_url = request.getfixturevalue(server)

    # Create first client
    c1 = znsocket.Client.from_url(server_url)

    def transform_callback(item, key, socket, converter=None, convert_nan=False):
        return znsocket.DictAdapter(key, socket, item, converter, convert_nan)

    # Create the ListAdapter
    _ = znsocket.ListAdapter(
        key="disconnect_test",
        socket=c1,
        object=list_data,
        item_transform_callback=transform_callback,
    )

    # Access an item to create a nested adapter
    lst = znsocket.List(c1, key="disconnect_test")
    dict_item = lst[0]  # This creates a DictAdapter with key "disconnect_test:0"

    # Verify the adapter exists
    assert isinstance(dict_item, znsocket.Dict)
    assert dict_item["name"] == "John"

    # Check that both the list adapter and nested dict adapter are registered
    list_adapter_key = "znsocket.List:disconnect_test"
    dict_adapter_key = "znsocket.Dict:znsocket.List:disconnect_test:0"

    exists_list_before = c1.call("adapter_exists", key=list_adapter_key)
    exists_dict_before = c1.call("adapter_exists", key=dict_adapter_key)
    assert exists_list_before is True
    assert exists_dict_before is True

    # Disconnect the client
    c1.sio.disconnect()

    # Wait a moment for cleanup
    time.sleep(0.1)

    # Create a new client and check if the adapters still exist
    c2 = znsocket.Client.from_url(server_url)
    exists_list_after = c2.call("adapter_exists", key=list_adapter_key)
    exists_dict_after = c2.call("adapter_exists", key=dict_adapter_key)

    # Both adapters should be cleaned up after disconnect
    assert exists_list_after is False, (
        "List adapter should be cleaned up after client disconnect"
    )
    assert exists_dict_after is False, (
        "Dict adapter should be cleaned up after client disconnect"
    )

    # Cleanup
    c2.flushall()
    c2.sio.disconnect()
