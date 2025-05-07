import pytest

import znsocket
from znsocket.exceptions import FrozenStorageError


@pytest.fixture
def empty() -> None:
    """Test against Python list implementation"""
    return None


@pytest.mark.parametrize(
    "client",
    ["znsclient"],  # "znsclient_w_redis", "redisclient", "empty" TODO
)
def test_list_adapter_len(client, request):
    c = request.getfixturevalue(client)
    key = "list:test"
    adapter = znsocket.ListAdapter(socket=c, key=key, object=[1, 2, 3, 4])
    lst = znsocket.List(r=c, key=key)
    assert len(adapter.object) == 4

    assert len(lst) == len(adapter.object)
    for idx, value in enumerate(adapter.object):
        assert lst[idx] == value

    assert lst._adapter_available


@pytest.mark.parametrize(
    "client",
    ["znsclient"],  # "znsclient_w_redis", "redisclient", "empty" TODO
)
def test_register_adapter_after_list_exists(client, request):
    c = request.getfixturevalue(client)
    key = "list:test"
    znsocket.List(r=c, key=key)
    with pytest.raises(KeyError):
        _ = znsocket.ListAdapter(socket=c, key=key, object=[1, 2, 3, 4])


# TODO: what if the room is modified?


@pytest.mark.parametrize(
    "client",
    ["znsclient"],  # "znsclient_w_redis", "redisclient", "empty" TODO
)
def test_list_adapter_modify(client, request):
    c = request.getfixturevalue(client)
    key = "list:test"
    znsocket.ListAdapter(socket=c, key=key, object=[1, 2, 3, 4])
    lst = znsocket.List(r=c, key=key)

    with pytest.raises(FrozenStorageError):
        lst.append(5)
    with pytest.raises(FrozenStorageError):
        lst.extend([6, 7])
    with pytest.raises(FrozenStorageError):
        lst.insert(0, 8)
    with pytest.raises(FrozenStorageError):
        lst.pop()
    with pytest.raises(FrozenStorageError):
        lst.remove(1)
    with pytest.raises(FrozenStorageError):
        lst.clear()
    with pytest.raises(FrozenStorageError):
        lst[0] = None


@pytest.mark.parametrize(
    "client",
    ["znsclient"],  # "znsclient_w_redis", "redisclient", "empty" TODO
)
def test_list_adapter_copy(client, request):
    """Test copying a list adapter"""
    c = request.getfixturevalue(client)
    key = "list:test"
    znsocket.ListAdapter(socket=c, key=key, object=[1, 2, 3, 4])
    lst = znsocket.List(r=c, key=key)
    lst_copy = lst.copy("list:test_copy")

    assert list(lst) == list(lst_copy)
