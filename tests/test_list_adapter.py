import pytest

import znsocket


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
    lst = znsocket.List(r=c, key=key)
    lst.append(1)
    with pytest.raises(KeyError):
        _ = znsocket.ListAdapter(socket=c, key=key, object=[1, 2, 3, 4])
