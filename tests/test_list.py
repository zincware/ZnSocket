import pytest

import znsocket


@pytest.fixture
def empty() -> None:
    """Test against Python list implementation"""
    return None


@pytest.mark.parametrize("client", ["znsclient", "redisclient", "empty"])
def test_list_extend(client, request):
    c = request.getfixturevalue(client)
    if c is not None:
        lst = znsocket.List(r=c, key="list:test")
    else:
        lst = []

    lst.extend(["1", "2", "3", "4"])
    # TODO assert lst == ["1", "2", "3", "4"]
    assert lst[:] == ["1", "2", "3", "4"]

    assert lst[0] == "1"
    assert lst[1::2] == ["2", "4"]
    assert lst[::-1] == ["4", "3", "2", "1"]
    assert len(lst) == 4


@pytest.mark.parametrize("client", ["znsclient", "redisclient", "empty"])
def test_list_setitem(client, request):
    c = request.getfixturevalue(client)
    if c is not None:
        lst = znsocket.List(r=c, key="list:test")
    else:
        lst = []
    lst.extend(["1", "2", "3", "4"])

    lst[2] = "c"
    assert lst[:] == ["1", "2", "c", "4"]

    lst[:2] = ["a", "b"]
    assert lst[:] == ["a", "b", "c", "4"]


@pytest.mark.parametrize("client", ["znsclient", "redisclient", "empty"])
def test_list_delitem(client, request):
    c = request.getfixturevalue(client)
    if c is not None:
        lst = znsocket.List(r=c, key="list:test")
    else:
        lst = []
    lst.extend(["1", "2", "3", "4"])

    del lst[-1]
    assert lst[:] == ["1", "2", "3"]
    assert len(lst) == 3

    del lst[:2]

    assert lst[:] == ["3"]
    assert len(lst) == 1

    del lst[:]
    assert lst[:] == []
    assert len(lst) == 0

    lst.extend(["1", "2", "3", "4"])

    del lst[1::2]
    assert lst[:] == ["1", "3"]
    assert len(lst) == 2


@pytest.mark.parametrize("client", ["znsclient", "redisclient", "empty"])
def test_list_append(client, request):
    c = request.getfixturevalue(client)
    if c is not None:
        lst = znsocket.List(r=c, key="list:test")
    else:
        lst = []
    lst.extend(["1", "2", "3", "4"])

    lst.append("5")

    assert lst[:] == ["1", "2", "3", "4", "5"]
    assert len(lst) == 5
