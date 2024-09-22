from unittest.mock import MagicMock

import numpy as np
import numpy.testing as npt
import pytest

import znsocket
from znsocket.utils import ZnSocketObject


@pytest.fixture
def empty() -> None:
    """Test against Python list implementation"""
    return None


@pytest.mark.parametrize("client", ["znsclient", "znsclient_w_redis", "redisclient", "empty"])
def test_list_extend(client, request):
    c = request.getfixturevalue(client)
    if c is not None:
        lst = znsocket.List(r=c, key="list:test")
        assert isinstance(lst, ZnSocketObject)
    else:
        lst = []

    lst.extend(["1", "2", "3", "4"])
    assert lst == ["1", "2", "3", "4"]
    assert lst[:] == ["1", "2", "3", "4"]

    lst.clear()
    lst.extend([1, 2, 3, 4])
    assert lst == [1, 2, 3, 4]

    lst.extend([5, 6.28, "7"])
    assert lst == [1, 2, 3, 4, 5, 6.28, "7"]


@pytest.mark.parametrize("client", ["znsclient", "znsclient_w_redis", "redisclient", "empty"])
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

    with pytest.raises(IndexError):
        lst[10] = "x"

    lst.clear()
    assert lst[:] == []
    lst.extend(["1", "2", "3", "4"])
    lst[1::2] = ["a", "b"]
    assert lst[:] == ["1", "a", "3", "b"]

    if c is not None:
        # Here we diverge from the Python list implementation
        # which replaces the slice and then inserts the rest
        with pytest.raises(
            ValueError,
            match="attempt to assign sequence of size 3 to extended slice of size 2",
        ):
            lst[:2] = ["a", "b", "c"]


@pytest.mark.parametrize("client", ["znsclient", "znsclient_w_redis", "redisclient", "empty"])
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


@pytest.mark.parametrize("client", ["znsclient", "znsclient_w_redis", "redisclient", "empty"])
def test_list_append(client, request):
    c = request.getfixturevalue(client)
    if c is not None:
        lst = znsocket.List(r=c, key="list:test")
    else:
        lst = []

    for idx in range(1, 6):
        lst.append(str(idx))

    assert lst[:] == ["1", "2", "3", "4", "5"]
    assert len(lst) == 5


@pytest.mark.parametrize("client", ["znsclient", "znsclient_w_redis", "redisclient", "empty"])
def test_list_insert(client, request):
    c = request.getfixturevalue(client)
    if c is not None:
        lst = znsocket.List(r=c, key="list:test")
    else:
        lst = []
    lst.extend(["1", "2", "3", "4"])

    lst.insert(1, "x")
    assert lst[:] == ["1", "x", "2", "3", "4"]

    lst.insert(0, "y")
    assert lst[:] == ["y", "1", "x", "2", "3", "4"]


@pytest.mark.parametrize("client", ["znsclient", "znsclient_w_redis", "redisclient", "empty"])
def test_list_iter(client, request):
    c = request.getfixturevalue(client)
    if c is not None:
        lst = znsocket.List(r=c, key="list:test")
    else:
        lst = []
    lst.extend(["1", "2", "3", "4"])

    assert lst[0] == "1"
    assert lst[1] == "2"
    assert lst[2] == "3"
    assert lst[3] == "4"

    for a, b in zip(lst, ["1", "2", "3", "4"]):
        assert a == b


@pytest.mark.parametrize("client", ["znsclient", "znsclient_w_redis", "redisclient"])
def test_list_repr(client, request):
    c = request.getfixturevalue(client)
    lst = znsocket.List(r=c, key="list:test")
    lst.extend(["1", "2", "3", "4"])

    lst.repr_type = "full"
    assert repr(lst) == "List(['1', '2', '3', '4'])"
    lst.repr_type = "length"
    assert repr(lst) == "List(len=4)"
    lst.repr_type = "minimal"
    assert repr(lst) == "List(<unknown>)"

    lst.repr_type = "unsupported"
    with pytest.raises(ValueError, match="Invalid repr_type: unsupported"):
        repr(lst)


@pytest.mark.parametrize("a", ["znsclient", "znsclient_w_redis", "redisclient", "empty"])
@pytest.mark.parametrize("b", ["znsclient", "znsclient_w_redis", "redisclient", "empty"])
def test_list_equal(a, b, request):
    a = request.getfixturevalue(a)
    b = request.getfixturevalue(b)
    if a is not None:
        lst1 = znsocket.List(r=a, key="list:test:a")
    else:
        lst1 = []

    if b is not None:
        lst2 = znsocket.List(r=b, key="list:test:b")
    else:
        lst2 = []

    lst1.extend(["1", "2", "3", "4"])
    lst2.extend(["1", "2", "3", "4"])

    assert lst1 == lst2

    lst1.pop()
    assert lst1 == ["1", "2", "3"]
    assert lst2 == ["1", "2", "3", "4"]
    assert lst1 != lst2

    assert lst1 != "unsupported"


@pytest.mark.parametrize("client", ["znsclient", "znsclient_w_redis", "redisclient", "empty"])
def test_list_getitem(client, request):
    c = request.getfixturevalue(client)
    if c is not None:
        lst = znsocket.List(r=c, key="list:test")
    else:
        lst = []
    lst.extend(["1", "2", "3", "4"])

    assert lst[0] == "1"
    assert lst[1::2] == ["2", "4"]
    assert lst[::-1] == ["4", "3", "2", "1"]
    assert len(lst) == 4

    with pytest.raises(IndexError):
        lst[10]


@pytest.mark.parametrize("client", ["znsclient", "znsclient_w_redis", "redisclient", "empty"])
def test_list_numpy(client, request):
    """Test ZnSocket with numpy arrays through znjson."""
    c = request.getfixturevalue(client)
    if c is not None:
        lst = znsocket.List(r=c, key="list:test")
    else:
        lst = []

    lst.extend([np.array([1, 2, 3]), np.array([4, 5, 6])])
    npt.assert_array_equal(lst[0], np.array([1, 2, 3]))
    npt.assert_array_equal(lst[1], np.array([4, 5, 6]))

    lst[1] = np.array([7, 8, 9])
    npt.assert_array_equal(lst[0], np.array([1, 2, 3]))
    npt.assert_array_equal(lst[1], np.array([7, 8, 9]))


@pytest.mark.parametrize("client", ["znsclient", "znsclient_w_redis", "redisclient", "empty"])
def test_list_set_dict(client, request):
    """Test ZnSocket with numpy arrays through znjson."""
    c = request.getfixturevalue(client)
    if c is not None:
        lst = znsocket.List(r=c, key="list:test")
    else:
        lst = []

    lst.append("Hello World")
    with pytest.raises(TypeError, match="list indices must be integers or slices"):
        lst["0"] = "Lorem ipsum"


@pytest.mark.parametrize("client", ["znsclient", "znsclient_w_redis", "redisclient", "empty"])
def test_list_set_get_negative(client, request):
    """Test ZnSocket with negative indices."""
    c = request.getfixturevalue(client)
    if c is not None:
        lst = znsocket.List(r=c, key="list:test")
    else:
        lst = []

    lst.extend(["Hello", "World"])
    assert lst[-1] == "World"
    assert lst[-2] == "Hello"

    lst[-1] = "Lorem"
    assert lst[-1] == "Lorem"
    assert lst[-2] == "Hello"


@pytest.mark.parametrize("client", ["znsclient", "znsclient_w_redis", "redisclient"])
def test_list_callbacks(client, request):
    """Test ZnSocket with negative indices."""
    c = request.getfixturevalue(client)
    append_callback = MagicMock()
    setitem_callback = MagicMock()
    delitem_callback = MagicMock()
    insert_callback = MagicMock()

    lst = znsocket.List(
        r=c,
        key="list:test",
        callbacks={
            "append": append_callback,
            "setitem": setitem_callback,
            "delitem": delitem_callback,
            "insert": insert_callback,
        },
    )
    lst.append(1)
    append_callback.assert_called_once_with(1)
    lst[0] = 2
    setitem_callback.assert_called_once_with([0], [2])
    del lst[0]
    delitem_callback.assert_called_once_with([0])
    lst.insert(0, 3)
    insert_callback.assert_called_once_with(0, 3)


# TODO: if different clients are used, things get weird therefore znsclient is not used in this test
@pytest.mark.parametrize("a", ["redisclient", "empty"])
@pytest.mark.parametrize("b", ["redisclient", "empty"])
def test_list_nested(a, b, request):
    a = request.getfixturevalue(a)
    b = request.getfixturevalue(b)
    if a is not None:
        lst1 = znsocket.List(r=a, key="list:test:a")
    else:
        lst1 = []

    if b is not None:
        lst2 = znsocket.List(r=b, key="list:test:b")
    else:
        lst2 = []

    lst1.extend(["1", "2", "3", "4"])
    lst2.extend([lst1, lst1])

    assert lst2 == [["1", "2", "3", "4"], ["1", "2", "3", "4"]]
