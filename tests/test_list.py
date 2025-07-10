from unittest.mock import MagicMock

import numpy as np
import numpy.testing as npt
import pytest
import znjson

import znsocket
import znsocket.client
from znsocket.abc import ZnSocketObject

SLEEP_TIME = 0.1


@pytest.fixture
def empty() -> None:
    """Test against Python list implementation"""
    return None


@pytest.mark.parametrize(
    "client", ["znsclient", "znsclient_w_redis", "redisclient", "empty"]
)
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

    lst.append(None)
    assert lst[-1] is None


@pytest.mark.parametrize(
    "client", ["znsclient", "znsclient_w_redis", "redisclient", "empty"]
)
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


@pytest.mark.parametrize(
    "client", ["znsclient", "znsclient_w_redis", "redisclient", "empty"]
)
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


@pytest.mark.parametrize(
    "client", ["znsclient", "znsclient_w_redis", "redisclient", "empty"]
)
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


@pytest.mark.parametrize(
    "client", ["znsclient", "znsclient_w_redis", "redisclient", "empty"]
)
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


@pytest.mark.parametrize(
    "client", ["znsclient", "znsclient_w_redis", "redisclient", "empty"]
)
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


@pytest.mark.parametrize(
    "a", ["znsclient", "znsclient_w_redis", "redisclient", "empty"]
)
@pytest.mark.parametrize(
    "b", ["znsclient", "znsclient_w_redis", "redisclient", "empty"]
)
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


@pytest.mark.parametrize(
    "client", ["znsclient", "znsclient_w_redis", "redisclient", "empty"]
)
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


@pytest.mark.parametrize(
    "client", ["znsclient", "znsclient_w_redis", "redisclient", "empty"]
)
def test_list_numpy(client, request):
    """Test ZnSocket with numpy arrays through znjson."""
    c = request.getfixturevalue(client)
    if c is not None:
        lst = znsocket.List(
            r=c, key="list:test", converter=[znjson.converter.NumpyConverter]
        )
    else:
        lst = []

    lst.extend([np.array([1, 2, 3]), np.array([4, 5, 6])])
    npt.assert_array_equal(lst[0], np.array([1, 2, 3]))
    npt.assert_array_equal(lst[1], np.array([4, 5, 6]))

    lst[1] = np.array([7, 8, 9])
    npt.assert_array_equal(lst[0], np.array([1, 2, 3]))
    npt.assert_array_equal(lst[1], np.array([7, 8, 9]))


@pytest.mark.parametrize(
    "client", ["znsclient", "znsclient_w_redis", "redisclient", "empty"]
)
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


@pytest.mark.parametrize(
    "client", ["znsclient", "znsclient_w_redis", "redisclient", "empty"]
)
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


@pytest.mark.parametrize("client", ["znsclient", "znsclient_w_redis", "redisclient"])
def test_list_refresh_append(client, request, znsclient):
    r = request.getfixturevalue(client)
    lst = znsocket.List(r=r, key="list:test", socket=znsclient)
    lst2 = znsocket.List(
        r=r, key="list:test", socket=znsocket.Client.from_url(znsclient.address)
    )
    mock = MagicMock()
    lst2.on_refresh(mock)
    assert len(lst) == 0
    lst.append(1)
    znsclient.sio.sleep(SLEEP_TIME)
    assert len(lst) == 1
    mock.assert_called_once_with({"indices": [0]})

    # append again
    lst.append(2)
    znsclient.sio.sleep(SLEEP_TIME)
    assert len(lst) == 2
    mock.assert_called_with({"indices": [1]})


@pytest.mark.parametrize("client", ["znsclient", "znsclient_w_redis", "redisclient"])
def test_list_refresh_insert(client, request, znsclient):
    r = request.getfixturevalue(client)
    lst = znsocket.List(r=r, key="list:test", socket=znsclient)
    lst2 = znsocket.List(
        r=r, key="list:test", socket=znsocket.Client.from_url(znsclient.address)
    )
    mock = MagicMock()
    lst2.on_refresh(mock)
    assert len(lst) == 0
    lst.insert(0, 1)
    znsclient.sio.sleep(SLEEP_TIME)
    assert len(lst) == 1
    mock.assert_called_once_with({"start": 0, "stop": None})

    # insert again
    lst.insert(1, 2)
    znsclient.sio.sleep(SLEEP_TIME)
    assert len(lst) == 2
    mock.assert_called_with({"start": 1, "stop": None})


@pytest.mark.parametrize("client", ["znsclient", "znsclient_w_redis", "redisclient"])
def test_list_refresh_delitem(client, request, znsclient):
    r = request.getfixturevalue(client)
    lst = znsocket.List(r=r, key="list:test", socket=znsclient)
    lst2 = znsocket.List(
        r=r, key="list:test", socket=znsocket.Client.from_url(znsclient.address)
    )
    mock = MagicMock()
    lst2.on_refresh(mock)
    lst.extend([1, 2, 3])
    znsclient.sio.sleep(SLEEP_TIME)
    # extend sends all at once
    assert mock.call_count == 1
    mock.reset_mock()

    assert len(lst) == 3
    lst.pop()
    znsclient.sio.sleep(SLEEP_TIME)
    assert len(lst) == 2
    mock.assert_called_once_with({"start": 2, "stop": None})

    # pop again
    del lst[0]
    znsclient.sio.sleep(SLEEP_TIME)
    assert len(lst) == 1
    mock.assert_called_with({"start": 0, "stop": None})


@pytest.mark.parametrize("client", ["znsclient", "znsclient_w_redis", "redisclient"])
def test_list_refresh_setitem(client, request, znsclient):
    r = request.getfixturevalue(client)
    lst = znsocket.List(r=r, key="list:test", socket=znsclient)
    lst2 = znsocket.List(
        r=r, key="list:test", socket=znsocket.Client.from_url(znsclient.address)
    )
    mock = MagicMock()
    lst2.on_refresh(mock)
    lst.extend([1, 2, 3])
    znsclient.sio.sleep(SLEEP_TIME)
    # extend sends all at once
    assert mock.call_count == 1
    mock.reset_mock()

    assert len(lst) == 3
    lst[0] = 4
    znsclient.sio.sleep(SLEEP_TIME)
    assert len(lst) == 3
    mock.assert_called_once_with({"indices": [0]})

    # set again
    lst[1] = 5
    znsclient.sio.sleep(SLEEP_TIME)
    assert len(lst) == 3
    mock.assert_called_with({"indices": [1]})


@pytest.mark.parametrize("client", ["znsclient", "znsclient_w_redis", "redisclient"])
def test_list_refresh_setitem_self_trigger(client, request, znsclient):
    r = request.getfixturevalue(client)
    lst = znsocket.List(r=r, key="list:test", socket=znsclient)
    mock = MagicMock()
    lst.on_refresh(mock)
    lst.extend([1, 2, 3])
    znsclient.sio.sleep(SLEEP_TIME)
    mock.assert_not_called()

    assert len(lst) == 3
    lst[0] = 4
    znsclient.sio.sleep(SLEEP_TIME)
    assert len(lst) == 3
    # assert mock was not called
    mock.assert_not_called()

    # set again
    lst[1] = 5
    znsclient.sio.sleep(SLEEP_TIME)
    assert len(lst) == 3
    mock.assert_not_called()


@pytest.mark.parametrize("client", ["znsclient", "znsclient_w_redis", "redisclient"])
def test_list_copy(client, request):
    c = request.getfixturevalue(client)
    lst = znsocket.List(r=c, key="list:test")
    assert isinstance(lst, ZnSocketObject)

    lst.extend(["1", "2", "3", "4"])
    lst2 = lst.copy(key="list:test:copy")

    assert lst == lst2
    assert lst is not lst2

    lst2.append("5")
    assert lst != lst2
    assert lst == ["1", "2", "3", "4"]
    assert lst2 == ["1", "2", "3", "4", "5"]

    with pytest.raises(ValueError):
        lst.copy(key="list:test:copy")


@pytest.mark.parametrize(
    "client", ["znsclient", "znsclient_w_redis", "redisclient", "empty"]
)
def test_list_delete_empty(client, request):
    c = request.getfixturevalue(client)
    if c is not None:
        lst = znsocket.List(r=c, key="list:test")
    else:
        lst = []

    lst.extend(["1", "2", "3", "4"])
    del lst[:]
    del lst[:]  # run twice
    assert lst == []
    assert len(lst) == 0
    assert lst[:] == []

    with pytest.raises(IndexError):
        del lst[0]


@pytest.mark.parametrize("client", ["znsclient", "znsclient_w_redis", "redisclient"])
def test_list_refresh_extend(client, request, znsclient):
    r = request.getfixturevalue(client)
    lst = znsocket.List(r=r, key="list:test", socket=znsclient)
    lst2 = znsocket.List(
        r=r, key="list:test", socket=znsocket.Client.from_url(znsclient.address)
    )
    mock = MagicMock()
    lst2.on_refresh(mock)
    assert len(lst) == 0
    lst.extend([1, 2, 3])
    znsclient.sio.sleep(SLEEP_TIME)
    assert len(lst) == 3
    mock.assert_called_once_with({"start": 0, "stop": None})

    mock.reset_mock()
    lst.extend([4, 5, 6])
    znsclient.sio.sleep(SLEEP_TIME)
    assert len(lst) == 6
    mock.assert_called_once_with({"start": 3, "stop": None})


@pytest.mark.parametrize("client", ["znsclient", "znsclient_w_redis", "redisclient"])
def test_list_refresh_extend_self_trigger(client, request, znsclient):
    r = request.getfixturevalue(client)
    lst = znsocket.List(r=r, key="list:test", socket=znsclient)

    mock = MagicMock()
    lst.on_refresh(mock)
    assert len(lst) == 0
    lst.extend([1, 2, 3])
    znsclient.sio.sleep(SLEEP_TIME)
    assert len(lst) == 3
    mock.assert_not_called()


@pytest.mark.parametrize("client", ["znsclient", "znsclient_w_redis", "redisclient"])
def test_invalid_json(client, request):
    c = request.getfixturevalue(client)
    dct = znsocket.List(r=c, key="list:test")

    with pytest.raises(ValueError):
        dct.append(float("inf"))
    with pytest.raises(ValueError):
        dct.append(float("nan"))
    with pytest.raises(ValueError):
        dct.append(float("-inf"))

    dct.convert_nan = True

    dct.append(float("inf"))
    dct.append(float("nan"))
    dct.append(float("-inf"))

    assert len(dct) == 3

    assert dct[0] is None
    assert dct[1] is None
    assert dct[2] is None


@pytest.mark.parametrize(
    "client",
    ["znsclient", "znsclient_w_redis"],
)
def test_list_list_fallback_get(client, request):
    """Test what happens when nested objects in the adapter are updated"""
    c = request.getfixturevalue(client)
    key = "list:test"
    fallback_key = "list:fallback"
    fallback_lst = znsocket.List(r=c, key=fallback_key)
    fallback_lst.extend(["a", "b", "c"])
    lst = znsocket.List(r=c, key=key, fallback=fallback_key, fallback_policy="frozen")

    assert len(lst) == 3
    assert lst[0] == "a"
    assert lst[1] == "b"
    assert lst[2] == "c"
    total = 0
    for val in lst:
        assert val in fallback_lst
        total += 1
    assert total == 3
