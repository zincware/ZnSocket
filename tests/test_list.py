import numpy as np
import numpy.testing as npt
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
    assert lst == ["1", "2", "3", "4"]
    assert lst[:] == ["1", "2", "3", "4"]

    lst.clear()
    lst.extend([1, 2, 3, 4])
    assert lst == [1, 2, 3, 4]


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


@pytest.mark.parametrize("client", ["znsclient", "redisclient", "empty"])
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


@pytest.mark.parametrize("client", ["znsclient", "redisclient", "empty"])
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

    assert [a for a in lst] == ["1", "2", "3", "4"]

    # for a, b in zip(lst, ["1", "2", "3", "4"]):
    #     assert a == b


@pytest.mark.parametrize("client", ["znsclient", "redisclient"])
def test_list_repr(client, request):
    c = request.getfixturevalue(client)
    lst = znsocket.List(r=c, key="list:test")
    lst.extend(["1", "2", "3", "4"])

    assert repr(lst) == "List(['1', '2', '3', '4'])"


@pytest.mark.parametrize("a", ["znsclient", "redisclient", "empty"])
@pytest.mark.parametrize("b", ["znsclient", "redisclient", "empty"])
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


@pytest.mark.parametrize("client", ["znsclient", "redisclient", "empty"])
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


@pytest.mark.parametrize("client", ["znsclient", "redisclient", "empty"])
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
