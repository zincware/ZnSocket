import numpy as np
import numpy.testing as npt
import pytest
import znjson

import znsocket
from znsocket.exceptions import FrozenStorageError


@pytest.fixture
def empty() -> None:
    """Test against Python list implementation"""
    return None


@pytest.mark.parametrize(
    "client",
    ["znsclient", "znsclient_w_redis"],  # , "empty" TODO
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
    ["znsclient", "znsclient_w_redis"],  # , "redisclient", "empty" TODO
)
def test_list_adapter_index_error(client, request):
    c = request.getfixturevalue(client)
    key = "list:test"
    _ = znsocket.ListAdapter(socket=c, key=key, object=[1, 2, 3, 4])
    lst = znsocket.List(r=c, key=key)
    with pytest.raises(IndexError):
        _ = lst[4]


@pytest.mark.parametrize(
    "client",
    ["znsclient", "znsclient_w_redis"],  # , "redisclient", "empty" TODO
)
def test_list_adapter_index_iter(client, request):
    c = request.getfixturevalue(client)
    key = "list:test"
    _ = znsocket.ListAdapter(socket=c, key=key, object=[1, 2, 3, 4])
    lst = znsocket.List(r=c, key=key)
    assert list(lst) == [1, 2, 3, 4]
    for value in lst:
        assert value in [1, 2, 3, 4]


@pytest.mark.parametrize(
    "client",
    ["znsclient", "znsclient_w_redis"],  # , "redisclient", "empty" TODO
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
    ["znsclient", "znsclient_w_redis"],  # , "redisclient", "empty" TODO
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
    ["znsclient", "znsclient_w_redis"],  # , "redisclient", "empty" TODO
)
def test_list_adapter_copy(client, request):
    """Test copying a list adapter"""
    c = request.getfixturevalue(client)
    key = "list:test"
    znsocket.ListAdapter(socket=c, key=key, object=[1, 2, 3, 4])
    lst = znsocket.List(r=c, key=key)
    lst_copy = lst.copy("list:test_copy")

    assert list(lst) == list(lst_copy)


@pytest.mark.parametrize(
    "client",
    ["znsclient", "znsclient_w_redis"],  # , "redisclient", "empty" TODO
)
def test_list_adapter_w_converter(client, request):
    """Test copying a list adapter"""
    c = request.getfixturevalue(client)
    key = "list:test"
    adapter = znsocket.ListAdapter(
        socket=c,
        key=key,
        object=np.arange(9).reshape(3, 3),
        converter=[znjson.converter.NumpyConverter],
    )
    lst = znsocket.List(r=c, key=key, converter=[znjson.converter.NumpyConverter])

    assert len(lst) == 3

    for idx, row in enumerate(adapter.object):
        npt.assert_array_equal(lst[idx], row)
        assert isinstance(lst[idx], np.ndarray)


@pytest.mark.parametrize(
    "client",
    ["znsclient", "znsclient_w_redis"],  # , "redisclient", "empty" TODO
)
def test_list_adapter_w_converter_iter(client, request):
    """Test copying a list adapter"""
    c = request.getfixturevalue(client)
    key = "list:test"
    adapter = znsocket.ListAdapter(
        socket=c,
        key=key,
        object=np.arange(9).reshape(3, 3),
        converter=[znjson.converter.NumpyConverter],
    )
    lst = znsocket.List(r=c, key=key, converter=[znjson.converter.NumpyConverter])

    assert len(lst) == 3

    data = np.array(lst)
    npt.assert_array_equal(data, adapter.object)


@pytest.mark.parametrize(
    "client",
    ["znsclient", "znsclient_w_redis"],  # , "redisclient", "empty" TODO
)
def test_list_adapter_w_converter_copy(client, request):
    """Test copying a list adapter"""
    c = request.getfixturevalue(client)
    key = "list:test"
    adapter = znsocket.ListAdapter(
        socket=c,
        key=key,
        object=np.arange(9).reshape(3, 3),
        converter=[znjson.converter.NumpyConverter],
    )
    lst = znsocket.List(r=c, key=key, converter=[znjson.converter.NumpyConverter])

    new_lst = lst.copy("list:test_copy")
    assert len(new_lst) == 3
    new_lst_array = np.array(new_lst)
    npt.assert_array_equal(new_lst_array, adapter.object)


@pytest.mark.parametrize(
    "client",
    ["znsclient", "znsclient_w_redis"],
)
def test_list_adapter_slice_basic(client, request):
    """Test basic slicing with list adapter"""
    c = request.getfixturevalue(client)
    key = "list:test"
    test_data = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
    _ = znsocket.ListAdapter(socket=c, key=key, object=test_data)
    lst = znsocket.List(r=c, key=key)
    
    # Test basic slice
    assert lst[1:5] == test_data[1:5]
    assert lst[:3] == test_data[:3]
    assert lst[7:] == test_data[7:]
    assert lst[:] == test_data[:]


@pytest.mark.parametrize(
    "client",
    ["znsclient", "znsclient_w_redis"],
)
def test_list_adapter_slice_with_step(client, request):
    """Test slicing with step with list adapter"""
    c = request.getfixturevalue(client)
    key = "list:test"
    test_data = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
    _ = znsocket.ListAdapter(socket=c, key=key, object=test_data)
    lst = znsocket.List(r=c, key=key)
    
    # Test slice with step
    assert lst[::2] == test_data[::2]
    assert lst[1::2] == test_data[1::2]
    assert lst[1:8:2] == test_data[1:8:2]


@pytest.mark.parametrize(
    "client",
    ["znsclient", "znsclient_w_redis"],
)
def test_list_adapter_slice_negative_indices(client, request):
    """Test slicing with negative indices with list adapter"""
    c = request.getfixturevalue(client)
    key = "list:test"
    test_data = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
    _ = znsocket.ListAdapter(socket=c, key=key, object=test_data)
    lst = znsocket.List(r=c, key=key)
    
    # Test slice with negative indices
    assert lst[-3:] == test_data[-3:]
    assert lst[:-2] == test_data[:-2]
    assert lst[-5:-1] == test_data[-5:-1]


@pytest.mark.parametrize(
    "client",
    ["znsclient", "znsclient_w_redis"],
)
def test_list_adapter_slice_empty(client, request):
    """Test slicing that returns empty list"""
    c = request.getfixturevalue(client)
    key = "list:test"
    test_data = [1, 2, 3, 4, 5]
    _ = znsocket.ListAdapter(socket=c, key=key, object=test_data)
    lst = znsocket.List(r=c, key=key)
    
    # Test empty slices
    assert lst[10:20] == test_data[10:20]  # Should be empty
    assert lst[3:3] == test_data[3:3]      # Should be empty


@pytest.mark.parametrize(
    "client",
    ["znsclient", "znsclient_w_redis"],
)
def test_list_adapter_slice_with_converter(client, request):
    """Test slicing with converter"""
    c = request.getfixturevalue(client)
    key = "list:test"
    test_data = [np.array([1, 2]), np.array([3, 4]), np.array([5, 6]), np.array([7, 8])]
    _ = znsocket.ListAdapter(
        socket=c,
        key=key,
        object=test_data,
        converter=[znjson.converter.NumpyConverter],
    )
    lst = znsocket.List(r=c, key=key, converter=[znjson.converter.NumpyConverter])
    
    # Test slice with converter
    sliced = lst[1:3]
    assert len(sliced) == 2
    npt.assert_array_equal(sliced[0], test_data[1])
    npt.assert_array_equal(sliced[1], test_data[2])
