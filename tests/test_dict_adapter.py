import numpy as np
import numpy.testing as npt
import pytest
import znjson

import znsocket
from znsocket.exceptions import FrozenStorageError


@pytest.fixture
def empty() -> None:
    """Test against Python dict implementation"""
    return None


@pytest.mark.parametrize(
    "client",
    ["znsclient", "znsclient_w_redis"],
)
def test_dict_adapter_len(client, request):
    c = request.getfixturevalue(client)
    key = "dict:test"
    adapter = znsocket.DictAdapter(socket=c, key=key, object={"a": 1, "b": 2, "c": 3})
    dct = znsocket.Dict(r=c, key=key)
    assert len(adapter.object) == 3

    assert len(dct) == len(adapter.object)
    for k, v in adapter.object.items():
        assert dct[k] == v

    assert dct._adapter_available


@pytest.mark.parametrize(
    "client",
    ["znsclient", "znsclient_w_redis"],
)
def test_dict_adapter_key_error(client, request):
    c = request.getfixturevalue(client)
    key = "dict:test"
    _ = znsocket.DictAdapter(socket=c, key=key, object={"a": 1, "b": 2})
    dct = znsocket.Dict(r=c, key=key)
    with pytest.raises(KeyError):
        _ = dct["nonexistent"]


@pytest.mark.parametrize(
    "client",
    ["znsclient", "znsclient_w_redis"],
)
def test_dict_adapter_keys_values_items(client, request):
    c = request.getfixturevalue(client)
    key = "dict:test"
    test_data = {"a": 1, "b": 2, "c": 3}
    _ = znsocket.DictAdapter(socket=c, key=key, object=test_data)
    dct = znsocket.Dict(r=c, key=key)
    
    assert list(dct.keys()) == list(test_data.keys())
    assert list(dct.values()) == list(test_data.values())
    assert list(dct.items()) == list(test_data.items())



@pytest.mark.parametrize(
    "client",
    ["znsclient", "znsclient_w_redis"],
)
def test_dict_adapter_contains(client, request):
    c = request.getfixturevalue(client)
    key = "dict:test"
    test_data = {"a": 1, "b": 2, "c": 3}
    _ = znsocket.DictAdapter(socket=c, key=key, object=test_data)
    dct = znsocket.Dict(r=c, key=key)
    
    assert "a" in dct
    assert "b" in dct
    assert "c" in dct
    assert "nonexistent" not in dct


@pytest.mark.parametrize(
    "client",
    ["znsclient", "znsclient_w_redis"],
)
def test_dict_adapter_get(client, request):
    c = request.getfixturevalue(client)
    key = "dict:test"
    test_data = {"a": 1, "b": 2, "c": 3}
    _ = znsocket.DictAdapter(socket=c, key=key, object=test_data)
    dct = znsocket.Dict(r=c, key=key)
    
    assert dct.get("a") == 1
    assert dct.get("nonexistent") is None
    assert dct.get("nonexistent", "default") == "default"


@pytest.mark.parametrize(
    "client",
    ["znsclient", "znsclient_w_redis"],
)
def test_register_adapter_after_dict_exists(client, request):
    c = request.getfixturevalue(client)
    key = "dict:test"
    znsocket.Dict(r=c, key=key)
    with pytest.raises(KeyError):
        _ = znsocket.DictAdapter(socket=c, key=key, object={"a": 1, "b": 2})


@pytest.mark.parametrize(
    "client",
    ["znsclient", "znsclient_w_redis"],
)
def test_dict_adapter_modify(client, request):
    c = request.getfixturevalue(client)
    key = "dict:test"
    znsocket.DictAdapter(socket=c, key=key, object={"a": 1, "b": 2})
    dct = znsocket.Dict(r=c, key=key)

    with pytest.raises(FrozenStorageError):
        dct["c"] = 3
    with pytest.raises(FrozenStorageError):
        dct.update({"d": 4})
    with pytest.raises(FrozenStorageError):
        del dct["a"]
    with pytest.raises(FrozenStorageError):
        dct.clear()
    with pytest.raises(FrozenStorageError):
        dct.pop("a")


@pytest.mark.parametrize(
    "client",
    ["znsclient", "znsclient_w_redis"],
)
def test_dict_adapter_copy(client, request):
    """Test copying a dict adapter"""
    c = request.getfixturevalue(client)
    key = "dict:test"
    test_data = {"a": 1, "b": 2, "c": 3}
    znsocket.DictAdapter(socket=c, key=key, object=test_data)
    dct = znsocket.Dict(r=c, key=key)
    dct_copy = dct.copy("dict:test_copy")

    assert dict(dct) == dict(dct_copy)


@pytest.mark.parametrize(
    "client",
    ["znsclient", "znsclient_w_redis"],
)
def test_dict_adapter_w_converter(client, request):
    """Test dict adapter with converter"""
    c = request.getfixturevalue(client)
    key = "dict:test"
    test_data = {"array1": np.array([1, 2, 3]), "array2": np.array([4, 5, 6])}
    adapter = znsocket.DictAdapter(
        socket=c,
        key=key,
        object=test_data,
        converter=[znjson.converter.NumpyConverter],
    )
    dct = znsocket.Dict(r=c, key=key, converter=[znjson.converter.NumpyConverter])

    assert len(dct) == 2
    
    for k, v in adapter.object.items():
        npt.assert_array_equal(dct[k], v)
        assert isinstance(dct[k], np.ndarray)


@pytest.mark.parametrize(
    "client",
    ["znsclient", "znsclient_w_redis"],
)
def test_dict_adapter_w_converter_copy(client, request):
    """Test copying a dict adapter with converter"""
    c = request.getfixturevalue(client)
    key = "dict:test"
    test_data = {"array1": np.array([1, 2, 3]), "array2": np.array([4, 5, 6])}
    adapter = znsocket.DictAdapter(
        socket=c,
        key=key,
        object=test_data,
        converter=[znjson.converter.NumpyConverter],
    )
    dct = znsocket.Dict(r=c, key=key, converter=[znjson.converter.NumpyConverter])

    new_dct = dct.copy("dict:test_copy")
    assert len(new_dct) == 2
    
    for k, v in adapter.object.items():
        npt.assert_array_equal(new_dct[k], v)
        assert isinstance(new_dct[k], np.ndarray)


@pytest.mark.parametrize(
    "client",
    ["znsclient", "znsclient_w_redis"],
)
def test_dict_adapter_complex_data(client, request):
    """Test dict adapter with complex nested data"""
    c = request.getfixturevalue(client)
    key = "dict:test"
    test_data = {
        "string": "hello",
        "number": 42,
        "list": [1, 2, 3],
        "nested_dict": {"inner": "value"},
        "boolean": True,
        "null": None,
    }
    _ = znsocket.DictAdapter(socket=c, key=key, object=test_data)
    dct = znsocket.Dict(r=c, key=key)

    assert len(dct) == 6
    assert dct["string"] == "hello"
    assert dct["number"] == 42
    assert dct["list"] == [1, 2, 3]
    assert dct["nested_dict"] == {"inner": "value"}
    assert dct["boolean"] is True
    assert dct["null"] is None


@pytest.mark.parametrize(
    "client",
    ["znsclient", "znsclient_w_redis"],
)
def test_dict_adapter_iteration(client, request):
    """Test iteration over dict adapter"""
    c = request.getfixturevalue(client)
    key = "dict:test"
    test_data = {"a": 1, "b": 2, "c": 3}
    _ = znsocket.DictAdapter(socket=c, key=key, object=test_data)
    dct = znsocket.Dict(r=c, key=key)

    # Test iteration over keys
    keys = list(dct)
    assert set(keys) == set(test_data.keys())
    
    # Test dict() conversion
    assert dict(dct) == test_data


@pytest.mark.parametrize(
    "client",
    ["znsclient", "znsclient_w_redis"],
)
def test_dict_adapter_empty_dict(client, request):
    """Test dict adapter with empty dict"""
    c = request.getfixturevalue(client)
    key = "dict:test"
    _ = znsocket.DictAdapter(socket=c, key=key, object={})
    dct = znsocket.Dict(r=c, key=key)

    assert len(dct) == 0
    assert list(dct.keys()) == []
    assert list(dct.values()) == []
    assert list(dct.items()) == []
    assert dict(dct) == {}


@pytest.mark.parametrize(
    "client",
    ["znsclient", "znsclient_w_redis"],
)
def test_dict_adapter_convert_nan(client, request):
    """Test dict adapter with convert_nan option"""
    c = request.getfixturevalue(client)
    key = "dict:test"
    test_data = {"inf": float("inf"), "nan": float("nan"), "ninf": float("-inf")}
    _ = znsocket.DictAdapter(socket=c, key=key, object=test_data, convert_nan=True)
    dct = znsocket.Dict(r=c, key=key, convert_nan=True)

    assert dct["inf"] is None
    assert dct["nan"] is None
    assert dct["ninf"] is None