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


@pytest.mark.parametrize(
    "client",
    ["znsclient", "znsclient_w_redis"],
)
def test_dict_adapter_object_update(client, request):
    """Test what happens when the adapter object is updated"""
    c = request.getfixturevalue(client)
    key = "dict:test"
    test_data = {"a": 1, "b": 2, "c": 3}
    adapter = znsocket.DictAdapter(socket=c, key=key, object=test_data)
    dct = znsocket.Dict(r=c, key=key)

    # Initial state
    assert len(dct) == 3
    assert dct["a"] == 1
    assert dct["b"] == 2
    assert dct["c"] == 3
    assert dict(dct) == {"a": 1, "b": 2, "c": 3}
    assert set(dct.keys()) == {"a", "b", "c"}
    assert set(dct.values()) == {1, 2, 3}

    # Update the adapter object
    adapter.object["d"] = 4
    adapter.object["a"] = 10
    adapter.object["e"] = 5

    # The Dict should reflect the changes immediately
    assert len(dct) == 5  # 3 + 2 new keys
    assert dct["a"] == 10  # Modified value
    assert dct["b"] == 2  # Unchanged
    assert dct["c"] == 3  # Unchanged
    assert dct["d"] == 4  # New key
    assert dct["e"] == 5  # New key

    # Test keys, values, items with updated object
    assert set(dct.keys()) == {"a", "b", "c", "d", "e"}
    assert set(dct.values()) == {10, 2, 3, 4, 5}
    assert set(dct.items()) == {("a", 10), ("b", 2), ("c", 3), ("d", 4), ("e", 5)}

    # Test contains with updated object
    assert "a" in dct
    assert "d" in dct
    assert "e" in dct
    assert "f" not in dct

    # Test get with updated object
    assert dct.get("a") == 10
    assert dct.get("d") == 4
    assert dct.get("f") is None
    assert dct.get("f", "default") == "default"

    # Remove elements
    del adapter.object["b"]
    adapter.object.pop("c")

    # Should reflect removals
    assert len(dct) == 3
    assert set(dct.keys()) == {"a", "d", "e"}
    assert dict(dct) == {"a": 10, "d": 4, "e": 5}
    assert "b" not in dct
    assert "c" not in dct


@pytest.mark.parametrize(
    "client",
    ["znsclient", "znsclient_w_redis"],
)
def test_dict_adapter_object_clear_and_update(client, request):
    """Test what happens when the adapter object is cleared and updated"""
    c = request.getfixturevalue(client)
    key = "dict:test"
    test_data = {"a": 1, "b": 2, "c": 3}
    adapter = znsocket.DictAdapter(socket=c, key=key, object=test_data)
    dct = znsocket.Dict(r=c, key=key)

    # Initial state
    assert len(dct) == 3
    assert dict(dct) == {"a": 1, "b": 2, "c": 3}

    # Clear the adapter object
    adapter.object.clear()

    # The Dict should reflect the empty state
    assert len(dct) == 0
    assert dict(dct) == {}
    assert list(dct.keys()) == []
    assert list(dct.values()) == []
    assert list(dct.items()) == []

    # Test accessing cleared dict
    assert "a" not in dct
    assert dct.get("a") is None

    # Update with new data
    adapter.object.update({"x": 10, "y": 20, "z": 30})

    # The Dict should reflect the new data
    assert len(dct) == 3
    assert dict(dct) == {"x": 10, "y": 20, "z": 30}
    assert dct["x"] == 10
    assert dct["y"] == 20
    assert dct["z"] == 30

    # Test operations with new data
    assert set(dct.keys()) == {"x", "y", "z"}
    assert set(dct.values()) == {10, 20, 30}
    assert "x" in dct
    assert "a" not in dct
    assert dct.get("x") == 10
    assert dct.get("a") is None


@pytest.mark.parametrize(
    "client",
    ["znsclient", "znsclient_w_redis"],
)
def test_dict_adapter_object_nested_update(client, request):
    """Test what happens when nested objects in the adapter are updated"""
    c = request.getfixturevalue(client)
    key = "dict:test"
    nested_list = [1, 2, 3]
    nested_dict = {"inner": "value"}
    test_data = {"list": nested_list, "dict": nested_dict, "simple": "text"}
    adapter = znsocket.DictAdapter(socket=c, key=key, object=test_data)
    dct = znsocket.Dict(r=c, key=key)

    # Initial state
    assert len(dct) == 3
    assert dct["list"] == [1, 2, 3]
    assert dct["dict"] == {"inner": "value"}
    assert dct["simple"] == "text"

    # Update nested objects
    nested_list.append(4)
    nested_list[0] = 10
    nested_dict["inner"] = "updated"
    nested_dict["new"] = "added"

    # The Dict should reflect the changes to nested objects
    assert dct["list"] == [10, 2, 3, 4]
    assert dct["dict"] == {"inner": "updated", "new": "added"}
    assert dct["simple"] == "text"  # Unchanged

    # Replace nested objects
    adapter.object["list"] = ["a", "b", "c"]
    adapter.object["dict"] = {"completely": "different"}

    # Should reflect the replacements
    assert dct["list"] == ["a", "b", "c"]
    assert dct["dict"] == {"completely": "different"}
    assert dct["simple"] == "text"


@pytest.mark.parametrize(
    "client",
    ["znsclient", "znsclient_w_redis"],
)
def test_dict_adapter_fallback_get(client, request):
    """Test what happens when nested objects in the adapter are updated"""
    c = request.getfixturevalue(client)
    key = "dict:test"
    fallback_key = "dict:fallback"
    test_data = {"a": 1, "b": 2, "c": 3}
    _ = znsocket.DictAdapter(socket=c, key=fallback_key, object=test_data)
    dct = znsocket.Dict(r=c, key=key, fallback=fallback_key, fallback_policy="frozen")

    assert len(dct) == 3
    assert dct["a"] == 1
    assert dct.values() == [1, 2, 3]
    assert dct.keys() == ["a", "b", "c"]
    assert dct.items() == [("a", 1), ("b", 2), ("c", 3)]
    assert dict(dct) == {"a": 1, "b": 2, "c": 3}
    total = 0
    for k, v in dct.items():
        assert dct[k] == v
        total += 1
    assert total == 3


@pytest.mark.parametrize(
    "client",
    ["znsclient", "znsclient_w_redis"],
)
def test_dict_adapter_fallback_set(client, request):
    """Test what happens when nested objects in the adapter are updated"""
    pass
    # Set is currently not supported when using a fallback object.
