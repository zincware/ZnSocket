from unittest.mock import MagicMock

import numpy as np
import numpy.testing as npt
import pytest
import znjson

import znsocket
from znsocket.abc import ZnSocketObject


@pytest.mark.parametrize(
    "client", ["znsclient", "znsclient_w_redis", "redisclient", "empty"]
)
def test_dct_set_get_item(client, request):
    c = request.getfixturevalue(client)
    if c is not None:
        dct = znsocket.Dict(r=c, key="list:test")
        assert isinstance(dct, ZnSocketObject)
    else:
        dct = {}

    dct["a"] = "1"
    assert dct == {"a": "1"}

    dct["b"] = "2"
    assert dct == {"a": "1", "b": "2"}

    assert dct["a"] == "1"
    assert dct["b"] == "2"

    with pytest.raises(KeyError, match="nonexistent"):
        dct["nonexistent"]


@pytest.mark.parametrize(
    "client", ["znsclient", "znsclient_w_redis", "redisclient", "empty"]
)
def test_dct_del_item(client, request):
    c = request.getfixturevalue(client)
    if c is not None:
        dct = znsocket.Dict(r=c, key="list:test")
    else:
        dct = {}

    dct["a"] = "1"
    dct["b"] = "2"
    del dct["a"]
    assert dct == {"b": "2"}
    with pytest.raises(KeyError, match="nonexistent"):
        del dct["nonexistent"]


@pytest.mark.parametrize(
    "client", ["znsclient", "znsclient_w_redis", "redisclient", "empty"]
)
def test_dct_update(client, request):
    c = request.getfixturevalue(client)
    if c is not None:
        dct = znsocket.Dict(r=c, key="list:test")
    else:
        dct = {}

    dct.update({"a": "1", "b": "2"})
    assert dct == {"a": "1", "b": "2"}

    dct.update({"b": "3", "c": "4"})
    assert dct == {"a": "1", "b": "3", "c": "4"}

    dct.update(d="5")
    assert dct == {"a": "1", "b": "3", "c": "4", "d": "5"}


@pytest.mark.parametrize(
    "client", ["znsclient", "znsclient_w_redis", "redisclient", "empty"]
)
def test_dct_iter(client, request):
    c = request.getfixturevalue(client)
    if c is not None:
        dct = znsocket.Dict(r=c, key="list:test")
    else:
        dct = {}

    dct.update({"a": "1", "b": "2"})

    assert list(dct) == ["a", "b"]
    assert list(dct.keys()) == ["a", "b"]
    assert list(dct.values()) == ["1", "2"]
    assert list(dct.items()) == [("a", "1"), ("b", "2")]


@pytest.mark.parametrize(
    "client", ["znsclient", "znsclient_w_redis", "redisclient", "empty"]
)
def test_dct_contains(client, request):
    c = request.getfixturevalue(client)
    if c is not None:
        dct = znsocket.Dict(r=c, key="list:test")
    else:
        dct = {}

    dct.update({"a": "1", "b": "2"})

    assert "a" in dct
    assert "b" in dct
    assert "c" not in dct


@pytest.mark.parametrize("client", ["znsclient", "znsclient_w_redis", "redisclient"])
def test_dct_repr(client, request):
    c = request.getfixturevalue(client)
    dct = znsocket.Dict(r=c, key="list:test")
    dct.update({"a": "1", "b": "2"})
    dct.repr_type = "full"
    assert repr(dct) == "Dict({'a': '1', 'b': '2'})"
    dct.repr_type = "keys"
    assert repr(dct) == "Dict(keys=['a', 'b'])"
    dct.repr_type = "minimal"
    assert repr(dct) == "Dict(<unknown>)"

    dct.repr_type = "unsupported"
    with pytest.raises(ValueError, match="Invalid repr_type: unsupported"):
        repr(dct)


@pytest.mark.parametrize(
    "a", ["znsclient", "znsclient_w_redis", "redisclient", "empty"]
)
@pytest.mark.parametrize(
    "b", ["znsclient", "znsclient_w_redis", "redisclient", "empty"]
)
def test_dict_equal(a, b, request):
    a = request.getfixturevalue(a)
    b = request.getfixturevalue(b)
    if a is not None:
        dct1 = znsocket.Dict(r=a, key="dict:test:a")
    else:
        dct1 = {}

    if b is not None:
        dct2 = znsocket.Dict(r=b, key="dict:test:b")
    else:
        dct2 = {}

    dct1.update({"a": "1", "b": "2"})
    dct2.update({"a": "1", "b": "2"})

    assert dct1 == dct2

    del dct1["b"]
    assert dct1 == {"a": "1"}
    assert dct2 == {"a": "1", "b": "2"}
    assert dct1 != dct2

    assert dct1 != "unsupported"


# @pytest.mark.parametrize(
#     "client", ["znsclient", "znsclient_w_redis", "redisclient", "empty"]
# )
# def test_dct_similar_keys(client, request):
#     c = request.getfixturevalue(client)
#     if c is not None:
#         dct = znsocket.Dict(r=c, key="list:test", repr_type="full")
#     else:
#         dct = {}

#     dct.update({1: 1, "1": "1"})
#     assert dct[1] == 1
#     assert dct["1"] == "1"
#     assert dct == {1: 1, "1": "1"}
#     if c is not None:
#         assert repr(dct) == "Dict({1: 1, '1': '1'})"

#     del dct[1]
#     assert dct == {"1": "1"}
#     del dct["1"]
#     assert dct == {}
# REDIS can not differentiate between int/float and str keys


@pytest.mark.parametrize(
    "client", ["znsclient", "znsclient_w_redis", "redisclient", "empty"]
)
def test_dct_numpy(client, request):
    c = request.getfixturevalue(client)
    if c is not None:
        dct = znsocket.Dict(
            r=c, key="list:test", converter=[znjson.converter.NumpyConverter]
        )
    else:
        dct = {}

    dct["a"] = np.array([1, 2, 3])
    npt.assert_array_equal(dct["a"], np.array([1, 2, 3]))


@pytest.mark.parametrize(
    "client", ["znsclient", "znsclient_w_redis", "redisclient", "empty"]
)
def test_dct_get(client, request):
    c = request.getfixturevalue(client)
    if c is not None:
        dct = znsocket.Dict(r=c, key="list:test")
    else:
        dct = {}

    dct["a"] = "1"
    assert dct.get("a") == "1"
    assert dct.get("b") is None


@pytest.mark.parametrize("client", ["znsclient", "znsclient_w_redis", "redisclient"])
def test_dict_callbacks(client, request):
    """Test ZnSocket with negative indices."""
    c = request.getfixturevalue(client)
    setitem_callback = MagicMock()
    delitem_callback = MagicMock()

    dct = znsocket.Dict(
        r=c,
        key="dict:test",
        callbacks={
            "setitem": setitem_callback,
            "delitem": delitem_callback,
        },
    )
    dct["a"] = 1
    setitem_callback.assert_called_once_with("a", 1)
    del dct["a"]
    delitem_callback.assert_called_once_with("a")


# TODO: if different clients are used, things get weird therefore znsclient is not used in this test
@pytest.mark.parametrize("a", ["redisclient", "empty"])
@pytest.mark.parametrize("b", ["redisclient", "empty"])
def test_dict_nested(a, b, request):
    a = request.getfixturevalue(a)
    b = request.getfixturevalue(b)
    if a is not None:
        dct1 = znsocket.Dict(r=a, key="dict:test:a")
    else:
        dct1 = {}

    if b is not None:
        dct2 = znsocket.Dict(r=b, key="dict:test:b")
    else:
        dct2 = {}

    dct1.update({"a": "1", "b": "2"})
    dct2.update({"dct1": dct1, "b": "2"})

    assert dct2 == {"dct1": {"a": "1", "b": "2"}, "b": "2"}
    assert dct2["dct1"] == {"a": "1", "b": "2"}


@pytest.mark.parametrize("client", ["znsclient", "znsclient_w_redis", "redisclient"])
def test_dict_refresh_setitem(client, request, znsclient):
    r = request.getfixturevalue(client)
    dct = znsocket.Dict(r=r, key="dct:test", socket=znsclient)
    dct2 = znsocket.Dict(
        r=r, key="dct:test", socket=znsocket.Client.from_url(znsclient.address)
    )
    mock = MagicMock()
    dct2.on_refresh(mock)

    dct["a"] = 1
    assert dct == {"a": 1}
    znsclient.sio.sleep(0.2)
    mock.assert_called_with({"keys": ["a"]})
    dct["b"] = [1, 2, 3]
    assert dct == {"a": 1, "b": [1, 2, 3]}
    znsclient.sio.sleep(0.2)
    mock.assert_called_with({"keys": ["b"]})


@pytest.mark.parametrize("client", ["znsclient", "znsclient_w_redis", "redisclient"])
def test_dict_refresh_delitem(client, request, znsclient):
    r = request.getfixturevalue(client)
    dct = znsocket.Dict(r=r, key="dct:test", socket=znsclient)
    dct2 = znsocket.Dict(
        r=r, key="dct:test", socket=znsocket.Client.from_url(znsclient.address)
    )
    mock = MagicMock()
    dct2.on_refresh(mock)

    dct["a"] = 1
    assert dct == {"a": 1}
    znsclient.sio.sleep(0.2)
    mock.assert_called_with({"keys": ["a"]})
    del dct["a"]
    assert dct == {}
    znsclient.sio.sleep(0.2)
    mock.assert_called_with({"keys": ["a"]})


@pytest.mark.parametrize("client", ["znsclient", "znsclient_w_redis", "redisclient"])
def test_dict_refresh_delitem_self(client, request, znsclient):
    r = request.getfixturevalue(client)
    dct = znsocket.Dict(r=r, key="dct:test", socket=znsclient)
    mock = MagicMock()
    dct.on_refresh(mock)

    dct["a"] = 1
    assert dct == {"a": 1}
    znsclient.sio.sleep(0.2)
    mock.assert_not_called()
    del dct["a"]
    assert dct == {}
    znsclient.sio.sleep(0.2)
    mock.assert_not_called()


@pytest.mark.parametrize(
    "client", ["znsclient", "znsclient_w_redis", "redisclient", "empty"]
)
def test_dct_clear(client, request):
    c = request.getfixturevalue(client)
    if c is not None:
        dct = znsocket.Dict(r=c, key="list:test")
        assert isinstance(dct, ZnSocketObject)
    else:
        dct = {}

    assert len(dct) == 0
    dct.clear()
    assert len(dct) == 0
    dct.update({"a": "1", "b": "2"})
    assert len(dct) == 2
    assert dct == {"a": "1", "b": "2"}


@pytest.mark.parametrize("client", ["znsclient", "znsclient_w_redis", "redisclient"])
def test_dct_copy(client, request):
    c = request.getfixturevalue(client)
    dct = znsocket.Dict(r=c, key="list:test")
    assert isinstance(dct, ZnSocketObject)

    dct.update({"a": "1", "b": "2"})
    dct_copy = dct.copy(key="list:test:copy")

    assert dct == dct_copy
    assert dct is not dct_copy

    dct_copy["c"] = "3"
    assert dct != dct_copy
    assert dct == {"a": "1", "b": "2"}
    assert dct_copy == {"a": "1", "b": "2", "c": "3"}

    with pytest.raises(ValueError):
        dct.copy(key="list:test:copy")


@pytest.mark.parametrize("client", ["znsclient", "znsclient_w_redis", "redisclient"])
def test_dct_pop(client, request):
    c = request.getfixturevalue(client)
    dct = znsocket.Dict(r=c, key="list:test")
    assert isinstance(dct, ZnSocketObject)

    dct.update({"a": "1", "b": "2"})

    assert dct.pop("a") == "1"
    assert dct == {"b": "2"}

    with pytest.raises(KeyError):
        dct.pop("a")

    assert dct.pop("a", "default") == "default"


@pytest.mark.parametrize(
    "client", ["znsclient", "znsclient_w_redis", "redisclient", "empty"]
)
def test_dct_merge(client, request):
    c = request.getfixturevalue(client)
    if c is not None:
        dct = znsocket.Dict(r=c, key="list:test")
    else:
        dct = {}

    dct.update({"a": "1", "b": "2"})

    new_dct = dct | {"b": "3", "c": "4"}

    assert new_dct == {"a": "1", "b": "3", "c": "4"}
    assert isinstance(new_dct, dict)

    assert dct == {"a": "1", "b": "2"}

    if c is not None:
        dct2 = znsocket.Dict(r=c, key="list:test2")

        dct2.update({"b": "3", "c": "4"})
        new_dct = dct | dct2
        assert new_dct == {"a": "1", "b": "3", "c": "4"}
        assert isinstance(new_dct, dict)


@pytest.mark.parametrize("client", ["znsclient", "znsclient_w_redis", "redisclient"])
def test_dict_refresh_update(client, request, znsclient):
    r = request.getfixturevalue(client)
    dct = znsocket.Dict(r=r, key="dct:test", socket=znsclient)
    dct2 = znsocket.Dict(
        r=r, key="dct:test", socket=znsocket.Client.from_url(znsclient.address)
    )
    mock = MagicMock()
    dct2.on_refresh(mock)

    dct.update({"a": 1, "b": [1, 2, 3]})
    assert dct == {"a": 1, "b": [1, 2, 3]}
    znsclient.sio.sleep(0.2)
    mock.assert_called_with({"keys": ["a", "b"]})


@pytest.mark.parametrize("client", ["znsclient", "znsclient_w_redis", "redisclient"])
def test_invalid_json(client, request):
    c = request.getfixturevalue(client)
    dct = znsocket.Dict(r=c, key="list:test")

    with pytest.raises(ValueError):
        dct.update({"a": float("inf")})
    with pytest.raises(ValueError):
        dct.update({"a": float("nan")})
    with pytest.raises(ValueError):
        dct.update({"a": float("-inf")})

    dct.convert_nan = True

    dct.update({"inf": float("inf"), "nan": float("nan"), "-inf": float("-inf")})

    assert dct["inf"] is None
    assert dct["nan"] is None
    assert dct["-inf"] is None


@pytest.mark.parametrize(
    "client",
    ["znsclient", "znsclient_w_redis"],
)
def test_dict_dict_fallback_get(client, request):
    """Test what happens when nested objects in the adapter are updated"""
    c = request.getfixturevalue(client)
    key = "dict:test"
    fallback_key = "dict:fallback"
    fallback_dct = znsocket.Dict(r=c, key=fallback_key)
    fallback_dct.update({"a": 1, "b": 2, "c": 3})
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
