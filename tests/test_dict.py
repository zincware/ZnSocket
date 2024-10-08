from unittest.mock import MagicMock

import numpy as np
import numpy.testing as npt
import pytest

import znsocket
from znsocket.utils import ZnSocketObject


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


@pytest.mark.parametrize(
    "client", ["znsclient", "znsclient_w_redis", "redisclient", "empty"]
)
def test_dct_similar_keys(client, request):
    c = request.getfixturevalue(client)
    if c is not None:
        dct = znsocket.Dict(r=c, key="list:test", repr_type="full")
    else:
        dct = {}

    dct.update({1: 1, "1": "1", None: "None"})
    assert dct[1] == 1
    assert dct["1"] == "1"
    assert dct[None] == "None"

    assert dct == {1: 1, "1": "1", None: "None"}
    if c is not None:
        assert repr(dct) == "Dict({1: 1, '1': '1', None: 'None'})"

    del dct[1]
    assert dct == {"1": "1", None: "None"}
    del dct["1"]
    assert dct == {None: "None"}
    del dct[None]
    assert dct == {}


@pytest.mark.parametrize(
    "client", ["znsclient", "znsclient_w_redis", "redisclient", "empty"]
)
def test_dct_None_key_values(client, request):
    c = request.getfixturevalue(client)
    if c is not None:
        dct = znsocket.Dict(r=c, key="list:test", repr_type="full")
    else:
        dct = {}

    dct[None] = "None"
    dct["None"] = None
    assert dct[None] == "None"
    assert dct["None"] is None
    assert dct == {None: "None", "None": None}
    if c is not None:
        assert repr(dct) == "Dict({None: 'None', 'None': None})"

    assert list(dct) == [None, "None"]
    assert list(dct.keys()) == [None, "None"]
    assert list(dct.values()) == ["None", None]
    assert list(dct.items()) == [(None, "None"), ("None", None)]


@pytest.mark.parametrize(
    "client", ["znsclient", "znsclient_w_redis", "redisclient", "empty"]
)
def test_dct_numpy(client, request):
    c = request.getfixturevalue(client)
    if c is not None:
        dct = znsocket.Dict(r=c, key="list:test")
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
