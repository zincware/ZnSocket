import numpy as np
import numpy.testing as npt
import pytest

import znsocket


@pytest.fixture
def empty() -> None:
    """Test against Python list implementation"""
    return None


@pytest.mark.parametrize("client", ["znsclient", "redisclient", "empty"])
def test_dct_set_get_item(client, request):
    c = request.getfixturevalue(client)
    if c is not None:
        dct = znsocket.Dict(r=c, key="list:test")
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


@pytest.mark.parametrize("client", ["znsclient", "redisclient", "empty"])
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


@pytest.mark.parametrize("client", ["znsclient", "redisclient", "empty"])
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


@pytest.mark.parametrize("client", ["znsclient", "redisclient", "empty"])
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


@pytest.mark.parametrize("client", ["znsclient", "redisclient", "empty"])
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


@pytest.mark.parametrize("client", ["znsclient", "redisclient"])
def test_dct_repr(client, request):
    c = request.getfixturevalue(client)
    if c is not None:
        dct = znsocket.Dict(r=c, key="list:test")
    else:
        dct = {}

    dct.update({"a": "1", "b": "2"})

    assert repr(dct) == "Dict({'a': '1', 'b': '2'})"
