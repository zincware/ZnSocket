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

    dct["a"] = 1
    assert dct["a"] == 1
    dct["numpy"] = np.arange(10)
    npt.assert_array_equal(dct["numpy"], np.arange(10))


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


@pytest.mark.parametrize("a", ["znsclient", "redisclient", "empty"])
@pytest.mark.parametrize("b", ["znsclient", "redisclient", "empty"])
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


@pytest.mark.parametrize("client", ["znsclient", "redisclient", "empty"])
def test_dct_similar_keys(client, request):
    c = request.getfixturevalue(client)
    if c is not None:
        dct = znsocket.Dict(r=c, key="list:test")
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


# @pytest.mark.parametrize("a", ["znsclient", "redisclient", "empty"])
# @pytest.mark.parametrize("b", ["znsclient", "redisclient", "empty"])
# def test_dict_nested(a, b, request):
#     a = request.getfixturevalue(a)
#     b = request.getfixturevalue(b)
#     if a is not None:
#         dct1 = znsocket.Dict(r=a, key="dict:test:a")
#     else:
#         dct1 = {}

#     if b is not None:
#         dct2 = znsocket.Dict(r=b, key="dict:test:b")
#     else:
#         dct2 = {}

#     dct1.update({"a": "1", "b": "2"})
#     dct2.update({"dct1": dct1, "b": "2"})
