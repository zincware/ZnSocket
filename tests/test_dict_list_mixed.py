import pytest
import znsocket


@pytest.mark.parametrize("client", ["znsclient", "redisclient", "empty"])
def test_dct_in_list(client, request):
    c = request.getfixturevalue(client)
    if c is not None:
        dct = znsocket.Dict(r=c, key="dict:test", repr_type="full")
        lst = znsocket.List(r=c, key="list:test", repr_type="full")
        # TODO: if they share the same key, something went wrong!
    else:
        dct = {}
        lst = []

    dct["a"] = "1"
    dct["b"] = "2"
    lst.append(dct)

    assert lst == [{"a": "1", "b": "2"}]
    assert dct == {"a": "1", "b": "2"}


@pytest.mark.parametrize("client", ["znsclient", "redisclient", "empty"])
def test_lst_in_dct(client, request):
    c = request.getfixturevalue(client)
    if c is not None:
        dct = znsocket.Dict(r=c, key="dict:test", repr_type="full")
        lst = znsocket.List(r=c, key="list:test", repr_type="full")
    else:
        dct = {}
        lst = []

    lst.append("1")
    lst.append("2")
    dct["a"] = lst

    assert lst == ["1", "2"]
    assert dct["a"] == ["1", "2"]
    assert dct == {"a": ["1", "2"]}


@pytest.mark.parametrize("client", ["znsclient", "redisclient", "empty"])
def test_lst_in_lst(client, request):
    c = request.getfixturevalue(client)
    if c is not None:
        lst1 = znsocket.List(r=c, key="list1:test", repr_type="full")
        lst2 = znsocket.List(r=c, key="list2:test", repr_type="full")
    else:
        lst1 = []
        lst2 = []

    lst1.append("1")
    lst1.append("2")
    lst2.append(lst1)

    assert lst1 == ["1", "2"]
    assert lst2 == [["1", "2"]]
    assert lst2[0] == ["1", "2"]
    assert lst2[0][0] == "1"
    assert lst2[0][1] == "2"


@pytest.mark.parametrize("client", ["znsclient", "redisclient", "empty"])
def test_dct_in_dct(client, request):
    c = request.getfixturevalue(client)
    if c is not None:
        dct1 = znsocket.Dict(r=c, key="dict1:test", repr_type="full")
        dct2 = znsocket.Dict(r=c, key="dict2:test", repr_type="full")
    else:
        dct1 = {}
        dct2 = {}

    dct1["a"] = "1"
    dct1["b"] = "2"
    dct2["c"] = dct1

    assert dct1 == {"a": "1", "b": "2"}
    assert dct2 == {"c": {"a": "1", "b": "2"}}
    assert dct2["c"] == {"a": "1", "b": "2"}
    assert dct2["c"]["a"] == "1"
    assert dct2["c"]["b"] == "2"