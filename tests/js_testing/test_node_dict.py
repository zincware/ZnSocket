from unittest.mock import MagicMock, call

import znsocket


def test_dict_keys_single_znsocket(znsclient, run_npm_test, request):
    dct = znsocket.Dict(r=znsclient, key="dict:test")
    dct["a"] = "b"
    run_npm_test(request.node.name, client_url=znsclient.address)


def test_dict_keys_multiple_znsocket(znsclient, run_npm_test, request):
    dct = znsocket.Dict(r=znsclient, key="dict:test")
    dct["a"] = "b"
    dct["c"] = "d"
    run_npm_test(request.node.name, client_url=znsclient.address)


def test_dict_values_single_znsocket(znsclient, run_npm_test, request):
    dct = znsocket.Dict(r=znsclient, key="dict:test")
    dct["a"] = "string"
    run_npm_test(request.node.name, client_url=znsclient.address)


def test_dict_values_multiple_znsocket(znsclient, run_npm_test, request):
    dct = znsocket.Dict(r=znsclient, key="dict:test")
    dct["a"] = {"lorem": "ipsum"}
    dct["c"] = 25
    run_npm_test(request.node.name, client_url=znsclient.address)


def test_dict_entries_znsocket(znsclient, run_npm_test, request):
    dct = znsocket.Dict(r=znsclient, key="dict:test")
    dct["a"] = {"lorem": "ipsum"}
    dct["b"] = 25
    assert list(dct.items()) == [("a", {"lorem": "ipsum"}), ("b", 25)]
    run_npm_test(request.node.name, client_url=znsclient.address)


def test_dict_get_znsocket(znsclient, run_npm_test, request):
    dct = znsocket.Dict(r=znsclient, key="dict:test")
    dct["a"] = {"lorem": "ipsum"}
    dct["b"] = 25
    run_npm_test(request.node.name, client_url=znsclient.address)


def test_dict_set_znsocket(znsclient, run_npm_test, request):
    dct = znsocket.Dict(r=znsclient, key="dict:test")
    mock = MagicMock()
    dct.on_refresh(mock)
    run_npm_test(request.node.name, client_url=znsclient.address)

    assert mock.call_count == 2
    assert mock.call_args_list == [call({"keys": ["b"]}), call({"keys": ["a"]})]

    assert dct["a"] == {"lorem": "ipsum"}
    assert dct["b"] == "25"


def test_dict_with_list_and_dict(znsclient, run_npm_test, request):
    dct = znsocket.Dict(r=znsclient, key="dict:test")
    referenced_list = znsocket.List(r=znsclient, key="list:referenced")
    referenced_list.append("Hello World")
    
    referenced_dict = znsocket.Dict(r=znsclient, key="dict:referenced")
    referenced_dict["key"] = "value"

    dct.update({"A": referenced_list, "B": referenced_dict})

    assert dct["A"][0] == "Hello World"
    assert dct["B"]["key"] == "value"

    run_npm_test(request.node.name, client_url=znsclient.address)

    assert referenced_list[1] == "New Value"
    assert referenced_dict["new_key"] == "new_value"
