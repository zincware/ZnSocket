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


def test_dict_items_znsocket(znsclient, run_npm_test, request):
    dct = znsocket.Dict(r=znsclient, key="dict:test")
    dct["a"] = {"lorem": "ipsum"}
    dct["b"] = 25
    assert list(dct.items()) == [("a", {"lorem": "ipsum"}), ("b", 25)]
    run_npm_test(request.node.name, client_url=znsclient.address)

def test_dict_getitem_znsocket(znsclient, run_npm_test, request):
    dct = znsocket.Dict(r=znsclient, key="dict:test")
    dct["a"] = {"lorem": "ipsum"}
    dct["b"] = 25
    run_npm_test(request.node.name, client_url=znsclient.address)


def test_dict_setitem_znsocket(znsclient, run_npm_test, request):
    run_npm_test(request.node.name, client_url=znsclient.address)
    dct = znsocket.Dict(r=znsclient, key="dict:test")
    assert dct["a"] == {"lorem": "ipsum"}
    assert dct["b"] == 25
