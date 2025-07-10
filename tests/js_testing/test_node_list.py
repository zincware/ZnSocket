import znsocket


def test_list_push_redis(redisclient, run_npm_test, request):
    lst = znsocket.List(r=redisclient, key="list:test")
    lst.extend(list(range(5)))
    assert lst == [0, 1, 2, 3, 4]
    run_npm_test(request.node.name)
    assert lst == [0, 1, 2, 3, 4, 5]


def test_list_push_znsocket(znsclient, run_npm_test, request):
    lst = znsocket.List(r=znsclient, key="list:test")
    lst.extend(list(range(5)))
    assert lst == [0, 1, 2, 3, 4]
    run_npm_test(request.node.name, client_url=znsclient.address)
    assert lst == [0, 1, 2, 3, 4, 5]


def test_list_with_list_and_dict(znsclient, run_npm_test, request):
    lst = znsocket.List(r=znsclient, key="list:test")
    referenced_list = znsocket.List(r=znsclient, key="list:referenced")
    referenced_list.append("Hello World")

    referenced_dict = znsocket.Dict(r=znsclient, key="dict:referenced")
    referenced_dict["key"] = "value"

    lst.extend([referenced_list, referenced_dict])

    assert lst[0][0] == "Hello World"
    assert lst[1]["key"] == "value"

    run_npm_test(request.node.name, client_url=znsclient.address)

    assert referenced_list[1] == "New Value"
    assert referenced_dict["new_key"] == "new_value"


def test_list_adapter_znsocket(znsclient, run_npm_test, request):
    data = [1, 2, 3, 4]
    _ = znsocket.ListAdapter(socket=znsclient, key="list:test", object=data)
    lst = znsocket.List(r=znsclient, key="list:test")
    assert list(lst) == data
    run_npm_test(request.node.name, client_url=znsclient.address)


def test_list_adapter_slice_basic(znsclient, run_npm_test, request):
    data = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
    _ = znsocket.ListAdapter(socket=znsclient, key="list:test", object=data)
    run_npm_test(request.node.name, client_url=znsclient.address)


def test_list_adapter_slice_with_step(znsclient, run_npm_test, request):
    data = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
    _ = znsocket.ListAdapter(socket=znsclient, key="list:test", object=data)
    run_npm_test(request.node.name, client_url=znsclient.address)


def test_list_adapter_slice_negative_indices(znsclient, run_npm_test, request):
    data = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
    _ = znsocket.ListAdapter(socket=znsclient, key="list:test", object=data)
    run_npm_test(request.node.name, client_url=znsclient.address)
