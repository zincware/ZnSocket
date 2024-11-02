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
