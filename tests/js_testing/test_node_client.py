import pytest

JEST_PATH = "npm test -- "


def test_does_not_exist_on_js(run_npm_test, request):
    with pytest.raises(AssertionError):
        run_npm_test(request.node.name)


def test_client_lLen(znsclient, run_npm_test, request):
    c = znsclient
    c.rpush("list:test", "element1")
    c.rpush("list:test", "element2")
    assert c.llen("list:test") == 2

    run_npm_test(request.node.name, client_url=c.address)


def test_client_lIndex(znsclient, run_npm_test, request):
    c = znsclient
    c.rpush("list:test", "element1")
    c.rpush("list:test", "element2")

    run_npm_test(request.node.name, client_url=c.address)


def test_client_lSet(znsclient, run_npm_test, request):
    c = znsclient
    c.rpush("list:test", "element1")

    run_npm_test(request.node.name, client_url=c.address)

    assert c.lindex("list:test", 0) == "element0"


def test_client_lRem(znsclient, run_npm_test, request):
    c = znsclient
    c.rpush("list:test", "element1")
    c.rpush("list:test", "element2")
    c.rpush("list:test", "element1")

    run_npm_test(request.node.name, client_url=c.address)

    assert c.llen("list:test") == 2


def test_client_rPush(znsclient, run_npm_test, request):
    c = znsclient
    run_npm_test(request.node.name, client_url=c.address)

    assert c.llen("list:test") == 2
    assert c.lindex("list:test", 0) == "element0"
    assert c.lindex("list:test", 1) == "element1"


def test_client_lPush(znsclient, run_npm_test, request):
    c = znsclient
    run_npm_test(request.node.name, client_url=c.address)

    assert c.llen("list:test") == 2
    assert c.lindex("list:test", 0) == "element1"
    assert c.lindex("list:test", 1) == "element0"


# def test_client_lInsert(znsclient, run_npm_test, request):
#     c = znsclient
#     c.rpush("list:test", "element1")
#     c.rpush("list:test", "element2")

#     run_npm_test(request.node.name, client_url=c.address)

#     assert c.llen("list:test") == 3
#     assert c.lindex("list:test", 0) == "element1"
#     assert c.lindex("list:test", 1) == "element0"
#     assert c.lindex("list:test", 2) == "element2"
