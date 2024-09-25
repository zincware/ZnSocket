import os

import pytest

import znsocket

JEST_PATH = "node --experimental-vm-modules /Users/fzills/tools/znsocket/node_modules/jest/bin/jest.js"


@pytest.fixture
def run_npm_test():
    def _run_npm_test(name: str, client_url: str = ""):
        import subprocess

        cmd = f"{JEST_PATH} . -t {name}"
        env = os.environ.copy()
        env["ZNSOCKET_URL"] = client_url.replace("http://", "ws://")
        run = subprocess.run(cmd, check=False, shell=True, env=env, capture_output=True)
        # assert that the test was actually run f'✓ {name}' in run.stdout
        assert f"{name} (" in run.stderr.decode("utf-8")
        # if there is an error raise it
        if run.returncode != 0:
            print(run.stdout.decode("utf-8"))
            raise AssertionError(run.stderr.decode("utf-8"))

    return _run_npm_test


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


def test_list_append_redis(redisclient, run_npm_test, request):
    lst = znsocket.List(r=redisclient, key="list:test")
    lst.extend(list(range(5)))
    assert lst == [0, 1, 2, 3, 4]
    run_npm_test(request.node.name)
    assert lst == [0, 1, 2, 3, 4, 5]


def test_list_append_znsocket(znsclient, run_npm_test, request):
    lst = znsocket.List(r=znsclient, key="list:test")
    lst.extend(list(range(5)))
    assert lst == [0, 1, 2, 3, 4]
    run_npm_test(request.node.name, client_url=znsclient.address)
    assert lst == [0, 1, 2, 3, 4, 5]
