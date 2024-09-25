import znsocket
import pytest
import os

JEST_PATH = "node --experimental-vm-modules /Users/fzills/tools/znsocket/node_modules/jest/bin/jest.js"

@pytest.fixture
def run_npm_test():
    def _run_npm_test(name: str, client_url: str = ""):
        import subprocess
        cmd = f"{JEST_PATH} . -t {name}"
        env = os.environ.copy()
        env["ZNSOCKET_URL"] = client_url.replace("http://", "ws://")
        subprocess.run(cmd, check=True, shell=True, env=env)
    return _run_npm_test

def test_client_lLen(znsclient, run_npm_test, request):
    c = znsclient
    c.rpush("list:test", "element1")
    c.rpush("list:test", "element2")
    assert c.llen("list:test") == 2

    run_npm_test(request.node.name, client_url=c.address) # does not work for redis, but we don't need to test pure redis for the client?


def test_list_append(redisclient, run_npm_test, request):
    lst = znsocket.List(r=redisclient, key="list:test")
    lst.extend(list(range(5)))
    assert lst == [0, 1, 2, 3, 4]
    run_npm_test(request.node.name)
    assert lst == [0, 1, 2, 3, 4, 5]
