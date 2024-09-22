import pytest


@pytest.mark.parametrize("client", ["znsclient", "znsclient_w_redis", "redisclient"])
def test_hmset(client, request):
    c = request.getfixturevalue(client)

    c.hmset("name", {"key": "value"})
    assert c.hgetall("name") == {"key": "value"}
