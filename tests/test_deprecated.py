import pytest


@pytest.mark.parametrize("client", ["znsclient", "redisclient"])
def test_hmset(client, request):
    c = request.getfixturevalue(client)

    c.hmset("name", {"key": "value"})
    assert c.hgetall("name") == {"key": "value"}
