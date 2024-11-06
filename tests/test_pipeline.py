import pytest


@pytest.mark.parametrize("client", ["znsclient", "znsclient_w_redis", "redisclient"])
def test_set(client, request):
    c = request.getfixturevalue(client)
    pipeline = c.pipeline()
    pipeline.set("foo", "bar")
    pipeline.set("lorem", "ipsum")

    assert pipeline.execute() == [True, True]
    assert c.get("foo") == "bar"
    assert c.get("lorem") == "ipsum"


@pytest.mark.parametrize("client", ["znsclient", "znsclient_w_redis", "redisclient"])
def test_get(client, request):
    c = request.getfixturevalue(client)
    c.set("foo", "bar")
    c.set("lorem", "ipsum")

    pipeline = c.pipeline()
    pipeline.get("foo")
    pipeline.get("lorem")

    assert pipeline.execute() == ["bar", "ipsum"]


@pytest.mark.parametrize("client", ["znsclient", "znsclient_w_redis", "redisclient"])
def test_delete(client, request):
    c = request.getfixturevalue(client)
    c.set("foo", "bar")
    c.set("lorem", "ipsum")

    pipeline = c.pipeline()
    pipeline.delete("foo")
    pipeline.delete("lorem")

    assert pipeline.execute() == [1, 1]
    assert c.get("foo") is None
    assert c.get("lorem") is None


@pytest.mark.parametrize("client", ["znsclient", "znsclient_w_redis", "redisclient"])
def test_hset(client, request):
    c = request.getfixturevalue(client)
    pipeline = c.pipeline()
    pipeline.hset("foo", "bar", "baz")
    pipeline.hset("lorem", "ipsum", "dolor")

    assert pipeline.execute() == [True, True]
    assert c.hget("foo", "bar") == "baz"
    assert c.hget("lorem", "ipsum") == "dolor"


@pytest.mark.parametrize("client", ["znsclient", "znsclient_w_redis", "redisclient"])
def test_hset_mapping(client, request):
    c = request.getfixturevalue(client)
    pipeline = c.pipeline()
    pipeline.hset("foo", mapping={"bar": "baz", "lorem": "ipsum"})
    pipeline.hset("lorem", mapping={"ipsum": "dolor"})

    assert pipeline.execute() == [2, 1]
    assert c.hget("foo", "bar") == "baz"
    assert c.hget("foo", "lorem") == "ipsum"
    assert c.hget("lorem", "ipsum") == "dolor"


@pytest.mark.parametrize("client", ["znsclient", "znsclient_w_redis", "redisclient"])
def test_hget(client, request):
    c = request.getfixturevalue(client)
    c.hset("foo", "bar", "baz")
    c.hset("lorem", "ipsum", "dolor")

    pipeline = c.pipeline()
    pipeline.hget("foo", "bar")
    pipeline.hget("lorem", "ipsum")

    assert pipeline.execute() == ["baz", "dolor"]


@pytest.mark.parametrize("client", ["znsclient", "znsclient_w_redis", "redisclient"])
def test_hkeys(client, request):
    c = request.getfixturevalue(client)
    c.hset("foo", "bar", "baz")
    c.hset("foo", "lorem", "ipsum")

    pipeline = c.pipeline()
    pipeline.hkeys("foo")

    assert pipeline.execute() == [["bar", "lorem"]]


@pytest.mark.parametrize("client", ["znsclient", "znsclient_w_redis", "redisclient"])
def test_exists(client, request):
    c = request.getfixturevalue(client)
    c.set("foo", "bar")

    pipeline = c.pipeline()

    assert pipeline.exists("foo").exists("lorem").execute() == [1, 0]


@pytest.mark.parametrize("client", ["znsclient", "znsclient_w_redis", "redisclient"])
def test_llen(client, request):
    c = request.getfixturevalue(client)
    c.lpush("foo", "bar")
    c.lpush("foo", "lorem")

    pipeline = c.pipeline()
    pipeline.llen("foo")

    assert pipeline.execute() == [2]


@pytest.mark.parametrize("client", ["znsclient", "znsclient_w_redis", "redisclient"])
def test_rpush(client, request):
    c = request.getfixturevalue(client)
    pipeline = c.pipeline()
    pipeline.rpush("foo", "bar")
    pipeline.rpush("foo", "lorem")

    assert pipeline.execute() == [1, 2]
    assert c.lindex("foo", 0) == "bar"
    assert c.lindex("foo", 1) == "lorem"


@pytest.mark.parametrize("client", ["znsclient", "znsclient_w_redis", "redisclient"])
def test_lpush(client, request):
    c = request.getfixturevalue(client)
    pipeline = c.pipeline()
    pipeline.lpush("foo", "bar")
    pipeline.lpush("foo", "lorem")

    assert pipeline.execute() == [1, 2]
    assert c.lindex("foo", 0) == "lorem"
    assert c.lindex("foo", 1) == "bar"


@pytest.mark.parametrize("client", ["znsclient", "znsclient_w_redis", "redisclient"])
def test_lindex(client, request):
    c = request.getfixturevalue(client)
    c.lpush("foo", "bar")
    c.lpush("foo", "lorem")

    pipeline = c.pipeline()
    pipeline.lindex("foo", 0)
    pipeline.lindex("foo", 1)

    assert pipeline.execute() == ["lorem", "bar"]


# fails
# @pytest.mark.parametrize("client", ["znsclient", "znsclient_w_redis", "redisclient"])
# def test_smembers(client, request):
#     c = request.getfixturevalue(client)
#     c.sadd("foo", "bar")
#     c.sadd("foo", "lorem")

#     pipeline = c.pipeline()
#     pipeline.smembers("foo")

#     assert pipeline.execute() == [{"bar", "lorem"}]


@pytest.mark.parametrize("client", ["znsclient", "znsclient_w_redis", "redisclient"])
def test_hgetall(client, request):
    c = request.getfixturevalue(client)
    c.hset("foo", "bar", "baz")
    c.hset("foo", "lorem", "ipsum")

    pipeline = c.pipeline()
    pipeline.hgetall("foo")

    assert pipeline.execute() == [{"bar": "baz", "lorem": "ipsum"}]
