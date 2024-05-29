import pytest


@pytest.mark.parametrize("client", ["znsclient", "redisclient"])
def test_set(client, request):
    c = request.getfixturevalue(client)
    c.set("name", "Alice")
    assert c.get("name") == "Alice"


@pytest.mark.parametrize("client", ["znsclient", "redisclient"])
def test_get(client, request):
    c = request.getfixturevalue(client)
    c.set("name", "Alice")
    assert c.get("name") == "Alice"


@pytest.mark.parametrize("client", ["znsclient", "redisclient"])
def test_hset_hget(client, request):
    c = request.getfixturevalue(client)
    c.hset("hash", "field", "value")
    assert c.hget("hash", "field") == "value"


@pytest.mark.parametrize("client", ["znsclient", "redisclient"])
def test_hmset_hgetall(client, request):
    c = request.getfixturevalue(client)
    data = {"field1": "value1", "field2": "value2"}
    c.hmset("hash", data)
    assert c.hgetall("hash") == data


@pytest.mark.parametrize("client", ["znsclient", "redisclient"])
def test_hmget(client, request):
    c = request.getfixturevalue(client)
    data = {"field1": "value1", "field2": "value2"}
    c.hmset("hash", data)
    assert c.hmget("hash", ["field1", "field2"]) == ["value1", "value2"]


@pytest.mark.parametrize("client", ["znsclient", "redisclient"])
def test_hkeys(client, request):
    c = request.getfixturevalue(client)
    data = {"field1": "value1", "field2": "value2"}
    c.hmset("hash", data)
    assert set(c.hkeys("hash")) == {"field1", "field2"}


@pytest.mark.parametrize("client", ["znsclient", "redisclient"])
def test_exists(client, request):
    c = request.getfixturevalue(client)
    c.set("name", "Alice")
    assert c.exists("name") == 1
    assert c.exists("nonexistent") == 0


@pytest.mark.parametrize("client", ["znsclient", "redisclient"])
def test_llen(client, request):
    c = request.getfixturevalue(client)
    c.rpush("list", "element1")
    c.rpush("list", "element2")
    assert c.llen("list") == 2


@pytest.mark.parametrize("client", ["znsclient", "redisclient"])
def test_lset(client, request):
    c = request.getfixturevalue(client)
    c.rpush("list", "element1")
    c.rpush("list", "element2")
    c.lset("list", 0, "new_element1")
    assert c.lindex("list", 0) == "new_element1"


@pytest.mark.parametrize("client", ["znsclient", "redisclient"])
def test_lrem(client, request):
    c = request.getfixturevalue(client)

    # Push elements to the list
    c.rpush("list", "element1")
    c.rpush("list", "element2")
    c.rpush("list", "element1")

    # Remove all occurrences of "element1"
    c.lrem("list", 0, "element1")
    assert c.lrange("list", 0, -1) == ["element2"]

    # Clear the list for the next part of the test
    c.delete("list")

    # Push elements to the list again
    c.rpush("list", "element1")
    c.rpush("list", "element2")
    c.rpush("list", "element1")
    c.rpush("list", "element1")

    # Remove one occurrence of "element1"
    c.lrem("list", 1, "element1")
    assert c.lrange("list", 0, -1) == ["element2", "element1", "element1"]

    # Clear the list for the next part of the test
    c.delete("list")

    # Push elements to the list again
    c.rpush("list", "element1")
    c.rpush("list", "element1")
    c.rpush("list", "element2")
    c.rpush("list", "element1")

    # Remove two occurrences of "element1"
    c.lrem("list", 2, "element1")
    assert c.lrange("list", 0, -1) == ["element2", "element1"]


@pytest.mark.parametrize("client", ["znsclient", "redisclient"])
def test_sadd_smembers(client, request):
    c = request.getfixturevalue(client)
    c.sadd("set", "member1")
    c.sadd("set", "member2")
    assert c.smembers("set") == {"member1", "member2"}


@pytest.mark.parametrize("client", ["znsclient", "redisclient"])
def test_lpush_lindex(client, request):
    c = request.getfixturevalue(client)
    c.rpush("list", "element1")
    c.rpush("list", "element2")
    assert c.lindex("list", 0) == "element1"
    assert c.lindex("list", 1) == "element2"


@pytest.mark.parametrize("client", ["znsclient", "redisclient"])
def test_lrange(client, request):
    c = request.getfixturevalue(client)
    c.rpush("list", "element1")
    c.rpush("list", "element2")
    assert c.lrange("list", 0, -1) == ["element1", "element2"]


@pytest.mark.parametrize("client", ["znsclient", "redisclient"])
def test_flushall(client, request):
    c = request.getfixturevalue(client)
    c.set("name", "Alice")
    c.flushall()
    assert c.get("name") is None


@pytest.mark.parametrize("client", ["znsclient", "redisclient"])
def test_rdlete(client, request):
    c = request.getfixturevalue(client)
    c.set("name", "Alice")
    c.delete("name")
    assert c.get("name") is None


@pytest.mark.parametrize("client", ["znsclient", "redisclient"])
def test_srem(client, request):
    c = request.getfixturevalue(client)
    c.sadd("set", "member1")
    c.sadd("set", "member2")
    c.srem("set", "member1")
    assert c.smembers("set") == {"member2"}


@pytest.mark.parametrize("client", ["znsclient", "redisclient"])
def test_linsert(client, request):
    c = request.getfixturevalue(client)
    c.rpush("list", "element1")
    c.rpush("list", "element3")
    c.linsert("list", "BEFORE", "element3", "element2")
    assert c.lrange("list", 0, -1) == ["element1", "element2", "element3"]
