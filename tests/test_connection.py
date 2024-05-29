import pytest
import redis.exceptions

import znsocket.exceptions


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
    assert c.hget("hash", "nonexistent") is None


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
    assert c.hmget("hash", ["field1", "nonexistent"]) == ["value1", None]


@pytest.mark.parametrize("client", ["znsclient", "redisclient"])
def test_hkeys(client, request):
    c = request.getfixturevalue(client)
    data = {"field1": "value1", "field2": "value2"}
    c.hmset("hash", data)
    assert set(c.hkeys("hash")) == {"field1", "field2"}
    assert c.hkeys("hash") == ["field1", "field2"]
    assert c.hkeys("nonexistent") == []


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

    with pytest.raises(
        (redis.exceptions.ResponseError, znsocket.exceptions.ResponseError),
        match="no such key",
    ):
        c.lset("list2", 0, "new_element1")

    with pytest.raises(
        (redis.exceptions.ResponseError, znsocket.exceptions.ResponseError),
        match="index out of range",
    ):
        c.lset("list", 10, "new_element2")


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

    # remove from non-existent key
    response = c.lrem("nonexistent", 0, "element1")
    assert response == 0
    # remove from non-existent key with count
    response = c.lrem("nonexistent", 1, "element1")
    assert response == 0


@pytest.mark.parametrize("client", ["znsclient", "redisclient"])
def test_sadd_smembers(client, request):
    c = request.getfixturevalue(client)
    c.sadd("set", "member1")
    c.sadd("set", "member2")
    assert c.smembers("set") == {"member1", "member2"}


@pytest.mark.parametrize("client", ["znsclient", "redisclient"])
def test_rpush_lindex(client, request):
    c = request.getfixturevalue(client)
    c.rpush("list", "element1")
    c.rpush("list", "element2")
    assert c.lindex("list", 0) == "element1"
    assert c.lindex("list", 1) == "element2"

    assert c.lindex("list", 2) is None
    assert c.lindex("nonexistent", 0) is None


@pytest.mark.parametrize("client", ["znsclient", "redisclient"])
def test_lrange(client, request):
    c = request.getfixturevalue(client)
    c.rpush("list", "element1")
    c.rpush("list", "element2")
    assert c.lrange("list", 0, -1) == ["element1", "element2"]
    assert c.lrange("list", 0, 0) == ["element1"]
    assert c.lrange("list", 1, 1) == ["element2"]
    assert c.lrange("list", 0, 1) == ["element1", "element2"]
    assert c.lrange("list", 1, 2) == ["element2"]
    assert c.lrange("list", 2, 3) == []
    assert c.lrange("list", 1, -2) == []
    assert c.lrange("list", -2, 1) == ["element1", "element2"]
    # assert c.lrange("list", 0, -2) == ["element1"] # Why is that?

    assert c.lrange("nonexistent", 0, 1) == []


@pytest.mark.parametrize("client", ["znsclient", "redisclient"])
def test_flushall(client, request):
    c = request.getfixturevalue(client)
    c.set("name", "Alice")
    c.flushall()
    assert c.get("name") is None


@pytest.mark.parametrize("client", ["znsclient", "redisclient"])
def test_delete(client, request):
    c = request.getfixturevalue(client)
    c.set("name", "Alice")
    response = c.delete("name")
    assert response == 1
    assert c.get("name") is None

    response = c.delete("nonexistent")  # No error should be raised
    assert response == 0


@pytest.mark.parametrize("client", ["znsclient", "redisclient"])
def test_srem(client, request):
    c = request.getfixturevalue(client)
    c.sadd("set", "member1")
    c.sadd("set", "member2")
    assert c.srem("set", "member1") == 1
    assert c.smembers("set") == {"member2"}
    assert c.smembers("nonexistent") == set()

    assert c.srem("nonexistent", "member1") == 0


@pytest.mark.parametrize("client", ["znsclient", "redisclient"])
def test_linsert(client, request):
    c = request.getfixturevalue(client)

    # Initial setup and basic linsert test
    c.rpush("list", "element1")
    c.rpush("list", "element3")
    c.linsert("list", "BEFORE", "element3", "element2")
    assert c.lrange("list", 0, -1) == ["element1", "element2", "element3"]

    # Cleanup the list for the next test
    c.delete("list")

    # Test linsert AFTER
    c.rpush("list", "element1")
    c.rpush("list", "element2")
    c.linsert("list", "AFTER", "element1", "element1.5")
    assert c.lrange("list", 0, -1) == ["element1", "element1.5", "element2"]

    # Cleanup the list for the next test
    c.delete("list")

    # Test linsert when pivot element does not exist
    c.rpush("list", "element1")
    result = c.linsert("list", "BEFORE", "element2", "element1.5")
    assert result == -1
    assert c.lrange("list", 0, -1) == ["element1"]

    # Cleanup the list for the next test
    c.delete("list")

    # Test linsert with multiple elements and different positions
    c.rpush("list", "element1")
    c.rpush("list", "element2")
    c.rpush("list", "element3")
    c.rpush("list", "element4")
    c.linsert("list", "BEFORE", "element4", "element3.5")
    assert c.lrange("list", 0, -1) == [
        "element1",
        "element2",
        "element3",
        "element3.5",
        "element4",
    ]

    c.linsert("list", "AFTER", "element1", "element1.5")
    assert c.lrange("list", 0, -1) == [
        "element1",
        "element1.5",
        "element2",
        "element3",
        "element3.5",
        "element4",
    ]

    # Cleanup the list for the next test
    c.delete("list")

    # Test linsert with an empty list
    result = c.linsert("list", "BEFORE", "element1", "element0")
    assert result == 0
    assert c.lrange("list", 0, -1) == []


@pytest.mark.parametrize("client", ["znsclient", "redisclient"])
def test_lpush_lindex(client, request):
    c = request.getfixturevalue(client)
    c.lpush("list", "element1")
    c.lpush("list", "element2")
    assert c.lindex("list", 0) == "element2"
    assert c.lindex("list", 1) == "element1"

    assert c.lindex("list", 2) is None
    assert c.lindex("nonexistent", 0) is None


@pytest.mark.parametrize("client", ["znsclient", "redisclient"])
def test_hexists(client, request):
    c = request.getfixturevalue(client)

    c.hset("hash", "field", "value")
    assert c.hexists("hash", "field") == 1
    assert c.hexists("hash", "nonexistent") == 0
    assert c.hexists("nonexistent", "field") == 0


@pytest.mark.parametrize("client", ["znsclient", "redisclient"])
def test_hdel(client, request):
    c = request.getfixturevalue(client)

    c.hset("hash", "field", "value")
    assert c.hdel("hash", "field") == 1

    assert c.hdel("hash", "nonexistent") == 0

    assert c.hdel("nonexistent", "field") == 0

    assert c.hdel("nonexistent", "nonexistent") == 0


@pytest.mark.parametrize("client", ["znsclient", "redisclient"])
def test_hlen(client, request):
    c = request.getfixturevalue(client)

    c.hset("hash", "field1", "value1")
    c.hset("hash", "field2", "value2")
    assert c.hlen("hash") == 2

    assert c.hlen("nonexistent") == 0


@pytest.mark.parametrize("client", ["znsclient", "redisclient"])
def test_hvals(client, request):
    c = request.getfixturevalue(client)

    c.hset("hash", "field1", "value1")
    c.hset("hash", "field2", "value2")
    assert set(c.hvals("hash")) == {"value1", "value2"}

    assert c.hvals("nonexistent") == []
