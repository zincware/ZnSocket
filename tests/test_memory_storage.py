import time

import pytest


@pytest.mark.parametrize("r", ["memory_storage", "redisclient"])
def test_storage_set(r, request):
    c = request.getfixturevalue(r)
    c.set("name", "Alice")
    assert c.get("name") == "Alice"


@pytest.mark.parametrize("r", ["memory_storage", "redisclient"])
def test_storage_set_json_string(r, request):
    """Test that set stores values as strings, not as dicts."""
    import json

    c = request.getfixturevalue(r)
    data = {"user": "alice", "age": 30, "active": True}
    json_string = json.dumps(data)

    # Store the JSON string
    c.set("user_data", json_string)

    # Get it back - should be a string, not a dict
    result = c.get("user_data")
    assert isinstance(result, str), f"Expected string, got {type(result)}"
    assert result == json_string

    # Should be able to parse it
    parsed = json.loads(result)
    assert parsed == data


@pytest.mark.parametrize("r", ["memory_storage", "redisclient"])
def test_storage_hset_json_string(r, request):
    """Test that hset stores values as strings, not as dicts."""
    import json

    c = request.getfixturevalue(r)
    data = {"user": "alice", "age": 30, "active": True}
    json_string = json.dumps(data)

    # Store the JSON string in a hash field
    c.hset("users", "user:1", json_string)

    # Get it back - should be a string, not a dict
    result = c.hget("users", "user:1")
    assert isinstance(result, str), f"Expected string, got {type(result)}"
    assert result == json_string

    # Should be able to parse it
    parsed = json.loads(result)
    assert parsed == data


@pytest.mark.parametrize("r", ["memory_storage", "redisclient"])
def test_storage_hset_mapping_json_string(r, request):
    """Test that hset with mapping stores values as strings."""
    import json

    c = request.getfixturevalue(r)
    data1 = {"name": "alice", "role": "admin"}
    data2 = {"name": "bob", "role": "user"}

    mapping = {
        "user:1": json.dumps(data1),
        "user:2": json.dumps(data2),
    }

    c.hset("users", mapping=mapping)

    # Get values back - should be strings
    result1 = c.hget("users", "user:1")
    result2 = c.hget("users", "user:2")

    assert isinstance(result1, str), f"Expected string, got {type(result1)}"
    assert isinstance(result2, str), f"Expected string, got {type(result2)}"

    # Should be able to parse them
    assert json.loads(result1) == data1
    assert json.loads(result2) == data2


@pytest.mark.parametrize("r", ["memory_storage", "redisclient"])
def test_storage_set_dict_object_should_convert_to_string(r, request):
    """Test that storing a dict object converts it to string representation."""

    c = request.getfixturevalue(r)
    data = {"user": "alice", "age": 30}

    # Try to store a dict directly (bad practice, but should not break)
    # Redis would convert this to str(data)
    c.set("data", str(data))

    result = c.get("data")
    # Should be a string representation
    assert isinstance(result, str)

    # Note: str(dict) is not valid JSON, so this will fail:
    # json.loads(result) would raise an error


@pytest.mark.parametrize("r", ["memory_storage", "redisclient"])
def test_storage_hset_dict_value_raises_error(r, request):
    """Test that storing dict values raises DataError (matching Redis)."""
    from redis.exceptions import DataError

    c = request.getfixturevalue(r)
    data = {"user": "alice", "age": 30}

    # Try to store a dict directly - should raise DataError
    with pytest.raises(DataError, match="Invalid input of type: 'dict'"):
        c.hset("users", "user:1", data)


@pytest.mark.parametrize("r", ["memory_storage", "redisclient"])
def test_storage_set_dict_value_raises_error(r, request):
    """Test that storing dict values raises DataError (matching Redis)."""
    from redis.exceptions import DataError

    c = request.getfixturevalue(r)
    data = {"user": "alice", "age": 30}

    # Try to store a dict directly - should raise DataError
    with pytest.raises(DataError, match="Invalid input of type: 'dict'"):
        c.set("data", data)


@pytest.mark.parametrize("r", ["memory_storage", "redisclient"])
def test_storage_set_list_value_raises_error(r, request):
    """Test that storing list values raises DataError (matching Redis)."""
    from redis.exceptions import DataError

    c = request.getfixturevalue(r)
    data = ["alice", "bob", "charlie"]

    # Try to store a list directly - should raise DataError
    with pytest.raises(DataError, match="Invalid input of type: 'list'"):
        c.set("data", data)


@pytest.mark.parametrize("r", ["memory_storage", "redisclient"])
def test_storage_set_bool_value_raises_error(r, request):
    """Test that storing bool values raises DataError (matching Redis)."""
    from redis.exceptions import DataError

    c = request.getfixturevalue(r)

    # Try to store a bool directly - should raise DataError
    with pytest.raises(DataError, match="Invalid input of type: 'bool'"):
        c.set("data", True)


@pytest.mark.parametrize("r", ["memory_storage", "redisclient"])
def test_storage_set_int_value_works(r, request):
    """Test that storing int values works (Redis converts to string)."""
    c = request.getfixturevalue(r)

    c.set("count", 42)
    result = c.get("count")

    # Should be stored as string
    assert isinstance(result, str)
    assert result == "42"


@pytest.mark.parametrize("r", ["memory_storage", "redisclient"])
def test_storage_set_float_value_works(r, request):
    """Test that storing float values works (Redis converts to string)."""
    c = request.getfixturevalue(r)

    c.set("pi", 3.14159)
    result = c.get("pi")

    # Should be stored as string
    assert isinstance(result, str)
    assert result == "3.14159"


@pytest.mark.parametrize("r", ["memory_storage", "redisclient"])
def test_storage_hgetall_type_consistency(r, request):
    """Test hgetall returns consistent types (all values should be strings)."""
    c = request.getfixturevalue(r)
    # Set some hash fields with different types
    c.hset("user:1", "name", "Alice")
    c.hset("user:1", "age", "30")
    c.hset("user:1", "active", "true")

    result = c.hgetall("user:1")

    # All values should be strings
    assert isinstance(result, dict)
    assert isinstance(result["name"], str)
    assert isinstance(result["age"], str)
    assert isinstance(result["active"], str)
    assert result["name"] == "Alice"
    assert result["age"] == "30"
    assert result["active"] == "true"


@pytest.mark.parametrize("r", ["memory_storage", "redisclient"])
def test_storage_hgetall_returns_copy(r, request):
    """Test that hgetall returns a copy, not a reference to internal data."""
    c = request.getfixturevalue(r)
    c.hset("user:1", "name", "Alice")
    c.hset("user:1", "age", "30")

    result1 = c.hgetall("user:1")
    result2 = c.hgetall("user:1")

    # Modifying result1 should not affect result2
    result1["name"] = "Bob"
    assert result2["name"] == "Alice"

    # Modifying result should not affect stored data
    result1["new_field"] = "value"
    result3 = c.hgetall("user:1")
    assert "new_field" not in result3


@pytest.mark.parametrize("r", ["memory_storage", "redisclient"])
def test_storage_hgetall_empty(r, request):
    """Test hgetall on non-existent key returns empty dict."""
    c = request.getfixturevalue(r)
    result = c.hgetall("nonexistent")
    assert result == {}
    assert isinstance(result, dict)


@pytest.mark.parametrize("r", ["memory_storage", "redisclient"])
def test_storage_hgetall_with_mapping(r, request):
    """Test hgetall after setting with mapping parameter."""
    c = request.getfixturevalue(r)
    mapping = {"field1": "value1", "field2": "value2", "field3": "value3"}
    c.hset("hash1", mapping=mapping)

    result = c.hgetall("hash1")
    assert result == mapping
    # Verify all values are strings
    for key, value in result.items():
        assert isinstance(key, str)
        assert isinstance(value, str)


@pytest.mark.parametrize("r", ["memory_storage", "redisclient"])
def test_storage_set_nx(r, request):
    """Test set with nx (only set if not exists)."""
    c = request.getfixturevalue(r)
    # Should set successfully when key doesn't exist
    assert c.set("key1", "value1", nx=True) is True
    assert c.get("key1") == "value1"

    # Should not set when key already exists
    assert c.set("key1", "value2", nx=True) is None
    assert c.get("key1") == "value1"  # Value unchanged


@pytest.mark.parametrize("r", ["memory_storage", "redisclient"])
def test_storage_set_xx(r, request):
    """Test set with xx (only set if exists)."""
    c = request.getfixturevalue(r)
    # Should not set when key doesn't exist
    assert c.set("key1", "value1", xx=True) is None
    assert c.get("key1") is None

    # Create the key first
    c.set("key1", "value1")

    # Should set successfully when key exists
    assert c.set("key1", "value2", xx=True) is True
    assert c.get("key1") == "value2"


@pytest.mark.parametrize("r", ["memory_storage", "redisclient"])
def test_storage_set_ex(r, request):
    """Test set with ex (expiration in seconds)."""
    c = request.getfixturevalue(r)
    # Set with 1 second expiration
    c.set("temp_key", "temp_value", ex=1)

    # Key should exist immediately
    assert c.get("temp_key") == "temp_value"

    # Key should expire after 1 second
    time.sleep(1.1)
    assert c.get("temp_key") is None


@pytest.mark.parametrize("r", ["memory_storage", "redisclient"])
def test_storage_set_nx_ex(r, request):
    """Test set with both nx and ex parameters."""
    c = request.getfixturevalue(r)
    # Should set with expiration when key doesn't exist
    assert c.set("key1", "value1", nx=True, ex=1) is True
    assert c.get("key1") == "value1"

    # Should not update when key exists
    assert c.set("key1", "value2", nx=True, ex=1) is None
    assert c.get("key1") == "value1"

    # Wait for expiration
    time.sleep(1.1)
    assert c.get("key1") is None


@pytest.mark.parametrize("r", ["memory_storage", "redisclient"])
def test_storage_incr_basic(r, request):
    """Test incr increments value by 1."""
    c = request.getfixturevalue(r)
    # Increment non-existent key should set to 1
    assert c.incr("counter") == 1
    assert c.get("counter") == "1"

    # Increment existing key
    assert c.incr("counter") == 2
    assert c.get("counter") == "2"

    assert c.incr("counter") == 3
    assert c.get("counter") == "3"


@pytest.mark.parametrize("r", ["memory_storage", "redisclient"])
def test_storage_incr_by_amount(r, request):
    """Test incr with custom amount."""
    c = request.getfixturevalue(r)
    # Increment by 5
    assert c.incr("counter", 5) == 5
    assert c.get("counter") == "5"

    # Increment by 10
    assert c.incr("counter", 10) == 15
    assert c.get("counter") == "15"


@pytest.mark.parametrize("r", ["memory_storage", "redisclient"])
def test_storage_incr_negative(r, request):
    """Test incr with negative amount (decrement)."""
    c = request.getfixturevalue(r)
    c.set("counter", "10")

    # Decrement using negative incr
    assert c.incr("counter", -3) == 7
    assert c.get("counter") == "7"


@pytest.mark.parametrize("r", ["memory_storage", "redisclient"])
def test_storage_decr_basic(r, request):
    """Test decr decrements value by 1."""
    c = request.getfixturevalue(r)
    c.set("counter", "10")

    assert c.decr("counter") == 9
    assert c.get("counter") == "9"

    assert c.decr("counter") == 8
    assert c.get("counter") == "8"


@pytest.mark.parametrize("r", ["memory_storage", "redisclient"])
def test_storage_decr_by_amount(r, request):
    """Test decr with custom amount."""
    c = request.getfixturevalue(r)
    c.set("counter", "100")

    assert c.decr("counter", 30) == 70
    assert c.get("counter") == "70"

    assert c.decr("counter", 20) == 50
    assert c.get("counter") == "50"


@pytest.mark.parametrize("r", ["memory_storage", "redisclient"])
def test_storage_incr_non_integer(r, request):
    """Test incr on non-integer value raises error."""
    c = request.getfixturevalue(r)
    c.set("key", "not_a_number")

    with pytest.raises(Exception):  # ResponseError
        c.incr("key")


# ============================================================================
# SORTED SET TESTS
# ============================================================================


@pytest.mark.parametrize("r", ["memory_storage", "redisclient"])
def test_storage_zadd_basic(r, request):
    """Test basic zadd operation."""
    c = request.getfixturevalue(r)
    # Add members with scores
    assert c.zadd("leaderboard", {"player1": 100, "player2": 200}) == 2
    assert c.zadd("leaderboard", {"player3": 150}) == 1
    assert c.zcard("leaderboard") == 3


@pytest.mark.parametrize("r", ["memory_storage", "redisclient"])
def test_storage_zadd_update(r, request):
    """Test zadd updates existing member scores."""
    c = request.getfixturevalue(r)
    c.zadd("scores", {"alice": 100})
    # Update existing member should return 0 (no new members added)
    assert c.zadd("scores", {"alice": 200}) == 0
    assert c.zcard("scores") == 1


@pytest.mark.parametrize("r", ["memory_storage", "redisclient"])
def test_storage_zcard(r, request):
    """Test zcard returns cardinality."""
    c = request.getfixturevalue(r)
    assert c.zcard("nonexistent") == 0
    c.zadd("myset", {"a": 1, "b": 2, "c": 3})
    assert c.zcard("myset") == 3


@pytest.mark.parametrize("r", ["memory_storage", "redisclient"])
def test_storage_zrange_basic(r, request):
    """Test zrange returns members in score order."""
    c = request.getfixturevalue(r)
    c.zadd("scores", {"alice": 100, "bob": 200, "charlie": 150})

    # Get all members
    result = c.zrange("scores", 0, -1)
    assert result == ["alice", "charlie", "bob"]

    # Get with scores
    result = c.zrange("scores", 0, -1, withscores=True)
    assert result == [("alice", 100.0), ("charlie", 150.0), ("bob", 200.0)]


@pytest.mark.parametrize("r", ["memory_storage", "redisclient"])
def test_storage_zrange_slice(r, request):
    """Test zrange with different slices."""
    c = request.getfixturevalue(r)
    c.zadd("nums", {"a": 1, "b": 2, "c": 3, "d": 4, "e": 5})

    # Get first 3
    assert c.zrange("nums", 0, 2) == ["a", "b", "c"]

    # Get last 2 with negative indices
    assert c.zrange("nums", -2, -1) == ["d", "e"]

    # Get middle elements
    assert c.zrange("nums", 1, 3) == ["b", "c", "d"]


@pytest.mark.parametrize("r", ["memory_storage", "redisclient"])
def test_storage_zrange_empty(r, request):
    """Test zrange on nonexistent key."""
    c = request.getfixturevalue(r)
    assert c.zrange("nonexistent", 0, -1) == []


@pytest.mark.parametrize("r", ["memory_storage", "redisclient"])
def test_storage_zrangebyscore_basic(r, request):
    """Test zrangebyscore with inclusive bounds."""
    c = request.getfixturevalue(r)
    c.zadd("scores", {"a": 1, "b": 2, "c": 3, "d": 4, "e": 5})

    # Get scores 2-4 (inclusive)
    result = c.zrangebyscore("scores", 2, 4)
    assert result == ["b", "c", "d"]

    # With scores
    result = c.zrangebyscore("scores", 2, 4, withscores=True)
    assert result == [("b", 2.0), ("c", 3.0), ("d", 4.0)]


@pytest.mark.parametrize("r", ["memory_storage", "redisclient"])
def test_storage_zrangebyscore_exclusive(r, request):
    """Test zrangebyscore with exclusive bounds."""
    c = request.getfixturevalue(r)
    c.zadd("scores", {"a": 1, "b": 2, "c": 3, "d": 4, "e": 5})

    # Exclusive min
    result = c.zrangebyscore("scores", "(2", 4)
    assert result == ["c", "d"]

    # Exclusive max
    result = c.zrangebyscore("scores", 2, "(4")
    assert result == ["b", "c"]

    # Both exclusive
    result = c.zrangebyscore("scores", "(2", "(4")
    assert result == ["c"]


@pytest.mark.parametrize("r", ["memory_storage", "redisclient"])
def test_storage_zrangebyscore_infinity(r, request):
    """Test zrangebyscore with -inf and +inf."""
    c = request.getfixturevalue(r)
    c.zadd("scores", {"a": 1, "b": 2, "c": 3})

    # All members
    result = c.zrangebyscore("scores", "-inf", "+inf")
    assert result == ["a", "b", "c"]

    # From start to 2
    result = c.zrangebyscore("scores", "-inf", 2)
    assert result == ["a", "b"]


@pytest.mark.parametrize("r", ["memory_storage", "redisclient"])
def test_storage_zrangebyscore_pagination(r, request):
    """Test zrangebyscore with start and num parameters."""
    c = request.getfixturevalue(r)
    c.zadd("scores", {"a": 1, "b": 2, "c": 3, "d": 4, "e": 5})

    # Skip first 2, get 2 results
    result = c.zrangebyscore("scores", 1, 5, start=2, num=2)
    assert result == ["c", "d"]

    # Skip first, get 3 results
    result = c.zrangebyscore("scores", 1, 5, start=1, num=3)
    assert result == ["b", "c", "d"]


@pytest.mark.parametrize("r", ["memory_storage", "redisclient"])
def test_storage_zrevrangebyscore_basic(r, request):
    """Test zrevrangebyscore returns results in reverse order."""
    c = request.getfixturevalue(r)
    c.zadd("scores", {"a": 1, "b": 2, "c": 3, "d": 4, "e": 5})

    # Get scores 4-2 in reverse (note: max comes first)
    result = c.zrevrangebyscore("scores", 4, 2)
    assert result == ["d", "c", "b"]

    # With scores
    result = c.zrevrangebyscore("scores", 4, 2, withscores=True)
    assert result == [("d", 4.0), ("c", 3.0), ("b", 2.0)]


@pytest.mark.parametrize("r", ["memory_storage", "redisclient"])
def test_storage_zrevrangebyscore_exclusive(r, request):
    """Test zrevrangebyscore with exclusive bounds."""
    c = request.getfixturevalue(r)
    c.zadd("scores", {"a": 1, "b": 2, "c": 3, "d": 4, "e": 5})

    # Exclusive max
    result = c.zrevrangebyscore("scores", "(4", 2)
    assert result == ["c", "b"]

    # Exclusive min
    result = c.zrevrangebyscore("scores", 4, "(2")
    assert result == ["d", "c"]


@pytest.mark.parametrize("r", ["memory_storage", "redisclient"])
def test_storage_zrevrangebyscore_pagination(r, request):
    """Test zrevrangebyscore with pagination."""
    c = request.getfixturevalue(r)
    c.zadd("scores", {"a": 1, "b": 2, "c": 3, "d": 4, "e": 5})

    # Skip first 2, get 2 results
    result = c.zrevrangebyscore("scores", 5, 1, start=2, num=2)
    assert result == ["c", "b"]


@pytest.mark.parametrize("r", ["memory_storage", "redisclient"])
def test_storage_zrem(r, request):
    """Test zrem removes members from sorted set."""
    c = request.getfixturevalue(r)
    c.zadd("scores", {"a": 1, "b": 2, "c": 3, "d": 4})

    # Remove one member
    assert c.zrem("scores", "b") == 1
    assert c.zcard("scores") == 3
    assert c.zrange("scores", 0, -1) == ["a", "c", "d"]

    # Remove multiple members
    assert c.zrem("scores", "a", "d") == 2
    assert c.zcard("scores") == 1

    # Remove nonexistent member
    assert c.zrem("scores", "nonexistent") == 0


@pytest.mark.parametrize("r", ["memory_storage", "redisclient"])
def test_storage_zrem_nonexistent_key(r, request):
    """Test zrem on nonexistent key."""
    c = request.getfixturevalue(r)
    assert c.zrem("nonexistent", "member") == 0


@pytest.mark.parametrize("r", ["memory_storage", "redisclient"])
def test_storage_zcount(r, request):
    """Test zcount counts members within score range."""
    c = request.getfixturevalue(r)
    c.zadd("scores", {"a": 1, "b": 2, "c": 3, "d": 4, "e": 5})

    # Count all
    assert c.zcount("scores", "-inf", "+inf") == 5

    # Count range
    assert c.zcount("scores", 2, 4) == 3  # b, c, d

    # Count with exclusive bounds
    assert c.zcount("scores", "(2", "(4") == 1  # only c


@pytest.mark.parametrize("r", ["memory_storage", "redisclient"])
def test_storage_zcount_empty(r, request):
    """Test zcount on nonexistent key."""
    c = request.getfixturevalue(r)
    assert c.zcount("nonexistent", 0, 10) == 0


# ============================================================================
# EXPIRY TESTS
# ============================================================================


@pytest.mark.parametrize("r", ["memory_storage", "redisclient"])
def test_storage_setex_basic(r, request):
    """Test setex sets value with expiration."""
    c = request.getfixturevalue(r)
    c.setex("temp_key", 1, "temp_value")

    # Key should exist immediately
    assert c.get("temp_key") == "temp_value"

    # Key should expire after 1 second
    time.sleep(1.1)
    assert c.get("temp_key") is None


@pytest.mark.parametrize("r", ["memory_storage", "redisclient"])
def test_storage_expire_basic(r, request):
    """Test expire sets expiration on existing key."""
    c = request.getfixturevalue(r)
    c.set("key", "value")

    # Set expiration
    assert c.expire("key", 1) == 1

    # Key should exist immediately
    assert c.get("key") == "value"

    # Key should expire
    time.sleep(1.1)
    assert c.get("key") is None


@pytest.mark.parametrize("r", ["memory_storage", "redisclient"])
def test_storage_expire_nonexistent(r, request):
    """Test expire on nonexistent key."""
    c = request.getfixturevalue(r)
    assert c.expire("nonexistent", 10) == 0


@pytest.mark.parametrize("r", ["memory_storage", "redisclient"])
def test_storage_expire_hash(r, request):
    """Test expiry works with hash keys."""
    c = request.getfixturevalue(r)
    c.hset("user:1", "name", "Alice")
    c.expire("user:1", 1)

    assert c.hget("user:1", "name") == "Alice"
    time.sleep(1.1)
    assert c.hget("user:1", "name") is None
    assert c.hlen("user:1") == 0


@pytest.mark.parametrize("r", ["memory_storage", "redisclient"])
def test_storage_expire_list(r, request):
    """Test expiry works with list keys."""
    c = request.getfixturevalue(r)
    c.rpush("mylist", "item1")
    c.expire("mylist", 1)

    assert c.llen("mylist") == 1
    time.sleep(1.1)
    assert c.llen("mylist") == 0


@pytest.mark.parametrize("r", ["memory_storage", "redisclient"])
def test_storage_expire_set(r, request):
    """Test expiry works with set keys."""
    c = request.getfixturevalue(r)
    c.sadd("myset", "member")
    c.expire("myset", 1)

    assert c.scard("myset") == 1
    time.sleep(1.1)
    assert c.scard("myset") == 0


@pytest.mark.parametrize("r", ["memory_storage", "redisclient"])
def test_storage_expire_sorted_set(r, request):
    """Test expiry works with sorted set keys."""
    c = request.getfixturevalue(r)
    c.zadd("scores", {"player1": 100})
    c.expire("scores", 1)

    assert c.zcard("scores") == 1
    time.sleep(1.1)
    assert c.zcard("scores") == 0


@pytest.mark.parametrize("r", ["memory_storage", "redisclient"])
def test_storage_expire_exists(r, request):
    """Test exists returns 0 for expired keys."""
    c = request.getfixturevalue(r)
    c.setex("temp", 1, "value")

    assert c.exists("temp") == 1
    time.sleep(1.1)
    assert c.exists("temp") == 0


@pytest.mark.parametrize("r", ["memory_storage", "redisclient"])
def test_storage_expire_multiple_operations(r, request):
    """Test that accessing expired keys removes them from storage."""
    c = request.getfixturevalue(r)
    # Set multiple keys with expiry
    c.setex("key1", 1, "value1")
    c.setex("key2", 1, "value2")
    c.setex("key3", 1, "value3")

    # All should exist
    assert c.exists("key1") == 1
    assert c.exists("key2") == 1
    assert c.exists("key3") == 1

    # Wait for expiry
    time.sleep(1.1)

    # Access should return None and clean up
    assert c.get("key1") is None
    assert c.get("key2") is None
    assert c.get("key3") is None

    # Keys should not exist
    assert c.exists("key1") == 0
    assert c.exists("key2") == 0
    assert c.exists("key3") == 0


@pytest.mark.parametrize("r", ["memory_storage", "redisclient"])
def test_storage_ttl_nonexistent_key(r, request):
    """Test ttl on nonexistent key returns -2."""
    c = request.getfixturevalue(r)
    assert c.ttl("nonexistent") == -2


@pytest.mark.parametrize("r", ["memory_storage", "redisclient"])
def test_storage_ttl_no_expiration(r, request):
    """Test ttl on key without expiration returns -1."""
    c = request.getfixturevalue(r)
    c.set("key", "value")
    assert c.ttl("key") == -1


@pytest.mark.parametrize("r", ["memory_storage", "redisclient"])
def test_storage_ttl_with_expiration(r, request):
    """Test ttl returns correct remaining time."""
    c = request.getfixturevalue(r)
    c.setex("key", 10, "value")

    # TTL should be approximately 10 seconds
    ttl_value = c.ttl("key")
    assert 9 <= ttl_value <= 10


@pytest.mark.parametrize("r", ["memory_storage", "redisclient"])
def test_storage_ttl_expired_key(r, request):
    """Test ttl on expired key returns -2."""
    c = request.getfixturevalue(r)
    c.setex("key", 1, "value")

    # Wait for expiry
    time.sleep(1.1)

    # TTL should return -2 (key doesn't exist)
    assert c.ttl("key") == -2


@pytest.mark.parametrize("r", ["memory_storage", "redisclient"])
def test_storage_ttl_after_expire_command(r, request):
    """Test ttl after using expire command."""
    c = request.getfixturevalue(r)
    c.set("key", "value")
    c.expire("key", 5)

    # TTL should be approximately 5 seconds
    ttl_value = c.ttl("key")
    assert 4 <= ttl_value <= 5


@pytest.mark.parametrize("r", ["memory_storage", "redisclient"])
def test_storage_ttl_hash(r, request):
    """Test ttl works with hash keys."""
    c = request.getfixturevalue(r)
    c.hset("user", "name", "Alice")
    c.expire("user", 10)

    ttl_value = c.ttl("user")
    assert 9 <= ttl_value <= 10


@pytest.mark.parametrize("r", ["memory_storage", "redisclient"])
def test_storage_ttl_list(r, request):
    """Test ttl works with list keys."""
    c = request.getfixturevalue(r)
    c.rpush("mylist", "item")
    c.expire("mylist", 10)

    ttl_value = c.ttl("mylist")
    assert 9 <= ttl_value <= 10


@pytest.mark.parametrize("r", ["memory_storage", "redisclient"])
def test_storage_ttl_set(r, request):
    """Test ttl works with set keys."""
    c = request.getfixturevalue(r)
    c.sadd("myset", "member")
    c.expire("myset", 10)

    ttl_value = c.ttl("myset")
    assert 9 <= ttl_value <= 10


@pytest.mark.parametrize("r", ["memory_storage", "redisclient"])
def test_storage_ttl_sorted_set(r, request):
    """Test ttl works with sorted set keys."""
    c = request.getfixturevalue(r)
    c.zadd("scores", {"player": 100})
    c.expire("scores", 10)

    ttl_value = c.ttl("scores")
    assert 9 <= ttl_value <= 10


@pytest.mark.parametrize("r", ["memory_storage", "redisclient"])
def test_storage_ttl_countdown(r, request):
    """Test ttl decreases over time."""
    c = request.getfixturevalue(r)
    c.setex("key", 5, "value")

    ttl1 = c.ttl("key")
    time.sleep(2)
    ttl2 = c.ttl("key")

    # TTL should have decreased
    assert ttl2 < ttl1
    assert ttl2 >= 0


@pytest.mark.parametrize("r", ["memory_storage", "redisclient"])
def test_storage_expire_hget(r, request):
    """Test hget returns None for expired hash."""
    c = request.getfixturevalue(r)
    c.hset("user", "name", "Alice")
    c.expire("user", 1)

    assert c.hget("user", "name") == "Alice"
    time.sleep(1.1)
    assert c.hget("user", "name") is None


@pytest.mark.parametrize("r", ["memory_storage", "redisclient"])
def test_storage_expire_hmget(r, request):
    """Test hmget returns None for expired hash fields."""
    c = request.getfixturevalue(r)
    c.hset("user", "name", "Alice")
    c.hset("user", "age", "30")
    c.expire("user", 1)

    assert c.hmget("user", ["name", "age"]) == ["Alice", "30"]
    time.sleep(1.1)
    assert c.hmget("user", ["name", "age"]) == [None, None]


@pytest.mark.parametrize("r", ["memory_storage", "redisclient"])
def test_storage_expire_hkeys(r, request):
    """Test hkeys returns empty list for expired hash."""
    c = request.getfixturevalue(r)
    c.hset("user", "name", "Alice")
    c.hset("user", "age", "30")
    c.expire("user", 1)

    keys = c.hkeys("user")
    assert len(keys) == 2
    time.sleep(1.1)
    assert c.hkeys("user") == []


# ============================================================================
# SCAN_ITER TESTS
# ============================================================================


@pytest.mark.parametrize("r", ["memory_storage", "redisclient"])
def test_storage_scan_iter_all(r, request):
    """Test scan_iter returns all keys when no pattern specified."""
    c = request.getfixturevalue(r)
    c.set("key1", "value1")
    c.set("key2", "value2")
    c.set("other", "value3")

    keys = sorted(c.scan_iter())
    assert "key1" in keys
    assert "key2" in keys
    assert "other" in keys


@pytest.mark.parametrize("r", ["memory_storage", "redisclient"])
def test_storage_scan_iter_pattern(r, request):
    """Test scan_iter with glob pattern."""
    c = request.getfixturevalue(r)
    c.set("user:1", "alice")
    c.set("user:2", "bob")
    c.set("post:1", "hello")

    user_keys = sorted(c.scan_iter("user:*"))
    assert user_keys == ["user:1", "user:2"]


@pytest.mark.parametrize("r", ["memory_storage", "redisclient"])
def test_storage_scan_iter_wildcard(r, request):
    """Test scan_iter with different wildcard patterns."""
    c = request.getfixturevalue(r)
    c.set("room:123:lock:frame", "value1")
    c.set("room:456:lock:user", "value2")
    c.set("room:123:data", "value3")

    # Match all locks
    lock_keys = sorted(c.scan_iter("*:lock:*"))
    assert len(lock_keys) == 2
    assert "room:123:lock:frame" in lock_keys
    assert "room:456:lock:user" in lock_keys


@pytest.mark.parametrize("r", ["memory_storage", "redisclient"])
def test_storage_scan_iter_no_match(r, request):
    """Test scan_iter with pattern that matches nothing."""
    c = request.getfixturevalue(r)
    c.set("key1", "value1")

    result = list(c.scan_iter("nomatch:*"))
    assert result == []


@pytest.mark.parametrize("r", ["memory_storage", "redisclient"])
def test_storage_scan_iter_expired_keys(r, request):
    """Test scan_iter does not return expired keys."""
    c = request.getfixturevalue(r)
    c.setex("temp1", 1, "value1")
    c.set("permanent", "value2")

    # Before expiry
    keys = list(c.scan_iter())
    assert len(keys) >= 2

    # After expiry
    time.sleep(1.1)
    keys = list(c.scan_iter())
    assert "permanent" in keys
    assert "temp1" not in keys


@pytest.mark.parametrize("r", ["memory_storage"])
def test_storage_scan_iter_count(r, request):
    """Test scan_iter with count parameter limits results (MemoryStorage only).

    Note: In Redis, count is just a hint for performance, not a hard limit.
    MemoryStorage implements it as a hard limit for simplicity.
    """
    c = request.getfixturevalue(r)
    # Create multiple keys
    for i in range(10):
        c.set(f"key{i}", f"value{i}")

    # Get only 3 keys
    keys = list(c.scan_iter(count=3))
    assert len(keys) == 3

    # Get only 5 keys
    keys = list(c.scan_iter(count=5))
    assert len(keys) == 5


@pytest.mark.parametrize("r", ["memory_storage"])
def test_storage_scan_iter_count_with_pattern(r, request):
    """Test scan_iter with both count and pattern (MemoryStorage only).

    Note: In Redis, count is just a hint for performance, not a hard limit.
    MemoryStorage implements it as a hard limit for simplicity.
    """
    c = request.getfixturevalue(r)
    # Create multiple user and post keys
    for i in range(10):
        c.set(f"user:{i}", f"user{i}")
        c.set(f"post:{i}", f"post{i}")

    # Get only 3 user keys
    user_keys = list(c.scan_iter("user:*", count=3))
    assert len(user_keys) == 3
    for key in user_keys:
        assert key.startswith("user:")


@pytest.mark.parametrize("r", ["memory_storage"])
def test_storage_scan_iter_count_larger_than_available(r, request):
    """Test scan_iter when count is larger than available keys (MemoryStorage only).

    Note: In Redis, count is just a hint for performance, not a hard limit.
    MemoryStorage implements it as a hard limit for simplicity.
    """
    c = request.getfixturevalue(r)
    c.set("key1", "value1")
    c.set("key2", "value2")

    # Request more keys than available
    keys = list(c.scan_iter(count=100))
    assert len(keys) == 2  # Should only return what's available


# ============================================================================
# PIPELINE TESTS
# ============================================================================


@pytest.mark.parametrize("r", ["memory_storage", "redisclient"])
def test_storage_pipeline_basic(r, request):
    """Test pipeline executes multiple commands."""
    c = request.getfixturevalue(r)
    pipe = c.pipeline()
    pipe.set("key1", "value1")
    pipe.set("key2", "value2")
    pipe.get("key1")
    results = pipe.execute()

    assert results[0] is True  # set returns True
    assert results[1] is True  # set returns True
    assert results[2] == "value1"  # get returns value


@pytest.mark.parametrize("r", ["memory_storage", "redisclient"])
def test_storage_pipeline_sorted_sets(r, request):
    """Test pipeline with sorted set operations."""
    c = request.getfixturevalue(r)
    pipe = c.pipeline()
    pipe.zadd("scores", {"alice": 100, "bob": 200})
    pipe.zadd("scores", {"charlie": 150})
    pipe.zcard("scores")
    pipe.zrange("scores", 0, -1)
    results = pipe.execute()

    assert results[0] == 2  # zadd returns count
    assert results[1] == 1  # zadd returns count
    assert results[2] == 3  # zcard returns 3
    assert results[3] == ["alice", "charlie", "bob"]  # zrange returns sorted members


@pytest.mark.parametrize("r", ["memory_storage", "redisclient"])
def test_storage_pipeline_with_expiry(r, request):
    """Test pipeline with expiry operations."""
    c = request.getfixturevalue(r)
    pipe = c.pipeline()
    pipe.set("key1", "value1")
    pipe.expire("key1", 1)
    pipe.set("key2", "value2")
    pipe.setex("key3", 1, "value3")
    results = pipe.execute()

    assert results[0] is True  # set
    assert results[1] == 1  # expire
    assert results[2] is True  # set
    assert results[3] is True  # setex


@pytest.mark.parametrize("r", ["memory_storage", "redisclient"])
def test_storage_pipeline_context_manager(r, request):
    """Test pipeline as context manager (must call execute explicitly)."""
    c = request.getfixturevalue(r)
    with c.pipeline() as pipe:
        pipe.set("key1", "value1")
        pipe.set("key2", "value2")
        pipe.get("key1")
        # Must explicitly call execute() - redis-py doesn't auto-execute
        pipe.execute()

    # Verify values were set
    assert c.get("key1") == "value1"
    assert c.get("key2") == "value2"


@pytest.mark.parametrize("r", ["memory_storage", "redisclient"])
def test_storage_pipeline_multiple_executions(r, request):
    """Test that pipeline can be executed multiple times."""
    c = request.getfixturevalue(r)
    pipe = c.pipeline()

    # First execution
    pipe.set("key1", "value1")
    pipe.get("key1")
    results1 = pipe.execute()
    assert results1[0] is True
    assert results1[1] == "value1"

    # Second execution with same pipeline
    pipe.set("key2", "value2")
    pipe.get("key2")
    results2 = pipe.execute()
    assert results2[0] is True
    assert results2[1] == "value2"

    # Verify both keys were set
    assert c.get("key1") == "value1"
    assert c.get("key2") == "value2"


# ============================================================================
# SMOVE TESTS
# ============================================================================


@pytest.mark.parametrize("r", ["memory_storage", "redisclient"])
def test_storage_smove_basic(r, request):
    """Test basic smove operation."""
    c = request.getfixturevalue(r)
    c.sadd("set1", "member1")
    c.sadd("set1", "member2")
    c.sadd("set2", "member3")

    # Move member1 from set1 to set2
    assert c.smove("set1", "set2", "member1") == 1

    # Verify member1 is now in set2 and not in set1
    assert "member1" in c.smembers("set2")
    assert "member1" not in c.smembers("set1")
    assert "member2" in c.smembers("set1")
    assert "member3" in c.smembers("set2")


@pytest.mark.parametrize("r", ["memory_storage", "redisclient"])
def test_storage_smove_to_new_destination(r, request):
    """Test smove to a non-existent destination creates it."""
    c = request.getfixturevalue(r)
    c.sadd("set1", "member1")

    # Move to non-existent set
    assert c.smove("set1", "set2", "member1") == 1

    # Verify destination was created
    assert "member1" in c.smembers("set2")
    assert c.scard("set2") == 1


@pytest.mark.parametrize("r", ["memory_storage", "redisclient"])
def test_storage_smove_nonexistent_member(r, request):
    """Test smove with member that doesn't exist in source."""
    c = request.getfixturevalue(r)
    c.sadd("set1", "member1")
    c.sadd("set2", "member2")

    # Try to move non-existent member
    assert c.smove("set1", "set2", "nonexistent") == 0

    # Verify sets are unchanged
    assert c.smembers("set1") == {"member1"}
    assert c.smembers("set2") == {"member2"}


@pytest.mark.parametrize("r", ["memory_storage", "redisclient"])
def test_storage_smove_nonexistent_source(r, request):
    """Test smove from non-existent source."""
    c = request.getfixturevalue(r)
    c.sadd("set2", "member2")

    # Try to move from non-existent set
    assert c.smove("nonexistent", "set2", "member1") == 0

    # Verify destination is unchanged
    assert c.smembers("set2") == {"member2"}


@pytest.mark.parametrize("r", ["memory_storage", "redisclient"])
def test_storage_smove_member_already_in_destination(r, request):
    """Test smove when member already exists in destination."""
    c = request.getfixturevalue(r)
    c.sadd("set1", "member1")
    c.sadd("set2", "member1")

    # Move member that's already in destination
    assert c.smove("set1", "set2", "member1") == 1

    # Verify member is removed from source and exists in destination
    assert "member1" not in c.smembers("set1")
    assert "member1" in c.smembers("set2")
    # Destination should still have only one copy (sets don't allow duplicates)
    assert c.scard("set2") == 1


@pytest.mark.parametrize("r", ["memory_storage", "redisclient"])
def test_storage_smove_wrong_type_source(r, request):
    """Test smove when source is not a set."""
    c = request.getfixturevalue(r)
    c.set("not_a_set", "value")
    c.sadd("set2", "member")

    # Try to move from a non-set
    with pytest.raises(Exception):  # ResponseError
        c.smove("not_a_set", "set2", "member")


@pytest.mark.parametrize("r", ["memory_storage", "redisclient"])
def test_storage_smove_wrong_type_destination(r, request):
    """Test smove when destination is not a set."""
    c = request.getfixturevalue(r)
    c.sadd("set1", "member1")
    c.set("not_a_set", "value")

    # Try to move to a non-set
    with pytest.raises(Exception):  # ResponseError
        c.smove("set1", "not_a_set", "member1")


@pytest.mark.parametrize("r", ["memory_storage", "redisclient"])
def test_storage_smove_same_source_and_destination(r, request):
    """Test smove with same source and destination."""
    c = request.getfixturevalue(r)
    c.sadd("set1", "member1")
    c.sadd("set1", "member2")

    # Move within same set (should be a no-op but return 1)
    assert c.smove("set1", "set1", "member1") == 1

    # Verify set is unchanged
    assert c.smembers("set1") == {"member1", "member2"}
    assert c.scard("set1") == 2


# ============================================================================
# TYPE TESTS
# ============================================================================


@pytest.mark.parametrize("r", ["memory_storage", "redisclient"])
def test_storage_type_string(r, request):
    """Test type returns 'string' for string values."""
    c = request.getfixturevalue(r)
    c.set("key1", "value1")
    assert c.type("key1") == "string"

    # Test with integer value (stored as string)
    c.set("key2", 42)
    assert c.type("key2") == "string"

    # Test with float value (stored as string)
    c.set("key3", 3.14)
    assert c.type("key3") == "string"


@pytest.mark.parametrize("r", ["memory_storage", "redisclient"])
def test_storage_type_hash(r, request):
    """Test type returns 'hash' for hash values."""
    c = request.getfixturevalue(r)
    c.hset("user:1", "name", "Alice")
    assert c.type("user:1") == "hash"

    # Test with multiple fields
    c.hset("user:2", mapping={"name": "Bob", "age": "30"})
    assert c.type("user:2") == "hash"


@pytest.mark.parametrize("r", ["memory_storage", "redisclient"])
def test_storage_type_list(r, request):
    """Test type returns 'list' for list values."""
    c = request.getfixturevalue(r)
    c.rpush("mylist", "item1")
    assert c.type("mylist") == "list"

    # Test with lpush
    c.lpush("mylist2", "item1")
    assert c.type("mylist2") == "list"


@pytest.mark.parametrize("r", ["memory_storage", "redisclient"])
def test_storage_type_set(r, request):
    """Test type returns 'set' for set values."""
    c = request.getfixturevalue(r)
    c.sadd("myset", "member1")
    assert c.type("myset") == "set"

    # Test with multiple members
    c.sadd("myset2", "member1")
    c.sadd("myset2", "member2")
    assert c.type("myset2") == "set"


@pytest.mark.parametrize("r", ["memory_storage", "redisclient"])
def test_storage_type_zset(r, request):
    """Test type returns 'zset' for sorted set values."""
    c = request.getfixturevalue(r)
    c.zadd("leaderboard", {"player1": 100})
    assert c.type("leaderboard") == "zset"

    # Test with multiple members
    c.zadd("scores", {"alice": 100, "bob": 200, "charlie": 150})
    assert c.type("scores") == "zset"


@pytest.mark.parametrize("r", ["memory_storage", "redisclient"])
def test_storage_type_nonexistent(r, request):
    """Test type returns 'none' for non-existent keys."""
    c = request.getfixturevalue(r)
    assert c.type("nonexistent") == "none"


@pytest.mark.parametrize("r", ["memory_storage", "redisclient"])
def test_storage_type_expired(r, request):
    """Test type returns 'none' for expired keys."""
    c = request.getfixturevalue(r)
    # Test with expired string
    c.setex("temp_string", 1, "value")
    assert c.type("temp_string") == "string"
    time.sleep(1.1)
    assert c.type("temp_string") == "none"

    # Test with expired hash
    c.hset("temp_hash", "field", "value")
    c.expire("temp_hash", 1)
    assert c.type("temp_hash") == "hash"
    time.sleep(1.1)
    assert c.type("temp_hash") == "none"


@pytest.mark.parametrize("r", ["memory_storage", "redisclient"])
def test_storage_type_after_delete(r, request):
    """Test type returns 'none' after deleting a key."""
    c = request.getfixturevalue(r)
    c.set("key", "value")
    assert c.type("key") == "string"

    c.delete("key")
    assert c.type("key") == "none"


@pytest.mark.parametrize("r", ["memory_storage", "redisclient"])
def test_storage_type_all_types(r, request):
    """Test type correctly identifies all different types in storage."""
    c = request.getfixturevalue(r)

    # Create keys of all types
    c.set("string_key", "value")
    c.hset("hash_key", "field", "value")
    c.rpush("list_key", "item")
    c.sadd("set_key", "member")
    c.zadd("zset_key", {"member": 1.0})

    # Verify all types
    assert c.type("string_key") == "string"
    assert c.type("hash_key") == "hash"
    assert c.type("list_key") == "list"
    assert c.type("set_key") == "set"
    assert c.type("zset_key") == "zset"
    assert c.type("nonexistent_key") == "none"


@pytest.mark.parametrize("r", ["memory_storage", "redisclient"])
def test_storage_type_with_hgetall(r, request):
    """Test hgetall behavior with different key types using type command."""
    c = request.getfixturevalue(r)

    # Hash type - should work
    c.hset("hash_key", "field1", "value1")
    assert c.type("hash_key") == "hash"
    assert c.hgetall("hash_key") == {"field1": "value1"}

    # Non-existent key - should return empty dict
    assert c.type("nonexistent") == "none"
    assert c.hgetall("nonexistent") == {}

    # String type - should raise error
    c.set("string_key", "value")
    assert c.type("string_key") == "string"
    # In Redis, hgetall on wrong type raises WRONGTYPE error
    # For now, verify the type is correct and that we can detect the difference
    with pytest.raises(Exception):  # Should raise ResponseError in redis
        c.hgetall("string_key")

    # List type - should raise error
    c.rpush("list_key", "item")
    assert c.type("list_key") == "list"
    with pytest.raises(Exception):
        c.hgetall("list_key")

    # Set type - should raise error
    c.sadd("set_key", "member")
    assert c.type("set_key") == "set"
    with pytest.raises(Exception):
        c.hgetall("set_key")

    # Sorted set type - should raise error
    c.zadd("zset_key", {"member": 1.0})
    assert c.type("zset_key") == "zset"
    with pytest.raises(Exception):
        c.hgetall("zset_key")
