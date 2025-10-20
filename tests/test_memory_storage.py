import time

import pytest


@pytest.mark.parametrize("r", ["memory_storage", "redisclient"])
def test_storage_set(r, request):
    c = request.getfixturevalue(r)
    c.set("name", "Alice")
    assert c.get("name") == "Alice"


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

    keys = sorted(list(c.scan_iter()))
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

    user_keys = sorted(list(c.scan_iter("user:*")))
    assert user_keys == ["user:1", "user:2"]


@pytest.mark.parametrize("r", ["memory_storage", "redisclient"])
def test_storage_scan_iter_wildcard(r, request):
    """Test scan_iter with different wildcard patterns."""
    c = request.getfixturevalue(r)
    c.set("room:123:lock:frame", "value1")
    c.set("room:456:lock:user", "value2")
    c.set("room:123:data", "value3")

    # Match all locks
    lock_keys = sorted(list(c.scan_iter("*:lock:*")))
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
