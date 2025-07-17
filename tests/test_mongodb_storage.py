import pytest
import znsocket
from znsocket.storages import MongoStorage
from znsocket.exceptions import DataError, ResponseError


@pytest.fixture
def mongo_storage():
    """Create a MongoDB storage instance for testing."""
    try:
        storage = MongoStorage(
            connection_string="mongodb://localhost:27017/",
            database_name="znsocket_test"
        )
        yield storage
        # Clean up: clear all collections
        storage.flushall()
    except ImportError:
        pytest.skip("pymongo not installed")
    except Exception as e:
        pytest.skip(f"MongoDB connection failed: {e}")


class TestMongoStorageHashes:
    """Test MongoDB storage hash operations."""

    def test_hset_hget(self, mongo_storage):
        """Test basic hash set and get operations."""
        result = mongo_storage.hset("test_hash", "key1", "value1")
        assert result == 1
        
        value = mongo_storage.hget("test_hash", "key1")
        assert value == "value1"
        
        # Test non-existent key
        value = mongo_storage.hget("test_hash", "nonexistent")
        assert value is None

    def test_hset_multiple(self, mongo_storage):
        """Test setting multiple hash fields."""
        result = mongo_storage.hset("test_hash", mapping={"key1": "value1", "key2": "value2"})
        assert result == 2
        
        assert mongo_storage.hget("test_hash", "key1") == "value1"
        assert mongo_storage.hget("test_hash", "key2") == "value2"

    def test_hmget(self, mongo_storage):
        """Test getting multiple hash fields."""
        mongo_storage.hset("test_hash", mapping={"key1": "value1", "key2": "value2"})
        
        values = mongo_storage.hmget("test_hash", ["key1", "key2", "nonexistent"])
        assert values == ["value1", "value2", None]

    def test_hkeys_hvals(self, mongo_storage):
        """Test getting hash keys and values."""
        mongo_storage.hset("test_hash", mapping={"key1": "value1", "key2": "value2"})
        
        keys = mongo_storage.hkeys("test_hash")
        assert set(keys) == {"key1", "key2"}
        
        values = mongo_storage.hvals("test_hash")
        assert set(values) == {"value1", "value2"}

    def test_hgetall(self, mongo_storage):
        """Test getting all hash fields and values."""
        mongo_storage.hset("test_hash", mapping={"key1": "value1", "key2": "value2"})
        
        all_data = mongo_storage.hgetall("test_hash")
        assert all_data == {"key1": "value1", "key2": "value2"}

    def test_hexists(self, mongo_storage):
        """Test checking if hash field exists."""
        mongo_storage.hset("test_hash", "key1", "value1")
        
        assert mongo_storage.hexists("test_hash", "key1") == 1
        assert mongo_storage.hexists("test_hash", "nonexistent") == 0

    def test_hdel(self, mongo_storage):
        """Test deleting hash fields."""
        mongo_storage.hset("test_hash", mapping={"key1": "value1", "key2": "value2"})
        
        result = mongo_storage.hdel("test_hash", "key1")
        assert result == 1
        
        assert mongo_storage.hget("test_hash", "key1") is None
        assert mongo_storage.hget("test_hash", "key2") == "value2"

    def test_hlen(self, mongo_storage):
        """Test getting hash length."""
        mongo_storage.hset("test_hash", mapping={"key1": "value1", "key2": "value2"})
        
        length = mongo_storage.hlen("test_hash")
        assert length == 2


class TestMongoStorageLists:
    """Test MongoDB storage list operations."""

    def test_rpush_llen(self, mongo_storage):
        """Test right push and list length."""
        result = mongo_storage.rpush("test_list", "item1")
        assert result == 1
        
        result = mongo_storage.rpush("test_list", "item2")
        assert result == 2
        
        length = mongo_storage.llen("test_list")
        assert length == 2

    def test_lpush(self, mongo_storage):
        """Test left push."""
        mongo_storage.rpush("test_list", "item1")
        mongo_storage.lpush("test_list", "item0")
        
        items = mongo_storage.lrange("test_list", 0, -1)
        assert items == ["item0", "item1"]

    def test_lindex(self, mongo_storage):
        """Test getting list item by index."""
        mongo_storage.rpush("test_list", "item1")
        mongo_storage.rpush("test_list", "item2")
        
        assert mongo_storage.lindex("test_list", 0) == "item1"
        assert mongo_storage.lindex("test_list", 1) == "item2"
        assert mongo_storage.lindex("test_list", 2) is None

    def test_lrange(self, mongo_storage):
        """Test getting list range."""
        for i in range(5):
            mongo_storage.rpush("test_list", f"item{i}")
        
        items = mongo_storage.lrange("test_list", 1, 3)
        assert items == ["item1", "item2", "item3"]
        
        items = mongo_storage.lrange("test_list", 0, -1)
        assert items == ["item0", "item1", "item2", "item3", "item4"]

    def test_lset(self, mongo_storage):
        """Test setting list item by index."""
        mongo_storage.rpush("test_list", "item1")
        mongo_storage.rpush("test_list", "item2")
        
        mongo_storage.lset("test_list", 1, "new_item")
        assert mongo_storage.lindex("test_list", 1) == "new_item"

    def test_lrem(self, mongo_storage):
        """Test removing list items."""
        mongo_storage.rpush("test_list", "item1")
        mongo_storage.rpush("test_list", "item2")
        mongo_storage.rpush("test_list", "item1")
        
        removed = mongo_storage.lrem("test_list", 1, "item1")
        assert removed == 1
        
        items = mongo_storage.lrange("test_list", 0, -1)
        assert items == ["item2", "item1"]

    def test_linsert(self, mongo_storage):
        """Test inserting into list."""
        mongo_storage.rpush("test_list", "item1")
        mongo_storage.rpush("test_list", "item3")
        
        result = mongo_storage.linsert("test_list", "AFTER", "item1", "item2")
        assert result == 3
        
        items = mongo_storage.lrange("test_list", 0, -1)
        assert items == ["item1", "item2", "item3"]

    def test_lpop(self, mongo_storage):
        """Test popping from left of list."""
        mongo_storage.rpush("test_list", "item1")
        mongo_storage.rpush("test_list", "item2")
        
        popped = mongo_storage.lpop("test_list")
        assert popped == "item1"
        
        items = mongo_storage.lrange("test_list", 0, -1)
        assert items == ["item2"]


class TestMongoStorageSets:
    """Test MongoDB storage set operations."""

    def test_sadd_smembers(self, mongo_storage):
        """Test adding to set and getting members."""
        mongo_storage.sadd("test_set", "item1")
        mongo_storage.sadd("test_set", "item2")
        mongo_storage.sadd("test_set", "item1")  # Duplicate
        
        members = mongo_storage.smembers("test_set")
        assert members == {"item1", "item2"}

    def test_srem(self, mongo_storage):
        """Test removing from set."""
        mongo_storage.sadd("test_set", "item1")
        mongo_storage.sadd("test_set", "item2")
        
        result = mongo_storage.srem("test_set", "item1")
        assert result == 1
        
        members = mongo_storage.smembers("test_set")
        assert members == {"item2"}

    def test_scard(self, mongo_storage):
        """Test set cardinality."""
        mongo_storage.sadd("test_set", "item1")
        mongo_storage.sadd("test_set", "item2")
        
        count = mongo_storage.scard("test_set")
        assert count == 2


class TestMongoStorageKeys:
    """Test MongoDB storage key operations."""

    def test_set_get(self, mongo_storage):
        """Test basic key-value operations."""
        result = mongo_storage.set("test_key", "test_value")
        assert result is True
        
        value = mongo_storage.get("test_key")
        assert value == "test_value"

    def test_delete(self, mongo_storage):
        """Test deleting keys."""
        mongo_storage.set("test_key", "test_value")
        
        result = mongo_storage.delete("test_key")
        assert result == 1
        
        value = mongo_storage.get("test_key")
        assert value is None

    def test_exists(self, mongo_storage):
        """Test checking key existence."""
        mongo_storage.set("test_key", "test_value")
        
        assert mongo_storage.exists("test_key") == 1
        assert mongo_storage.exists("nonexistent") == 0

    def test_copy(self, mongo_storage):
        """Test copying keys."""
        mongo_storage.set("src_key", "test_value")
        
        result = mongo_storage.copy("src_key", "dst_key")
        assert result is True
        
        assert mongo_storage.get("dst_key") == "test_value"

    def test_flushall(self, mongo_storage):
        """Test flushing all data."""
        mongo_storage.set("test_key", "test_value")
        mongo_storage.hset("test_hash", "key", "value")
        
        mongo_storage.flushall()
        
        assert mongo_storage.get("test_key") is None
        assert mongo_storage.hget("test_hash", "key") is None


class TestMongoStorageErrors:
    """Test MongoDB storage error handling."""

    def test_hset_no_args(self, mongo_storage):
        """Test hset with no arguments raises error."""
        with pytest.raises(DataError):
            mongo_storage.hset("test_hash")

    def test_lindex_none(self, mongo_storage):
        """Test lindex with None index raises error."""
        with pytest.raises(DataError):
            mongo_storage.lindex("test_list", None)

    def test_set_none_value(self, mongo_storage):
        """Test set with None value raises error."""
        with pytest.raises(DataError):
            mongo_storage.set("test_key", None)


class TestMongoStorageIntegration:
    """Test MongoDB storage integration with znsocket client."""

    def test_basic_operations(self, znsclient_w_mongodb):
        """Test basic operations through znsocket client."""
        # Test key-value operations
        znsclient_w_mongodb.set("test_key", "test_value")
        assert znsclient_w_mongodb.get("test_key") == "test_value"
        
        # Test hash operations
        znsclient_w_mongodb.hset("test_hash", "key1", "value1")
        assert znsclient_w_mongodb.hget("test_hash", "key1") == "value1"
        
        # Test list operations
        znsclient_w_mongodb.rpush("test_list", "item1")
        znsclient_w_mongodb.rpush("test_list", "item2")
        assert znsclient_w_mongodb.lrange("test_list", 0, -1) == ["item1", "item2"]
        
        # Test set operations
        znsclient_w_mongodb.sadd("test_set", "item1")
        znsclient_w_mongodb.sadd("test_set", "item2")
        members = znsclient_w_mongodb.smembers("test_set")
        assert set(members) == {"item1", "item2"}

    def test_persistence(self, mongoclient):
        """Test that data persists in MongoDB."""
        # Create a storage instance and add data
        storage = MongoStorage(
            connection_string="mongodb://localhost:27017/",
            database_name="znsocket_test"
        )
        
        storage.set("persist_key", "persist_value")
        storage.hset("persist_hash", "key", "value")
        
        # Create a new storage instance to test persistence
        new_storage = MongoStorage(
            connection_string="mongodb://localhost:27017/",
            database_name="znsocket_test"
        )
        
        assert new_storage.get("persist_key") == "persist_value"
        assert new_storage.hget("persist_hash", "key") == "value"
        
        # Clean up
        new_storage.flushall()