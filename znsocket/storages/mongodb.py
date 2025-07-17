"""MongoDB storage backend."""

import dataclasses
import typing as t

from ..exceptions import DataError, ResponseError
from .abc import StorageBackend


@dataclasses.dataclass
class MongoStorage(StorageBackend):
    """MongoDB storage backend for znsocket server.

    The MongoStorage class provides MongoDB-backed storage that implements the same
    Redis-compatible interface as the in-memory NativeStorage class. It uses MongoDB collections
    to store different data types (hashes, lists, sets, keys) with appropriate indexing.

    Parameters
    ----------
    connection_string : str, optional
        MongoDB connection string. Default is 'mongodb://localhost:27017/znsocket'.
    database_name : str, optional
        Name of the database to use. Default is 'znsocket'.

    Attributes
    ----------
    client : pymongo.MongoClient
        The MongoDB client instance.
    db : pymongo.database.Database
        The MongoDB database instance.

    Examples
    --------
    >>> storage = MongoStorage('mongodb://localhost:27017/test')
    >>> storage.hset("users", "user1", "John")
    1
    >>> storage.hget("users", "user1")
    'John'
    """

    connection_string: str = "mongodb://localhost:27017/znsocket"
    database_name: str = "znsocket"

    def __post_init__(self):
        """Initialize MongoDB connection after dataclass initialization."""
        try:
            import pymongo
        except ImportError:
            raise ImportError("pymongo is required for MongoDB storage backend")

        self.client = pymongo.MongoClient(self.connection_string)
        self.db = self.client[self.database_name]

        # Create collections with appropriate indexes
        self._setup_collections()

    def _setup_collections(self):
        """Set up MongoDB collections with appropriate indexes."""
        # Hash collection: stores hash tables
        self.hashes = self.db.hashes
        self.hashes.create_index("name")

        # Lists collection: stores lists with order preserved
        self.lists = self.db.lists
        self.lists.create_index([("name", 1), ("index", 1)])

        # Sets collection: stores sets
        self.sets = self.db.sets
        self.sets.create_index("name")

        # Keys collection: stores simple key-value pairs
        self.keys = self.db.keys
        self.keys.create_index("name")

    def hset(
        self,
        name: str,
        key: t.Optional[str] = None,
        value: t.Optional[str] = None,
        mapping: t.Optional[dict] = None,
        items: t.Optional[list] = None,
    ):
        """Set field(s) in a hash stored in MongoDB."""
        if key is None and not mapping and not items:
            raise DataError("'hset' with no key value pairs")
        if value is None and not mapping and not items:
            raise DataError(f"Invalid input of type {type(value)}")

        pieces = []
        if items:
            pieces.extend(items)
        if key is not None:
            pieces.extend((key, value))
        if mapping:
            for pair in mapping.items():
                pieces.extend(pair)

        # Convert to field updates
        updates = {}
        for i in range(0, len(pieces), 2):
            updates[f"fields.{pieces[i]}"] = pieces[i + 1]

        # Upsert the hash document
        self.hashes.update_one({"name": name}, {"$set": updates}, upsert=True)

        return len(pieces) // 2

    def hget(self, name: str, key: str):
        """Get the value of a hash field from MongoDB."""
        doc = self.hashes.find_one({"name": name})
        if doc and "fields" in doc and key in doc["fields"]:
            return doc["fields"][key]
        return None

    def hmget(self, name: str, keys: list):
        """Get multiple hash field values from MongoDB."""
        doc = self.hashes.find_one({"name": name})
        response = []
        for key in keys:
            if doc and "fields" in doc and key in doc["fields"]:
                response.append(doc["fields"][key])
            else:
                response.append(None)
        return response

    def hkeys(self, name: str):
        """Get all field names in a hash from MongoDB."""
        doc = self.hashes.find_one({"name": name})
        if doc and "fields" in doc:
            return list(doc["fields"].keys())
        return []

    def hvals(self, name: str):
        """Get all values in a hash from MongoDB."""
        doc = self.hashes.find_one({"name": name})
        if doc and "fields" in doc:
            return list(doc["fields"].values())
        return []

    def hgetall(self, name: str):
        """Get all fields and values in a hash from MongoDB."""
        doc = self.hashes.find_one({"name": name})
        if doc and "fields" in doc:
            return doc["fields"]
        return {}

    def hexists(self, name: str, key: str):
        """Check if a hash field exists in MongoDB."""
        doc = self.hashes.find_one({"name": name})
        if doc and "fields" in doc and key in doc["fields"]:
            return 1
        return 0

    def hdel(self, name: str, key: str):
        """Delete a hash field from MongoDB."""
        result = self.hashes.update_one(
            {"name": name}, {"$unset": {f"fields.{key}": ""}}
        )
        return 1 if result.modified_count > 0 else 0

    def hlen(self, name: str):
        """Get the number of fields in a hash from MongoDB."""
        doc = self.hashes.find_one({"name": name})
        if doc and "fields" in doc:
            return len(doc["fields"])
        return 0

    def llen(self, name: str):
        """Get the length of a list from MongoDB."""
        return self.lists.count_documents({"name": name})

    def rpush(self, name: str, value: str):
        """Push a value to the right end of a list in MongoDB."""
        # Get the current maximum index
        max_doc = self.lists.find_one({"name": name}, sort=[("index", -1)])
        next_index = (max_doc["index"] + 1) if max_doc else 0

        # Insert the new item
        self.lists.insert_one({"name": name, "index": next_index, "value": value})

        return next_index + 1

    def lpush(self, name: str, value: str):
        """Push a value to the left end of a list in MongoDB."""
        # Increment all existing indexes
        self.lists.update_many({"name": name}, {"$inc": {"index": 1}})

        # Insert the new item at index 0
        self.lists.insert_one({"name": name, "index": 0, "value": value})

        return self.llen(name)

    def lindex(self, name: str, index: int):
        """Get a list element by index from MongoDB."""
        if index is None:
            raise DataError("Invalid input of type None")

        doc = self.lists.find_one({"name": name, "index": index})
        return doc["value"] if doc else None

    def lrange(self, name: str, start: int, end: int):
        """Get a range of list elements from MongoDB."""
        if end == -1:
            # Get all items from start to the end
            docs = self.lists.find(
                {"name": name, "index": {"$gte": start}}, sort=[("index", 1)]
            )
        else:
            docs = self.lists.find(
                {"name": name, "index": {"$gte": start, "$lte": end}},
                sort=[("index", 1)],
            )

        return [doc["value"] for doc in docs]

    def lset(self, name: str, index: int, value: str):
        """Set a list element by index in MongoDB."""
        result = self.lists.update_one(
            {"name": name, "index": index}, {"$set": {"value": value}}
        )
        if result.matched_count == 0:
            raise ResponseError(
                "no such key" if self.llen(name) == 0 else "index out of range"
            )

    def lrem(self, name: str, count: int, value: str):
        """Remove elements from a list in MongoDB."""
        if count is None or value is None or name is None:
            raise DataError("Invalid input of type None")

        if count == 0:
            # Remove all occurrences
            result = self.lists.delete_many({"name": name, "value": value})
            # Reindex remaining items
            self._reindex_list(name)
            return result.deleted_count
        else:
            # Remove up to count occurrences
            docs = self.lists.find(
                {"name": name, "value": value}, sort=[("index", 1)]
            ).limit(count)

            deleted = 0
            for doc in docs:
                self.lists.delete_one({"_id": doc["_id"]})
                deleted += 1

            # Reindex remaining items
            self._reindex_list(name)
            return deleted

    def _reindex_list(self, name: str):
        """Reindex a list to maintain sequential order."""
        docs = self.lists.find({"name": name}, sort=[("index", 1)])
        for new_index, doc in enumerate(docs):
            if doc["index"] != new_index:
                self.lists.update_one(
                    {"_id": doc["_id"]}, {"$set": {"index": new_index}}
                )

    def linsert(self, name: str, where: str, pivot: str, value: str):
        """Insert an element in a list before or after a pivot value."""
        pivot_doc = self.lists.find_one({"name": name, "value": pivot})
        if not pivot_doc:
            return -1 if self.llen(name) > 0 else 0

        pivot_index = pivot_doc["index"]
        insert_index = pivot_index if where == "BEFORE" else pivot_index + 1

        # Increment indexes of items at or after the insert position
        self.lists.update_many(
            {"name": name, "index": {"$gte": insert_index}}, {"$inc": {"index": 1}}
        )

        # Insert the new item
        self.lists.insert_one({"name": name, "index": insert_index, "value": value})

        return self.llen(name)

    def lpop(self, name: str):
        """Remove and return the leftmost element from a list."""
        doc = self.lists.find_one({"name": name, "index": 0})
        if not doc:
            return None

        value = doc["value"]
        self.lists.delete_one({"_id": doc["_id"]})

        # Decrement all remaining indexes
        self.lists.update_many({"name": name}, {"$inc": {"index": -1}})

        return value

    def smembers(self, name: str):
        """Get all members of a set from MongoDB."""
        doc = self.sets.find_one({"name": name})
        if doc and "members" in doc:
            return set(doc["members"])
        return set()

    def sadd(self, name: str, value: str):
        """Add a member to a set in MongoDB."""
        self.sets.update_one(
            {"name": name}, {"$addToSet": {"members": value}}, upsert=True
        )

    def srem(self, name: str, value: str):
        """Remove a member from a set in MongoDB."""
        result = self.sets.update_one({"name": name}, {"$pull": {"members": value}})
        return 1 if result.modified_count > 0 else 0

    def scard(self, name: str):
        """Get the cardinality (number of members) of a set from MongoDB."""
        doc = self.sets.find_one({"name": name})
        if doc and "members" in doc:
            return len(doc["members"])
        return 0

    def set(self, name: str, value: str):
        """Set a key-value pair in MongoDB."""
        if value is None or name is None:
            raise DataError("Invalid input of type None")

        self.keys.update_one({"name": name}, {"$set": {"value": value}}, upsert=True)
        return True

    def get(self, name: str, default=None):
        """Get a value by key from MongoDB."""
        doc = self.keys.find_one({"name": name})
        return doc["value"] if doc else default

    def delete(self, name: str):
        """Delete a key from all collections in MongoDB."""
        deleted = 0

        # Delete from all collections
        deleted += self.hashes.delete_one({"name": name}).deleted_count
        deleted += self.lists.delete_many({"name": name}).deleted_count
        deleted += self.sets.delete_one({"name": name}).deleted_count
        deleted += self.keys.delete_one({"name": name}).deleted_count

        return 1 if deleted > 0 else 0

    def exists(self, name: str):
        """Check if a key exists in any collection in MongoDB."""
        if self.hashes.find_one({"name": name}):
            return 1
        if self.lists.find_one({"name": name}):
            return 1
        if self.sets.find_one({"name": name}):
            return 1
        if self.keys.find_one({"name": name}):
            return 1
        return 0

    def flushall(self):
        """Clear all data from MongoDB."""
        self.hashes.delete_many({})
        self.lists.delete_many({})
        self.sets.delete_many({})
        self.keys.delete_many({})

    def copy(self, src: str, dst: str):
        """Copy a key to another key in MongoDB."""
        if src == dst:
            return False

        if self.exists(dst):
            return False

        # Copy from each collection type
        copied = False

        # Copy hash
        hash_doc = self.hashes.find_one({"name": src})
        if hash_doc:
            new_doc = hash_doc.copy()
            new_doc["name"] = dst
            del new_doc["_id"]
            self.hashes.insert_one(new_doc)
            copied = True

        # Copy list
        list_docs = list(self.lists.find({"name": src}))
        if list_docs:
            for doc in list_docs:
                new_doc = doc.copy()
                new_doc["name"] = dst
                del new_doc["_id"]
                self.lists.insert_one(new_doc)
            copied = True

        # Copy set
        set_doc = self.sets.find_one({"name": src})
        if set_doc:
            new_doc = set_doc.copy()
            new_doc["name"] = dst
            del new_doc["_id"]
            self.sets.insert_one(new_doc)
            copied = True

        # Copy key
        key_doc = self.keys.find_one({"name": src})
        if key_doc:
            new_doc = key_doc.copy()
            new_doc["name"] = dst
            del new_doc["_id"]
            self.keys.insert_one(new_doc)
            copied = True

        return copied if self.exists(src) else False
