import typing as t
from collections.abc import MutableMapping, MutableSequence

import znjson

from .client import Client


class ZnSocketObject:
    """Base class for all znsocket objects."""


class List(MutableSequence, ZnSocketObject):
    def __init__(self, r: Client | t.Any, key: str):
        """Synchronized list object.

        The content of this list is stored/read from the
        server. The data is not stored in this object at all.

        Parameters
        ----------
        r: znsocket.Client|redis.Redis
            Connection to the server.
        key: str
            The key in the server to store the data from this list.

        Limitations
        -----------
        - currently this list will convert int/float to str datatypes
        """
        self.redis = r
        self.key = key

    def __len__(self) -> int:
        return int(self.redis.llen(self.key))

    def __getitem__(self, index: int | list | slice) -> t.Any | list[t.Any]:
        single_item = isinstance(index, int)
        if single_item:
            index = [index]
        if isinstance(index, slice):
            index = list(range(*index.indices(len(self))))

        items = []
        for i in index:
            value = self.redis.lindex(self.key, i)
            try:
                item = znjson.loads(value)
            except TypeError:
                item = value
            if item is None:
                raise IndexError("list index out of range")
            items.append(item)
        return items[0] if single_item else items

    def __setitem__(self, index: int | list | slice, value: t.Any) -> None:
        single_item = isinstance(index, int)
        if single_item:
            index = [index]
            value = [value]

        LENGTH = len(self)

        if isinstance(index, slice):
            index = list(range(*index.indices(LENGTH)))

        if any(not isinstance(i, int) for i in index):
            raise TypeError("list indices must be integers or slices")

        if len(index) != len(value):
            raise ValueError(
                f"attempt to assign sequence of size {len(value)} to extended slice of size {len(index)}"
            )

        for i, v in zip(index, value):
            if i >= LENGTH or i < -LENGTH:
                raise IndexError("list index out of range")
            self.redis.lset(self.key, i, znjson.dumps(v))

    def __delitem__(self, index: int | list | slice) -> None:
        single_item = isinstance(index, int)
        if single_item:
            index = [index]
        if isinstance(index, slice):
            index = list(range(*index.indices(len(self))))

        for i in index:
            self.redis.lset(self.key, i, "__DELETED__")
        self.redis.lrem(self.key, 0, "__DELETED__")

    def insert(self, index: int, value: t.Any) -> None:
        if index >= len(self):
            self.redis.rpush(self.key, znjson.dumps(value))
        elif index == 0:
            self.redis.lpush(self.key, znjson.dumps(value))
        else:
            pivot = self.redis.lindex(self.key, index)
            self.redis.linsert(self.key, "BEFORE", pivot, znjson.dumps(value))

    def __eq__(self, value: object) -> bool:
        if isinstance(value, List):
            return self[:] == value[:]
        elif isinstance(value, list):
            return self[:] == value
        return False

    def __repr__(self) -> str:
        data = self.redis.lrange(self.key, 0, -1)
        data = [znjson.loads(i) for i in data]

        return f"List({data})"

    def append(self, value: t.Any) -> None:
        """Append an item to the end of the list.

        Override default method for better performance
        """
        self.redis.rpush(self.key, znjson.dumps(value))


class Dict(MutableMapping, ZnSocketObject):
    def __init__(self, r: Client | t.Any, key: str):
        """Synchronized dict object.

        The content of this dict is stored/read from the
        server. The data is not stored in this object at all.

        Parameters
        ----------
        r: znsocket.Client|redis.Redis
            Connection to the server.
        key: str
            The key in the server to store the data from this dict.
        """
        self.redis = r
        self.key = key

    def __getitem__(self, key: str) -> t.Any:
        value = self.redis.hget(self.key, znjson.dumps(key))
        if value is None:
            raise KeyError(key)
        return znjson.loads(value)

    def __setitem__(self, key: str, value: t.Any) -> None:
        self.redis.hset(self.key, znjson.dumps(key), znjson.dumps(value))

    def __delitem__(self, key: str) -> None:
        if not self.redis.hexists(self.key, znjson.dumps(key)):
            raise KeyError(key)
        self.redis.hdel(self.key, znjson.dumps(key))

    def __iter__(self):
        return iter(self.keys())

    def __len__(self) -> int:
        return self.redis.hlen(self.key)

    def keys(self) -> list[t.Any]:
        return [znjson.loads(k) for k in self.redis.hkeys(self.key)]

    def values(self) -> list[t.Any]:
        response = []
        for v in self.redis.hvals(self.key):
            response.append(znjson.loads(v))
        return response

    def items(self) -> list[t.Tuple[t.Any, t.Any]]:
        response = []
        for k, v in self.redis.hgetall(self.key).items():
            response.append((znjson.loads(k), znjson.loads(v)))
        return response

    def __contains__(self, key: object) -> bool:
        return self.redis.hexists(self.key, znjson.dumps(key))

    def __repr__(self) -> str:
        data = {a: b for a, b in self.items()}
        return f"Dict({data})"
