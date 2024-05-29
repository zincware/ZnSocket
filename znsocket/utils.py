import json
import typing as t
from collections.abc import MutableSequence

import znjson

from .client import Client


class List(MutableSequence):
    def __init__(self, r: Client | t.Any, key: str):
        """Synchronized list object.

        The content of this list is stored/read from the
        server. The data is not stored in this object at all.
        For this, all data has to be JSON-serializeable.

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

    def __getitem__(self, index: int | list | slice):
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

    def __setitem__(self, index: int | list | slice, value: str | list[str]):
        single_item = isinstance(index, int)
        if single_item:
            index = [index]
            value = [value]

        if isinstance(index, slice):
            index = list(range(*index.indices(len(self))))

        index = [int(i) for i in index]

        if len(index) != len(value):
            raise ValueError(
                f"attempt to assign sequence of size {len(value)} to extended slice of size {len(index)}"
            )

        for i, v in zip(index, value):
            if i >= self.__len__() or i < -self.__len__():
                raise IndexError("list index out of range")
            self.redis.lset(self.key, i, znjson.dumps(v))

    def __delitem__(self, index: int | list | slice):
        single_item = isinstance(index, int)
        if single_item:
            index = [index]
        if isinstance(index, slice):
            index = list(range(*index.indices(len(self))))

        for i in index:
            self.redis.lset(self.key, i, "__DELETED__")
        self.redis.lrem(self.key, 0, "__DELETED__")

    def insert(self, index, value):
        if index >= self.__len__():
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

    def __repr__(self):
        data = self.redis.lrange(self.key, 0, -1)
        data = [znjson.loads(i) for i in data]

        return f"List({data})"
