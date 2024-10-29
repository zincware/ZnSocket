import json
import typing as t
from collections.abc import MutableMapping, MutableSequence

import znjson

from znsocket.abc import (
    DictCallbackTypedDict,
    DictRepr,
    ListCallbackTypedDict,
    ListRepr,
    RefreshDataTypeDict,
    RefreshTypeDict,
)
from znsocket.client import Client


class ZnSocketObject:
    """Base class for all znsocket objects."""


def _encode(self, data: t.Any) -> str:
    if self.converter is not None:
        return json.dumps(data, cls=znjson.ZnEncoder.from_converters(self.converter))
    return json.dumps(data)


def _decode(self, data: str) -> t.Any:
    if self.converter is not None:
        return json.loads(data, cls=znjson.ZnDecoder.from_converters(self.converter))
    return json.loads(data)


class List(MutableSequence, ZnSocketObject):
    def __init__(
        self,
        r: Client | t.Any,
        key: str,
        socket: Client | None = None,
        callbacks: ListCallbackTypedDict | None = None,
        repr_type: ListRepr = "length",
        converter: list[t.Type[znjson.ConverterBase]] | None = None,
    ):
        """Synchronized list object.

        The content of this list is stored/read from the
        server. The data is not stored in this object at all.

        Parameters
        ----------
        r: znsocket.Client|redis.Redis
            Connection to the server.
        socket: znsocket.Client|None
            Socket connection for callbacks.
            If None, the connection from `r` will be used if it is a Client.
        key: str
            The key in the server to store the data from this list.
        callbacks: dict[str, Callable]
            optional function callbacks for methods
            which modify the database.
        repr_type: str
            Control the `repr` appearance of the object.
            Reduce for better performance.
        converter: list[znjson.ConverterBase]|None
            Optional list of znjson converters
            to use for encoding/decoding the data.

        """
        self.redis = r
        self.key = key
        self.repr_type = repr_type
        self.socket = socket if socket else (r if isinstance(r, Client) else None)
        self.converter = converter
        self._on_refresh = lambda x: None

        self._callbacks = {
            "setitem": None,
            "delitem": None,
            "insert": None,
            "append": None,
        }
        if callbacks:
            self._callbacks.update(callbacks)

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
            if value is None:
                item = None
            else:
                item = _decode(self, value)
            if item is None:
                raise IndexError("list index out of range")
            if isinstance(item, str):
                if item.startswith("znsocket.List:"):
                    key = item.split(":", 1)[1]
                    item = List(r=self.redis, key=key)
                elif item.startswith("znsocket.Dict:"):
                    key = item.split(":", 1)[1]
                    item = Dict(r=self.redis, key=key)
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
            if isinstance(v, Dict):
                v = f"znsocket.Dict:{v.key}"
            if isinstance(v, List):
                if value.key == self.key:
                    raise ValueError("Can not set circular reference to self")
                v = f"znsocket.List:{v.key}"
            self.redis.lset(self.key, i, _encode(self, v))

        if callback := self._callbacks["setitem"]:
            callback(index, value)
        if self.socket is not None:
            refresh: RefreshTypeDict = {"indices": index}
            refresh_data: RefreshDataTypeDict = {"target": self.key, "data": refresh}
            self.socket.sio.emit(f"refresh", refresh_data, namespace="/znsocket")

    def __delitem__(self, index: int | list | slice) -> None:
        single_item = isinstance(index, int)
        if single_item:
            index = [index]
        if isinstance(index, slice):
            index = list(range(*index.indices(len(self))))

        for i in index:
            self.redis.lset(self.key, i, "__DELETED__")
        self.redis.lrem(self.key, 0, "__DELETED__")

        if self._callbacks["delitem"]:
            self._callbacks["delitem"](index)

        if self.socket is not None:
            refresh: RefreshTypeDict = {"start": min(index), "stop": None}
            refresh_data: RefreshDataTypeDict = {"target": self.key, "data": refresh}
            self.socket.sio.emit(f"refresh", refresh_data, namespace="/znsocket")

    def insert(self, index: int, value: t.Any) -> None:
        if isinstance(value, Dict):
            value = f"znsocket.Dict:{value.key}"
        if isinstance(value, List):
            if value.key == self.key:
                raise ValueError("Can not set circular reference to self")
            value = f"znsocket.List:{value.key}"

        if index >= len(self):
            self.redis.rpush(self.key, _encode(self, value))
        elif index == 0:
            self.redis.lpush(self.key, _encode(self, value))
        else:
            pivot = self.redis.lindex(self.key, index)
            self.redis.linsert(self.key, "BEFORE", pivot, _encode(self, value))

        if callback := self._callbacks["insert"]:
            callback(index, value)

        if self.socket is not None:
            refresh: RefreshTypeDict = {"start": index, "stop": None}
            refresh_data: RefreshDataTypeDict = {"target": self.key, "data": refresh}
            self.socket.sio.emit(f"refresh", refresh_data, namespace="/znsocket")

    def __eq__(self, value: object) -> bool:
        if isinstance(value, List):
            return self[:] == value[:]
        elif isinstance(value, list):
            return self[:] == value
        return False

    def __repr__(self) -> str:
        if self.repr_type == "length":
            return f"List(len={len(self)})"
        elif self.repr_type == "minimal":
            return "List(<unknown>)"
        elif self.repr_type == "full":
            data = self.redis.lrange(self.key, 0, -1)
            data = [_decode(self, i) for i in data]

            return f"List({data})"
        else:
            raise ValueError(f"Invalid repr_type: {self.repr_type}")

    def append(self, value: t.Any) -> None:
        """Append an item to the end of the list.

        Override default method for better performance
        """
        if callback := self._callbacks["append"]:
            callback(value)
        if isinstance(value, Dict):
            value = f"znsocket.Dict:{value.key}"
        if isinstance(value, List):
            if value.key == self.key:
                raise ValueError("Can not set circular reference to self")
            value = f"znsocket.List:{value.key}"
        self.redis.rpush(self.key, _encode(self, value))
        if self.socket is not None:
            refresh: RefreshTypeDict = {"indices": [len(self) - 1]}
            refresh_data: RefreshDataTypeDict = {"target": self.key, "data": refresh}
            self.socket.sio.emit(f"refresh", refresh_data, namespace="/znsocket")

    def on_refresh(self, callback: t.Callable[[RefreshDataTypeDict], None]) -> None:
        if self.socket is None:
            raise ValueError("No socket connection available")

        self.socket.refresh_callbacks[self.key] = callback


class Dict(MutableMapping, ZnSocketObject):
    def __init__(
        self,
        r: Client | t.Any,
        key: str,
        socket: Client | None = None,
        callbacks: DictCallbackTypedDict | None = None,
        repr_type: DictRepr = "keys",
        converter: list[t.Type[znjson.ConverterBase]] | None = None,
    ):
        """Synchronized dict object.

        The content of this dict is stored/read from the
        server. The data is not stored in this object at all.

        Parameters
        ----------
        r: znsocket.Client|redis.Redis
            Connection to the server.
        socket: znsocket.Client|None
            Socket connection for callbacks.
            If None, the connection from `r` will be used if it is a Client.
        key: str
            The key in the server to store the data from this dict.
        callbacks: dict[str, Callable]
            optional function callbacks for methods
            which modify the database.
        repr_type: str
            Control the `repr` appearance of the object.
            Reduce for better performance.

        converter: list[znjson.ConverterBase]|None
            Optional list of znjson converters
            to use for encoding/decoding the data.
        """
        self.redis = r
        self.socket = socket if socket else (r if isinstance(r, Client) else None)
        self.converter = converter
        self.key = key
        self.repr_type = repr_type
        self._callbacks = {
            "setitem": None,
            "delitem": None,
        }
        if callbacks:
            self._callbacks.update(callbacks)

    def __getitem__(self, key: str | float | int) -> t.Any:
        value = self.redis.hget(self.key, json.dumps(key))
        if value is None:
            raise KeyError(key)  # TODO: items can not be None?
        value = _decode(self, value)
        if isinstance(value, str):
            if value.startswith("znsocket.List:"):
                key = value.split(":", 1)[1]
                value = List(r=self.redis, key=key)
            elif value.startswith("znsocket.Dict:"):
                key = value.split(":", 1)[1]
                value = Dict(r=self.redis, key=key)
        return value

    def __setitem__(self, key: str | float | int, value: t.Any) -> None:
        if isinstance(value, List):
            value = f"znsocket.List:{value.key}"
        if isinstance(value, Dict):
            if value.key == self.key:
                raise ValueError("Can not set circular reference to self")
            value = f"znsocket.Dict:{value.key}"
        self.redis.hset(self.key, _encode(self, key), _encode(self, value))
        if callback := self._callbacks["setitem"]:
            callback(key, value)
        if self.socket is not None:
            refresh: RefreshTypeDict = {"keys": [key]}
            refresh_data: RefreshDataTypeDict = {"target": self.key, "data": refresh}
            self.socket.sio.emit(f"refresh", refresh_data, namespace="/znsocket")

    def __delitem__(self, key: str) -> None:
        json_key = _encode(self, key)

        if not self.redis.hexists(self.key, json_key):
            raise KeyError(key)
        self.redis.hdel(self.key, json_key)
        if callback := self._callbacks["delitem"]:
            callback(key)
        if self.socket is not None:
            refresh: RefreshTypeDict = {"keys": [key]}
            refresh_data: RefreshDataTypeDict = {"target": self.key, "data": refresh}
            self.socket.sio.emit(f"refresh", refresh_data, namespace="/znsocket")

    def __iter__(self):
        return iter(self.keys())

    def __len__(self) -> int:
        return self.redis.hlen(self.key)

    def keys(self) -> list[t.Any]:
        return [_decode(self, k) for k in self.redis.hkeys(self.key)]

    def values(self) -> list[t.Any]:
        response = []
        for v in self.redis.hvals(self.key):
            response.append(_decode(self, v))
        return response

    def items(self) -> list[t.Tuple[t.Any, t.Any]]:
        response = []
        for k, v in self.redis.hgetall(self.key).items():
            response.append((_decode(self, k), _decode(self, v)))
        return response

    def __contains__(self, key: object) -> bool:
        return self.redis.hexists(self.key, _encode(self, key))

    def __repr__(self) -> str:
        if self.repr_type == "keys":
            return f"Dict(keys={self.keys()})"
        elif self.repr_type == "minimal":
            return "Dict(<unknown>)"
        elif self.repr_type == "full":
            data = {a: b for a, b in self.items()}
            return f"Dict({data})"
        else:
            raise ValueError(f"Invalid repr_type: {self.repr_type}")

    def __eq__(self, value: object) -> bool:
        if isinstance(value, Dict):
            return dict(self) == dict(value)
        elif isinstance(value, dict):
            return dict(self) == value
        return False

    def on_refresh(self, callback: t.Callable[[RefreshDataTypeDict], None]) -> None:
        if self.socket is None:
            raise ValueError("No socket connection available")

        self.socket.refresh_callbacks[self.key] = callback
