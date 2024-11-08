import json
import typing as t
from collections.abc import MutableMapping, MutableSequence

import redis.exceptions
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
        try:
            return json.dumps(
                data,
                cls=znjson.ZnEncoder.from_converters(self.converter),
                allow_nan=False,
            )
        except ValueError:
            if self.convert_nan:
                value = json.dumps(
                    data,
                    cls=znjson.ZnEncoder.from_converters(self.converter),
                    allow_nan=True,
                )
                return (
                    value.replace("NaN", "null")
                    .replace("-Infinity", "null")
                    .replace("Infinity", "null")
                )
            raise

    try:
        return json.dumps(data, allow_nan=False)
    except ValueError:
        if self.convert_nan:
            value = json.dumps(data, allow_nan=True)
            return (
                value.replace("NaN", "null")
                .replace("-Infinity", "null")
                .replace("Infinity", "null")
            )
        raise


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
        max_commands_per_call: int = 1_000_000,
        convert_nan: bool = False,
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
        max_commands_per_call: int
            Maximum number of commands to send in a
            single call when using pipelines.
            Reduce for large list operations to avoid
            hitting the message size limit.
            Only applies when using `znsocket.Client`.
        convert_nan: bool
            Convert NaN and Infinity to None. Both are no native
            JSON values and can not be encoded/decoded.

        """
        self.redis = r
        self.key = key
        self.repr_type = repr_type
        self.socket = socket if socket else (r if isinstance(r, Client) else None)
        self.converter = converter
        self._on_refresh = lambda x: None
        self.convert_nan = convert_nan

        if isinstance(r, Client):
            self._pipeline_kwargs = {"max_commands_per_call": max_commands_per_call}
        else:
            self._pipeline_kwargs = {}

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

        pipeline = self.redis.pipeline(**self._pipeline_kwargs)
        for i in index:
            pipeline.lindex(self.key, i)
        data = pipeline.execute()

        items = []
        for value in data:
            if value is None:
                raise IndexError("list index out of range")

            item = _decode(self, value)
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

        pipeline = self.redis.pipeline(**self._pipeline_kwargs)
        for i, v in zip(index, value):
            if i >= LENGTH or i < -LENGTH:
                raise IndexError("list index out of range")
            if isinstance(v, Dict):
                v = f"znsocket.Dict:{v.key}"
            if isinstance(v, List):
                if value.key == self.key:
                    raise ValueError("Can not set circular reference to self")
                v = f"znsocket.List:{v.key}"
            pipeline.lset(self.key, i, _encode(self, v))
        pipeline.execute()

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

        if len(index) == 0:
            return  # nothing to delete

        pipeline = self.redis.pipeline(**self._pipeline_kwargs)
        for i in index:
            pipeline.lset(self.key, i, "__DELETED__")
        pipeline.lrem(self.key, 0, "__DELETED__")
        try:
            pipeline.execute()
        except redis.exceptions.ResponseError:
            raise IndexError("list index out of range")

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

    def extend(self, values: t.Iterable) -> None:
        """Extend the list with an iterable using redis pipelines."""
        if self.socket is not None:
            refresh: RefreshTypeDict = {"start": len(self), "stop": None}
        pipe = self.redis.pipeline(**self._pipeline_kwargs)
        for value in values:
            if isinstance(value, Dict):
                value = f"znsocket.Dict:{value.key}"
            if isinstance(value, List):
                if value.key == self.key:
                    raise ValueError("Can not set circular reference to self")
                value = f"znsocket.List:{value.key}"
            pipe.rpush(self.key, _encode(self, value))
        pipe.execute()

        if self.socket is not None:
            refresh_data: RefreshDataTypeDict = {"target": self.key, "data": refresh}
            self.socket.sio.emit(f"refresh", refresh_data, namespace="/znsocket")

    def pop(self, index: int = -1) -> t.Any:
        """Pop an item from the list."""
        if index < 0:
            index = len(self) + index

        value = self.redis.lindex(self.key, index)
        if value is None:
            raise IndexError("pop index out of range")

        pipeline = self.redis.pipeline(**self._pipeline_kwargs)
        pipeline.lset(self.key, index, "__DELETED__")
        pipeline.lrem(self.key, 0, "__DELETED__")
        try:
            pipeline.execute()
        except redis.exceptions.ResponseError:
            raise IndexError("pop index out of range")
        if self.socket is not None:
            refresh: RefreshTypeDict = {"start": index, "stop": None}
            refresh_data: RefreshDataTypeDict = {"target": self.key, "data": refresh}
            self.socket.sio.emit(f"refresh", refresh_data, namespace="/znsocket")
        return _decode(self, value)

    def copy(self, key: str) -> "List":
        """Copy the list to a new key.

        This will not trigger any callbacks as
        the data is not modified.
        """
        if not self.redis.copy(self.key, key):
            raise ValueError("Could not copy list")

        return List(r=self.redis, key=key, socket=self.socket)

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
        convert_nan: bool = False,
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
        repr_type: "keys"|"minimal"|"full"
            Control the `repr` appearance of the object.
            Reduce for better performance.
        converter: list[znjson.ConverterBase]|None
            Optional list of znjson converters
            to use for encoding/decoding the data.
        convert_nan: bool
            Convert NaN and Infinity to None. Both are no native
            JSON values and can not be encoded/decoded.
        """
        self.redis = r
        self.socket = socket if socket else (r if isinstance(r, Client) else None)
        self.converter = converter
        self.key = key
        self.repr_type = repr_type
        self.convert_nan = convert_nan
        self._callbacks = {
            "setitem": None,
            "delitem": None,
        }
        if callbacks:
            self._callbacks.update(callbacks)

    def __getitem__(self, key: str) -> t.Any:
        value = self.redis.hget(self.key, key)
        if value is None:
            raise KeyError(key)  # TODO: items can not be None?
        value = _decode(self, value)
        if isinstance(value, str):
            if value.startswith("znsocket.List:"):
                key = value.split(":", 1)[1]
                value = List(r=self.redis, key=key)
            elif value.startswith("znsocket.Dict:"):
                key = value.split(":", 1)[1]
                value = Dict(r=self.redis, key=key, repr_type=self.repr_type)
        return value

    def __setitem__(self, key: str, value: t.Any) -> None:
        if isinstance(value, List):
            value = f"znsocket.List:{value.key}"
        if isinstance(value, Dict):
            if value.key == self.key:
                raise ValueError("Can not set circular reference to self")
            value = f"znsocket.Dict:{value.key}"
        self.redis.hset(self.key, key, _encode(self, value))
        if callback := self._callbacks["setitem"]:
            callback(key, value)
        if self.socket is not None:
            refresh: RefreshTypeDict = {"keys": [key]}
            refresh_data: RefreshDataTypeDict = {"target": self.key, "data": refresh}
            self.socket.sio.emit(f"refresh", refresh_data, namespace="/znsocket")

    def __delitem__(self, key: str) -> None:
        if not self.redis.hexists(self.key, key):
            raise KeyError(key)
        self.redis.hdel(self.key, key)
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

    def keys(self) -> list[str]:
        return self.redis.hkeys(self.key)

    def values(self) -> list[t.Any]:
        response = []
        for v in self.redis.hvals(self.key):
            value = _decode(self, v)
            if isinstance(value, str):
                if value.startswith("znsocket.List:"):
                    key = value.split(":", 1)[1]
                    value = List(r=self.redis, key=key)
                elif value.startswith("znsocket.Dict:"):
                    key = value.split(":", 1)[1]
                    value = Dict(r=self.redis, key=key, repr_type=self.repr_type)
            response.append(value)
        return response

    def items(self) -> list[t.Tuple[str, t.Any]]:
        response = []
        for k, v in self.redis.hgetall(self.key).items():
            value = _decode(self, v)
            if isinstance(value, str):
                if value.startswith("znsocket.List:"):
                    key = value.split(":", 1)[1]
                    value = List(r=self.redis, key=key)
                elif value.startswith("znsocket.Dict:"):
                    key = value.split(":", 1)[1]
                    value = Dict(r=self.redis, key=key, repr_type=self.repr_type)

            response.append((k, value))
        return response

    def __contains__(self, key: str) -> bool:
        return self.redis.hexists(self.key, key)

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

    def copy(self, key: str) -> "Dict":
        """Copy the dict to a new key.

        This will not trigger any callbacks as
        the data is not modified.
        """
        if not self.redis.copy(self.key, key):
            raise ValueError("Could not copy dict")

        return Dict(r=self.redis, key=key, socket=self.socket)

    def on_refresh(self, callback: t.Callable[[RefreshDataTypeDict], None]) -> None:
        if self.socket is None:
            raise ValueError("No socket connection available")

        self.socket.refresh_callbacks[self.key] = callback

    def update(self, *args, **kwargs):
        """Update the dict with another dict or iterable."""
        if len(args) > 1:
            raise TypeError("update expected at most 1 argument, got %d" % len(args))
        if args:
            other = args[0]
            if isinstance(other, Dict):
                other = dict(other)
            elif isinstance(other, MutableMapping):
                pass
            else:
                raise TypeError(
                    "update expected at most 1 argument, got %d" % len(args)
                )
        else:
            other = kwargs

        pipeline = self.redis.pipeline()
        for key, value in other.items():
            if isinstance(value, Dict):
                if value.key == self.key:
                    raise ValueError("Can not set circular reference to self")
                value = f"znsocket.Dict:{value.key}"
            if isinstance(value, List):
                value = f"znsocket.List:{value.key}"
            pipeline.hset(self.key, key, _encode(self, value))
        pipeline.execute()

        if self.socket is not None:
            refresh: RefreshTypeDict = {"keys": list(other.keys())}
            refresh_data: RefreshDataTypeDict = {"target": self.key, "data": refresh}
            self.socket.sio.emit(f"refresh", refresh_data, namespace="/znsocket")

    def __or__(self, value: "dict|Dict") -> dict:
        if isinstance(value, Dict):
            value = dict(value)
        return dict(self) | value
