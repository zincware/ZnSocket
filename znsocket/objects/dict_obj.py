import typing as t
from collections.abc import MutableMapping

import redis.exceptions
import znjson

from znsocket.abc import (
    DictCallbackTypedDict,
    DictRepr,
    RefreshDataTypeDict,
    RefreshTypeDict,
    ZnSocketObject,
)
from znsocket.client import Client
from znsocket.utils import decode, encode

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
        from znsocket.objects.list_obj import List

        value = self.redis.hget(self.key, key)
        if value is None:
            raise KeyError(key)  # TODO: items can not be None?
        value = decode(self, value)
        if isinstance(value, str):
            if value.startswith("znsocket.List:"):
                key = value.split(":", 1)[1]
                value = List(r=self.redis, key=key)
            elif value.startswith("znsocket.Dict:"):
                key = value.split(":", 1)[1]
                value = Dict(r=self.redis, key=key, repr_type=self.repr_type)
        return value

    def __setitem__(self, key: str, value: t.Any) -> None:
        from znsocket.objects.list_obj import List

        if isinstance(value, List):
            value = f"znsocket.List:{value.key}"
        if isinstance(value, Dict):
            if value.key == self.key:
                raise ValueError("Can not set circular reference to self")
            value = f"znsocket.Dict:{value.key}"
        self.redis.hset(self.key, key, encode(self, value))
        if callback := self._callbacks["setitem"]:
            callback(key, value)
        if self.socket is not None:
            refresh: RefreshTypeDict = {"keys": [key]}
            refresh_data: RefreshDataTypeDict = {"target": self.key, "data": refresh}
            self.socket.sio.emit("refresh", refresh_data, namespace="/znsocket")

    def __delitem__(self, key: str) -> None:
        if not self.redis.hexists(self.key, key):
            raise KeyError(key)
        self.redis.hdel(self.key, key)
        if callback := self._callbacks["delitem"]:
            callback(key)
        if self.socket is not None:
            refresh: RefreshTypeDict = {"keys": [key]}
            refresh_data: RefreshDataTypeDict = {"target": self.key, "data": refresh}
            self.socket.sio.emit("refresh", refresh_data, namespace="/znsocket")

    def __iter__(self):
        return iter(self.keys())

    def __len__(self) -> int:
        return self.redis.hlen(self.key)

    def keys(self) -> list[str]:
        return self.redis.hkeys(self.key)

    def values(self) -> list[t.Any]:
        from znsocket.objects.list_obj import List

        response = []
        for v in self.redis.hvals(self.key):
            value = decode(self, v)
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
        from znsocket.objects.list_obj import List

        response = []
        for k, v in self.redis.hgetall(self.key).items():
            value = decode(self, v)
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
            data = dict(self.items())
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
        from znsocket.objects.list_obj import List

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
            pipeline.hset(self.key, key, encode(self, value))
        pipeline.execute()

        if self.socket is not None:
            refresh: RefreshTypeDict = {"keys": list(other.keys())}
            refresh_data: RefreshDataTypeDict = {"target": self.key, "data": refresh}
            self.socket.sio.emit("refresh", refresh_data, namespace="/znsocket")

    def __or__(self, value: "dict|Dict") -> dict:
        if isinstance(value, Dict):
            value = dict(value)
        return dict(self) | value
