import typing as t
from collections.abc import MutableMapping

import znjson

from znsocket.abc import (
    DictCallbackTypedDict,
    DictRepr,
    RefreshDataTypeDict,
    RefreshTypeDict,
    ZnSocketObject,
)
from znsocket.client import Client
from znsocket.utils import decode, encode, handle_error


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
        fallback: str | None = None,
        fallback_policy: t.Literal["copy", "frozen"] | None = None,
    ):
        """Synchronized dict object.

        The content of this dict is stored/read from the server. The data is not stored
        in this object at all, making it suitable for distributed applications and
        real-time synchronization.

        Parameters
        ----------
        r : znsocket.Client or redis.Redis
            Connection to the server.
        key : str
            The key in the server to store the data from this dict.
        socket : znsocket.Client, optional
            Socket connection for callbacks. If None, the connection from `r` will be
            used if it is a Client.
        callbacks : dict[str, Callable], optional
            Optional function callbacks for methods which modify the database.
        repr_type : {'keys', 'minimal', 'full'}, optional
            Control the `repr` appearance of the object. Reduce for better performance.
            Default is 'keys'.
        converter : list[znjson.ConverterBase], optional
            Optional list of znjson converters to use for encoding/decoding the data.
        convert_nan : bool, optional
            Convert NaN and Infinity to None. Both are not native JSON values and
            cannot be encoded/decoded. Default is False.
        fallback : str, optional
            The key of a fallback dict to use if this dict is empty.
        fallback_policy : {'copy', 'frozen'}, optional
            The policy to use for the fallback dict.
            'copy': Copy the fallback dict to this dict on initialization.
            'frozen': Use the fallback dict as a read-only source of data.
        fallback : str, optional
            The key of a fallback dict to use if this dict is empty.
        fallback_policy : {'copy', 'frozen'}, optional
            The policy to use for the fallback dict.
            'copy': Copy the fallback dict to this dict on initialization.
            'frozen': Use the fallback dict as a read-only source of data.

        Examples
        --------
        >>> client = znsocket.Client("http://localhost:5000")
        >>> my_dict = znsocket.Dict(client, "my_dict")
        >>> my_dict["key1"] = "value1"
        >>> my_dict["key2"] = "value2"
        >>> len(my_dict)
        2
        >>> my_dict["key1"]
        'value1'
        """
        self.redis = r
        self.socket = socket if socket else (r if isinstance(r, Client) else None)
        self.converter = converter
        self._key = key
        self.repr_type = repr_type
        self.convert_nan = convert_nan
        self.fallback = fallback
        self.fallback_policy = fallback_policy

        self._callbacks = {
            "setitem": None,
            "delitem": None,
        }
        if callbacks:
            self._callbacks.update(callbacks)
        self._adapter_available = False
        if self.socket is not None:
            # check from the server if the adapter is available
            self._adapter_available = self.socket.call("check_adapter", key=self.key)

        self._fallback_object: t.Optional["Dict"] = None

        if (
            self.fallback is not None
            and int(self.redis.hlen(self.key)) == 0
            and not self._adapter_available  # TODO: what should happen, if adapter is available but empty
        ):
            self._fallback_object = type(self)(
                r=self.redis,
                key=self.fallback,
                socket=self.socket,
                convert_nan=self.convert_nan,
                converter=self.converter,
                repr_type=self.repr_type,
            )
            if len(self._fallback_object) > 0 and self.fallback_policy == "copy":
                self._fallback_object.copy(self._key)

    @property
    def key(self) -> str:
        """The key in the server to store the data from this dict.

        Returns
        -------
        str
            The prefixed key used to store this dict in the server.
        """
        return f"znsocket.Dict:{self._key}"

    def __getitem__(self, key: str) -> t.Any:
        from znsocket.objects.list_obj import List

        value = self.redis.hget(self.key, key)
        if value is None:
            if self._adapter_available:
                value = self.socket.call(
                    "adapter:get", key=self.key, method="__getitem__", dict_key=key
                )
            if value is None and self._fallback_object is not None:
                return self._fallback_object.get(key, None)
            if value is None:
                raise KeyError(key)
        value = decode(self, value)
        if isinstance(value, str):
            if value.startswith("znsocket.List:"):
                ref_key = value.split(":", 1)[1]
                value = List(r=self.redis, key=ref_key)
            elif value.startswith("znsocket.Dict:"):
                ref_key = value.split(":", 1)[1]
                value = Dict(r=self.redis, key=ref_key, repr_type=self.repr_type)
        return value

    def __setitem__(self, key: str, value: t.Any) -> None:
        from znsocket.objects.list_obj import List

        if self._adapter_available:
            from znsocket.exceptions import FrozenStorageError

            raise FrozenStorageError(key=self.key)

        if isinstance(value, List):
            value = value.key
        if isinstance(value, Dict):
            if value.key == self.key:
                raise ValueError("Can not set circular reference to self")
            value = value.key
        self.redis.hset(self.key, key, encode(self, value))
        if callback := self._callbacks["setitem"]:
            callback(key, value)
        if self.socket is not None:
            refresh: RefreshTypeDict = {"keys": [key]}
            refresh_data: RefreshDataTypeDict = {"target": self.key, "data": refresh}
            self.socket.sio.emit("refresh", refresh_data, namespace="/znsocket")

    def __delitem__(self, key: str) -> None:
        if self._adapter_available:
            from znsocket.exceptions import FrozenStorageError

            raise FrozenStorageError(key=self.key)

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
        result = self.redis.hlen(self.key)
        if result == 0 and self._adapter_available:
            result = int(
                self.socket.call("adapter:get", key=self.key, method="__len__")
            )
        if result == 0 and self._fallback_object is not None:
            result = len(self._fallback_object)
        return result

    def keys(self) -> list[str]:
        result = self.redis.hkeys(self.key)
        if len(result) == 0 and self._adapter_available:
            result = self.socket.call("adapter:get", key=self.key, method="keys")
        if len(result) == 0 and self._fallback_object is not None:
            result = self._fallback_object.keys()
        return result

    def values(self) -> list[t.Any]:  # noqa: C901
        vals = self.redis.hvals(self.key)
        if len(vals) == 0 and self._adapter_available:
            adapter_values = self.socket.call(
                "adapter:get", key=self.key, method="values"
            )
            response = []
            for value in adapter_values:
                value = decode(self, value)
                if isinstance(value, str):
                    if value.startswith("znsocket.List:"):
                        from znsocket import List

                        ref_key = value.split(":", 1)[1]
                        value = List(r=self.redis, key=ref_key)
                    elif value.startswith("znsocket.Dict:"):
                        ref_key = value.split(":", 1)[1]
                        value = Dict(
                            r=self.redis, key=ref_key, repr_type=self.repr_type
                        )
                response.append(value)
            return response
        if len(vals) == 0 and self._fallback_object is not None:
            return self._fallback_object.values()

        response = []
        for v in vals:
            value = decode(self, v)
            if isinstance(value, str):
                if value.startswith("znsocket.List:"):
                    from znsocket import List

                    ref_key = value.split(":", 1)[1]
                    value = List(r=self.redis, key=ref_key)
                elif value.startswith("znsocket.Dict:"):
                    ref_key = value.split(":", 1)[1]
                    value = Dict(r=self.redis, key=ref_key, repr_type=self.repr_type)
            response.append(value)
        return response

    def items(self) -> list[t.Tuple[str, t.Any]]:  # noqa: C901
        from znsocket.objects.list_obj import List

        all_items = self.redis.hgetall(self.key)
        if len(all_items) == 0 and self._adapter_available:
            adapter_items = self.socket.call(
                "adapter:get", key=self.key, method="items"
            )
            response = []
            for k, v in adapter_items:
                value = decode(self, v)
                if isinstance(value, str):
                    if value.startswith("znsocket.List:"):
                        ref_key = value.split(":", 1)[1]
                        value = List(r=self.redis, key=ref_key)
                    elif value.startswith("znsocket.Dict:"):
                        ref_key = value.split(":", 1)[1]
                        value = Dict(
                            r=self.redis, key=ref_key, repr_type=self.repr_type
                        )
                response.append((k, value))
            return response
        if len(all_items) == 0 and self._fallback_object is not None:
            return self._fallback_object.items()

        response = []
        for k, v in all_items.items():
            value = decode(self, v)
            if isinstance(value, str):
                if value.startswith("znsocket.List:"):
                    ref_key = value.split(":", 1)[1]
                    value = List(r=self.redis, key=ref_key)
                elif value.startswith("znsocket.Dict:"):
                    ref_key = value.split(":", 1)[1]
                    value = Dict(r=self.redis, key=ref_key, repr_type=self.repr_type)

            response.append((k, value))
        return response

    def __contains__(self, key: str) -> bool:
        result = self.redis.hexists(self.key, key)
        if not result and self._adapter_available:
            result = self.socket.call(
                "adapter:get", key=self.key, method="__contains__", dict_key=key
            )
        if not result and self._fallback_object is not None:
            result = key in self._fallback_object
        return result

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
        if self._adapter_available:
            result = self.socket.call(
                "adapter:get", key=self.key, method="copy", target=key
            )
            handle_error(result)
            return Dict(
                r=self.redis,
                key=key,
                socket=self.socket,
                converter=self.converter,
                convert_nan=self.convert_nan,
            )
        else:
            if not self.redis.copy(self.key, f"znsocket.Dict:{key}"):
                raise ValueError("Could not copy dict")
            return Dict(
                r=self.redis,
                key=key,
                socket=self.socket,
                converter=self.converter,
                convert_nan=self.convert_nan,
            )

    def on_refresh(self, callback: t.Callable[[RefreshDataTypeDict], None]) -> None:
        if self.socket is None:
            raise ValueError("No socket connection available")

        self.socket.refresh_callbacks[self.key] = callback

    def update(self, *args, **kwargs):  # noqa: C901
        """Update the dict with another dict or iterable."""
        from znsocket.objects.list_obj import List

        if self._adapter_available:
            from znsocket.exceptions import FrozenStorageError

            raise FrozenStorageError(key=self.key)

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
                value = value.key
            if isinstance(value, List):
                value = value.key
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
