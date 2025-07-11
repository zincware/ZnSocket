import typing as t
from collections.abc import MutableSequence

import redis.exceptions
import znjson

from znsocket.abc import (
    ListCallbackTypedDict,
    ListRepr,
    RefreshDataTypeDict,
    RefreshTypeDict,
    ZnSocketObject,
)
from znsocket.client import Client
from znsocket.exceptions import FrozenStorageError
from znsocket.utils import decode, encode, handle_error


# TODO: cache for self.key
def _used_fallback(self: "List") -> bool:
    result = int(self.redis.llen(self.key))
    # I don't know
    # if result == 0 and self._adapter_available:
    #     result = int(
    #         self.socket.call("adapter:get", key=self.key, method="__len__")
    #     )
    if result == 0 and self.fallback is not None:
        return True
    return False


class List(MutableSequence, ZnSocketObject):
    def __init__(
        self,
        r: Client | t.Any,
        key: str,
        socket: Client | None = None,
        callbacks: ListCallbackTypedDict | None = None,
        repr_type: ListRepr = "length",
        converter: list[t.Type[znjson.ConverterBase]] | None = None,
        convert_nan: bool = False,
        fallback: str | None = None,
        fallback_policy: t.Literal["copy", "frozen"] | None = None,
    ):
        """Synchronized list object.

        The content of this list is stored/read from the server. The data is not stored
        in this object at all, making it suitable for distributed applications and
        real-time synchronization.

        Parameters
        ----------
        r : znsocket.Client or redis.Redis
            Connection to the server.
        key : str
            The key in the server to store the data from this list.
        socket : znsocket.Client, optional
            Socket connection for callbacks. If None, the connection from `r` will be
            used if it is a Client.
        callbacks : dict[str, Callable], optional
            Optional function callbacks for methods which modify the database.
        repr_type : {'length', 'minimal', 'full'}, optional
            Control the `repr` appearance of the object. Reduce for better performance.
            Default is 'length'.
        converter : list[znjson.ConverterBase], optional
            Optional list of znjson converters to use for encoding/decoding the data.
        convert_nan : bool, optional
            Convert NaN and Infinity to None. Both are not native JSON values and
            cannot be encoded/decoded. Default is False.
        fallback : str, optional
            The key of a fallback list to use if this list is empty.
        fallback_policy : {'copy', 'frozen'}, optional
            The policy to use for the fallback list.
            'copy': Copy the fallback list to this list on initialization.
            'frozen': Use the fallback list as a read-only source of data.

        Examples
        --------
        >>> client = znsocket.Client("http://localhost:5000")
        >>> my_list = znsocket.List(client, "my_list")
        >>> my_list.append("item1")
        >>> my_list.append("item2")
        >>> len(my_list)
        2
        >>> my_list[0]
        'item1'
        """
        self.redis = r
        self._key = key
        self.repr_type = repr_type
        self.socket = socket if socket else (r if isinstance(r, Client) else None)
        self.converter = converter
        self._on_refresh = lambda x: None
        self.convert_nan = convert_nan
        self.fallback = fallback
        self.fallback_policy = fallback_policy

        if isinstance(r, Client):
            self._pipeline_kwargs = {}
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

        self._adapter_available = False
        if self.socket is not None:
            # check from the server if the adapter is available
            self._adapter_available = self.socket.call("check_adapter", key=self.key)

        # If fallback policy is "copy" and the list is empty, copy from fallback
        if (
            self.fallback is not None
            and self.fallback_policy == "copy"
            and int(self.redis.llen(self.key)) == 0
            and not self._adapter_available
        ):
            fallback_lst = type(self)(
                r=self.redis,
                key=self.fallback,
                socket=self.socket,
                convert_nan=self.convert_nan,
                converter=self.converter,
            )
            if len(fallback_lst) > 0:
                # here we use the internal key, because we create a new list object.
                fallback_lst.copy(self._key)

    @property
    def key(self) -> str:
        """The key of the list in the server.

        Returns
        -------
        str
            The prefixed key used to store this list in the server.
        """
        return f"znsocket.List:{self._key}"

    def __len__(self) -> int:
        result = int(self.redis.llen(self.key))
        if result == 0 and self._adapter_available:
            result = int(
                self.socket.call("adapter:get", key=self.key, method="__len__")
            )
        if (
            result == 0
            and self.fallback is not None
            and self.fallback_policy is not None
            and self.fallback_policy != "copy"
        ):
            # Use fallback for length if policy is "frozen" or other non-copy policies
            # If policy is "copy", data should have been copied during initialization
            fallback_lst = type(self)(
                r=self.redis,
                key=self.fallback,
                socket=self.socket,
                convert_nan=self.convert_nan,
            )
            result = len(fallback_lst)

        return result

    def __getitem__(self, index: int | list | slice) -> t.Any | list[t.Any]:  # noqa C901
        from znsocket.objects.dict_obj import Dict

        single_item = isinstance(index, int)
        original_slice = isinstance(index, slice)

        if single_item:
            index = [index]
        elif original_slice:
            # If we have an adapter and it's a slice, use the efficient slice method
            if self._adapter_available:
                start, stop, step = index.indices(len(self))
                adapter_values = self.socket.call(
                    "adapter:get",
                    key=self.key,
                    method="slice",
                    start=start,
                    stop=stop,
                    step=step,
                )
                items = []
                for value in adapter_values:
                    item = decode(self, value)
                    if isinstance(item, str):
                        if item.startswith("znsocket.List:"):
                            key = item.split(":", 1)[1]
                            item = List(r=self.redis, key=key)
                        elif item.startswith("znsocket.Dict:"):
                            key = item.split(":", 1)[1]
                            item = Dict(r=self.redis, key=key)
                    items.append(item)
                return items
            else:
                # Fallback to individual index calls for non-adapter slices
                index = list(range(*index.indices(len(self))))

        pipeline = self.redis.pipeline(**self._pipeline_kwargs)
        for i in index:
            pipeline.lindex(self.key, i)
        data = pipeline.execute()

        items = []
        for idx, value in zip(index, data):
            if value is None:
                if self._adapter_available:
                    value = self.socket.call(
                        "adapter:get",
                        key=self.key,
                        method="__getitem__",
                        index=idx,
                    )
                if (
                    value is None
                    and self.fallback is not None
                    and self.fallback_policy != "copy"
                ):
                    # Only use fallback for item access if policy is not "copy"
                    # If policy is "copy", data should have been copied during initialization
                    fallback_lst = type(self)(
                        r=self.redis,
                        key=self.fallback,
                        socket=self.socket,
                        convert_nan=self.convert_nan,
                        converter=self.converter,
                    )
                    try:
                        value = fallback_lst[idx]
                        value = encode(self, value)
                    except IndexError:
                        # Fallback doesn't have this index either
                        pass

            if value is None:  # check if the value is still None
                raise IndexError("list index out of range")

            item = decode(self, value)
            if isinstance(item, str):
                if item.startswith("znsocket.List:"):
                    key = item.split(":", 1)[1]
                    item = List(r=self.redis, key=key)
                elif item.startswith("znsocket.Dict:"):
                    key = item.split(":", 1)[1]
                    item = Dict(r=self.redis, key=key)
            items.append(item)
        return items[0] if single_item else items

    def __setitem__(self, index: int | list | slice, value: t.Any) -> None:  # noqa C901
        from znsocket.objects.dict_obj import Dict

        if self._adapter_available:
            raise FrozenStorageError(key=self.key)

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
                v = v.key
            if isinstance(v, List):
                if value.key == self.key:
                    raise ValueError("Can not set circular reference to self")
                v = v.key
            pipeline.lset(self.key, i, encode(self, v))
        pipeline.execute()

        if callback := self._callbacks["setitem"]:
            callback(index, value)
        if self.socket is not None:
            refresh: RefreshTypeDict = {"indices": index}
            refresh_data: RefreshDataTypeDict = {"target": self.key, "data": refresh}
            self.socket.sio.emit("refresh", refresh_data, namespace="/znsocket")

    def __delitem__(self, index: int | list | slice) -> None:
        if self._adapter_available:
            raise FrozenStorageError(key=self.key)
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
            self.socket.sio.emit("refresh", refresh_data, namespace="/znsocket")

    def insert(self, index: int, value: t.Any) -> None:
        from znsocket.objects.dict_obj import Dict

        if self._adapter_available:
            raise FrozenStorageError(key=self.key)

        if isinstance(value, Dict):
            value = value.key
        if isinstance(value, List):
            if value.key == self.key:
                raise ValueError("Can not set circular reference to self")
            value = value.key

        if index >= len(self):
            self.redis.rpush(self.key, encode(self, value))
        elif index == 0:
            self.redis.lpush(self.key, encode(self, value))
        else:
            pivot = self.redis.lindex(self.key, index)
            self.redis.linsert(self.key, "BEFORE", pivot, encode(self, value))

        if callback := self._callbacks["insert"]:
            callback(index, value)

        if self.socket is not None:
            refresh: RefreshTypeDict = {"start": index, "stop": None}
            refresh_data: RefreshDataTypeDict = {"target": self.key, "data": refresh}
            self.socket.sio.emit("refresh", refresh_data, namespace="/znsocket")

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
            data = [decode(self, i) for i in data]

            return f"List({data})"
        else:
            raise ValueError(f"Invalid repr_type: {self.repr_type}")

    def append(self, value: t.Any) -> None:
        """Append an item to the end of the list.

        Parameters
        ----------
        value : Any
            The item to append to the list.

        Raises
        ------
        FrozenStorageError
            If the list is backed by an adapter and is read-only.
        ValueError
            If attempting to create a circular reference to self.

        Examples
        --------
        >>> my_list = znsocket.List(client, "my_list")
        >>> my_list.append("new_item")
        >>> my_list[-1]
        'new_item'
        """
        from znsocket.objects.dict_obj import Dict

        if self._adapter_available:
            raise FrozenStorageError(key=self.key)
        # check if the list has a fallback option and it would go to the fallback list
        # For frozen policy, we need to copy fallback data before modifying if list is empty
        if (
            self.fallback is not None
            and self.fallback_policy is not None
            and int(self.redis.llen(self.key)) == 0
            and not self._adapter_available
        ):
            fallback_lst = type(self)(
                r=self.redis,
                key=self.fallback,
                socket=self.socket,
                convert_nan=self.convert_nan,
                converter=self.converter,
            )
            if len(fallback_lst) > 0:
                # here we use the internal key, because we create a new list object.
                fallback_lst.copy(self._key)

        if callback := self._callbacks["append"]:
            callback(value)
        if isinstance(value, Dict):
            value = value.key
        if isinstance(value, List):
            if value.key == self.key:
                raise ValueError("Can not set circular reference to self")
            value = value.key
        self.redis.rpush(self.key, encode(self, value))
        if self.socket is not None:
            refresh: RefreshTypeDict = {"indices": [len(self) - 1]}
            refresh_data: RefreshDataTypeDict = {"target": self.key, "data": refresh}
            self.socket.sio.emit("refresh", refresh_data, namespace="/znsocket")

    def extend(self, values: t.Iterable) -> None:
        """Extend the list with an iterable using redis pipelines."""
        from znsocket.objects.dict_obj import Dict

        if self._adapter_available:
            raise FrozenStorageError(key=self.key)

        if self.socket is not None:
            refresh: RefreshTypeDict = {"start": len(self), "stop": None}
        pipe = self.redis.pipeline(**self._pipeline_kwargs)
        for value in values:
            if isinstance(value, Dict):
                value = value.key
            if isinstance(value, List):
                if value.key == self.key:
                    raise ValueError("Can not set circular reference to self")
                value = value.key
            pipe.rpush(self.key, encode(self, value))
        pipe.execute()

        if self.socket is not None:
            refresh_data: RefreshDataTypeDict = {"target": self.key, "data": refresh}
            self.socket.sio.emit("refresh", refresh_data, namespace="/znsocket")

    def pop(self, index: int = -1) -> t.Any:
        """Pop an item from the list."""
        if self._adapter_available:
            raise FrozenStorageError(key=self.key)
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
            self.socket.sio.emit("refresh", refresh_data, namespace="/znsocket")
        return decode(self, value)

    def copy(self, key: str) -> "List":
        """Copy the list to a new key.

        Creates a new list with the same content but under a different key.
        This operation does not trigger any callbacks as the original data
        is not modified.

        Parameters
        ----------
        key : str
            The new key for the copied list.

        Returns
        -------
        List
            A new List instance pointing to the copied data.

        Raises
        ------
        ValueError
            If the copy operation fails.

        Examples
        --------
        >>> original_list = znsocket.List(client, "original")
        >>> original_list.extend([1, 2, 3])
        >>> copied_list = original_list.copy("copied")
        >>> len(copied_list)
        3
        """
        # TODO!! currently, it is not possible to do copy(list.key)
        # because it will be prefixed with "znsocket.List:" twice!
        # The same is true for the Dict object.

        if self._adapter_available:
            success = self.socket.call(
                "adapter:get",
                method="copy",
                key=self.key,
                target=key,
            )
            handle_error(success)
        elif not self.redis.copy(self.key, f"znsocket.List:{key}"):
            raise ValueError("Could not copy list")

        return List(
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
