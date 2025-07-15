import json
import logging
from collections.abc import Sequence
from dataclasses import dataclass
from typing import Any, Optional, Protocol

from znsocket.client import Client
from znsocket.utils import encode, handle_error

log = logging.getLogger(__name__)


class ItemTransformCallback(Protocol):
    """Protocol for item transformation callbacks.

    This callback is called when a list item is accessed and can return either
    a raw value or construct and return an adapter directly.
    """

    def __call__(
        self,
        item: Any,
        key: str,
        socket: Client,
        converter: list[type] | None = None,
        convert_nan: bool = False,
    ) -> Any:
        """Transform a list item.

        Parameters
        ----------
        item : Any
            The original item from the sequence.
        key : str
            The suggested key to use for adapter creation (format: "{list_key}:{index}").
            Using other keys will cause conflicts.
        socket : Client
            The socket client connection.
        converter : list[type] | None
            Optional converters to pass to the created adapter.
        convert_nan : bool
            Whether to convert NaN values.

        Returns
        -------
        Any
            Either a raw value (which will be encoded directly) or an adapter
            instance (which will have its key returned).

        Examples
        --------
        >>> def ase_transform(item, key, socket, converter=None, convert_nan=False):
        ...     from zndraw.converter import ASEConverter
        ...     import znsocket
        ...     transformed = ASEConverter().encode(item)
        ...     return znsocket.DictAdapter(key, socket, transformed, converter, convert_nan)

        >>> def simple_transform(item, key, socket, converter=None, convert_nan=False):
        ...     return item * 2  # Return raw value directly
        """
        ...


@dataclass
class ListAdapter:
    """Connect any object to a znsocket server to be used instead of loading data from the database.

    The ListAdapter allows you to expose any sequence-like object through the znsocket
    server, making it accessible to clients as if it were a regular List. Data is
    transmitted via sockets through the server to the client on demand.

    Parameters
    ----------
    key : str
        The key identifier for this adapter in the server.
    socket : Client
        The znsocket client connection to use for communication.
    object : Sequence
        The sequence object to expose through the adapter.
    item_transform_callback : ItemTransformCallback, optional
        Optional callback to transform list items when accessed. If provided,
        the callback will be called for each item access and can return transformed
        data along with the adapter type to create ("Dict", "List", or None).
    converter : list[type], optional
        Optional list of znjson converters to use for encoding/decoding the data.
    convert_nan : bool, optional
        Convert NaN and Infinity to None. Both are not native JSON values and
        cannot be encoded/decoded. Default is False.
    r : Client, optional
        Alternative client connection. If None, uses the socket connection.

    Examples
    --------
    >>> client = znsocket.Client("http://localhost:5000")
    >>> my_data = [1, 2, 3, 4, 5]
    >>> adapter = znsocket.ListAdapter("my_adapter", client, my_data)
    >>> # Now clients can access my_data as znsocket.List(client, "my_adapter")

    >>> # With custom transformation callback
    >>> def ase_transform(item, key, socket, converter=None, convert_nan=False):
    ...     from zndraw.converter import ASEConverter
    ...     import znsocket
    ...     transformed = ASEConverter().encode(item)
    ...     return znsocket.DictAdapter(key, socket, transformed, converter, convert_nan)
    >>> ase_data = [atoms1, atoms2, atoms3]  # ASE Atoms objects
    >>> adapter = znsocket.ListAdapter("ase_adapter", client, ase_data, ase_transform)

    >>> # Accessing items creates adapters automatically
    >>> client_list = znsocket.List(client, "ase_adapter")
    >>> dict_item = client_list[0]  # Creates DictAdapter with key "ase_adapter:0"
    >>> isinstance(dict_item, znsocket.Dict)
    True
    >>> dict_item["some_key"]  # Access the transformed data
    """

    key: str
    socket: Client
    object: Sequence
    item_transform_callback: Optional[ItemTransformCallback] = None
    converter: list[type] | None = None
    convert_nan: bool = False
    r: Client | None = None

    def __post_init__(self):
        self.key = f"znsocket.List:{self.key}"

        result = self.socket.call("register_adapter", key=self.key)
        handle_error(result)

        self.socket.register_adapter_callback(self.key, self.map_callback)
        if self.r is None:
            self.r = self.socket

    def _handle_transform_callback(self, value: Any, index: int) -> str:
        """Handle item transformation callback with pre-check for existing adapters."""
        from znsocket import Dict, DictAdapter, List, Segments
        # TODO: use a znsocket base class mixin

        suggested_key = f"{self.key}:{index}"

        # Check if an adapter with this key already exists
        # We need to check both Dict and List adapter patterns
        dict_key = f"znsocket.Dict:{suggested_key}"
        list_key = f"znsocket.List:{suggested_key}"

        if self.socket.call("adapter_exists", key=dict_key):
            return json.dumps(dict_key)
        elif self.socket.call("adapter_exists", key=list_key):
            return json.dumps(list_key)

        # Assert that callback is not None (this method is only called when callback is not None)
        assert self.item_transform_callback is not None

        try:
            result = self.item_transform_callback(
                item=value,
                key=suggested_key,
                socket=self.socket,
                converter=self.converter,
                convert_nan=self.convert_nan,
            )
        except Exception as e:
            log.error(f"Error in item transform callback for key {suggested_key}: {e}")
            raise RuntimeError(f"Failed to transform item at index {index}: {e}") from e

        # Check if result is an adapter (has a .key attribute)
        if isinstance(result, (Dict, List, ListAdapter, DictAdapter, Segments)):
            return json.dumps(result.key)
        else:
            return encode(self, result)

    def map_callback(self, data: tuple[list, dict]) -> Any:
        """Map a callback to the object.

        This method handles incoming requests from clients and routes them to the
        appropriate methods on the wrapped object.

        Parameters
        ----------
        data : tuple
            The request data containing arguments and keyword arguments.

        Returns
        -------
        Any
            The result of the requested operation, encoded if necessary.

        Raises
        ------
        NotImplementedError
            If the requested method is not implemented by the adapter.
        """
        # args = data[0]
        kwargs = data[1]

        method = kwargs["method"]
        if method == "__len__":
            return len(self.object)
        elif method == "__getitem__":
            index = kwargs["index"]
            log.debug(f"Getting item at index: {index} from {self.key}")
            try:
                value = self.object[index]

                # If transform callback is provided, use it
                if self.item_transform_callback is not None:
                    return self._handle_transform_callback(value, index)

                # If no transform callback, return the value directly
                return encode(self, value)

            except Exception as e:
                value = {"error": {"msg": str(e), "type": type(e).__name__}}
                return encode(self, value)
        elif method == "slice":
            start: int = kwargs.get("start", 0)
            stop: int = kwargs.get("stop", len(self.object))
            step: int = kwargs.get("step", 1)
            try:
                values = self.object[start:stop:step]
                return [encode(self, value) for value in values]
            except Exception as e:
                return {"error": {"msg": str(e), "type": type(e).__name__}}
        elif method == "copy":
            from znsocket import List
            # TODO: support Segments and List

            target = kwargs["target"]
            new_list = List(
                r=self.r,
                key=target,
                socket=self.socket,
                converter=self.converter,
                convert_nan=self.convert_nan,
            )
            if new_list._adapter_available:
                return json.dumps(
                    {
                        "error": {
                            "msg": "Adapter already registered to this key. Please select a different one.",
                            "type": "KeyError",
                        }
                    }
                )
            new_list.extend(self.object)
            return True
        else:
            raise NotImplementedError(f"Method {method} not implemented")
