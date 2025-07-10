import json
from collections.abc import Mapping
from dataclasses import dataclass

from znsocket.client import Client
from znsocket.utils import encode, handle_error


@dataclass
class DictAdapter:
    """Connect any object to a znsocket server to be used instead of loading data from the database.

    The DictAdapter allows you to expose any mapping-like object through the znsocket
    server, making it accessible to clients as if it were a regular Dict. Data is
    transmitted via sockets through the server to the client on demand.

    Parameters
    ----------
    key : str
        The key identifier for this adapter in the server.
    socket : Client
        The znsocket client connection to use for communication.
    object : Mapping
        The mapping object to expose through the adapter.
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
    >>> my_data = {"a": 1, "b": 2, "c": 3}
    >>> adapter = znsocket.DictAdapter("my_adapter", client, my_data)
    >>> # Now clients can access my_data as znsocket.Dict(client, "my_adapter")
    """

    key: str
    socket: Client
    object: Mapping
    converter: list[type] | None = None
    convert_nan: bool = False
    r: Client | None = None

    def __post_init__(self):
        self.key = f"znsocket.Dict:{self.key}"

        result = self.socket.call("register_adapter", key=self.key)
        handle_error(result)

        self.socket.adapter_callback = self.map_callback
        if self.r is None:
            self.r = self.socket

    def map_callback(self, data):  # noqa: C901
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
            dict_key: str = kwargs["dict_key"]
            try:
                value = self.object[dict_key]
            except Exception as e:
                value = {"error": {"msg": str(e), "type": type(e).__name__}}
            return encode(self, value)
        elif method == "__contains__":
            dict_key: str = kwargs["dict_key"]
            return dict_key in self.object
        elif method == "keys":
            return list(self.object.keys())
        elif method == "values":
            return [encode(self, value) for value in self.object.values()]
        elif method == "items":
            return [(key, encode(self, value)) for key, value in self.object.items()]
        elif method == "get":
            dict_key: str = kwargs["dict_key"]
            default = kwargs.get("default", None)
            try:
                value = self.object.get(dict_key, default)
            except Exception as e:
                value = {"error": {"msg": str(e), "type": type(e).__name__}}
            return encode(self, value)
        elif method == "copy":
            from znsocket import Dict

            target = kwargs["target"]
            new_dict = Dict(
                r=self.r,
                key=target,
                socket=self.socket,
                converter=self.converter,
                convert_nan=self.convert_nan,
            )
            if new_dict._adapter_available:
                return json.dumps(
                    {
                        "error": {
                            "msg": "Adapter already registered to this key. Please select a different one.",
                            "type": "KeyError",
                        }
                    }
                )
            new_dict.update(self.object)
            return True
        else:
            raise NotImplementedError(f"Method {method} not implemented")
