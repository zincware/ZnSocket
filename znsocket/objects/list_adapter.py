import json
from collections.abc import Sequence
from dataclasses import dataclass

from znsocket.client import Client
from znsocket.utils import encode, handle_error


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
    """

    key: str
    socket: Client
    object: Sequence
    converter: list[type] | None = None
    convert_nan: bool = False
    r: Client | None = None

    def __post_init__(self):
        self.key = f"znsocket.List:{self.key}"

        result = self.socket.call("register_adapter", key=self.key)
        handle_error(result)

        self.socket.adapter_callback = self.map_callback
        if self.r is None:
            self.r = self.socket

    def map_callback(self, data):
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
            try:
                value = self.object[index]
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
