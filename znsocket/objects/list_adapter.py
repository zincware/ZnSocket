import json
from collections.abc import Sequence
from dataclasses import dataclass

from znsocket.client import Client
from znsocket.utils import handle_error


@dataclass
class ListAdapter:
    """Connect any object to a znsocket server to be used instead of loading data from the database.

    Data will be send via sockets through the server to the client.
    """

    key: str
    socket: Client
    object: Sequence

    def __post_init__(self):
        result = self.socket.call("register_adapter", key=self.key)
        handle_error(result)

        self.socket.adapter_callback = self.map_callback

    def map_callback(self, data):
        """Map a callback to the object."""
        args = data[0]
        kwargs = data[1]

        method = kwargs["method"]
        if method == "__len__":
            return len(self.object)
        elif method == "__getitem__":
            index: list[int] = kwargs["index"]
            try:
                value = self.object[index]
            except Exception as e:
                value = {"error": {"msg": str(e), "type": type(e).__name__}}
            return json.dumps(value)
