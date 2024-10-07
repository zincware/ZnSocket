import dataclasses

import socketio.exceptions
import typing_extensions as tyex

from znsocket import exceptions
from znsocket.abc import RefreshDataTypeDict


@dataclasses.dataclass(frozen=True)
class Client:
    address: str
    decode_responses: bool = True
    sio: socketio.Client = dataclasses.field(
        default_factory=socketio.Client, repr=False, init=False
    )
    namespace: str = "/znsocket"
    refresh_callbacks: dict = dataclasses.field(default_factory=dict)

    @classmethod
    def from_url(cls, url, namespace: str = "/znsocket", **kwargs) -> "Client":
        """Connect to a znsocket server using a URL.

        Parameters
        ----------
        url : str
            The URL of the znsocket server. Should be in the format
            "znsocket://127.0.0.1:5000".
        namespace : str
            The namespace to connect to. Default is "/znsocket".
        """
        return cls(
            address=url.replace("znsocket://", "http://"), namespace=namespace, **kwargs
        )

    def __post_init__(self):
        @self.sio.on("refresh", namespace=self.namespace)
        def refresh(data: RefreshDataTypeDict):
            for key in self.refresh_callbacks:
                if data["target"] == key:
                    self.refresh_callbacks[key](data["data"])

        try:
            self.sio.connect(self.address, namespaces=[self.namespace], wait=True)
        except socketio.exceptions.ConnectionError as err:
            raise exceptions.ConnectionError(self.address) from err

        if not self.decode_responses:
            raise NotImplementedError("decode_responses=False is not supported yet")

    def delete(self, name):
        return self.sio.call("delete", {"name": name}, namespace=self.namespace)

    def hget(self, name, key):
        return self.sio.call(
            "hget", {"name": name, "key": key}, namespace=self.namespace
        )

    def hset(self, name, key=None, value=None, mapping=None):
        if key is not None and value is None:
            raise exceptions.DataError(f"Invalid input of type {type(value)}")
        if (key is None or value is None) and mapping is None:
            raise exceptions.DataError("'hset' with no key value pairs")
        if mapping is None:
            mapping = {key: value}
        if len(mapping) == 0:
            raise exceptions.DataError("Mapping must not be empty")
        return self.sio.call(
            "hset", {"name": name, "mapping": mapping}, namespace=self.namespace
        )

    def hmget(self, name, keys):
        return self.sio.call(
            "hmget", {"name": name, "keys": keys}, namespace=self.namespace
        )

    def hkeys(self, name):
        return self.sio.call("hkeys", {"name": name}, namespace=self.namespace)

    def exists(self, name):
        return self.sio.call("exists", {"name": name}, namespace=self.namespace)

    def llen(self, name):
        return self.sio.call("llen", {"name": name}, namespace=self.namespace)

    def rpush(self, name, value):
        return self.sio.call(
            "rpush", {"name": name, "value": value}, namespace=self.namespace
        )

    def lpush(self, name, value):
        return self.sio.call(
            "lpush", {"name": name, "value": value}, namespace=self.namespace
        )

    def lindex(self, name, index):
        return self.sio.call(
            "lindex", {"name": name, "index": index}, namespace=self.namespace
        )

    def set(self, name, value):
        return self.sio.call(
            "set", {"name": name, "value": value}, namespace=self.namespace
        )

    def get(self, name):
        return self.sio.call("get", {"name": name}, namespace=self.namespace)

    def hgetall(self, name):
        return self.sio.call("hgetall", {"name": name}, namespace=self.namespace)

    def smembers(self, name):
        response = self.sio.call("smembers", {"name": name}, namespace=self.namespace)
        # check if response should raise an exception
        if isinstance(response, dict) and "error" in response:
            raise exceptions.ResponseError(response["error"])
        return set(response)

    def lrange(self, name, start, end):
        return self.sio.call(
            "lrange",
            {"name": name, "start": start, "end": end},
            namespace=self.namespace,
        )

    def lset(self, name, index, value):
        response = self.sio.call(
            "lset",
            {"name": name, "index": index, "value": value},
            namespace=self.namespace,
        )
        if isinstance(response, bool):
            return response
        if response is not None:
            raise exceptions.ResponseError(str(response))

    def lrem(self, name: str, count: int, value: str):
        return self.sio.call(
            "lrem",
            {"name": name, "count": count, "value": value},
            namespace=self.namespace,
        )

    def sadd(self, name, value):
        return self.sio.call(
            "sadd", {"name": name, "value": value}, namespace=self.namespace
        )

    def srem(self, name, value):
        return self.sio.call(
            "srem", {"name": name, "value": value}, namespace=self.namespace
        )

    def linsert(self, name, where, pivot, value):
        return self.sio.call(
            "linsert",
            {"name": name, "where": where, "pivot": pivot, "value": value},
            namespace=self.namespace,
        )

    def flushall(self):
        return self.sio.call("flushall", {}, namespace=self.namespace)

    def hexists(self, name, key):
        return self.sio.call(
            "hexists", {"name": name, "key": key}, namespace=self.namespace
        )

    def hdel(self, name, key):
        return self.sio.call(
            "hdel", {"name": name, "key": key}, namespace=self.namespace
        )

    def hlen(self, name):
        return self.sio.call("hlen", {"name": name}, namespace=self.namespace)

    def hvals(self, name):
        return self.sio.call("hvals", {"name": name}, namespace=self.namespace)

    def lpop(self, name):
        return self.sio.call("lpop", {"name": name}, namespace=self.namespace)

    def scard(self, name):
        return self.sio.call("scard", {"name": name}, namespace=self.namespace)

    @tyex.deprecated("hmset() is deprecated. Use hset() instead.")
    def hmset(self, name, mapping):
        return self.hset(name, mapping=mapping)
