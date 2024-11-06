import dataclasses

import socketio.exceptions
import typing_extensions as tyex

from znsocket import exceptions
from znsocket.abc import RefreshDataTypeDict
from znsocket.utils import parse_url
from redis import Redis

import functools


@dataclasses.dataclass(frozen=True)
class Client:
    address: str
    decode_responses: bool = True
    sio: socketio.Client = dataclasses.field(
        default_factory=socketio.Client, repr=False, init=False
    )
    namespace: str = "/znsocket"
    refresh_callbacks: dict = dataclasses.field(default_factory=dict)

    def pipeline(self):
        return Pipeline(self)

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

        _url, _path = parse_url(self.address)
        try:
            self.sio.connect(
                _url,
                namespaces=[self.namespace],
                wait=True,
                socketio_path=f"{_path}/socket.io" if _path else "socket.io",
            )

        except socketio.exceptions.ConnectionError as err:
            raise exceptions.ConnectionError(self.address) from err

        if not self.decode_responses:
            raise NotImplementedError("decode_responses=False is not supported yet")

    @functools.wraps(Redis.delete)
    def delete(self, *args, **kwargs):
        return self.sio.call("delete", [args, kwargs], namespace=self.namespace)

    @functools.wraps(Redis.hget)
    def hget(self, *args, **kwargs):
        return self.sio.call(
            "hget", [args, kwargs], namespace=self.namespace
        )

    @functools.wraps(Redis.hset)
    def hset(self, *args, **kwargs):
        return self.sio.call("hset", [args, kwargs], namespace=self.namespace)

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
        if name is None or index is None:
            raise exceptions.DataError("Invalid input")
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
        if name is None or count is None or value is None:
            raise exceptions.DataError("Invalid input")
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

    def copy(self, src, dst):
        return self.sio.call("copy", {"src": src, "dst": dst}, namespace=self.namespace)

    @tyex.deprecated("hmset() is deprecated. Use hset() instead.")
    def hmset(self, name, mapping):
        return self.hset(name, mapping=mapping)

@dataclasses.dataclass
class Pipeline:
    client: Client
    pipeline: list = dataclasses.field(default_factory=list)
    # TODO: max number of messages to be sent at once (check size?)

    def set(self, name, value):
        self.pipeline.append(("set", {"name": name, "value": value}))
        return self
    
    def get(self, name):
        self.pipeline.append(("get", {"name": name}))
        return self
    
    def delete(self, name):
        self.pipeline.append(("delete", {"name": name}))
        return self
    
    def hset(self, name, key=None, value=None, mapping=None):
        if key is not None and value is None:
            raise exceptions.DataError(f"Invalid input of type {type(value)}")
        if (key is None or value is None) and mapping is None:
            raise exceptions.DataError("'hset' with no key value pairs")
        if mapping is None:
            mapping = {key: value}
        if len(mapping) == 0:
            raise exceptions.DataError("Mapping must not be empty")

        self.pipeline.append(("hset", {"name": name, "mapping": mapping}))
        return self
    
    def hget(self, name, key):
        self.pipeline.append(("hget", {"name": name, "key": key}))
        return self
    
    def hkeys(self, name):
        self.pipeline.append(("hkeys", {"name": name}))
        return self
    
    def exists(self, name):
        self.pipeline.append(("exists", {"name": name}))
        return self
    
    def llen(self, name):
        self.pipeline.append(("llen", {"name": name}))
        return self
    
    def rpush(self, name, value):
        self.pipeline.append(("rpush", {"name": name, "value": value}))
        return self
    
    def lpush(self, name, value):
        self.pipeline.append(("lpush", {"name": name, "value": value}))
        return self
    
    def lindex(self, name, index):
        self.pipeline.append(("lindex", {"name": name, "index": index}))
        return self
    
    def smembers(self, name):
        self.pipeline.append(("smembers", {"name": name}))
        return self
    
    def hgetall(self, name):
        self.pipeline.append(("hgetall", {"name": name}))
        return self

    def execute(self):
        # TODO: what about errors / sets / etc?
        return self.client.sio.call(
            "pipeline", {"pipeline": self.pipeline}, namespace=self.client.namespace
        )