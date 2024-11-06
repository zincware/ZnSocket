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

    def _redis_command(self, command, *args, **kwargs):
        """Generic handler for Redis commands."""
        result = self.sio.call(command, [args, kwargs], namespace=self.namespace)
        if "error" in result:
            if result["error"]["type"] == "DataError":
                raise exceptions.DataError(result["error"]["msg"])
            elif result["error"]["type"] == "TypeError":
                raise TypeError(result["error"]["msg"])
            elif result["error"]["type"] == "IndexError":
                raise IndexError(result["error"]["msg"])
            elif result["error"]["type"] == "KeyError":
                raise KeyError(result["error"]["msg"])
            elif result["error"]["type"] == "UnknownEventError":
                raise exceptions.UnknownEventError(result["error"]["msg"])
            elif result["error"]["type"] == "ResponseError":
                raise exceptions.ResponseError(result["error"]["msg"])
            else:
                raise exceptions.ZnSocketError(f"{result['error']['type']}: {result['error']['msg']} -- for command {command}")
            # raise exceptions.DataError(result["error"])
        if "type" in result and result["type"] == "set":
            return set(result["data"])
        return result["data"]

    def __getattr__(self, name):
        """Intercepts method calls to dynamically route Redis commands."""
        # Check if name corresponds to a Redis command
        if hasattr(Redis, name):
            return functools.partial(self._redis_command, name)
        raise AttributeError(f"'{type(self).__name__}' object has no attribute '{name}'")

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