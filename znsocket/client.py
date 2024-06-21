import dataclasses

import socketio.exceptions
import typing_extensions as tyex

from znsocket import exceptions


@dataclasses.dataclass(frozen=True)
class Client:
    address: str
    decode_responses: bool = True
    sio: socketio.SimpleClient = dataclasses.field(
        default_factory=socketio.SimpleClient, repr=False, init=False
    )

    @classmethod
    def from_url(cls, url, **kwargs) -> "Client":
        """Connect to a znsocket server using a URL.

        Parameters
        ----------
        url : str
            The URL of the znsocket server. Should be in the format
            "znsocket://127.0.0.1:5000".
        """
        return cls(address=url.replace("znsocket://", "http://"), **kwargs)

    def __post_init__(self):
        try:
            self.sio.connect(self.address)
        except socketio.exceptions.ConnectionError as err:
            raise exceptions.ConnectionError(self.address) from err

        if not self.decode_responses:
            raise NotImplementedError("decode_responses=False is not supported yet")

    def delete(self, name):
        return self.sio.call("delete", {"name": name})

    def hget(self, name, key):
        return self.sio.call("hget", {"name": name, "key": key})

    def hset(self, name, key, value):
        return self.sio.call("hset", {"name": name, "key": key, "value": value})

    def hmget(self, name, keys):
        return self.sio.call("hmget", {"name": name, "keys": keys})

    def hkeys(self, name):
        return self.sio.call("hkeys", {"name": name})

    def exists(self, name):
        return self.sio.call("exists", {"name": name})

    def llen(self, name):
        return self.sio.call("llen", {"name": name})

    def rpush(self, name, value):
        return self.sio.call("rpush", {"name": name, "value": value})

    def lpush(self, name, value):
        return self.sio.call("lpush", {"name": name, "value": value})

    def lindex(self, name, index):
        return self.sio.call("lindex", {"name": name, "index": index})

    def set(self, name, value):
        return self.sio.call("set", {"name": name, "value": value})

    def get(self, name):
        return self.sio.call("get", {"name": name})

    @tyex.deprecated("Use hset instead")
    def hmset(self, name, data):
        if len(data) == 0:
            raise exceptions.DataError("data must not be empty")
        return self.sio.call("hmset", {"name": name, "data": data})

    def hgetall(self, name):
        return self.sio.call("hgetall", {"name": name})

    def smembers(self, name):
        response = self.sio.call("smembers", {"name": name})
        # check if response should raise an exception
        if isinstance(response, dict) and "error" in response:
            raise exceptions.ResponseError(response["error"])
        return set(response)

    def lrange(self, name, start, end):
        return self.sio.call("lrange", {"name": name, "start": start, "end": end})

    def lset(self, name, index, value):
        response = self.sio.call("lset", {"name": name, "index": index, "value": value})
        if response is not None:
            raise exceptions.ResponseError(response)

    def lrem(self, name: str, count: int, value: str):
        return self.sio.call("lrem", {"name": name, "count": count, "value": value})

    def sadd(self, name, value):
        return self.sio.call("sadd", {"name": name, "value": value})

    def srem(self, name, value):
        return self.sio.call("srem", {"name": name, "value": value})

    def linsert(self, name, where, pivot, value):
        return self.sio.call(
            "linsert", {"name": name, "where": where, "pivot": pivot, "value": value}
        )

    def flushall(self):
        return self.sio.call("flushall", {})

    def hexists(self, name, key):
        return self.sio.call("hexists", {"name": name, "key": key})

    def hdel(self, name, key):
        return self.sio.call("hdel", {"name": name, "key": key})

    def hlen(self, name):
        return self.sio.call("hlen", {"name": name})

    def hvals(self, name):
        return self.sio.call("hvals", {"name": name})

    def lpop(self, name):
        return self.sio.call("lpop", {"name": name})

    def scard(self, name):
        return self.sio.call("scard", {"name": name})
