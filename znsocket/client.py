import dataclasses

import socketio


@dataclasses.dataclass
class Client:
    address: str
    sio: socketio.SimpleClient = dataclasses.field(default=None, repr=False, init=False)

    @classmethod
    def from_url(cls, url):
        """Connect to a znsocket server using a URL.

        Parameters
        ----------
        url : str
            The URL of the znsocket server. Should be in the format
            "znsocket://127.0.0.1:5000".
        """
        return cls(address=url.replace("znsocket://", "http://"))

    def __post_init__(self):
        self.sio = socketio.SimpleClient()
        self.sio.connect(self.address)

    def delete(self, name):
        return self.sio.emit("delete", {"name": name})

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

    def lindex(self, name, index):
        return self.sio.call("lindex", {"name": name, "index": index})

    def set(self, name, value):
        return self.sio.call("set", {"name": name, "value": value})

    def get(self, name):
        return self.sio.call("get", {"name": name})

    def hmset(self, name, data):
        return self.sio.call("hmset", {"name": name, "data": data})

    def hgetall(self, name):
        return self.sio.call("hgetall", {"name": name})

    def smembers(self, name):
        return set(self.sio.call("smembers", {"name": name}))

    def lrange(self, name, start, end):
        return self.sio.call("lrange", {"name": name, "start": start, "end": end})

    def lset(self, name, index, value):
        return self.sio.call("lset", {"name": name, "index": index, "value": value})

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
