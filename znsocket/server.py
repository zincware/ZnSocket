import dataclasses
import typing as t

import eventlet.wsgi
import socketio

from znsocket.abc import RefreshDataTypeDict


@dataclasses.dataclass
class Storage:
    content: dict = dataclasses.field(default_factory=dict)

    def hset(self, name, mapping):
        for key, value in mapping.items():
            try:
                self.content[name][key] = value
            except KeyError:
                self.content[name] = {key: value}

    def hget(self, name, key):
        try:
            return self.content[name][key]
        except KeyError:
            return None

    def hmget(self, name, keys):
        response = []
        for key in keys:
            try:
                response.append(self.content[name][key])
            except KeyError:
                response.append(None)
        return response

    def hkeys(self, name):
        try:
            return list(self.content[name].keys())
        except KeyError:
            return []

    def delete(self, name):
        try:
            del self.content[name]
            return 1
        except KeyError:
            return 0

    def exists(self, name):
        return 1 if name in self.content else 0

    def llen(self, name):
        try:
            return len(self.content[name])
        except KeyError:
            return 0

    def rpush(self, name, value):
        try:
            self.content[name].append(value)
        except KeyError:
            self.content[name] = [value]

        return len(self.content[name])

    def lpush(self, name, value):
        try:
            self.content[name].insert(0, value)
        except KeyError:
            self.content[name] = [value]

    def lindex(self, name, index):
        try:
            return self.content[name][index]
        except KeyError:
            return None
        except IndexError:
            return None

    def set(self, name, value):
        self.content[name] = value

    def get(self, name, default=None):
        return self.content.get(name, default)

    def smembers(self, name):
        try:
            response = self.content[name]
        except KeyError:
            response = set()

        if not isinstance(response, set):
            raise ValueError(
                "WRONGTYPE Operation against a key holding the wrong kind of value"
            )
        return response

    def lrange(self, name, start, end):
        if end == -1:
            end = None
        elif end >= 0:
            end += 1
        try:
            return self.content[name][start:end]
        except KeyError:
            return []

    def lset(self, name, index, value):
        try:
            self.content[name][index] = value
        except KeyError:
            return "no such key"
        except IndexError:
            return "index out of range"

    def lrem(self, name, count, value):
        if count == 0:
            try:
                self.content[name] = [x for x in self.content[name] if x != value]
            except KeyError:
                return 0
        else:
            removed = 0
            while removed < count:
                try:
                    self.content[name].remove(value)
                    removed += 1
                except KeyError:
                    return 0

    def sadd(self, name, value):
        try:
            self.content[name].add(value)
        except KeyError:
            self.content[name] = {value}

    def flushall(self):
        self.content.clear()

    def srem(self, name, value):
        try:
            self.content[name].remove(value)
            return 1
        except KeyError:
            return 0

    def linsert(self, name, where, pivot, value):
        try:
            index = self.content[name].index(pivot)
            if where == "BEFORE":
                self.content[name].insert(index, value)
            elif where == "AFTER":
                self.content[name].insert(index + 1, value)
        except KeyError:
            return 0
        except ValueError:
            return -1

    def hexists(self, name, key):
        try:
            return 1 if key in self.content[name] else 0
        except KeyError:
            return 0

    def hdel(self, name, key):
        try:
            del self.content[name][key]
            return 1
        except KeyError:
            return 0

    def hlen(self, name):
        try:
            return len(self.content[name])
        except KeyError:
            return 0

    def hvals(self, name):
        try:
            return list(self.content[name].values())
        except KeyError:
            return []

    def lpop(self, name):
        try:
            return self.content[name].pop(0)
        except KeyError:
            return None
        except IndexError:
            return None

    def scard(self, name):
        try:
            return len(self.content[name])
        except KeyError:
            return 0

    def hgetall(self, name):
        try:
            return self.content[name]
        except KeyError:
            return {}


@dataclasses.dataclass
class Server:
    port: int = 5000
    max_http_buffer_size: t.Optional[int] = None
    async_mode: t.Optional[str] = None

    @classmethod
    def from_url(cls, url: str, **kwargs) -> "Server":
        # server url looks like "znsocket://127.0.0.1:5000"
        if not url.startswith("znsocket://"):
            raise ValueError("Invalid URL")
        port = int(url.split(":")[-1])
        return cls(port=port, **kwargs)

    def run(self) -> None:
        """Run the server (blocking)."""
        sio = get_sio(
            max_http_buffer_size=self.max_http_buffer_size,
            async_mode=self.async_mode,
        )
        server_app = socketio.WSGIApp(sio)
        eventlet.wsgi.server(eventlet.listen(("0.0.0.0", self.port)), server_app)


def get_sio(
    max_http_buffer_size: t.Optional[int] = None,
    async_mode: t.Optional[str] = None,
) -> socketio.Server:
    kwargs = {}
    if max_http_buffer_size is not None:
        kwargs["max_http_buffer_size"] = max_http_buffer_size
    if async_mode is not None:
        kwargs["async_mode"] = async_mode
    sio = socketio.Server(**kwargs)
    attach_events(sio, namespace="/znsocket")
    return sio


def attach_events(
    sio: socketio.Server, namespace: str = "/znsocket", storage=None
) -> None:
    if storage is None:
        storage = Storage()

    @sio.event(namespace=namespace)
    def hset(sid, data):
        name = data.pop("name")
        mapping = data.pop("mapping")
        return storage.hset(name, mapping=mapping)

    @sio.event(namespace=namespace)
    def hget(sid, data):
        name = data.pop("name")
        key = data.pop("key")
        return storage.hget(name, key)

    @sio.event(namespace=namespace)
    def hmget(sid, data):
        name = data.pop("name")
        keys = data.pop("keys")
        return storage.hmget(name, keys)

    @sio.event(namespace=namespace)
    def hkeys(sid, data):
        name = data.pop("name")
        return storage.hkeys(name)

    @sio.event(namespace=namespace)
    def delete(sid, data):
        name = data.pop("name")
        return storage.delete(name)

    @sio.event(namespace=namespace)
    def exists(sid, data):
        name = data.pop("name")
        return storage.exists(name)

    @sio.event(namespace=namespace)
    def llen(sid, data):
        name = data.pop("name")
        return storage.llen(name)

    @sio.event(namespace=namespace)
    def rpush(sid, data) -> int:
        name = data.pop("name")
        value = data.pop("value")
        return storage.rpush(name, value)

    @sio.event(namespace=namespace)
    def lpush(sid, data):
        name = data.pop("name")
        value = data.pop("value")
        return storage.lpush(name, value)

    @sio.event(namespace=namespace)
    def lindex(sid, data):
        name = data.pop("name")
        index = data.pop("index")
        return storage.lindex(name, index)

    @sio.on("set", namespace=namespace)
    def set_(sid, data):
        name = data.pop("name")
        value = data.pop("value")
        return storage.set(name, value)

    @sio.event(namespace=namespace)
    def get(sid, data):
        name = data.pop("name")
        return storage.get(name)

    @sio.event(namespace=namespace)
    def hgetall(sid, data):
        name = data.pop("name")
        return storage.hgetall(name)

    @sio.event(namespace=namespace)
    def smembers(sid, data):
        name = data.pop("name")
        try:
            return list(storage.smembers(name))
        except Exception as e:
            return {"error": str(e)}

    @sio.event(namespace=namespace)
    def lrange(sid, data):
        name = data.pop("name")
        start = data.pop("start")
        end = data.pop("end")
        return storage.lrange(name, start, end)

    @sio.event(namespace=namespace)
    def lset(sid, data):
        name = data.pop("name")
        index = data.pop("index")
        value = data.pop("value")
        try:
            return storage.lset(name, index, value)
        except Exception as e:
            return str(e)

    @sio.event(namespace=namespace)
    def lrem(sid, data):
        name = data.pop("name")
        count = data.pop("count")
        value = data.pop("value")

        return storage.lrem(name, count, value)

    @sio.event(namespace=namespace)
    def sadd(sid, data):
        name = data.pop("name")
        value = data.pop("value")
        return storage.sadd(name, value)

    @sio.event(namespace=namespace)
    def flushall(sid, data):
        return storage.flushall()

    @sio.event(namespace=namespace)
    def srem(sid, data):
        name = data.pop("name")
        value = data.pop("value")
        return storage.srem(name, value)

    @sio.event(namespace=namespace)
    def linsert(sid, data):
        name = data.pop("name")
        where = data.pop("where")
        pivot = data.pop("pivot")
        value = data.pop("value")
        return storage.linsert(name, where, pivot, value)

    @sio.event(namespace=namespace)
    def hexists(sid, data):
        name = data.pop("name")
        key = data.pop("key")
        return storage.hexists(name, key)

    @sio.event(namespace=namespace)
    def hdel(sid, data):
        name = data.pop("name")
        key = data.pop("key")
        return storage.hdel(name, key)

    @sio.event(namespace=namespace)
    def hlen(sid, data):
        name = data.pop("name")
        return storage.hlen(name)

    @sio.event(namespace=namespace)
    def hvals(sid, data):
        name = data.pop("name")
        return storage.hvals(name)

    @sio.event(namespace=namespace)
    def lpop(sid, data) -> t.Optional[t.Any]:
        name = data.pop("name")
        return storage.lpop(name)

    @sio.event(namespace=namespace)
    def scard(sid, data) -> int:
        name = data.pop("name")
        return storage.scard(name)

    @sio.event(namespace=namespace)
    def refresh(sid, data: RefreshDataTypeDict) -> None:
        sio.emit("refresh", data, namespace=namespace, skip_sid=sid)

    return sio
