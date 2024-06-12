import dataclasses
import typing as t

import eventlet.wsgi
import socketio

storage = {}


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
        eventlet.wsgi.server(eventlet.listen(("localhost", self.port)), server_app)


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

    @sio.event
    def hset(sid, data):
        name = data.pop("name")
        key = data.pop("key")
        value = data.pop("value")
        try:
            storage[name][key] = value
        except KeyError:
            storage[name] = {key: value}

    @sio.event
    def hget(sid, data):
        name = data.pop("name")
        key = data.pop("key")
        try:
            return storage[name][key]
        except KeyError:
            return None

    @sio.event
    def hmget(sid, data):
        name = data.pop("name")
        keys = data.pop("keys")
        response = []
        for key in keys:
            try:
                response.append(storage[name][key])
            except KeyError:
                response.append(None)
        return response

    @sio.event
    def hkeys(sid, data):
        name = data.pop("name")
        try:
            return list(storage[name].keys())
        except KeyError:
            return []

    @sio.event
    def delete(sid, data):
        name = data.pop("name")
        try:
            del storage[name]
            return 1
        except KeyError:
            return 0

    @sio.event
    def exists(sid, data):
        name = data.pop("name")
        return 1 if name in storage else 0

    @sio.event
    def llen(sid, data):
        name = data.pop("name")
        try:
            return len(storage[name])
        except KeyError:
            return 0

    @sio.event
    def rpush(sid, data) -> int:
        name = data.pop("name")
        value = data.pop("value")
        try:
            storage[name].append(value)
        except KeyError:
            storage[name] = [value]

        return len(storage[name])

    @sio.event
    def lpush(sid, data):
        name = data.pop("name")
        value = data.pop("value")
        try:
            storage[name].insert(0, value)
        except KeyError:
            storage[name] = [value]

    @sio.event
    def lindex(sid, data):
        name = data.pop("name")
        index = data.pop("index")
        try:
            return storage[name][index]
        except KeyError:
            return None
        except IndexError:
            return None

    @sio.on("set")
    def set_(sid, data):
        name = data.pop("name")
        value = data.pop("value")
        storage[name] = value

    @sio.event
    def get(sid, data):
        name = data.pop("name")
        return storage.get(name)

    @sio.event
    def hmset(sid, data):
        name = data.pop("name")
        items = data.pop("data")
        try:
            storage[name].update(items)
        except KeyError:
            storage[name] = items

    @sio.event
    def hgetall(sid, data):
        name = data.pop("name")
        return storage.get(name, {})

    @sio.event
    def smembers(sid, data):
        name = data.pop("name")
        try:
            response = storage[name]
        except KeyError:
            response = set()

        if not isinstance(response, set):
            return {
                "error": "WRONGTYPE Operation against a key holding the wrong kind of value"
            }
        return list(response)

    @sio.event
    def lrange(sid, data):
        name = data.pop("name")
        start = data.pop("start")
        end = data.pop("end")
        if end == -1:
            end = None
        elif end >= 0:
            end += 1
        try:
            return storage[name][start:end]
        except KeyError:
            return []

    @sio.event
    def lset(sid, data):
        name = data.pop("name")
        index = data.pop("index")
        value = data.pop("value")
        try:
            storage[name][index] = value
        except KeyError:
            return "no such key"
        except IndexError:
            return "index out of range"

    @sio.event
    def lrem(sid, data):
        name = data.pop("name")
        count = data.pop("count")
        value = data.pop("value")

        if count == 0:
            try:
                storage[name] = [x for x in storage[name] if x != value]
            except KeyError:
                return 0
        else:
            removed = 0
            while removed < count:
                try:
                    storage[name].remove(value)
                    removed += 1
                except KeyError:
                    return 0

    @sio.event
    def sadd(sid, data):
        name = data.pop("name")
        value = data.pop("value")
        try:
            storage[name].add(value)
        except KeyError:
            storage[name] = {value}

    @sio.event
    def flushall(sid, data):
        storage.clear()

    @sio.event
    def srem(sid, data):
        name = data.pop("name")
        value = data.pop("value")
        try:
            storage[name].remove(value)
            return 1
        except KeyError:
            return 0

    @sio.event
    def linsert(sid, data):
        name = data.pop("name")
        where = data.pop("where")
        pivot = data.pop("pivot")
        value = data.pop("value")
        try:
            index = storage[name].index(pivot)
            if where == "BEFORE":
                storage[name].insert(index, value)
            elif where == "AFTER":
                storage[name].insert(index + 1, value)
        except KeyError:
            return 0
        except ValueError:
            return -1

    @sio.event
    def hexists(sid, data):
        name = data.pop("name")
        key = data.pop("key")
        try:
            return 1 if key in storage[name] else 0
        except KeyError:
            return 0

    @sio.event
    def hdel(sid, data):
        name = data.pop("name")
        key = data.pop("key")
        try:
            del storage[name][key]
            return 1
        except KeyError:
            return 0

    @sio.event
    def hlen(sid, data):
        name = data.pop("name")
        try:
            return len(storage[name])
        except KeyError:
            return 0

    @sio.event
    def hvals(sid, data):
        name = data.pop("name")
        try:
            return list(storage[name].values())
        except KeyError:
            return []

    @sio.event
    def lpop(sid, data) -> t.Optional[t.Any]:
        name = data.pop("name")
        try:
            return storage[name].pop(0)
        except KeyError:
            return None
        except IndexError:
            return None

    @sio.event
    def scard(sid, data) -> int:
        name = data.pop("name")
        try:
            return len(storage[name])
        except KeyError:
            return 0

    return sio
