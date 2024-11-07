import dataclasses
import typing as t
from copy import deepcopy

import eventlet.wsgi
import socketio

from znsocket.abc import RefreshDataTypeDict
from znsocket.exceptions import DataError, ResponseError


@dataclasses.dataclass
class Storage:
    content: dict = dataclasses.field(default_factory=dict)

    def hset(
        self,
        name: str,
        key: t.Optional[str] = None,
        value: t.Optional[str] = None,
        mapping: t.Optional[dict] = None,
        items: t.Optional[list] = None,
    ):
        if key is None and not mapping and not items:
            raise DataError("'hset' with no key value pairs")
        if value is None and not mapping and not items:
            raise DataError(f"Invalid input of type {type(value)}")
        pieces = []
        if items:
            pieces.extend(items)
        if key is not None:
            pieces.extend((key, value))
        if mapping:
            for pair in mapping.items():
                pieces.extend(pair)

        if name not in self.content:
            self.content[name] = {}
        for i in range(0, len(pieces), 2):
            self.content[name][pieces[i]] = pieces[i + 1]
        return len(pieces) // 2

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

        return len(self.content[name])

    def lindex(self, name, index):
        if index is None:
            raise DataError("Invalid input of type None")
        try:
            return self.content[name][index]
        except KeyError:
            return None
        except IndexError:
            return None
        except TypeError:  # index is not an integer
            return None

    def set(self, name, value):
        if value is None or name is None:
            raise DataError("Invalid input of type None")
        self.content[name] = value
        return True

    def get(self, name, default=None):
        return self.content.get(name, default)

    def smembers(self, name):
        try:
            response = self.content[name]
        except KeyError:
            response = set()

        if not isinstance(response, set):
            raise ResponseError(
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
            raise ResponseError("no such key")
        except IndexError:
            raise ResponseError("index out of range")

    def lrem(self, name, count, value):
        if count is None or value is None or name is None:
            raise DataError("Invalid input of type None")
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

    def copy(self, src, dst):
        if src == dst:
            return False
        if src not in self.content:
            return False
        if dst in self.content:
            return False
        self.content[dst] = deepcopy(self.content[src])
        return True


@dataclasses.dataclass
class Server:
    port: int = 5000
    max_http_buffer_size: t.Optional[int] = None
    async_mode: t.Optional[str] = None
    logger: bool = False

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
            logger=self.logger,
            engineio_logger=self.logger,
        )
        server_app = socketio.WSGIApp(sio)
        eventlet.wsgi.server(eventlet.listen(("0.0.0.0", self.port)), server_app)


def get_sio(
    max_http_buffer_size: t.Optional[int] = None,
    async_mode: t.Optional[str] = None,
    **kwargs,
) -> socketio.Server:
    # We set these as kwargs, because their default
    # is not None, so if None we leave them out
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

    @sio.on("*", namespace=namespace)
    def handle_all_events(event, sid, data):
        """Handle any event dynamically by mapping event name to storage method."""
        if hasattr(storage, event):
            try:
                result = {"data": getattr(storage, event)(*data[0], **data[1])}
                if isinstance(result["data"], set):
                    result["data"] = list(result["data"])
                    result["type"] = "set"
                return result
            except TypeError as e:
                return {
                    "error": {
                        "msg": f"Invalid arguments for {event}: {str(e)}",
                        "type": "TypeError",
                    }
                }
            except Exception as e:
                return {"error": {"msg": str(e), "type": type(e).__name__}}
        else:
            return {
                "error": {"msg": f"Unknown event: {event}", "type": "UnknownEventError"}
            }

    @sio.event(namespace=namespace)
    def refresh(sid, data: RefreshDataTypeDict) -> None:
        sio.emit("refresh", data, namespace=namespace, skip_sid=sid)

    @sio.event(namespace=namespace)
    def pipeline(sid, data):
        commands = data.pop("pipeline")
        results = []
        for cmd in commands:
            event = cmd[0]
            args = cmd[1][0]
            kwargs = cmd[1][1]

            if hasattr(storage, event):
                try:
                    result = {"data": getattr(storage, event)(*args, **kwargs)}
                    if isinstance(result["data"], set):
                        result["data"] = list(result["data"])
                        result["type"] = "set"
                    results.append(result)
                except TypeError as e:
                    return {
                        "error": {
                            "msg": f"Invalid arguments for {event}: {str(e)}",
                            "type": "TypeError",
                        }
                    }
                except Exception as e:
                    return {"error": {"msg": str(e), "type": type(e).__name__}}
            else:
                return {
                    "error": {
                        "msg": f"Unknown event: {event}",
                        "type": "UnknownEventError",
                    }
                }
        return {"data": results}

    return sio
