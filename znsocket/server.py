import typing as t

import socketio

storage = {}


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
        try:
            return [storage[name][key] for key in keys]
        except KeyError:
            return [None for key in keys]

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
        except KeyError:
            pass

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
    def rpush(sid, data):
        name = data.pop("name")
        value = data.pop("value")
        try:
            storage[name].append(value)
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

    @sio.event
    def set(sid, data):
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
            return list(storage[name])
        except KeyError:
            return []

    @sio.event
    def lrange(sid, data):
        name = data.pop("name")
        start = data.pop("start")
        end = data.pop("end")
        if end == -1:
            end = None
        try:
            return storage[name][start:end]
        except KeyError:
            return []
        except IndexError:
            return []

    @sio.event
    def lset(sid, data):
        name = data.pop("name")
        index = data.pop("index")
        value = data.pop("value")
        try:
            storage[name][index] = value
        except KeyError:
            pass
        except IndexError:
            pass

    @sio.event
    def lrem(sid, data):
        name = data.pop("name")
        count = data.pop("count")
        value = data.pop("value")
        removed = 0
        while removed < count:
            try:
                storage[name].remove(value)
                removed += 1
            except KeyError:
                break
            except IndexError:
                break

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

    return sio
