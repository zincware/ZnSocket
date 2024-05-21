import typing as t

import socketio

from znsocket.db import Database, MemoryDatabase

# 3 Options
# - like this, running single process max performance, concurrency could lead to data corruption
# - database through SQLAlchemy
# - Redis


def get_sio(
    db: t.Optional[Database] = None, max_http_buffer_size: t.Optional[int] = None
) -> socketio.Server:
    kwargs = {}
    if max_http_buffer_size is not None:
        kwargs["max_http_buffer_size"] = max_http_buffer_size
    sio = socketio.Server(**kwargs)

    if db is None:
        db = MemoryDatabase()

    @sio.event
    def connect(sid, environ, auth):
        print("connect ", sid)

    @sio.event
    def disconnect(sid):
        print("disconnect ", sid)
        db.remove_client(sid)

    @sio.event
    def join(sid, data):
        room = data.pop("room")
        if room is None:
            room = sid
        sio.enter_room(sid, room)
        db.join_room(sid, room)

    @sio.on("set")  # TODO: rename to something else?
    def set_event(sid, data):
        name = data.pop("name")
        value = data.pop("value")
        db.set_room_storage(sid, name, value)

    @sio.event
    def get(sid, data):
        name = data.pop("name")
        return db.get_room_storage(sid, name)

    return sio
