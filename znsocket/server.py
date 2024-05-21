import socketio
from znsocket.db import Database

MAX_HTTP_BUFFER_SIZE = 1e12

# create a Socket.IO server
sio = socketio.Server(max_http_buffer_size=MAX_HTTP_BUFFER_SIZE)

db = Database()

# 3 Options
# - like this, running single process max performance, concurrency could lead to data corruption
# - database through SQLAlchemy
# - Redis


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
