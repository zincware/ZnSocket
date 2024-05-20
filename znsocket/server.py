import socketio
import dataclasses

# create a Socket.IO server
sio = socketio.Server()

# wrap with a WSGI application
app = socketio.WSGIApp(sio)

ROOM_STORAGE = {}
ROOM_OCCUPANCY = {}


@sio.event
def connect(sid, environ, auth):
    print("connect ", sid)


@sio.event
def disconnect(sid):
    print("disconnect ", sid)
    for room in ROOM_OCCUPANCY:
        if sid in ROOM_OCCUPANCY[room]:
            ROOM_OCCUPANCY[room].remove(sid)
            if len(ROOM_OCCUPANCY[room]) == 0:
                del ROOM_OCCUPANCY[room]
                del ROOM_STORAGE[room]
                break  # can only be in one room at a time


@sio.event
def join(sid, data):
    room = data.pop("room")
    if room is None:
        room = sid
    for old_room in ROOM_OCCUPANCY:
        if sid in ROOM_OCCUPANCY[old_room]:
            ROOM_OCCUPANCY[old_room].remove(sid)
            sio.leave_room(sid, old_room)
            if len(ROOM_OCCUPANCY[old_room]) == 0:
                del ROOM_OCCUPANCY[old_room]
                del ROOM_STORAGE[old_room]

    sio.enter_room(sid, room)
    if room not in ROOM_STORAGE:
        ROOM_STORAGE[room] = {}
    if room not in ROOM_OCCUPANCY:
        ROOM_OCCUPANCY[room] = {sid}
    else:
        ROOM_OCCUPANCY[room].add(sid)


@sio.on("set")
def set_event(sid, data):
    name = data.pop("name")
    value = data.pop("value")
    for room in ROOM_OCCUPANCY:
        if sid in ROOM_OCCUPANCY[room]:
            ROOM_STORAGE[room][name] = value
            return
    # if the client is not in a room, store the data in the SID_STORAGE


@sio.event
def get(sid, data):
    name = data.pop("name")
    print(ROOM_OCCUPANCY)
    for room in ROOM_OCCUPANCY:
        if sid in ROOM_OCCUPANCY[room]:
            if name in ROOM_STORAGE[room]:
                return ROOM_STORAGE[room][name]

    return {"AttributeError": "AttributeError"}


# such that the client can raise an AttributeError or similar

if __name__ == "__main__":
    import eventlet
    import eventlet.wsgi

    eventlet.wsgi.server(eventlet.listen(("localhost", 5000)), app)
