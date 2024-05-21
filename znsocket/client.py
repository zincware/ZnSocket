import dataclasses
import uuid
from typing import Any

import socketio

from znsocket.db.base import Database


@dataclasses.dataclass
class Client:
    address: str
    sio: socketio.Client = dataclasses.field(default=None, repr=False, init=False)
    room: str = None

    def __post_init__(self):
        self.sio = socketio.Client()
        self.sio.connect(self.address)
        self.sio.emit("join", {"room": self.room})

    def __setattr__(self, name: str, value: Any) -> None:
        if name not in [x.name for x in dataclasses.fields(self)]:
            # send everything that is not a dataclass field to the server
            self.sio.emit("set", {"name": name, "value": value})
        else:
            super().__setattr__(name, value)

    def __getattribute__(self, name: str) -> Any:
        if name.startswith("_"):
            return super().__getattribute__(name)
        if name not in [x.name for x in dataclasses.fields(self)]:
            data = self.sio.call("get", {"name": name})
            if isinstance(data, dict) and data == {"AttributeError": "AttributeError"}:
                raise AttributeError(
                    f"znsocket.Client '{self}' can not access attribute '{name}'"
                )
            return data
        else:
            return super().__getattribute__(name)


@dataclasses.dataclass
class FrozenClient:
    """A frozen version of the Client.

    This version does load all attributes upon initialization.
    Attributes are changed in place and synced with the
    server.

    """

    address: str
    sio: socketio.Client = dataclasses.field(default=None, repr=False, init=False)
    room: str = None
    _data: dict = dataclasses.field(default_factory=dict, init=False, repr=False)

    def __post_init__(self):
        self.sio = socketio.Client()
        self.sio.connect(self.address)
        self.sio.emit("join", {"room": self.room})

    def sync(self, push=False, pull=False):
        # TODO: only sync changed attributes
        if pull:
            self._pull()
        if push:
            self._push()

    def _pull(self):
        data: dict = self.sio.call("get", {})
        # TODO: chunk the data, it might be too much at once.
        for key, value in data.items():
            self._data[key] = value

    def _push(self):
        for key, value in self._data.items():
            self.sio.emit("set", {"name": key, "value": value})

    def __setattr__(self, name: str, value: Any) -> None:
        if (
            name not in [x.name for x in dataclasses.fields(self)]
            and name not in type(self).__dict__
        ):
            # send everything that is not a dataclass field to the server
            self._data[name] = value
        else:
            super().__setattr__(name, value)

    def __getattribute__(self, name: str) -> Any:
        if name.startswith("_") or name in type(self).__dict__:
            return super().__getattribute__(name)
        if name not in [x.name for x in dataclasses.fields(self)]:
            return self._data[name]
        else:
            return super().__getattribute__(name)


@dataclasses.dataclass
class DBClient:
    db: Database
    sid: str = None
    room: str = None

    def __post_init__(self):
        self.sid = uuid.uuid4().hex
        self.db.join_room(self.sid, self.room)

    def __setattr__(self, name: str, value: Any) -> None:
        if name not in [x.name for x in dataclasses.fields(self)]:
            self.db.set_room_storage(self.sid, name, value)
        else:
            super().__setattr__(name, value)

    def __getattribute__(self, name: str) -> Any:
        if name.startswith("_"):
            return super().__getattribute__(name)
        if name not in [x.name for x in dataclasses.fields(self)]:
            data = self.db.get_room_storage(self.sid, name)
            if isinstance(data, dict) and data == {"AttributeError": "AttributeError"}:
                raise AttributeError(
                    f"znsocket.DBClient '{self}' can not access attribute '{name}'"
                )
            return data
        else:
            return super().__getattribute__(name)
