import dataclasses
import typing as t

from .base import Client, Database, Room


@dataclasses.dataclass
class MemoryDatabase(Database):
    """In-memory database for storing room data.

    This database is NOT THREAD-SAFE!

    Attributes
    ----------
    rooms : list[Room]
        A list of rooms. Each room has a name, a list of clients
        and a storage dictionary.
    """

    rooms: list[Room] = dataclasses.field(default_factory=list)

    def set_room_storage(self, sid: str, key: str, value: t.Any) -> None:
        """Store room data in the database.

        Primary method for storing data.

        Attributes
        ----------
        sid : str
            The session ID of the client. This unique identifier
            is sufficient, because the client is only in one room at a time.
        key : str
            The key to store the value under.
        value : t.Any
            The value to store.

        """
        for room in self.rooms:
            if any(client.sid == sid for client in room.clients):
                room.storage[key] = value
                return

    def get_room_storage(self, sid: str, key: t.Optional[str]) -> t.Any:
        for room in self.rooms:
            if any(client.sid == sid for client in room.clients):
                if key is None:
                    return room.storage
                return room.storage.get(key, {"AttributeError": "AttributeError"})
        return {
            "AttributeError": "AttributeError"
        }  # client not found, this should never happen

    def remove_client(self, sid: str) -> None:
        """Remove a client from the database.

        Remove the client from all rooms.
        Remove empty rooms.

        Attributes
        ----------
        sid : str
            The session ID of the client.
        """
        client = Client(sid)
        for room in self.rooms:
            if any(client.sid == sid for client in room.clients):
                room.clients.remove(client)
        # remove all rooms with no clients
        self.rooms = [room for room in self.rooms if room.clients]

    def join_room(self, sid: str, room_name: str) -> None:
        """Make the client join a room.

        A client can only be in one room at a time.
        It will be removed from all other rooms.

        Attributes
        ----------
        sid : str
            The session ID of the client.
        room_name : str
            The name of the room to join.
        """
        client = Client(sid)
        for room in self.rooms:
            if any(client.sid == sid for client in room.clients):
                room.clients.remove(client)
        for room in self.rooms:
            if room.name == room_name:
                room.clients.append(client)
                return
        self.rooms.append(Room(room_name, [client], {}))

    def get_client_name(self, sid: str) -> str:
        """Get the name of the client.

        Attributes
        ----------
        sid : str
            The session ID of the client.
        """
        for room in self.rooms:
            for client in room.clients:
                if client.sid == sid:
                    return client.name
        raise ValueError(f"Client with sid {sid} not found")
