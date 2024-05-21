import abc
import dataclasses
import typing as t


@dataclasses.dataclass(eq=True, frozen=True)
class Client:
    sid: str


@dataclasses.dataclass
class Room:
    name: str
    clients: list[Client]
    storage: dict[str, str]


class Database(abc.ABC):
    @abc.abstractmethod
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

    @abc.abstractmethod
    def get_room_storage(self, sid: str, key: str) -> t.Any:
        """Retrieve room data from the database.

        Attributes
        ----------
        sid : str
            The session ID of the client.
        key : str
            The key to retrieve the value from.
        """

    @abc.abstractmethod
    def remove_client(self, sid: str) -> None:
        """Remove a client from the database.

        Remove the client from all rooms.
        Remove empty rooms.

        Attributes
        ----------
        sid : str
            The session ID of the client.
        """

    # TODO: rename add client
    @abc.abstractmethod
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
