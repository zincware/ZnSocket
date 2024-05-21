import dataclasses
import typing as t

from sqlmodel import Field, Relationship, Session, SQLModel, create_engine, select

from znsocket.server import Database


class RoomData(SQLModel, table=True):
    id: t.Optional[int] = Field(default=None, primary_key=True)
    key: str
    value: str
    room_id: t.Optional[int] = Field(default=None, foreign_key="room.id")
    room: t.Optional["Room"] = Relationship(back_populates="data")


class Client(SQLModel, table=True):
    id: t.Optional[int] = Field(default=None, primary_key=True)
    sid: str
    room_id: t.Optional[int] = Field(default=None, foreign_key="room.id")
    room: t.Optional["Room"] = Relationship(back_populates="clients")


class Room(SQLModel, table=True):
    id: t.Optional[int] = Field(default=None, primary_key=True)
    name: str
    clients: list[Client] = Relationship(back_populates="room")
    data: list[RoomData] = Relationship(back_populates="room")


@dataclasses.dataclass
class SqlDatabase(Database):
    engine: str

    def __post_init__(self):
        self._engine = create_engine(self.engine)
        SQLModel.metadata.create_all(self._engine)

    def set_room_storage(self, sid: str, key: str, value: t.Any) -> None:
        with Session(self._engine) as session:
            client = session.exec(select(Client).where(Client.sid == sid)).first()
            if client:
                room_data = session.exec(
                    select(RoomData).where(
                        RoomData.key == key, RoomData.room_id == client.room_id
                    )
                ).first()
                if room_data:
                    room_data.value = value
                else:
                    room_data = RoomData(key=key, value=value, room=client.room)
                    session.add(room_data)
                session.commit()
                session.refresh(room_data)

    def get_room_storage(self, sid: str, key: str) -> t.Any:
        with Session(self._engine) as session:
            client = session.exec(select(Client).where(Client.sid == sid)).first()
            if client:
                room_data = session.exec(
                    select(RoomData).where(
                        RoomData.key == key, RoomData.room_id == client.room_id
                    )
                ).first()
                if room_data:
                    return room_data.value
                else:
                    return {"AttributeError": "AttributeError"}

    def remove_client(self, sid: str) -> None:
        with Session(self._engine) as session:
            client = session.exec(select(Client).where(Client.sid == sid)).first()
            if client:
                session.delete(client)
                session.commit()

            # remove the room if it has no clients
            room = session.exec(select(Room).where(Room.clients == None)).first()
            if room:
                # delete all room data
                for room_data in room.data:
                    session.delete(room_data)
                session.delete(room)
                session.commit()

    def join_room(self, sid: str, room_name: str) -> None:
        with Session(self._engine) as session:
            # check if the client exists
            client = session.exec(select(Client).where(Client.sid == sid)).first()
            if not client:
                client = Client(sid=sid)
                session.add(client)
                session.commit()
                session.refresh(client)
            else:
                # remove the client from the room
                # TODO: should clients be allowed to change rooms?
                client.room_id = None

            # get the room
            room = session.exec(select(Room).where(Room.name == room_name)).first()
            if not room:
                room = Room(name=room_name)
                session.add(room)
                session.commit()
                session.refresh(room)

            # add the client to the room
            client.room = room
            session.commit()
