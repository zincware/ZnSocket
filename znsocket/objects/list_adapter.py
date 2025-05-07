from dataclasses import dataclass
from znsocket.client import Client, handle_error
from collections.abc import Sequence

@dataclass
class ListAdapter:
   """Connect any object to a znsocket server to be used instead of loading data from the database.
   
   Data will be send via sockets through the server to the client.
   """
   key: str
   socket: Client
   object: Sequence

   def __post_init__(self):
      result = self.socket.call("register_adapter", key=self.key)
      handle_error(result)

      self.socket.adapter_callback = lambda data: "Lorem ipsum"  # TODO: implement this



