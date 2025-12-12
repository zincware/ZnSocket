from importlib.metadata import version

from znsocket.client import Client
from znsocket.objects import Dict, DictAdapter, List, ListAdapter, Segments
from znsocket.server import Server, attach_events
from znsocket.storages import MemoryStorage

__version__ = version("znsocket")

__all__ = [
    "Client",
    "Server",
    "List",
    "Dict",
    "attach_events",
    "MemoryStorage",
    "ListAdapter",
    "Segments",
    "DictAdapter",
]
