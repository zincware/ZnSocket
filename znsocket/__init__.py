from importlib.metadata import version

from znsocket.client import Client
from znsocket.objects import Dict, DictAdapter, List, ListAdapter, Segments
from znsocket.server import Server, Storage, attach_events

__version__ = version("znsocket")

__all__ = [
    "Client",
    "Server",
    "List",
    "Dict",
    "attach_events",
    "Storage",
    "ListAdapter",
    "Segments",
    "DictAdapter",
]
