from znsocket.client import Client
from znsocket.objects import Dict, DictAdapter, List, ListAdapter, Segments
from znsocket.server import Server, Storage, attach_events

from importlib.metadata import version
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
