.. znsocket documentation master file, created by
   sphinx-quickstart on Wed Jul  9 18:59:46 2025.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

znsocket documentation
======================

znsocket is a Python and JavaScript library that provides Redis-compatible API using websockets.
It enables real-time synchronization of data structures between multiple clients and supports
distributed applications with automatic reconnection capabilities.

Key features:

- **Distributed data structures**: List, Dict, and Segments that sync across clients
- **Nested structures**: Lists can contain Dicts, Dicts can contain Lists, and Segments work with both
- **Real-time synchronization**: Changes are immediately visible to all connected clients
- **Cross-language support**: Use the same data structures from Python and JavaScript
- **Adapter pattern**: Expose existing data structures through the znsocket interface
- **Automatic chunking**: Large messages are automatically split and reassembled for reliable transmission
- **Copy-on-write operations**: Efficient data copying using Segments and fallback mechanisms

.. toctree::
   :maxdepth: 2
   :caption: Contents:

   quickstart
   chunking
   copy_on_write
   python_api
   javascript_api
   examples
   develop
