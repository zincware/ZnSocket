[![PyPI version](https://badge.fury.io/py/znsocket.svg)](https://badge.fury.io/py/znsocket)
[![Coverage Status](https://coveralls.io/repos/github/zincware/ZnSocket/badge.svg?branch=main)](https://coveralls.io/github/zincware/ZnSocket?branch=main)
![PyTest](https://github.com/zincware/ZnSocket/actions/workflows/pytest.yaml/badge.svg)
[![zincware](https://img.shields.io/badge/Powered%20by-zincware-darkcyan)](https://github.com/zincware)
# ZnSocket - Redis-like Key-Value Store in Python

ZnSocket provides a [Redis](https://redis.io/)-compatible API using [python-socketio](https://python-socketio.readthedocs.io/en/stable/) and Python objects for storage. It is designed for testing and applications requiring key-value storage while being easily installable via `pip`. For production, consider using [redis-py](https://redis-py.readthedocs.io/) and a Redis instance.


## Installation
To install ZnSocket, use:

```bash
pip install znsocket
```

## Example
Start the ZnSocket server using the CLI:

```bash
znsocket --port 5000
```
For additional options, run:
```bash
znsocket --help
```

Here's a simple example of how to use the ZnSocket client:
```python
from znsocket import Client

# Connect to the ZnSocket server
c = Client.from_url("znsocket://127.0.0.1:5000")

# Set and get a value
c.set("name", "Fabian")
assert c.get("name") == "Fabian"
```

> [!NOTE]
> ZnSocket does not decode strings automatically. Using it is equivalent to using `Redis.from_url(storage, decode_responses=True)` in the Redis client.


## Lists
ZnSocket provides a synchronized version of the Python `list` implementation. Unlike a regular Python list, the data in `znsocket.List` is not stored locally; instead, it is dynamically pushed to and pulled from the server.

Below is a step-by-step example of how to use `znsocket.List` to interact with a ZnSocket server.

```python
from znsocket import Client, List

# Connect to the ZnSocket server using the provided URL
client = Client.from_url("znsocket://127.0.0.1:5000")

# Create a synchronized list associated with the specified key
sync_list = List(client=client, key="list:1")

# Extend the list with multiple elements
sync_list.extend(["a", "b", "c", "d"])

# Print every second element from the list
print(sync_list[::2])
```
