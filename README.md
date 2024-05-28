[![Coverage Status](https://coveralls.io/repos/github/zincware/ZnSocket/badge.svg?branch=main)](https://coveralls.io/github/zincware/ZnSocket?branch=main)
![PyTest](https://github.com/zincware/ZnSocket/actions/workflows/pytest.yaml/badge.svg)
[![zincware](https://img.shields.io/badge/Powered%20by-zincware-darkcyan)](https://github.com/zincware)
# ZnSocket - [Redis](https://redis.io/) but in Python

This package provides a [Redis](https://redis.io/) compatible API but uses [python-socketio](https://python-socketio.readthedocs.io/en/stable/) and a Python object as storage.

This package is designed for testing and applications that need a `key-value` storage but still want to be native pip installable.
For production it should be replaced by [redis-py](https://redis-py.readthedocs.io/) and a [Redis](https://redis.io/) instance.

## Installation
Install via `pip install znsocket`.

## Example
You can run the server via the CLI `znsocket --port 5000`. For more information run `znsocket --help`.

```python
from znsocket import Client

c = Client.from_url("znsocket://127.0.0.1:5000")
c.set("name", "Fabian")
assert c.get("name") == "Fabian"
```

> [!NOTE]
> The `znsocket` package does not decode the strings, thus using it is equivalent to `Redis.from_url(storage, decode_responses=True)`.
