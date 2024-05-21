[![Coverage Status](https://coveralls.io/repos/github/zincware/ZnSocket/badge.svg?branch=main)](https://coveralls.io/github/zincware/ZnSocket?branch=main)
![PyTest](https://github.com/zincware/ZnSocket/actions/workflows/pytest.yaml/badge.svg)
[![zincware](https://img.shields.io/badge/Powered%20by-zincware-darkcyan)](https://github.com/zincware)
# ZnSocket - share data using SocketIO

This package provides an interface to share data using WebSockets via the SocketIO protocol.

## Example

Run the server using the CLI via `znsocket`.
You can connect via

```python
from znsocket import Client

c1 = Client(address='http://localhost:5000', room="MyRoom")
c1.text = "Hello World"
```

As long as any client is connected to the server in `MyRoom` you will be able to access the data.
Run this code in a separate Python kernel:

```python
from znsocket import Client

c2 = Client(address='http://localhost:5000', room="MyRoom")
print(c2.text)
```

You can set any attribute of the `Client`, except `{"address", "room", "sio"}` and it will be shared.
The data must be JSON-serializable to be shared.

## Special Clients
For performance reasons there are two special clients
### FrozenClient
The `znsocket.FrozenClient` can be used to operate on data and only push or pull once requested.

```python
import znsocket

client = znsocket.FrozenClient(
    address='http://localhost:5000', room="MyRoom"
)

client.sync(push=True, pull=True)
```

### DBClient
The `znsocket.DBClient` can be used to directly operate on the database without going through the server.

```python
import znsocket

client = znsocket.DBClient(
    db=znsocket.SqlDatabase(engine=f"sqlite:///znsocket.db"), room="MyRoom"
)
```
