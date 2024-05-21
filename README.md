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


# TODOs
- direct database access
- frozen client
