[![PyPI version](https://badge.fury.io/py/znsocket.svg)](https://badge.fury.io/py/znsocket)
[![npm version](https://badge.fury.io/js/znsocket.svg)](https://badge.fury.io/js/znsocket)
[![Coverage Status](https://coveralls.io/repos/github/zincware/ZnSocket/badge.svg?branch=main)](https://coveralls.io/github/zincware/ZnSocket?branch=main)
![PyTest](https://github.com/zincware/ZnSocket/actions/workflows/pytest.yaml/badge.svg)
[![zincware](https://img.shields.io/badge/Powered%20by-zincware-darkcyan)](https://github.com/zincware)

# ZnSocket - Redis-like Key-Value Store in Python

ZnSocket provides a [Redis](https://redis.io/)-compatible API using [python-socketio](https://python-socketio.readthedocs.io/en/stable/) and Python objects for storage. It is designed for testing and applications requiring key-value storage while being easily installable via `pip`. For production, consider using [redis-py](https://redis-py.readthedocs.io/) and a Redis instance.

> [!IMPORTANT]
> ZnSocket automatically handles large data transfers through message chunking.
> Messages larger than the configured size limit (default: 1MB or server limit) are automatically split into smaller chunks and transmitted seamlessly.
> For extremely large data transfers, consider using dedicated file transfer mechanisms or databases.

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
> ZnSocket does not encode/decode strings. Using it is equivalent to using `Redis.from_url(storage, decode_responses=True)` in the Redis client.

## Lists

ZnSocket provides a synchronized version of the Python `list` implementation. Unlike a regular Python list, the data in `znsocket.List` is not stored locally; instead, it is dynamically pushed to and pulled from the server.

Below is a step-by-step example of how to use `znsocket.List` to interact with a ZnSocket server.

```python
from znsocket import Client, List

# Connect to the ZnSocket server using the provided URL
client = Client.from_url("znsocket://127.0.0.1:5000")

# Create a synchronized list associated with the specified key
sync_list = List(r=client, key="list:1")

# Extend the list with multiple elements
sync_list.extend(["a", "b", "c", "d"])

# Print every second element from the list
print(sync_list[::2])
```

## Dicts

ZnSocket provides a synchronized version of the Python `dict` implementation similar to the `list` implementation.

Below is a step-by-step example of how to use `znsocket.Dict` to interact with a ZnSocket server.

```python
from znsocket import Client, Dict

# Connect to the ZnSocket server using the provided URL
client = Client.from_url("znsocket://127.0.0.1:5000")

# Create a synchronized dict associated with the specified key
sync_dict = Dict(r=client, key="dict:1")

# Add an item to the synchronized dict
sync_dict["Hello"] = "World"

# Print the added item
print(sync_dict["Hello"])
```

## Adapters

ZnSocket provides adapter classes that allow you to expose existing Python objects through the ZnSocket interface. This enables real-time access to your data structures from both Python and JavaScript clients without copying or modifying the original data.

### ListAdapter

The `ListAdapter` exposes any list-like Python object through the ZnSocket `List` interface:

```python
from znsocket import Client, List, ListAdapter
import numpy as np

# Connect to the ZnSocket server
client = Client.from_url("znsocket://127.0.0.1:5000")

# Create some data (can be any list-like object)
data = [1, 2, 3, 4, 5]
# Or numpy array: data = np.array([1, 2, 3, 4, 5])

# Expose the data through an adapter
adapter = ListAdapter(socket=client, key="data", object=data)

# Access the data through a List interface
shared_list = List(r=client, key="data")
print(len(shared_list))  # 5
print(shared_list[0])    # 1
print(shared_list[1:3])  # [2, 3] - supports slicing!

# Changes to the original data are immediately visible
data.append(6)
print(len(shared_list))  # 6
print(shared_list[-1])   # 6
```

### DictAdapter

The `DictAdapter` exposes any dict-like Python object through the ZnSocket `Dict` interface:

```python
from znsocket import Client, Dict, DictAdapter

# Connect to the ZnSocket server
client = Client.from_url("znsocket://127.0.0.1:5000")

# Create some data (can be any dict-like object)
data = {"name": "John", "age": 30, "city": "Berlin"}

# Expose the data through an adapter
adapter = DictAdapter(socket=client, key="user_data", object=data)

# Access the data through a Dict interface
shared_dict = Dict(r=client, key="user_data")
print(shared_dict["name"])           # "John"
print(list(shared_dict.keys()))      # ["name", "age", "city"]
print("age" in shared_dict)          # True

# Changes to the original data are immediately visible
data["country"] = "Germany"
print(shared_dict["country"])        # "Germany"
print(len(shared_dict))              # 4
```

### Key Features of Adapters

- **Real-time synchronization**: Changes to the underlying object are immediately visible through the adapter
- **Cross-language support**: Access your Python data from JavaScript clients
- **Efficient slicing**: ListAdapter supports efficient slicing operations (e.g., `list[1:5:2]`)
- **Read-only access**: Adapters provide read-only access to prevent accidental modifications
- **Nested data**: Adapters work with complex nested data structures
- **No data copying**: Adapters reference the original data directly

### JavaScript Access

Both adapters can be accessed from JavaScript clients:

```javascript
import { createClient, List, Dict } from 'znsocket';

// Connect to the server
const client = createClient({ url: 'znsocket://127.0.0.1:5000' });
await client.connect();

// Access Python data through adapters
const sharedList = new List({ client, key: 'data' });
const sharedDict = new Dict({ client, key: 'user_data' });

// All operations work seamlessly
console.log(await sharedList.length());     // Real-time length
console.log(await sharedList.slice(1, 3));  // Efficient slicing
console.log(await sharedDict.get('name'));  // Access dict values
```

## Automatic Message Chunking

ZnSocket automatically handles large data transfers by splitting messages into smaller chunks when they exceed the configured size limit. This feature is transparent to users and works seamlessly with all ZnSocket operations.

### How It Works

- **Automatic Detection**: When a message exceeds the size limit, ZnSocket automatically splits it into chunks
- **Transparent Transmission**: Chunks are sent sequentially and reassembled on the server
- **Compression Support**: Large messages are automatically compressed using gzip to reduce transfer size
- **Error Handling**: If any chunk fails to transmit, the entire message transmission is retried

### Configuration

The chunking behavior can be configured when creating a client:

```python
from znsocket import Client

# Configure chunking parameters
client = Client.from_url(
    "znsocket://127.0.0.1:5000",
    max_message_size_bytes=500000,  # 500KB limit (default: 1MB or server limit)
    enable_compression=True,        # Enable gzip compression (default: True)
    compression_threshold=1024      # Compress messages larger than 1KB (default: 1KB)
)
```

### Example with Large Data

```python
import numpy as np
from znsocket import Client, Dict

# Connect with chunking enabled
client = Client.from_url("znsocket://127.0.0.1:5000")

# Create a large dataset
large_data = np.random.rand(1000, 1000)  # ~8MB array

# Store the data - chunking happens automatically
data_dict = Dict(r=client, key="large_dataset")
data_dict["array"] = large_data  # Automatically chunked and compressed

# Retrieve the data - chunks are automatically reassembled
retrieved_data = data_dict["array"]
```

### Key Features

- **Seamless Operation**: No changes needed to existing code
- **Automatic Compression**: Large messages are compressed to reduce bandwidth
- **Configurable Limits**: Customize chunk size based on your network conditions
- **Error Recovery**: Built-in retry mechanism for failed transmissions
- **Performance Optimization**: Efficient binary serialization and compression
