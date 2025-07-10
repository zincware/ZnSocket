Quick Start
===========

Installation
------------

Python
~~~~~~

Install znsocket using pip:

.. code-block:: bash

   pip install znsocket

JavaScript
~~~~~~~~~~

Install znsocket using npm:

.. code-block:: bash

   npm install znsocket

Basic Usage
-----------

Python Server
~~~~~~~~~~~~~

Start a znsocket server:

.. code-block:: python

   import znsocket

   # Start server on port 5000
   server = znsocket.Server(port=5000)
   server.run()

Python Client
~~~~~~~~~~~~~

Connect to the server and use data structures:

.. code-block:: python

   import znsocket

   # Connect to server
   client = znsocket.Client("http://localhost:5000")

   # Create a distributed list
   my_list = znsocket.List(client, "my_list")
   my_list.append("item1")
   my_list.append("item2")

   # Create a distributed dict
   my_dict = znsocket.Dict(client, "my_dict")
   my_dict["key1"] = "value1"
   my_dict["key2"] = "value2"

JavaScript Client
~~~~~~~~~~~~~~~~~

Connect to the server from JavaScript:

.. code-block:: javascript

   import { createClient, List, Dict } from 'znsocket';

   // Connect to server
   const client = createClient({ url: 'http://localhost:5000' });
   await client.connect();

   // Create a distributed list
   const myList = new List({ client, key: 'my_list' });
   await myList.push('item1');
   await myList.push('item2');

   // Create a distributed dict
   const myDict = new Dict({ client, key: 'my_dict' });
   await myDict.set('key1', 'value1');
   await myDict.set('key2', 'value2');

Real-time Synchronization
~~~~~~~~~~~~~~~~~~~~~~~~~

Multiple clients can share the same data structures and receive real-time updates:

.. code-block:: python

   # Client 1
   client1 = znsocket.Client("http://localhost:5000")
   shared_list = znsocket.List(client1, "shared_data")

   # Client 2
   client2 = znsocket.Client("http://localhost:5000")
   shared_list2 = znsocket.List(client2, "shared_data")

   # Changes from client1 are immediately visible to client2
   shared_list.append("new_item")
   print(shared_list2[-1])  # "new_item"

Nested Data Structures
~~~~~~~~~~~~~~~~~~~~~~

znsocket supports nesting data structures within each other:

.. code-block:: python

   # Python: Store a List inside a Dict
   user_data = znsocket.Dict(client, "user_data")
   user_scores = znsocket.List(client, "user_scores")
   user_scores.extend([85, 92, 78, 95])

   user_data["name"] = "John"
   user_data["scores"] = user_scores  # List inside Dict

   # Access nested data
   print(user_data["scores"][0])  # 85

.. code-block:: javascript

   // JavaScript: Store a Dict inside a List
   const users = new List({ client, key: 'users' });
   const user1 = new Dict({ client, key: 'user1' });

   await user1.set('name', 'Alice');
   await user1.set('age', 30);
   await users.push(user1);  // Dict inside List

   // Access nested data
   const firstUser = await users.get(0);
   const userName = await firstUser.get('name');  // 'Alice'

All data structures (List, Dict, Segments) can be nested within each other, providing maximum flexibility for complex data organization.

Working with Adapters
~~~~~~~~~~~~~~~~~~~~~~

Adapters allow you to expose existing Python objects through the znsocket interface without copying data. This is particularly useful for sharing live data with JavaScript clients or other Python processes.

ListAdapter Example
*******************

.. code-block:: python

   import znsocket
   import numpy as np

   # Connect to server
   client = znsocket.Client("http://localhost:5000")

   # Create some data (any list-like object)
   sensor_data = [23.5, 24.1, 23.8, 24.0, 23.9]
   # Or: sensor_data = np.array([23.5, 24.1, 23.8, 24.0, 23.9])

   # Expose through adapter
   adapter = znsocket.ListAdapter(
       socket=client,
       key="temperature_readings",
       object=sensor_data
   )

   # Access from any client
   readings = znsocket.List(client, "temperature_readings")
   print(f"Current temperature: {readings[-1]}°C")
   print(f"Average: {sum(readings) / len(readings):.1f}°C")

   # Real-time updates: modify original data
   sensor_data.append(24.2)
   print(f"New reading: {readings[-1]}°C")  # Immediately available

DictAdapter Example
*******************

.. code-block:: python

   import znsocket

   # Connect to server
   client = znsocket.Client("http://localhost:5000")

   # Create configuration data
   app_config = {
       "database_url": "postgresql://localhost/myapp",
       "debug": False,
       "max_users": 1000
   }

   # Expose through adapter
   adapter = znsocket.DictAdapter(
       socket=client,
       key="app_settings",
       object=app_config
   )

   # Access from any client
   settings = znsocket.Dict(client, "app_settings")
   print(f"Database: {settings['database_url']}")
   print(f"Debug mode: {settings['debug']}")

   # Real-time configuration updates
   app_config["debug"] = True
   print(f"Debug now: {settings['debug']}")  # Immediately updated

JavaScript Access to Adapters
******************************

.. code-block:: javascript

   import { createClient, List, Dict } from 'znsocket';

   const client = createClient({ url: 'http://localhost:5000' });
   await client.connect();

   // Access Python data from JavaScript
   const temperatures = new List({ client, key: 'temperature_readings' });
   const settings = new Dict({ client, key: 'app_settings' });

   // All operations work seamlessly
   console.log(`Latest temp: ${await temperatures.get(-1)}°C`);
   console.log(`Database: ${await settings.get('database_url')}`);

   // Efficient slicing
   const recent = await temperatures.slice(-5);  // Last 5 readings
   console.log(`Recent readings: ${recent}`);

Key Adapter Benefits:

- **No data copying**: Adapters reference original data directly
- **Real-time updates**: Changes to source data are immediately visible
- **Cross-language access**: JavaScript clients can access Python data
- **Efficient operations**: Slicing and other operations are optimized
- **Read-only safety**: Adapters prevent accidental modifications
