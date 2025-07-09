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
