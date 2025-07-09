Examples
========

Real-time Chat Application
--------------------------

This example shows how to build a simple real-time chat application using znsocket.

Server Setup
~~~~~~~~~~~~

.. code-block:: python

   import znsocket

   # Start the server
   server = znsocket.Server(port=5000)
   server.run()

Python Client
~~~~~~~~~~~~~

.. code-block:: python

   import znsocket
   import threading

   # Connect to server
   client = znsocket.Client("http://localhost:5000")

   # Create shared message list
   messages = znsocket.List(client, "chat_messages")

   # Listen for new messages
   def on_message_added(data):
       if 'indices' in data:
           for idx in data['indices']:
               print(f"New message: {messages[idx]}")

   messages.on_refresh(on_message_added)

   # Send messages
   while True:
       message = input("Enter message: ")
       messages.append(f"User: {message}")

JavaScript Client
~~~~~~~~~~~~~~~~~

.. code-block:: javascript

   import { createClient, List } from 'znsocket';

   // Connect to server
   const client = createClient({ url: 'http://localhost:5000' });
   await client.connect();

   // Create shared message list
   const messages = new List({ client, key: 'chat_messages' });

   // Listen for new messages
   messages.onRefresh((data) => {
       if (data.indices) {
           data.indices.forEach(async (idx) => {
               const message = await messages.get(idx);
               console.log(`New message: ${message}`);
           });
       }
   });

   // Send messages
   document.getElementById('send').addEventListener('click', async () => {
       const input = document.getElementById('message');
       await messages.push(`User: ${input.value}`);
       input.value = '';
   });

Distributed Task Queue
----------------------

This example demonstrates using znsocket for a distributed task queue.

Task Producer
~~~~~~~~~~~~~

.. code-block:: python

   import znsocket
   import json

   client = znsocket.Client("http://localhost:5000")
   task_queue = znsocket.List(client, "task_queue")

   # Add tasks to queue
   tasks = [
       {"id": 1, "type": "process_data", "data": "sample1"},
       {"id": 2, "type": "process_data", "data": "sample2"},
       {"id": 3, "type": "send_email", "to": "user@example.com"},
   ]

   for task in tasks:
       task_queue.append(json.dumps(task))

   print(f"Added {len(tasks)} tasks to queue")

Task Consumer
~~~~~~~~~~~~~

.. code-block:: python

   import znsocket
   import json
   import time

   client = znsocket.Client("http://localhost:5000")
   task_queue = znsocket.List(client, "task_queue")

   def process_task(task_data):
       task = json.loads(task_data)
       print(f"Processing task {task['id']}: {task['type']}")
       time.sleep(1)  # Simulate work
       print(f"Task {task['id']} completed")

   # Process tasks
   while True:
       if len(task_queue) > 0:
           task = task_queue.pop(0)  # Get first task
           process_task(task)
       else:
           time.sleep(0.1)  # Wait for new tasks

Shared Configuration
--------------------

Use znsocket.Dict for shared configuration across multiple services.

Configuration Server
~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

   import znsocket

   client = znsocket.Client("http://localhost:5000")
   config = znsocket.Dict(client, "app_config")

   # Set initial configuration
   config.update({
       "database_url": "postgresql://localhost/myapp",
       "cache_enabled": True,
       "max_connections": 100,
       "debug": False
   })

   print("Configuration initialized")

Service Consumer
~~~~~~~~~~~~~~~~

.. code-block:: python

   import znsocket

   client = znsocket.Client("http://localhost:5000")
   config = znsocket.Dict(client, "app_config")

   # Read configuration
   db_url = config["database_url"]
   cache_enabled = config["cache_enabled"]

   print(f"Database URL: {db_url}")
   print(f"Cache enabled: {cache_enabled}")

   # Listen for configuration changes
   def on_config_change(data):
       print(f"Configuration changed: {data}")

   config.on_refresh(on_config_change)

Data Synchronization with Adapters
----------------------------------

Use ListAdapter to expose existing data structures through znsocket.

Data Source
~~~~~~~~~~~

.. code-block:: python

   import znsocket
   import numpy as np

   # Create some data
   data = np.array([1, 2, 3, 4, 5])

   # Connect to server
   client = znsocket.Client("http://localhost:5000")

   # Expose data through adapter
   adapter = znsocket.ListAdapter(
       key="scientific_data",
       socket=client,
       object=data
   )

   print("Data exposed through adapter")

Data Consumer
~~~~~~~~~~~~~

.. code-block:: python

   import znsocket

   # Connect to server
   client = znsocket.Client("http://localhost:5000")

   # Access data through List interface
   data = znsocket.List(client, "scientific_data")

   # Read data
   print(f"Data length: {len(data)}")
   print(f"First item: {data[0]}")
   print(f"All items: {data[:]}")

Nested Data Structures
----------------------

znsocket supports nesting data structures within each other. You can put Lists into Dicts, Dicts into Lists, and even use Segments in nested structures.

List in Dict Example
~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

   import znsocket

   client = znsocket.Client("http://localhost:5000")

   # Create a main dictionary
   user_data = znsocket.Dict(client, "user_data")

   # Create lists for different user attributes
   user_scores = znsocket.List(client, "user_scores")
   user_scores.extend([85, 92, 78, 95, 88])

   user_tags = znsocket.List(client, "user_tags")
   user_tags.extend(["python", "javascript", "redis", "websockets"])

   # Store lists in the dictionary
   user_data["name"] = "John Doe"
   user_data["email"] = "john@example.com"
   user_data["scores"] = user_scores  # List inside Dict
   user_data["tags"] = user_tags      # Another List inside Dict

   # Access nested data
   print(f"User name: {user_data['name']}")
   print(f"First score: {user_data['scores'][0]}")
   print(f"All tags: {user_data['tags'][:]}")

Dict in List Example
~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

   import znsocket

   client = znsocket.Client("http://localhost:5000")

   # Create a main list to store user records
   users_list = znsocket.List(client, "users_list")

   # Create individual user dictionaries
   user1 = znsocket.Dict(client, "user1")
   user1.update({
       "name": "Alice",
       "age": 30,
       "department": "Engineering"
   })

   user2 = znsocket.Dict(client, "user2")
   user2.update({
       "name": "Bob",
       "age": 25,
       "department": "Design"
   })

   # Store dictionaries in the list
   users_list.append(user1)  # Dict inside List
   users_list.append(user2)  # Another Dict inside List

   # Access nested data
   print(f"First user name: {users_list[0]['name']}")
   print(f"Second user department: {users_list[1]['department']}")

   # Iterate through nested structures
   for i, user in enumerate(users_list):
       print(f"User {i+1}: {user['name']} ({user['age']} years old)")

Complex Nested Structures
~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

   import znsocket

   client = znsocket.Client("http://localhost:5000")

   # Create a complex nested structure: Dict -> List -> Dict
   company = znsocket.Dict(client, "company")
   company["name"] = "Tech Corp"
   company["founded"] = 2020

   # Create departments list
   departments = znsocket.List(client, "departments")

   # Create individual department dictionaries
   engineering = znsocket.Dict(client, "engineering_dept")
   engineering.update({
       "name": "Engineering",
       "employees": 50,
       "budget": 2000000
   })

   marketing = znsocket.Dict(client, "marketing_dept")
   marketing.update({
       "name": "Marketing",
       "employees": 20,
       "budget": 500000
   })

   # Build the nested structure
   departments.append(engineering)
   departments.append(marketing)
   company["departments"] = departments

   # Access deeply nested data
   print(f"Company: {company['name']}")
   print(f"Engineering budget: {company['departments'][0]['budget']}")
   print(f"Marketing employees: {company['departments'][1]['employees']}")

JavaScript Nested Structures
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: javascript

   import { createClient, List, Dict } from 'znsocket';

   // Connect to server
   const client = createClient({ url: 'http://localhost:5000' });
   await client.connect();

   // Create nested structure: Dict with List inside
   const userProfile = new Dict({ client, key: 'user_profile' });
   const userHobbies = new List({ client, key: 'user_hobbies' });

   // Add hobbies to the list
   await userHobbies.push('coding');
   await userHobbies.push('gaming');
   await userHobbies.push('reading');

   // Store basic info and nested list in dict
   await userProfile.set('username', 'developer123');
   await userProfile.set('hobbies', userHobbies);

   // Access nested data
   const username = await userProfile.get('username');
   const hobbies = await userProfile.get('hobbies');
   const firstHobby = await hobbies.get(0);

   console.log(`User: ${username}`);
   console.log(`First hobby: ${firstHobby}`);

Segments for Large Data
-----------------------

Segments work seamlessly with nested structures and provide efficient handling of large datasets.

.. code-block:: python

   import znsocket

   client = znsocket.Client("http://localhost:5000")

   # Create large original list
   large_list = znsocket.List(client, "large_data")
   large_list.extend(range(1000000))  # 1 million items

   # Create segments for efficient copying
   segments = znsocket.Segments.from_list(large_list, "data_segments")

   # Segments behave like lists but with efficient storage
   print(f"Segments length: {len(segments)}")
   print(f"First item: {segments[0]}")
   print(f"Last item: {segments[-1]}")

   # Segments can also be stored in nested structures
   data_container = znsocket.Dict(client, "data_container")
   data_container["original"] = large_list
   data_container["segments"] = segments  # Segments in Dict
   data_container["metadata"] = {"size": len(segments), "type": "segments"}

   # Access segments through nested structure
   stored_segments = data_container["segments"]
   print(f"Accessed through dict: {stored_segments[0]}")

   # Create a list of different segments
   segments_collection = znsocket.List(client, "segments_collection")
   segments_collection.append(segments)  # Segments in List

   # All data structures (List, Dict, Segments) can be nested within each other
   # This provides maximum flexibility for complex data organization
