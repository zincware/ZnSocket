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

Adapters allow you to expose existing Python objects through the znsocket interface, enabling real-time access to your data from both Python and JavaScript clients.

Basic ListAdapter Example
~~~~~~~~~~~~~~~~~~~~~~~~~

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

ListAdapter with Real-time Updates
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

   import znsocket
   import time
   import threading

   # Create dynamic data
   sensor_data = []

   # Connect to server
   client = znsocket.Client("http://localhost:5000")

   # Expose data through adapter
   adapter = znsocket.ListAdapter(
       key="sensor_readings",
       socket=client,
       object=sensor_data
   )

   # Simulate real-time sensor data updates
   def update_sensor_data():
       import random
       for i in range(100):
           sensor_data.append(random.randint(0, 100))
           time.sleep(0.1)  # New reading every 100ms

   # Start updating data in background
   thread = threading.Thread(target=update_sensor_data)
   thread.daemon = True
   thread.start()

   print("Real-time sensor data available at key: sensor_readings")

Data Consumer with Slicing
~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

   import znsocket

   # Connect to server
   client = znsocket.Client("http://localhost:5000")

   # Access data through List interface
   data = znsocket.List(client, "scientific_data")

   # Read data with various operations
   print(f"Data length: {len(data)}")
   print(f"First item: {data[0]}")
   print(f"All items: {data[:]}")

   # Efficient slicing operations
   print(f"First 3 items: {data[:3]}")
   print(f"Every 2nd item: {data[::2]}")
   print(f"Last 3 items: {data[-3:]}")
   print(f"Items 2-4: {data[1:4]}")

Basic DictAdapter Example
~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

   import znsocket

   # Create configuration data
   config = {
       "database_host": "localhost",
       "database_port": 5432,
       "cache_enabled": True,
       "max_connections": 100
   }

   # Connect to server
   client = znsocket.Client("http://localhost:5000")

   # Expose config through adapter
   adapter = znsocket.DictAdapter(
       key="app_config",
       socket=client,
       object=config
   )

   print("Configuration exposed through adapter")

DictAdapter with Dynamic Updates
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

   import znsocket
   import time
   import threading

   # Create dynamic configuration
   system_status = {
       "cpu_usage": 0.0,
       "memory_usage": 0.0,
       "disk_usage": 0.0,
       "active_connections": 0
   }

   # Connect to server
   client = znsocket.Client("http://localhost:5000")

   # Expose status through adapter
   adapter = znsocket.DictAdapter(
       key="system_status",
       socket=client,
       object=system_status
   )

   # Simulate system monitoring
   def update_system_status():
       import random
       while True:
           system_status["cpu_usage"] = random.uniform(0, 100)
           system_status["memory_usage"] = random.uniform(0, 100)
           system_status["disk_usage"] = random.uniform(0, 100)
           system_status["active_connections"] = random.randint(0, 500)
           time.sleep(1)  # Update every second

   # Start monitoring in background
   thread = threading.Thread(target=update_system_status)
   thread.daemon = True
   thread.start()

   print("System status available at key: system_status")

Configuration Consumer
~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

   import znsocket

   # Connect to server
   client = znsocket.Client("http://localhost:5000")

   # Access configuration through Dict interface
   config = znsocket.Dict(client, "app_config")

   # Read configuration
   print(f"Database host: {config['database_host']}")
   print(f"Database port: {config['database_port']}")
   print(f"Cache enabled: {config['cache_enabled']}")

   # Check if keys exist
   if "debug_mode" in config:
       print(f"Debug mode: {config['debug_mode']}")

   # Get all keys and values
   print(f"All keys: {list(config.keys())}")
   print(f"All values: {list(config.values())}")

JavaScript Client Access
~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: javascript

   import { createClient, List, Dict } from 'znsocket';

   // Connect to server
   const client = createClient({ url: 'http://localhost:5000' });
   await client.connect();

   // Access sensor data
   const sensorData = new List({ client, key: 'sensor_readings' });
   console.log(`Sensor readings: ${await sensorData.length()}`);
   console.log(`Latest 5 readings: ${await sensorData.slice(-5)}`);

   // Access system status
   const systemStatus = new Dict({ client, key: 'system_status' });
   console.log(`CPU Usage: ${await systemStatus.get('cpu_usage')}%`);
   console.log(`Memory Usage: ${await systemStatus.get('memory_usage')}%`);

   // Access configuration
   const appConfig = new Dict({ client, key: 'app_config' });
   const dbHost = await appConfig.get('database_host');
   console.log(`Database host: ${dbHost}`);

Complex Nested Adapters
~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

   import znsocket

   # Create complex nested data structure
   user_profiles = {
       "user1": {
           "name": "Alice",
           "scores": [85, 92, 78, 95],
           "preferences": {"theme": "dark", "notifications": True}
       },
       "user2": {
           "name": "Bob",
           "scores": [78, 85, 90, 88],
           "preferences": {"theme": "light", "notifications": False}
       }
   }

   # Connect to server
   client = znsocket.Client("http://localhost:5000")

   # Expose the nested structure
   adapter = znsocket.DictAdapter(
       key="user_profiles",
       socket=client,
       object=user_profiles
   )

   # Access nested data
   profiles = znsocket.Dict(client, "user_profiles")

   # Direct access to nested structures
   user1 = profiles["user1"]
   print(f"User 1 name: {user1['name']}")
   print(f"User 1 scores: {user1['scores']}")
   print(f"User 1 theme: {user1['preferences']['theme']}")

   # Modify the original data - changes are immediately visible
   user_profiles["user1"]["scores"].append(99)
   print(f"Updated scores: {profiles['user1']['scores']}")

Machine Learning Model Serving
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

   import znsocket
   import numpy as np
   from sklearn.ensemble import RandomForestClassifier
   from sklearn.datasets import make_classification

   # Train a simple model
   X, y = make_classification(n_samples=1000, n_features=20, n_classes=2, random_state=42)
   model = RandomForestClassifier(n_estimators=100, random_state=42)
   model.fit(X, y)

   # Create model metadata and results storage
   model_info = {
       "model_type": "RandomForestClassifier",
       "n_features": 20,
       "n_classes": 2,
       "accuracy": model.score(X, y),
       "feature_importance": model.feature_importances_.tolist()
   }

   predictions = []
   prediction_probabilities = []

   # Connect to server
   client = znsocket.Client("http://localhost:5000")

   # Expose model information
   info_adapter = znsocket.DictAdapter(
       key="model_info",
       socket=client,
       object=model_info
   )

   # Expose predictions
   pred_adapter = znsocket.ListAdapter(
       key="predictions",
       socket=client,
       object=predictions
   )

   # Expose prediction probabilities
   prob_adapter = znsocket.ListAdapter(
       key="prediction_probabilities",
       socket=client,
       object=prediction_probabilities
   )

   # Simulate real-time predictions
   def make_predictions():
       import time
       import random

       while True:
           # Generate random input
           sample = np.random.rand(1, 20)

           # Make prediction
           pred = model.predict(sample)[0]
           prob = model.predict_proba(sample)[0].tolist()

           # Store results
           predictions.append(int(pred))
           prediction_probabilities.append(prob)

           # Update model info
           model_info["total_predictions"] = len(predictions)

           time.sleep(2)  # New prediction every 2 seconds

   # Start prediction service
   import threading
   thread = threading.Thread(target=make_predictions)
   thread.daemon = True
   thread.start()

   print("ML model serving started!")
   print("Access model info at: model_info")
   print("Access predictions at: predictions")
   print("Access probabilities at: prediction_probabilities")

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
