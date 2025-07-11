Automatic Message Chunking
==========================

ZnSocket automatically handles large data transfers by splitting messages into smaller chunks when they exceed the configured size limit. This feature is transparent to users and works seamlessly with all ZnSocket operations.

Overview
--------

When working with large datasets (such as numpy arrays, large lists, or complex nested structures), ZnSocket automatically detects when a message exceeds the size limit and splits it into manageable chunks. This ensures reliable transmission over networks with size constraints while maintaining optimal performance.

How It Works
------------

The chunking process involves several steps:

1. **Size Detection**: Before sending a message, ZnSocket calculates the serialized size
2. **Automatic Splitting**: If the size exceeds the limit, the message is split into chunks
3. **Compression**: Large messages are automatically compressed using gzip to reduce bandwidth
4. **Sequential Transmission**: Chunks are sent one by one with metadata for reassembly
5. **Server Reassembly**: The server receives chunks and reconstructs the original message
6. **Error Recovery**: Failed transmissions are automatically retried

Configuration Options
---------------------

You can configure chunking behavior when creating a client:

.. code-block:: python

    from znsocket import Client

    # Configure chunking parameters
    client = Client.from_url(
        "znsocket://127.0.0.1:5000",
        max_message_size_bytes=500000,  # 500KB limit (default: 1MB or server limit)
        enable_compression=True,        # Enable gzip compression (default: True)
        compression_threshold=1024      # Compress messages larger than 1KB (default: 1KB)
    )

Configuration Parameters
~~~~~~~~~~~~~~~~~~~~~~~~

- **max_message_size_bytes**: Maximum size for a single message before chunking (default: server limit or 1MB)
- **enable_compression**: Whether to compress large messages (default: True)
- **compression_threshold**: Minimum size for compression activation (default: 1024 bytes)

Example Usage
-------------

Large Data Storage
~~~~~~~~~~~~~~~~~~

.. code-block:: python

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

Working with Lists
~~~~~~~~~~~~~~~~~~

.. code-block:: python

    from znsocket import Client, List
    import numpy as np

    client = Client.from_url("znsocket://127.0.0.1:5000")
    large_list = List(r=client, key="large_data_list")

    # Add large items - each will be chunked if necessary
    for i in range(10):
        large_array = np.random.rand(500, 500)  # ~2MB each
        large_list.append(large_array)  # Automatically chunked

Pipeline Operations
~~~~~~~~~~~~~~~~~~~

Chunking also works seamlessly with pipeline operations:

.. code-block:: python

    from znsocket import Client, Dict
    import numpy as np

    client = Client.from_url("znsocket://127.0.0.1:5000")

    # Use pipeline for batch operations
    pipeline = client.pipeline()

    # Large data operations in pipeline
    for i in range(5):
        data_dict = Dict(r=pipeline, key=f"dataset_{i}")
        large_data = np.random.rand(800, 800)  # ~5MB each
        data_dict["array"] = large_data  # Will be chunked during execution

    # Execute all operations - chunking handled automatically
    pipeline.execute()

Performance Considerations
--------------------------

Chunk Size Optimization
~~~~~~~~~~~~~~~~~~~~~~~

The optimal chunk size depends on your network conditions:

- **Fast, reliable networks**: Larger chunks (1-5MB) for better throughput
- **Slow or unreliable networks**: Smaller chunks (100-500KB) for better reliability
- **High-latency networks**: Moderate chunks (500KB-1MB) to balance overhead and reliability


Monitoring Chunking
-------------------

You can monitor chunking activity by enabling debug logging:

.. code-block:: python

    import logging

    # Enable debug logging for chunking
    logging.basicConfig(level=logging.DEBUG)
    logger = logging.getLogger("znsocket.client")
    logger.setLevel(logging.DEBUG)

This will show messages like:

.. code-block:: text

    DEBUG:znsocket.client:Message size (8,000,000 bytes) exceeds limit (1,000,000 bytes). Using chunked transmission.
    DEBUG:znsocket.client:Splitting message into 8 chunks
    DEBUG:znsocket.client:Sent chunk 1/8 for message abc123

Troubleshooting
---------------

Common Issues
~~~~~~~~~~~~~

**Messages timing out**
  - Reduce chunk size with ``max_message_size_bytes``
  - Increase client timeout settings
  - Check network stability

**High memory usage**
  - Reduce data size before transmission
  - Process data in smaller batches
  - Consider alternative storage mechanisms for very large datasets

**Slow transmission**
  - Enable compression if not already active
  - Increase chunk size for stable networks
  - Use pipeline operations for batch transfers

**Chunks failing to reassemble**
  - Check server logs for error messages
  - Verify network stability
  - Ensure sufficient server memory
