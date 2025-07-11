Copy-on-Write Operations
========================

ZnSocket provides efficient copy-on-write functionality through two primary mechanisms: Segments and List fallbacks. These features allow you to create logical copies of large datasets while only storing the differences, making them ideal for scenarios where you need to modify a few elements of a large collection without duplicating the entire dataset.

Overview
--------

Copy-on-write (COW) is a resource management technique where data is not physically copied until it's modified. ZnSocket implements this pattern to enable:

- **Memory efficiency**: Only modified elements consume additional storage
- **Performance**: Fast "copying" operations that don't duplicate data
- **Flexibility**: Ability to create multiple variations of a dataset
- **Data integrity**: Original data remains unchanged

Implementation Approaches
-------------------------

ZnSocket offers two main approaches for copy-on-write operations:

1. **Segments**: Purpose-built for copy-on-write with piece table architecture
2. **List Fallbacks**: Using fallback mechanisms for transparent copy-on-write

Segments: True Copy-on-Write
----------------------------

Segments provide the most efficient copy-on-write implementation using a piece table data structure.

Basic Usage
~~~~~~~~~~~

.. code-block:: python

    import znsocket
    from znjson.converter import NumpyConverter

    client = znsocket.Client("http://localhost:5000")

    # Create original dataset
    original_data = znsocket.List(r=client, key="dataset", converter=[NumpyConverter])
    original_data.extend([
        {"name": "item_0", "value": 100, "metadata": {"type": "A"}},
        {"name": "item_1", "value": 200, "metadata": {"type": "B"}},
        {"name": "item_2", "value": 300, "metadata": {"type": "C"}},
    ])

    # Create copy-on-write view using Segments
    dataset_copy = znsocket.Segments(
        r=client,
        origin=original_data,    # Reference to original data
        key="dataset_copy"       # Key for storing modifications
    )

    # Access works transparently
    print(f"Length: {len(dataset_copy)}")          # 3
    print(f"First item: {dataset_copy[0]}")        # From original
    print(f"Second item: {dataset_copy[1]}")       # From original

    # Modify a single element
    modified_item = dict(dataset_copy[1])           # Convert to regular dict
    modified_item["value"] = 999
    modified_item["metadata"]["modified"] = True
    dataset_copy[1] = modified_item

    # Now only the modified element is stored separately
    print(f"Modified item: {dataset_copy[1]}")     # Modified version
    print(f"Original unchanged: {original_data[1]}") # Original version
    print(f"Other items unchanged: {dataset_copy[0]}") # Still from original

Advanced Segments Usage
~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

    # Working with complex nested data
    complex_data = [
        {
            "id": i,
            "config": {
                "parameters": {"learning_rate": 0.01, "epochs": 100},
                "metadata": {"created": "2025-01-01", "version": "1.0"}
            },
            "results": {"accuracy": 0.95, "loss": 0.05}
        }
        for i in range(1000)  # Large dataset
    ]

    original_experiments = znsocket.List(r=client, key="experiments")
    original_experiments.extend(complex_data)

    # Create experimental variation
    experiment_variant = znsocket.Segments(
        r=client,
        origin=original_experiments,
        key="experiment_variant_a"
    )

    # Modify specific experiments
    for exp_id in [10, 50, 100]:
        experiment = dict(experiment_variant[exp_id])
        experiment["config"]["parameters"]["learning_rate"] = 0.001  # Different LR
        experiment["metadata"] = {"variant": "low_lr", "modified": True}
        experiment_variant[exp_id] = experiment

    # Only 3 modified experiments are stored, 997 reference original
    print(f"Total experiments: {len(experiment_variant)}")  # 1000
    print(f"Modified experiment: {experiment_variant[10]['metadata']['variant']}")  # "low_lr"
    print(f"Original unchanged: {original_experiments[10]['metadata']['version']}")  # "1.0"

List Fallbacks: Transparent Copy-on-Write
------------------------------------------

List fallbacks provide copy-on-write behavior using ZnSocket's built-in fallback mechanism.

Basic Fallback Usage
~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

    # Create original dataset
    original_data = znsocket.List(r=client, key="original_dataset")
    original_data.extend([
        {"name": "sample_0", "score": 10},
        {"name": "sample_1", "score": 20},
        {"name": "sample_2", "score": 30},
    ])

    # Create copy using fallback mechanism
    dataset_copy = znsocket.List(
        r=client,
        key="dataset_copy",
        fallback="original_dataset",     # Fall back to original
        fallback_policy="frozen",        # Read-only fallback
        converter=[NumpyConverter]
    )

    # Access transparently falls back to original
    print(f"Copy length: {len(dataset_copy)}")     # 3 (from fallback)
    print(f"First item: {dataset_copy[0]}")        # From original

    # Modify element - triggers copy-on-write
    modified_item = dict(dataset_copy[1])
    modified_item["score"] = 999
    modified_item["source"] = "modified"
    dataset_copy[1] = modified_item

    # Copy-on-write behavior activated
    print(f"Modified: {dataset_copy[1]['score']}")      # 999
    print(f"Original: {original_data[1]['score']}")     # 20 (unchanged)
    print(f"Fallback still works: {dataset_copy[0]}")   # From original

Fallback Policies
~~~~~~~~~~~~~~~~~

.. code-block:: python

    # Frozen policy: Read-only fallback, copy-on-write for modifications
    frozen_copy = znsocket.List(
        r=client,
        key="frozen_copy",
        fallback="original_dataset",
        fallback_policy="frozen"
    )

    # Copy policy: Full copy of fallback data on initialization
    full_copy = znsocket.List(
        r=client,
        key="full_copy",
        fallback="original_dataset",
        fallback_policy="copy"
    )

JavaScript Integration
----------------------

Copy-on-write operations work seamlessly with the JavaScript client:

.. code-block:: javascript

    import { createClient, List, Segments } from 'znsocket';

    const client = createClient({ url: 'znsocket://127.0.0.1:5000' });
    await client.connect();

    // Work with the copy-on-write data from JavaScript
    const datasetCopy = new List({ client, key: 'dataset_copy' });

    // Access data (will use fallback or modifications as appropriate)
    const length = await datasetCopy.length();
    const firstItem = await datasetCopy.get(0);
    const modifiedItem = await datasetCopy.get(1);

    // Modify from JavaScript side
    await datasetCopy.set(2, {
        name: 'js_modified',
        score: 777,
        source: 'javascript'
    });

    // Use slice operations
    const subset = await datasetCopy.slice(0, 2);
    console.log('First two items:', subset);

Cross-Language Copy-on-Write Example
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

This example demonstrates copy-on-write behavior across Python and JavaScript:

**Python side (creating original data and modifications):**

.. code-block:: python

    import znsocket

    client = znsocket.Client("http://localhost:5000")

    # Create original dataset with Dict objects
    lst = znsocket.List(r=client, key="test:data")

    data = [
        {"value": [1, 2, 3]},
        {"value": [4, 5, 6]},
        {"value": [7, 8, 9]},
        {"value": [10, 11, 12]},
    ]

    # Use pipeline for efficient batch operations
    p = client.pipeline()
    msg = []
    for idx, value in enumerate(data):
        atoms_dict = znsocket.Dict(r=p, key=f"test:data/{idx}")
        for k, v in value.items():
            atoms_dict[k] = v
        msg.append(atoms_dict)
    p.execute()
    lst.extend(msg)

    # Create copy-on-write view using Segments
    segments = znsocket.Segments(r=client, origin=lst, key="test:data/segments")

    # Modify a single element (copy-on-write)
    value_to_modify = segments[2]
    modified_value = value_to_modify.copy("test:data/segments/2")
    modified_value["value"] = [100, 200, 300]
    segments[2] = modified_value

    # Original list remains unchanged: lst[2]["value"] == [7, 8, 9]
    # Segments shows modification: segments[2]["value"] == [100, 200, 300]

**JavaScript side (accessing and extending modifications):**

.. code-block:: javascript

    import { createClient, Dict, List } from 'znsocket';

    const client = createClient({ url: 'znsocket://127.0.0.1:5000' });
    await client.connect();

    // Access the original data created by Python
    const lst = new List({ client, key: 'test:data' });

    // Verify original data is accessible
    const item2 = await lst.get(2);
    console.log(await item2.get('value')); // [7, 8, 9] - original unchanged

    // Access Python's copy-on-write modification
    const modifiedSegment = new Dict({ client, key: 'test:data/segments/2' });
    console.log(await modifiedSegment.get('value')); // [100, 200, 300] - Python modification

    // Create JavaScript-side copy-on-write modification
    const jsModified = new Dict({ client, key: 'test:data/js_copy/1' });
    await jsModified.clear();
    await jsModified.set('value', [400, 500, 600]);
    await jsModified.set('modified_by', 'javascript');

    // Verify copy-on-write behavior
    const originalItem1 = await lst.get(1);
    console.log(await originalItem1.get('value')); // [4, 5, 6] - still original
    console.log(await jsModified.get('value')); // [400, 500, 600] - JS modification

    // Both languages can work with the same logical dataset
    // while maintaining independent modifications

ListAdapter + Segments Integration
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

This example demonstrates copy-on-write with ListAdapter and Segments across languages:

**Python side (ListAdapter setup and Segments modifications):**

.. code-block:: python

    import znsocket

    client = znsocket.Client("http://localhost:5000")

    # Start with a regular Python list
    original_data = [
        {"name": "item_0", "score": 85, "category": "A"},
        {"name": "item_1", "score": 92, "category": "B"},
        {"name": "item_2", "score": 78, "category": "A"},
        {"name": "item_3", "score": 96, "category": "C"},
        {"name": "item_4", "score": 83, "category": "B"},
    ]

    # Use ListAdapter to expose Python list via ZnSocket
    znsocket.ListAdapter(
        socket=client,
        key="test:adapter_data",
        object=original_data
    )

    # Create a List view of the adapted data
    lst = znsocket.List(r=client, key="test:adapter_data")

    # Create copy-on-write view using Segments
    segments = znsocket.Segments(
        r=client,
        origin=lst,
        key="test:adapter_segments"
    )

    # Create modified versions using copy-on-write
    modified_dict = znsocket.Dict(r=client, key="test:adapter_segments/2")
    modified_dict.clear()
    modified_dict.update({
        "name": "item_2_modified",
        "score": 95,
        "category": "A+",
        "modified": True,
        "source": "segments_copy"
    })
    segments[2] = modified_dict

    # Original Python list remains unchanged: original_data[2]["score"] == 78
    # Adapter list remains unchanged: lst[2]["score"] == 78
    # Segments shows modification: segments[2]["score"] == 95

**JavaScript side (accessing adapter data and creating more modifications):**

.. code-block:: javascript

    import { createClient, Dict, List } from 'znsocket';

    const client = createClient({ url: 'znsocket://127.0.0.1:5000' });
    await client.connect();

    // Access the ListAdapter data from JavaScript
    const lst = new List({ client, key: 'test:adapter_data' });

    // Verify original adapter data is accessible
    const originalItems = [];
    for (let i = 0; i < await lst.length(); i++) {
        originalItems.push(await lst.get(i));
    }
    console.log('Original adapter data:', originalItems);

    // Access Python's segment modification
    const pythonModified = new Dict({ client, key: 'test:adapter_segments/2' });
    console.log('Python modification:', {
        name: await pythonModified.get('name'),      // "item_2_modified"
        score: await pythonModified.get('score'),    // 95
        source: await pythonModified.get('source')   // "segments_copy"
    });

    // Create JavaScript-side segment modification
    const jsModified = new Dict({ client, key: 'test:adapter_segments/1_js' });
    await jsModified.clear();
    await jsModified.set('name', 'item_1_js_enhanced');
    await jsModified.set('score', 100);
    await jsModified.set('category', 'S+');
    await jsModified.set('enhanced_by', 'javascript');

    // Verify copy-on-write behavior across languages
    const stillOriginal = await lst.get(1);
    console.log('Original unchanged:', stillOriginal.score);    // 92
    console.log('JS modification:', await jsModified.get('score')); // 100

    // Both Python list, ListAdapter, and all modifications coexist independently

Use Cases and Patterns
----------------------

Scientific Computing
~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

    # Large simulation dataset
    base_simulation = znsocket.List(r=client, key="base_sim")
    # ... populate with expensive simulation results

    # Create parameter variations
    high_temp_sim = znsocket.Segments(r=client, origin=base_simulation, key="high_temp")
    low_pressure_sim = znsocket.Segments(r=client, origin=base_simulation, key="low_pressure")

    # Modify only specific conditions
    for i in temperature_sensitive_indices:
        result = dict(high_temp_sim[i])
        result["temperature"] = result["temperature"] * 1.2
        high_temp_sim[i] = result

Data Preprocessing Pipelines
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

    # Raw dataset
    raw_data = znsocket.List(r=client, key="raw_data")

    # Create preprocessing variants
    normalized_data = znsocket.Segments(r=client, origin=raw_data, key="normalized")
    filtered_data = znsocket.Segments(r=client, origin=raw_data, key="filtered")

    # Apply transformations only where needed
    for i, sample in enumerate(raw_data):
        if sample["quality_score"] < threshold:
            cleaned_sample = preprocess(sample)
            filtered_data[i] = cleaned_sample

A/B Testing and Experimentation
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: python

    # Base configuration
    base_config = znsocket.List(r=client, key="base_config")

    # Create test variants
    variant_a = znsocket.List(
        r=client,
        key="variant_a",
        fallback="base_config",
        fallback_policy="frozen"
    )

    variant_b = znsocket.List(
        r=client,
        key="variant_b",
        fallback="base_config",
        fallback_policy="frozen"
    )

    # Modify only test parameters
    variant_a[config_index] = {"feature_x": True, "algorithm": "new_algo"}
    variant_b[config_index] = {"feature_x": False, "algorithm": "baseline"}

Performance Considerations
--------------------------

Storage Efficiency
~~~~~~~~~~~~~~~~~~

- **Segments**: Only modified elements consume additional storage
- **List Fallbacks**: Modified elements stored in new key, rest referenced
- **Memory Usage**: Minimal overhead for unchanged data

Access Patterns
~~~~~~~~~~~~~~~

- **Read Operations**: Efficient fallback to original data
- **Write Operations**: Copy-on-write triggers only for modified elements
- **Slice Operations**: Supported across both original and modified data

Network Efficiency
~~~~~~~~~~~~~~~~~~

- **Large Datasets**: Only deltas transmitted over network
- **Batch Operations**: Modifications can be batched for efficiency
- **Compression**: Automatic compression for large modifications

Best Practices
--------------

1. **Choose the Right Approach**

   - Use **Segments** for true copy-on-write with maximum efficiency
   - Use **List Fallbacks** for simpler scenarios with automatic fallback

2. **Data Structure Design**

   .. code-block:: python

       # Good: Structured data that can be selectively modified
       structured_data = {
           "metadata": {...},
           "parameters": {...},
           "results": {...}
       }

       # Avoid: Monolithic structures that require full replacement
       monolithic_data = "large_serialized_blob"

3. **Modification Patterns**

   .. code-block:: python

       # Good: Modify copy of original data
       original_item = dict(copy_segments[index])
       original_item["field"] = new_value
       copy_segments[index] = original_item

       # Avoid: Direct mutation (may not trigger copy-on-write)
       copy_segments[index]["field"] = new_value  # Problematic

4. **Key Management**

   .. code-block:: python

       # Good: Descriptive keys for tracking variants
       experiment_high_lr = znsocket.Segments(r=client, origin=base, key="exp_high_lr_v1")

       # Good: Use timestamps or IDs for versioning
       variant_key = f"experiment_{experiment_id}_{timestamp}"

Error Handling and Edge Cases
-----------------------------

.. code-block:: python

    # Handle missing original data
    try:
        copy_segments = znsocket.Segments(r=client, origin=original_list, key="copy")
    except Exception as e:
        print(f"Original data not available: {e}")
        # Create new list or handle gracefully

    # Check if fallback is available
    copy_list = znsocket.List(r=client, key="copy", fallback="original", fallback_policy="frozen")
    if len(copy_list) == 0:
        print("No data available from fallback")

    # Verify data integrity
    assert copy_segments[unchanged_index] == original_list[unchanged_index]
    assert copy_segments[modified_index] != original_list[modified_index]

Troubleshooting
---------------

Common Issues
~~~~~~~~~~~~~

**Fallback not working**
  - Verify fallback key exists and contains data
  - Check fallback_policy is set correctly
  - Ensure original data is populated before creating copy

**Modifications not persisting**
  - Confirm you're modifying a copy of the data, not the original reference
  - Use dict() conversion for complex objects before modification
  - Verify the key has write permissions

**Performance issues**
  - Monitor the number of modified elements vs. total elements
  - Consider batching modifications for large datasets
  - Use appropriate chunking for very large copy operations

**Memory usage concerns**
  - Profile actual storage usage vs. expected savings
  - Consider cleanup of unused copy variants
  - Monitor original data lifecycle and copy dependencies
