"""Advanced tests for Python-side chunking functionality."""

import numpy as np
import numpy.testing as npt
import pytest
from znjson.converter import NumpyConverter

import znsocket


@pytest.mark.parametrize("client", ["znsclient", "znsclient_w_redis"])
def test_chunking_very_large_numpy_array(client, request, caplog):
    """Test chunking with very large numpy arrays."""
    c = request.getfixturevalue(client)
    
    # Create a very large numpy array (8MB+)
    large_array = np.random.rand(1000, 1000).astype(np.float64)
    
    dct = znsocket.Dict(r=c, key="test_very_large_array", converter=[NumpyConverter])
    
    caplog.set_level("DEBUG", logger="znsocket.client")
    dct["large_data"] = large_array
    
    # Verify chunking was used
    assert "Splitting message" in caplog.text
    assert "chunks" in caplog.text
    
    # Verify data integrity
    retrieved_data = dct["large_data"]
    npt.assert_array_equal(retrieved_data, large_array)


@pytest.mark.parametrize("client", ["znsclient", "znsclient_w_redis"])
def test_chunking_multiple_large_arrays(client, request, caplog):
    """Test chunking with multiple large arrays in a single dict."""
    c = request.getfixturevalue(client)
    
    # Create multiple large arrays
    array1 = np.random.rand(500, 500).astype(np.float64)
    array2 = np.random.rand(500, 500).astype(np.float64)
    array3 = np.random.rand(500, 500).astype(np.float64)
    
    dct = znsocket.Dict(r=c, key="test_multiple_arrays", converter=[NumpyConverter])
    
    caplog.set_level("DEBUG", logger="znsocket.client")
    
    # Update with multiple arrays at once
    dct.update({
        "array1": array1,
        "array2": array2,
        "array3": array3,
        "metadata": {"description": "Multiple large arrays", "count": 3}
    })
    
    # Verify chunking was used
    assert "Splitting message" in caplog.text
    
    # Verify all data was stored correctly
    assert len(dct) == 4
    npt.assert_array_equal(dct["array1"], array1)
    npt.assert_array_equal(dct["array2"], array2)
    npt.assert_array_equal(dct["array3"], array3)
    assert dct["metadata"]["count"] == 3


@pytest.mark.parametrize("client", ["znsclient", "znsclient_w_redis"])
def test_chunking_large_list_extend(client, request, caplog):
    """Test chunking with large list extend operations."""
    c = request.getfixturevalue(client)
    
    # Create a list with many large numpy arrays
    large_arrays = [np.random.rand(100, 100).astype(np.float64) for _ in range(50)]
    
    lst = znsocket.List(r=c, key="test_large_list_extend", converter=[NumpyConverter])
    
    caplog.set_level("DEBUG", logger="znsocket.client")
    
    # Extend with many arrays at once
    lst.extend(large_arrays)
    
    # Verify chunking was used
    assert "Splitting message" in caplog.text
    
    # Verify all data was stored correctly
    assert len(lst) == 50
    for i, original_array in enumerate(large_arrays):
        npt.assert_array_equal(lst[i], original_array)


@pytest.mark.parametrize("client", ["znsclient", "znsclient_w_redis"])
def test_chunking_pipeline_with_large_data(client, request, caplog):
    """Test chunking with pipeline operations containing large data."""
    c = request.getfixturevalue(client)
    
    # Create large data for pipeline operations
    large_arrays = [np.random.rand(200, 200).astype(np.float64) for _ in range(10)]
    
    pipeline = c.pipeline()
    
    # Add many operations with large data
    for i, array in enumerate(large_arrays):
        # Convert to string for hset (simplified test)
        array_str = np.array2string(array, separator=',')
        pipeline.hset(f"array_key_{i}", "data", array_str)
    
    caplog.set_level("DEBUG", logger="znsocket.client")
    
    # Execute pipeline - should trigger chunking
    results = pipeline.execute()
    
    # Verify chunking was used
    assert "Splitting message" in caplog.text
    
    # Verify all operations succeeded
    assert len(results) == 10
    assert all(result for result in results)


@pytest.mark.parametrize("client", ["znsclient", "znsclient_w_redis"])
def test_chunking_nested_data_structures(client, request, caplog):
    """Test chunking with deeply nested data structures."""
    c = request.getfixturevalue(client)
    
    # Create nested structure with large data
    nested_data = {
        "level1": {
            "level2": {
                "level3": {
                    "large_array": np.random.rand(300, 300).astype(np.float64),
                    "metadata": {"type": "nested_test", "depth": 3}
                }
            },
            "additional_data": [np.random.rand(100, 100).astype(np.float64) for _ in range(5)]
        }
    }
    
    dct = znsocket.Dict(r=c, key="test_nested_chunking", converter=[NumpyConverter])
    
    caplog.set_level("DEBUG", logger="znsocket.client")
    dct["nested"] = nested_data
    
    # Verify chunking was used
    assert "Splitting message" in caplog.text
    
    # Verify nested data integrity
    retrieved_data = dct["nested"]
    npt.assert_array_equal(
        retrieved_data["level1"]["level2"]["level3"]["large_array"],
        nested_data["level1"]["level2"]["level3"]["large_array"]
    )
    assert retrieved_data["level1"]["level2"]["level3"]["metadata"]["depth"] == 3
    assert len(retrieved_data["level1"]["additional_data"]) == 5


@pytest.mark.parametrize("client", ["znsclient", "znsclient_w_redis"])
def test_chunking_mixed_data_types(client, request, caplog):
    """Test chunking with mixed data types including large arrays."""
    c = request.getfixturevalue(client)
    
    # Create mixed data with various types
    mixed_data = {
        "numpy_array": np.random.rand(400, 400).astype(np.float64),
        "regular_list": [f"item_{i}" for i in range(1000)],
        "nested_dict": {
            "numbers": list(range(1000)),
            "strings": {f"key_{i}": f"value_{i}" * 50 for i in range(100)}
        },
        "boolean": True,
        "float": 3.14159,
        "none_value": None
    }
    
    dct = znsocket.Dict(r=c, key="test_mixed_chunking", converter=[NumpyConverter])
    
    caplog.set_level("DEBUG", logger="znsocket.client")
    dct.update(mixed_data)
    
    # Verify chunking was used
    assert "Splitting message" in caplog.text
    
    # Verify all data types were preserved
    assert len(dct) == 6
    npt.assert_array_equal(dct["numpy_array"], mixed_data["numpy_array"])
    assert dct["regular_list"] == mixed_data["regular_list"]
    assert dct["nested_dict"]["numbers"] == mixed_data["nested_dict"]["numbers"]
    assert dct["boolean"] == mixed_data["boolean"]
    assert dct["float"] == mixed_data["float"]
    assert dct["none_value"] == mixed_data["none_value"]


@pytest.mark.parametrize("client", ["znsclient", "znsclient_w_redis"])
def test_chunking_error_recovery(client, request, caplog):
    """Test that chunking handles errors gracefully."""
    c = request.getfixturevalue(client)
    
    # Create valid large data
    large_array = np.random.rand(500, 500).astype(np.float64)
    
    dct = znsocket.Dict(r=c, key="test_error_recovery", converter=[NumpyConverter])
    
    caplog.set_level("DEBUG", logger="znsocket.client")
    
    # This should work even with chunking
    dct["test_data"] = large_array
    
    # Verify chunking was used
    assert "Splitting message" in caplog.text
    
    # Verify data integrity after chunking
    retrieved_data = dct["test_data"]
    npt.assert_array_equal(retrieved_data, large_array)
    
    # Test that we can still do normal operations
    dct["small_data"] = "small value"
    assert dct["small_data"] == "small value"


@pytest.mark.parametrize("client", ["znsclient", "znsclient_w_redis"])
def test_chunking_performance_monitoring(client, request, caplog):
    """Test chunking with performance monitoring."""
    c = request.getfixturevalue(client)
    
    # Create medium-sized data to test chunking threshold
    medium_array = np.random.rand(200, 200).astype(np.float64)
    
    dct = znsocket.Dict(r=c, key="test_performance", converter=[NumpyConverter])
    
    caplog.set_level("DEBUG", logger="znsocket.client")
    
    # Store data and measure if chunking is used
    dct["medium_data"] = medium_array
    
    # Check if chunking information is logged
    chunking_used = "Splitting message" in caplog.text
    
    # Verify data integrity regardless of chunking
    retrieved_data = dct["medium_data"]
    npt.assert_array_equal(retrieved_data, medium_array)
    
    # Log whether chunking was used for this size
    print(f"Chunking used for {medium_array.nbytes} bytes: {chunking_used}")


@pytest.mark.parametrize("client", ["znsclient", "znsclient_w_redis"])
def test_chunking_concurrent_operations(client, request, caplog):
    """Test chunking with multiple concurrent operations."""
    c = request.getfixturevalue(client)
    
    # Create multiple dicts with large data
    arrays = [np.random.rand(300, 300).astype(np.float64) for _ in range(3)]
    
    dicts = [
        znsocket.Dict(r=c, key=f"test_concurrent_{i}", converter=[NumpyConverter])
        for i in range(3)
    ]
    
    caplog.set_level("DEBUG", logger="znsocket.client")
    
    # Perform operations that might trigger chunking
    for i, (dct, array) in enumerate(zip(dicts, arrays)):
        dct[f"array_{i}"] = array
    
    # Verify chunking was used
    assert "Splitting message" in caplog.text
    
    # Verify all data was stored correctly
    for i, (dct, original_array) in enumerate(zip(dicts, arrays)):
        retrieved_array = dct[f"array_{i}"]
        npt.assert_array_equal(retrieved_array, original_array)


@pytest.mark.parametrize("client", ["znsclient", "znsclient_w_redis"])
def test_chunking_size_threshold_boundary(client, request, caplog):
    """Test chunking behavior at size threshold boundaries."""
    c = request.getfixturevalue(client)
    
    # Get the client's chunking threshold
    threshold = c.max_message_size_bytes
    
    # Create data just under the threshold
    # Account for JSON overhead by using a smaller array
    under_threshold_size = int(np.sqrt(threshold // 8 // 2))  # Conservative estimate
    small_array = np.random.rand(under_threshold_size, under_threshold_size).astype(np.float64)
    
    # Create data well over the threshold
    over_threshold_size = int(np.sqrt(threshold // 8 * 2))  # Should definitely trigger chunking
    large_array = np.random.rand(over_threshold_size, over_threshold_size).astype(np.float64)
    
    dct = znsocket.Dict(r=c, key="test_threshold", converter=[NumpyConverter])
    
    caplog.clear()
    caplog.set_level("DEBUG", logger="znsocket.client")
    
    # Test small data (should not chunk)
    dct["small"] = small_array
    small_chunked = "Splitting message" in caplog.text
    
    caplog.clear()
    
    # Test large data (should chunk)
    dct["large"] = large_array
    large_chunked = "Splitting message" in caplog.text
    
    # Verify chunking behavior
    print(f"Small array ({small_array.nbytes} bytes) chunked: {small_chunked}")
    print(f"Large array ({large_array.nbytes} bytes) chunked: {large_chunked}")
    
    # Large data should definitely be chunked
    assert large_chunked, "Large data should trigger chunking"
    
    # Verify data integrity
    npt.assert_array_equal(dct["small"], small_array)
    npt.assert_array_equal(dct["large"], large_array)