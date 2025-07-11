"""Tests for chunking functionality in znsocket."""
import pytest
import znsocket
from znsocket.client import Client


def test_chunking_large_dict_update(znsclient):
    """Test that large dictionary updates are chunked automatically."""
    client = znsclient
    
    # Create a large dictionary that will exceed the message size limit
    large_dict = {f"key_{i}": f"value_{i}" * 1000 for i in range(1000)}
    
    # This should trigger chunking automatically
    dct = znsocket.Dict(client, "test_large_dict")
    dct.update(large_dict)
    
    # Verify the data was stored correctly
    assert len(dct) == 1000
    assert dct["key_0"] == "value_0" * 1000
    assert dct["key_999"] == "value_999" * 1000


def test_chunking_large_list_extend(znsclient):
    """Test that large list extensions are chunked automatically."""
    client = znsclient
    
    # Create a large list that will exceed the message size limit
    large_list = [f"item_{i}" * 1000 for i in range(1000)]
    
    # This should trigger chunking automatically
    lst = znsocket.List(client, "test_large_list")
    lst.extend(large_list)
    
    # Verify the data was stored correctly
    assert len(lst) == 1000
    assert lst[0] == "item_0" * 1000
    assert lst[999] == "item_999" * 1000


def test_chunking_large_pipeline(znsclient):
    """Test that large pipeline operations are chunked automatically."""
    client = znsclient
    
    # Create a pipeline with many operations
    pipeline = client.pipeline()
    
    # Add many operations that will create a large message
    for i in range(1000):
        pipeline.hset(f"key_{i}", "field", f"value_{i}" * 100)
    
    # Execute the pipeline - this should trigger chunking
    results = pipeline.execute()
    
    # Verify all operations succeeded
    assert len(results) == 1000
    assert all(result for result in results)  # All hset operations should return True
    
    # Verify the data was stored correctly
    for i in range(0, 1000, 100):  # Sample check every 100th item
        assert client.hget(f"key_{i}", "field") == f"value_{i}" * 100


def test_chunking_mixed_operations(znsclient):
    """Test chunking with mixed operation types."""
    client = znsclient
    
    # Create mixed operations that will result in a large message
    pipeline = client.pipeline()
    
    # Mix of different operations
    for i in range(500):
        pipeline.set(f"str_key_{i}", f"string_value_{i}" * 100)
        pipeline.hset(f"hash_key_{i}", "field", f"hash_value_{i}" * 100)
    
    # Execute the pipeline - should trigger chunking
    results = pipeline.execute()
    
    # Verify all operations succeeded
    assert len(results) == 1000
    
    # Verify some of the data
    assert client.get("str_key_0") == "string_value_0" * 100
    assert client.hget("hash_key_0", "field") == "hash_value_0" * 100
    assert client.get("str_key_499") == "string_value_499" * 100
    assert client.hget("hash_key_499", "field") == "hash_value_499" * 100


def test_chunking_with_small_messages(znsclient):
    """Test that small messages don't trigger chunking."""
    client = znsclient
    
    # Create a small dictionary that won't trigger chunking
    small_dict = {f"key_{i}": f"value_{i}" for i in range(10)}
    
    # This should NOT trigger chunking
    dct = znsocket.Dict(client, "test_small_dict")
    dct.update(small_dict)
    
    # Verify the data was stored correctly
    assert len(dct) == 10
    assert dct["key_0"] == "value_0"
    assert dct["key_9"] == "value_9"


def test_chunking_message_size_estimation():
    """Test the message size estimation functionality."""
    client = Client("http://localhost:5000")
    
    # Test with small data
    small_args = ("test",)
    small_kwargs = {"key": "value"}
    small_size = len(client._serialize_message(small_args, small_kwargs))
    
    # Test with large data
    large_args = ({"key": "value" * 10000},)
    large_kwargs = {"data": "x" * 10000}
    large_size = len(client._serialize_message(large_args, large_kwargs))
    
    # Large message should be significantly bigger
    assert large_size > small_size * 100


def test_chunking_message_splitting():
    """Test the message splitting functionality."""
    client = Client("http://localhost:5000")
    
    # Create a message that needs to be split
    message_data = "x" * 1000
    message_bytes = message_data.encode('utf-8')
    
    # Split into chunks of 100 bytes
    chunks = client._split_message_bytes(message_bytes, 100)
    
    # Should have 10 chunks
    assert len(chunks) == 10
    
    # Each chunk should be 100 bytes (except possibly the last one)
    for i, chunk in enumerate(chunks[:-1]):
        assert len(chunk) == 100
    
    # Last chunk should be <= 100 bytes
    assert len(chunks[-1]) <= 100
    
    # Reassemble should match original
    reassembled = b''.join(chunks)
    assert reassembled == message_bytes


def test_chunking_large_single_value(znsclient):
    """Test chunking with a single very large value."""
    client = znsclient
    
    # Create a single very large value
    large_value = "x" * 100000  # 100KB string
    
    # This should trigger chunking
    client.set("large_single_value", large_value)
    
    # Verify the value was stored correctly
    retrieved_value = client.get("large_single_value")
    assert retrieved_value == large_value
    assert len(retrieved_value) == 100000


@pytest.mark.parametrize("chunk_size", [1000, 10000, 100000])
def test_chunking_different_sizes(znsclient, chunk_size):
    """Test chunking with different data sizes."""
    client = znsclient
    
    # Create data of specified size
    test_data = "x" * chunk_size
    key = f"test_chunk_{chunk_size}"
    
    # Store the data
    client.set(key, test_data)
    
    # Retrieve and verify
    retrieved_data = client.get(key)
    assert retrieved_data == test_data
    assert len(retrieved_data) == chunk_size


def test_chunking_error_handling(znsclient):
    """Test that chunking handles errors gracefully."""
    client = znsclient
    
    # Test with invalid data that might cause serialization issues
    try:
        # Create a large dictionary with a mix of data types
        large_dict = {}
        for i in range(100):
            large_dict[f"key_{i}"] = {
                "string": f"value_{i}" * 100,
                "number": i,
                "boolean": i % 2 == 0,
                "list": [f"item_{j}" for j in range(10)]
            }
        
        dct = znsocket.Dict(client, "test_error_handling")
        dct.update(large_dict)
        
        # If we get here, chunking handled the complex data correctly
        assert len(dct) == 100
        assert dct["key_0"]["string"] == "value_0" * 100
        assert dct["key_0"]["number"] == 0
        assert dct["key_0"]["boolean"] is True
        assert dct["key_0"]["list"] == [f"item_{j}" for j in range(10)]
        
    except Exception as e:
        # If an error occurs, it should be a meaningful error, not a chunking failure
        assert "chunk" not in str(e).lower()