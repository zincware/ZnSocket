"""Stress tests and edge cases for Python chunking functionality."""

import numpy as np
import pytest
import time
from znjson.converter import NumpyConverter

import znsocket


@pytest.mark.slow
@pytest.mark.parametrize("client", ["znsclient", "znsclient_w_redis"])
def test_chunking_stress_many_operations(client, request, caplog):
    """Stress test with many chunked operations."""
    c = request.getfixturevalue(client)
    
    # Create multiple dicts with large data
    num_dicts = 5
    arrays_per_dict = 10
    
    caplog.set_level("DEBUG", logger="znsocket.client")
    
    start_time = time.time()
    
    for dict_idx in range(num_dicts):
        dct = znsocket.Dict(
            r=c, 
            key=f"stress_test_dict_{dict_idx}", 
            converter=[NumpyConverter]
        )
        
        # Create multiple arrays per dict
        arrays = {
            f"array_{i}": np.random.rand(200, 200).astype(np.float64) 
            for i in range(arrays_per_dict)
        }
        
        # This should trigger chunking
        dct.update(arrays)
        
        # Verify a few arrays were stored correctly
        assert len(dct) == arrays_per_dict
        np.testing.assert_array_equal(dct["array_0"], arrays["array_0"])
        np.testing.assert_array_equal(dct[f"array_{arrays_per_dict-1}"], arrays[f"array_{arrays_per_dict-1}"])
    
    end_time = time.time()
    duration = end_time - start_time
    
    # Verify chunking was used
    assert "Splitting message" in caplog.text
    
    print(f"✅ Stress test completed: {num_dicts} dicts × {arrays_per_dict} arrays in {duration:.2f}s")


@pytest.mark.slow
@pytest.mark.parametrize("client", ["znsclient", "znsclient_w_redis"])
def test_chunking_very_large_single_item(client, request, caplog):
    """Test chunking with a single very large item."""
    c = request.getfixturevalue(client)
    
    # Create a very large array (should be much larger than chunk size)
    huge_array = np.random.rand(1500, 1500).astype(np.float64)  # ~18MB
    
    dct = znsocket.Dict(r=c, key="huge_single_item", converter=[NumpyConverter])
    
    caplog.set_level("DEBUG", logger="znsocket.client")
    
    start_time = time.time()
    dct["huge_data"] = huge_array
    end_time = time.time()
    
    # Verify chunking was used
    assert "Splitting message" in caplog.text
    
    # Verify data integrity
    retrieved_data = dct["huge_data"]
    np.testing.assert_array_equal(retrieved_data, huge_array)
    
    print(f"✅ Very large single item ({huge_array.nbytes:,} bytes) handled in {end_time - start_time:.2f}s")


@pytest.mark.parametrize("client", ["znsclient", "znsclient_w_redis"])
def test_chunking_edge_case_empty_data(client, request, caplog):
    """Test chunking behavior with empty data structures."""
    c = request.getfixturevalue(client)
    
    dct = znsocket.Dict(r=c, key="empty_data_test")
    
    caplog.set_level("DEBUG", logger="znsocket.client")
    
    # Test various empty data structures
    test_cases = {
        "empty_dict": {},
        "empty_list": [],
        "empty_string": "",
        "none_value": None,
        "empty_array": np.array([]),
    }
    
    for key, value in test_cases.items():
        dct[key] = value
    
    # These should not trigger chunking
    assert "Splitting message" not in caplog.text
    
    # Verify all empty data was stored correctly
    assert len(dct) == len(test_cases)
    for key, expected_value in test_cases.items():
        retrieved_value = dct[key]
        if isinstance(expected_value, np.ndarray):
            np.testing.assert_array_equal(retrieved_value, expected_value)
        else:
            assert retrieved_value == expected_value


@pytest.mark.parametrize("client", ["znsclient", "znsclient_w_redis"])
def test_chunking_unicode_and_special_characters(client, request, caplog):
    """Test chunking with Unicode and special characters."""
    c = request.getfixturevalue(client)
    
    # Create large data with Unicode characters
    unicode_data = {
        "emoji_data": "🚀🌟💫⭐🎯🔥💎🌈🎪🎨" * 10000,
        "multilingual": "Hello世界Привет мир🌍مرحبا বিশ্ব" * 5000,
        "special_chars": "!@#$%^&*()_+-=[]{}|;:,.<>?`~" * 8000,
        "mixed_content": {
            "text": "Testing with émojis 🧪 and spëcial chars" * 1000,
            "numbers": list(range(5000)),
            "floats": [i * 3.14159 for i in range(3000)]
        }
    }
    
    dct = znsocket.Dict(r=c, key="unicode_chunking_test")
    
    caplog.set_level("DEBUG", logger="znsocket.client")
    dct.update(unicode_data)
    
    # Should trigger chunking due to size
    assert "Splitting message" in caplog.text
    
    # Verify Unicode data integrity
    assert len(dct) == 4
    assert dct["emoji_data"] == unicode_data["emoji_data"]
    assert dct["multilingual"] == unicode_data["multilingual"]
    assert dct["special_chars"] == unicode_data["special_chars"]
    assert dct["mixed_content"]["text"] == unicode_data["mixed_content"]["text"]
    assert dct["mixed_content"]["numbers"] == unicode_data["mixed_content"]["numbers"]
    assert dct["mixed_content"]["floats"] == unicode_data["mixed_content"]["floats"]


@pytest.mark.parametrize("client", ["znsclient", "znsclient_w_redis"])
def test_chunking_boundary_conditions(client, request, caplog):
    """Test chunking at various boundary conditions."""
    c = request.getfixturevalue(client)
    
    # Get chunking threshold
    threshold = c.max_message_size_bytes
    
    # Test data sizes around the threshold
    test_sizes = [
        ("well_under", threshold // 10),
        ("under", threshold // 2),
        ("near_threshold", int(threshold * 0.9)),
        ("over_threshold", int(threshold * 1.1)),
        ("well_over", threshold * 2),
    ]
    
    dct = znsocket.Dict(r=c, key="boundary_test")
    
    for name, target_size in test_sizes:
        caplog.clear()
        caplog.set_level("DEBUG", logger="znsocket.client")
        
        # Create data of approximately target size
        # Use string data for predictable size
        approx_char_count = target_size // 2  # UTF-8 overhead approximation
        test_data = "x" * approx_char_count
        
        dct[name] = test_data
        
        chunked = "Splitting message" in caplog.text
        actual_size = len(test_data.encode('utf-8'))
        
        print(f"{name:15} | Target: {target_size:>10,} | Actual: {actual_size:>10,} | "
              f"Chunked: {'YES' if chunked else 'NO'}")
        
        # Verify data integrity
        assert dct[name] == test_data
        
        # Verify chunking behavior for clearly over-threshold data
        if target_size > threshold:
            assert chunked, f"Data of size {actual_size} should be chunked (threshold: {threshold})"


@pytest.mark.parametrize("client", ["znsclient", "znsclient_w_redis"])
def test_chunking_mixed_operations_single_session(client, request, caplog):
    """Test mixing chunked and non-chunked operations in a single session."""
    c = request.getfixturevalue(client)
    
    caplog.set_level("DEBUG", logger="znsocket.client")
    
    # Test 1: Small operation (no chunking)
    dct1 = znsocket.Dict(r=c, key="mixed_test_small")
    dct1["small_data"] = "small value"
    
    small_chunked = "Splitting message" in caplog.text
    caplog.clear()
    
    # Test 2: Large operation (chunking)
    dct2 = znsocket.Dict(r=c, key="mixed_test_large", converter=[NumpyConverter])
    large_array = np.random.rand(400, 400).astype(np.float64)
    dct2["large_data"] = large_array
    
    large_chunked = "Splitting message" in caplog.text
    caplog.clear()
    
    # Test 3: Another small operation (no chunking)
    dct3 = znsocket.Dict(r=c, key="mixed_test_small2")
    dct3["another_small"] = {"key": "value", "number": 42}
    
    small2_chunked = "Splitting message" in caplog.text
    
    # Verify chunking behavior
    assert not small_chunked, "Small operation should not be chunked"
    assert large_chunked, "Large operation should be chunked"
    assert not small2_chunked, "Second small operation should not be chunked"
    
    # Verify data integrity
    assert dct1["small_data"] == "small value"
    np.testing.assert_array_equal(dct2["large_data"], large_array)
    assert dct3["another_small"]["key"] == "value"
    assert dct3["another_small"]["number"] == 42


@pytest.mark.parametrize("client", ["znsclient", "znsclient_w_redis"])
def test_chunking_error_conditions(client, request, caplog):
    """Test chunking behavior under error conditions."""
    c = request.getfixturevalue(client)
    
    dct = znsocket.Dict(r=c, key="error_test", converter=[NumpyConverter])
    
    caplog.set_level("DEBUG", logger="znsocket.client")
    
    # Test with data that should chunk successfully
    large_array = np.random.rand(300, 300).astype(np.float64)
    
    try:
        dct["test_data"] = large_array
        
        # Verify chunking was used
        assert "Splitting message" in caplog.text
        
        # Verify data integrity
        retrieved_data = dct["test_data"]
        np.testing.assert_array_equal(retrieved_data, large_array)
        
        print("✅ Error condition test passed - chunking handled gracefully")
        
    except Exception as e:
        # If any error occurs, it should be meaningful
        assert "chunk" not in str(e).lower(), f"Chunking-related error: {e}"
        raise


@pytest.mark.parametrize("client", ["znsclient", "znsclient_w_redis"])
def test_chunking_performance_comparison(client, request, caplog):
    """Compare performance of chunked vs non-chunked operations."""
    c = request.getfixturevalue(client)
    
    # Test small data (no chunking)
    small_data = {"key": "value", "number": 42}
    dct_small = znsocket.Dict(r=c, key="perf_test_small")
    
    start_time = time.time()
    dct_small.update(small_data)
    small_time = time.time() - start_time
    
    # Test large data (chunking)
    large_data = {"array": np.random.rand(300, 300).astype(np.float64)}
    dct_large = znsocket.Dict(r=c, key="perf_test_large", converter=[NumpyConverter])
    
    caplog.set_level("DEBUG", logger="znsocket.client")
    start_time = time.time()
    dct_large.update(large_data)
    large_time = time.time() - start_time
    
    # Verify chunking was used for large data
    assert "Splitting message" in caplog.text
    
    # Verify data integrity
    assert dct_small["key"] == "value"
    assert dct_small["number"] == 42
    np.testing.assert_array_equal(dct_large["array"], large_data["array"])
    
    print(f"Performance comparison:")
    print(f"  Small data (no chunking): {small_time:.4f}s")
    print(f"  Large data (chunking):    {large_time:.4f}s")
    print(f"  Chunking overhead:        {large_time - small_time:.4f}s")
    
    # Chunking should not be prohibitively slow
    assert large_time < 30.0, "Chunking should not take more than 30 seconds"


if __name__ == "__main__":
    pytest.main([__file__, "-v", "-s"])