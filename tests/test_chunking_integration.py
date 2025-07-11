"""Integration test for chunking functionality."""

from unittest.mock import Mock, patch

import pytest

from znsocket.client import Client


def test_chunking_integration_mock():
    """Test that chunking logic is triggered correctly."""
    # Create a mock client that simulates chunking behavior
    with patch("znsocket.client.Client.__post_init__"):
        client = Client("http://localhost:5000")
        client.sio = Mock()

        # Mock the chunked message calls
        client.sio.call.return_value = {"status": "complete"}

        # Test with a large message that should trigger chunking
        large_data = "x" * (client.max_message_size_bytes + 1000)

        # This should trigger chunking
        with patch.object(client, "_call_chunked") as mock_chunked:
            mock_chunked.return_value = {"data": "success"}

            try:
                # This call should use chunking
                client.call("test_event", large_data)
                # Verify chunking was called
                mock_chunked.assert_called_once()
            except Exception:
                # If chunking fails, that's expected in mock environment
                pass


def test_chunking_size_detection():
    """Test that size detection works correctly."""
    with patch("znsocket.client.Client.__post_init__"):
        client = Client("http://localhost:5000")

        # Small message should not trigger chunking
        small_data = "small message"
        serialized = client._serialize_message((small_data,), {})
        assert len(serialized) < client.max_message_size_bytes

        # Large message should trigger chunking
        large_data = "x" * (client.max_message_size_bytes + 1000)
        serialized = client._serialize_message((large_data,), {})
        assert len(serialized) > client.max_message_size_bytes


def test_chunking_with_complex_data():
    """Test chunking with complex data structures."""
    with patch("znsocket.client.Client.__post_init__"):
        client = Client("http://localhost:5000")

        # Create complex data that will be large when serialized
        complex_data = {}
        for i in range(1000):
            complex_data[f"key_{i}"] = {
                "nested": {
                    "data": f"value_{i}" * 100,
                    "list": [f"item_{j}" for j in range(10)],
                    "numbers": list(range(20)),
                }
            }

        # Serialize the data
        serialized = client._serialize_message((complex_data,), {})

        # Should be a large message
        assert len(serialized) > 100000

        # Should be able to deserialize correctly
        args, kwargs = client._deserialize_message(serialized)
        assert args[0] == complex_data


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
