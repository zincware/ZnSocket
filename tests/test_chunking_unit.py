"""Unit tests for chunking functionality without server connection."""
import pytest
from unittest.mock import Mock, patch
from znsocket.client import Client


class TestChunkingUnit:
    """Unit tests for chunking methods that don't require server connection."""
    
    def create_mock_client(self):
        """Create a mock client without connecting to server."""
        with patch('znsocket.client.Client.__post_init__'):
            client = Client("http://localhost:5000")
            client.sio = Mock()
            return client
    
    def test_serialize_message(self):
        """Test message serialization."""
        client = self.create_mock_client()
        
        # Test with simple data
        args = ("test",)
        kwargs = {"key": "value"}
        serialized = client._serialize_message(args, kwargs)
        
        assert isinstance(serialized, bytes)
        assert b"test" in serialized
        assert b"key" in serialized
        assert b"value" in serialized
    
    def test_split_message_bytes(self):
        """Test message byte splitting."""
        client = self.create_mock_client()
        
        # Create a test message
        message = b"x" * 1000
        
        # Split into chunks of 100 bytes
        chunks = client._split_message_bytes(message, 100)
        
        # Should have 10 chunks
        assert len(chunks) == 10
        
        # Each chunk should be 100 bytes
        for chunk in chunks:
            assert len(chunk) == 100
        
        # Reassemble should match original
        reassembled = b''.join(chunks)
        assert reassembled == message
    
    def test_split_message_bytes_uneven(self):
        """Test message byte splitting with uneven division."""
        client = self.create_mock_client()
        
        # Create a test message that doesn't divide evenly
        message = b"x" * 1050
        
        # Split into chunks of 100 bytes
        chunks = client._split_message_bytes(message, 100)
        
        # Should have 11 chunks
        assert len(chunks) == 11
        
        # First 10 chunks should be 100 bytes
        for chunk in chunks[:10]:
            assert len(chunk) == 100
        
        # Last chunk should be 50 bytes
        assert len(chunks[-1]) == 50
        
        # Reassemble should match original
        reassembled = b''.join(chunks)
        assert reassembled == message
    
    def test_deserialize_message(self):
        """Test message deserialization."""
        client = self.create_mock_client()
        
        # Test with simple data
        original_args = ("test", 123)
        original_kwargs = {"key": "value", "number": 456}
        
        # Serialize and deserialize
        serialized = client._serialize_message(original_args, original_kwargs)
        args, kwargs = client._deserialize_message(serialized)
        
        assert args == original_args
        assert kwargs == original_kwargs
    
    def test_message_size_estimation(self):
        """Test message size estimation."""
        client = self.create_mock_client()
        
        # Small message
        small_args = ("test",)
        small_kwargs = {"key": "value"}
        small_serialized = client._serialize_message(small_args, small_kwargs)
        small_size = len(small_serialized)
        
        # Large message
        large_args = ("test" * 1000,)
        large_kwargs = {"key": "value" * 1000}
        large_serialized = client._serialize_message(large_args, large_kwargs)
        large_size = len(large_serialized)
        
        # Large message should be significantly bigger
        assert large_size > small_size * 10
    
    def test_chunking_threshold(self):
        """Test that chunking threshold is correctly set."""
        with patch('znsocket.client.Client.__post_init__'):
            client = Client("http://localhost:5000")
            # Default should be 80MB
            assert client.max_message_size_bytes == 80 * 1024 * 1024
            
            # Custom threshold
            custom_client = Client("http://localhost:5000", max_message_size_bytes=50 * 1024 * 1024)
            assert custom_client.max_message_size_bytes == 50 * 1024 * 1024
    
    def test_round_trip_serialization(self):
        """Test round-trip serialization with complex data."""
        client = self.create_mock_client()
        
        # Complex data structure
        complex_args = (
            {"nested": {"key": "value"}, "list": [1, 2, 3]},
            [{"item": i} for i in range(10)]
        )
        complex_kwargs = {
            "data": {"complex": True, "items": [f"item_{i}" for i in range(5)]},
            "metadata": {"version": "1.0", "type": "test"}
        }
        
        # Serialize and deserialize
        serialized = client._serialize_message(complex_args, complex_kwargs)
        args, kwargs = client._deserialize_message(serialized)
        
        assert args == complex_args
        assert kwargs == complex_kwargs
    
    def test_large_data_serialization(self):
        """Test serialization of large data structures."""
        client = self.create_mock_client()
        
        # Create large data
        large_dict = {f"key_{i}": f"value_{i}" * 100 for i in range(1000)}
        args = (large_dict,)
        kwargs = {"metadata": "test"}
        
        # Should be able to serialize without error
        serialized = client._serialize_message(args, kwargs)
        assert isinstance(serialized, bytes)
        assert len(serialized) > 100000  # Should be large
        
        # Should be able to deserialize
        deserialized_args, deserialized_kwargs = client._deserialize_message(serialized)
        assert deserialized_args == args
        assert deserialized_kwargs == kwargs
    
    def test_chunking_with_unicode(self):
        """Test chunking with Unicode characters."""
        client = self.create_mock_client()
        
        # Unicode data
        unicode_args = ("Test with émojis 🚀 and spëcial chars",)
        unicode_kwargs = {"unicode_key": "Valué with accénts"}
        
        # Serialize and deserialize
        serialized = client._serialize_message(unicode_args, unicode_kwargs)
        args, kwargs = client._deserialize_message(serialized)
        
        assert args == unicode_args
        assert kwargs == unicode_kwargs
    
    def test_empty_data_handling(self):
        """Test handling of empty data."""
        client = self.create_mock_client()
        
        # Empty data
        empty_args = ()
        empty_kwargs = {}
        
        # Should handle empty data gracefully
        serialized = client._serialize_message(empty_args, empty_kwargs)
        args, kwargs = client._deserialize_message(serialized)
        
        assert args == empty_args
        assert kwargs == empty_kwargs


if __name__ == "__main__":
    pytest.main([__file__, "-v"])