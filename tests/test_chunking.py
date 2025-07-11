"""Tests for chunking functionality in znsocket."""

import numpy as np
import numpy.testing as npt
import pytest
from znjson.converter import NumpyConverter

import znsocket


@pytest.mark.slow
@pytest.mark.parametrize(
    "client",
    ["znsclient", "znsclient_w_redis", "redisclient"],
)
def test_chunking_large_dict_set(client, request, caplog):
    """Test that large dictionary updates are chunked automatically."""
    c = request.getfixturevalue(client)
    if client in ["znsclient", "znsclient_w_redis"]:
        c.max_message_size_bytes = 10000  # Set a smaller max size for testing

    dct = znsocket.Dict(r=c, key="test_large_dict", converter=[NumpyConverter])

    # Create a large numpy array
    large_array = np.random.rand(1000, 1000)

    caplog.set_level("DEBUG", logger="znsocket.client")
    dct["data"] = large_array
    # Retrieve the data
    retrieved_data = dct["data"]

    npt.assert_array_equal(retrieved_data, large_array)
    if client in ["znsclient", "znsclient_w_redis"]:
        assert "Splitting message" in caplog.text


@pytest.mark.slow
@pytest.mark.parametrize(
    "client",
    ["znsclient", "znsclient_w_redis", "redisclient"],
)
def test_chunking_large_list_append(client, request, caplog):
    """Test that large list appends are chunked automatically."""
    c = request.getfixturevalue(client)
    if client in ["znsclient", "znsclient_w_redis"]:
        c.max_message_size_bytes = 10000  # Set a smaller max size for testing

    lst = znsocket.List(r=c, key="test_large_list", converter=[NumpyConverter])

    # Create a large numpy array
    large_array = np.random.rand(1000, 1000)

    caplog.set_level("DEBUG", logger="znsocket.client")
    lst.append(large_array)
    # Retrieve the data
    retrieved_data = lst[:]

    npt.assert_array_equal(retrieved_data, [large_array])
    if client in ["znsclient", "znsclient_w_redis"]:
        assert "Splitting message" in caplog.text


@pytest.mark.slow
@pytest.mark.parametrize(
    "client",
    ["znsclient", "znsclient_w_redis", "redisclient"],
)
def test_chunking_dict_pipeline(client, request, caplog):
    """Test that large dictionary updates are chunked automatically."""
    c = request.getfixturevalue(client)
    if client in ["znsclient", "znsclient_w_redis"]:
        c.max_message_size_bytes = 10000  # Set a smaller max size for testing

    pipeline = c.pipeline()
    data = np.random.rand(1000, 1000)
    keys = [f"test_large_dict_{idx}" for idx in range(3)]
    for key in keys:
        dct = znsocket.Dict(r=pipeline, key=key, converter=[NumpyConverter])
        dct["data"] = data
    caplog.set_level("DEBUG", logger="znsocket.client")
    pipeline.execute()
    if client in ["znsclient", "znsclient_w_redis"]:
        assert "Splitting message" in caplog.text

    for key in keys:
        dct = znsocket.Dict(r=c, key=key, converter=[NumpyConverter])
        retrieved_data = dct["data"]
        npt.assert_array_equal(retrieved_data, data)


@pytest.mark.slow
@pytest.mark.parametrize(
    "client",
    ["znsclient", "znsclient_w_redis"],
)
def test_compression_without_chunking(client, request, caplog):
    """Test that large data compresses well enough to avoid chunking.

    This tests the scenario where raw data exceeds server limits but
    compression makes it small enough for single transmission.
    """
    c = request.getfixturevalue(client)
    # Set a very small limit to force compression decision
    c.max_message_size_bytes = 500000  # 500KB limit (server is 5MB)

    dct = znsocket.Dict(r=c, key="test_compression_dict", converter=[NumpyConverter])

    # Create highly compressible data - repeated patterns compress very well
    # This will be ~8MB raw but compress to much less than 500KB
    large_array = np.zeros((1000, 1000))  # Array of all zeros compresses extremely well
    large_array[::100, ::100] = 1  # Add some sparse non-zero values for realism

    caplog.set_level("DEBUG", logger="znsocket.client")
    dct["data"] = large_array

    # Retrieve the data
    retrieved_data = dct["data"]

    npt.assert_array_equal(retrieved_data, large_array)

    # Should see compression message but NOT chunking message
    assert "Compressed message from" in caplog.text
    assert "Splitting message" not in caplog.text  # Should not be chunked
    assert "Using chunked transmission" not in caplog.text
