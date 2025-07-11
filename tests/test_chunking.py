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
