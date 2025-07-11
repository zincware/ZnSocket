"""Tests for chunking functionality in znsocket."""
import pytest
import znsocket
import numpy as np
from znjson.converter import NumpyConverter
import numpy.testing as npt


@pytest.mark.parametrize(
    "client",
    ["znsclient", "znsclient_w_redis", "redisclient"],
)
def test_chunking_large_dict_set(client, request, caplog):
    """Test that large dictionary updates are chunked automatically."""
    c = request.getfixturevalue(client)

    dct = znsocket.Dict(
        r=c,
        key="test_large_dict",
        converter=[NumpyConverter]
    )

    # Create a large numpy array
    large_array = np.random.rand(1000, 1000)

    caplog.set_level("DEBUG", logger="znsocket.client")
    dct["data"] = large_array
    # Retrieve the data
    retrieved_data = dct["data"]

    npt.assert_array_equal(retrieved_data, large_array)
    if client in ["znsclient", "znsclient_w_redis"]:
        assert "Splitting message" in caplog.text


@pytest.mark.parametrize(
    "client",
    ["znsclient", "znsclient_w_redis", "redisclient"],
)
def test_chunking_large_list_append(client, request, caplog):
    """Test that large list appends are chunked automatically."""
    c = request.getfixturevalue(client)

    lst = znsocket.List(
        r=c,
        key="test_large_list",
        converter=[NumpyConverter]
    )

    # Create a large numpy array
    large_array = np.random.rand(1000, 1000)

    caplog.set_level("DEBUG", logger="znsocket.client")
    lst.append(large_array)
    # Retrieve the data
    retrieved_data = lst[:]

    npt.assert_array_equal(retrieved_data, [large_array])
    if client in ["znsclient", "znsclient_w_redis"]:
        assert "Splitting message" in caplog.text
