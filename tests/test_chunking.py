"""Tests for chunking functionality in znsocket."""
import pytest
import znsocket
from znsocket.client import Client
import numpy as np
from znjson.converter import NumpyConverter
import numpy.testing as npt


@pytest.mark.parametrize(
    "client",
    ["znsclient", "znsclient_w_redis", "redisclient"],
)
def test_chunking_large_dict_set(client, request):
    """Test that large dictionary updates are chunked automatically."""
    client = request.getfixturevalue(client)

    dct = znsocket.Dict(
        r=client,
        key="test_large_dict",
        converter=[NumpyConverter]
    )

    # Create a large numpy array
    large_array = np.random.rand(1000, 1000)

    dct["data"] = large_array
    # Retrieve the data
    retrieved_data = dct["data"]

    npt.assert_array_equal(retrieved_data, large_array)


@pytest.mark.parametrize(
    "client",
    ["znsclient", "znsclient_w_redis", "redisclient"],
)
def test_chunking_large_list_append(client, request):
    """Test that large list appends are chunked automatically."""
    client = request.getfixturevalue(client)

    lst = znsocket.List(
        r=client,
        key="test_large_list",
        converter=[NumpyConverter]
    )

    # Create a large numpy array
    large_array = np.random.rand(1000, 1000)

    lst.append(large_array)
    # Retrieve the data
    retrieved_data = lst[:]

    npt.assert_array_equal(retrieved_data, [large_array])
