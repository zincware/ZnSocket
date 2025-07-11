import numpy as np
import pytest
from znjson.converter import NumpyConverter

import znsocket


@pytest.mark.slow
def test_chunked_large_dict_python_to_js(znsclient, run_npm_test, request, caplog):
    """Test that large chunked data from Python can be read by JavaScript."""
    # Create very large data that will definitely trigger chunking (>5MB)
    large_array = np.random.rand(1000, 1000).astype(np.float64)  # ~8MB
    znsclient.max_message_size_bytes = 100000  # Set a smaller max size for testing

    # Store using Python client with chunking
    dct = znsocket.Dict(
        r=znsclient, key="chunked_test_dict", converter=[NumpyConverter]
    )
    caplog.set_level("DEBUG", logger="znsocket.client")
    # This should trigger chunking
    dct["large_data"] = large_array
    dct["metadata"] = {
        "size": large_array.shape,
        "type": "chunked_numpy_array",
        "description": "Large array sent from Python with chunking",
    }
    assert "Splitting message" in caplog.text
    # Run the JavaScript test to verify JS can read the chunked data
    run_npm_test(request.node.name, client_url=znsclient.address)


@pytest.mark.slow
def test_chunked_large_list_python_to_js(znsclient, run_npm_test, request, caplog):
    """Test that large chunked list data from Python can be read by JavaScript."""
    # Create a large list with many items to trigger chunking
    znsclient.max_message_size_bytes = 100000  # Set a smaller max size for testing
    large_list = []
    for i in range(800):
        # Each item is a large string to ensure we exceed the 5MB limit
        large_item = f"item_{i}_" + "x" * 10000  # 10KB per item, 800 items = 8MB
        large_list.append(large_item)

    # Store using Python client
    lst = znsocket.List(r=znsclient, key="chunked_test_list")
    caplog.set_level("DEBUG", logger="znsocket.client")
    lst.extend(large_list)

    # Add metadata
    metadata_dict = znsocket.Dict(r=znsclient, key="chunked_list_metadata")
    metadata_dict["total_items"] = len(large_list)
    metadata_dict["description"] = "Large list sent from Python with chunking"

    assert (
        "Splitting message" not in caplog.text
    )  # compression was used and very efficient

    # Run the JavaScript test to verify JS can read the chunked data
    run_npm_test(request.node.name, client_url=znsclient.address)


@pytest.mark.slow
def test_chunked_large_list_python_to_js_2(znsclient, run_npm_test, request, caplog):
    """Test that large chunked list data from Python can be read by JavaScript."""
    # Create a large list with many items to trigger chunking
    znsclient.max_message_size_bytes = 100000  # Set a smaller max size for testing
    large_list = []
    for i in range(800):
        # Each item is a large random string to ensure we exceed the 5MB limit
        large_item = f"item_{i}_x" + str(
            np.random.rand(1000).astype(np.float64).tobytes()
        )
        large_list.append(large_item)

    # Store using Python client
    lst = znsocket.List(r=znsclient, key="chunked_test_list")
    caplog.set_level("DEBUG", logger="znsocket.client")
    lst.extend(large_list)

    # Add metadata
    metadata_dict = znsocket.Dict(r=znsclient, key="chunked_list_metadata")
    metadata_dict["total_items"] = len(large_list)
    metadata_dict["description"] = "Large list sent from Python with chunking"

    assert "Splitting message" in caplog.text

    # Run the JavaScript test to verify JS can read the chunked data
    run_npm_test(request.node.name, client_url=znsclient.address)
