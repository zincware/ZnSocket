"""Tests for copy-on-write functionality using Segments and List fallbacks."""

import pytest

import znsocket


@pytest.mark.parametrize(
    "client",
    ["znsclient", "znsclient_w_redis"],
)
def test_segments_with_dict(client, request, run_npm_test):
    """Test copy-on-write behavior using Segments."""
    c = request.getfixturevalue(client)

    lst = znsocket.List(r=c, key="test:data")

    data = [
        {"value": [1, 2, 3]},
        {"value": [4, 5, 6]},
        {"value": [7, 8, 9]},
        {"value": [10, 11, 12]},
    ]

    p = c.pipeline()
    msg = []
    for idx, value in enumerate(data):
        atoms_dict = znsocket.Dict(
            r=p,
            key=f"test:data/{idx}",
        )
        # can not use atoms_dict.update when providing a pipeline
        for k, v in value.items():
            atoms_dict[k] = v
        msg.append(atoms_dict)
    p.execute()
    lst.extend(msg)

    segments = znsocket.Segments(r=c, origin=lst, key="test:data/segments")

    value_to_modify = segments[2]  # Get the third element
    modified_value = value_to_modify.copy("test:data/segments/2")
    modified_value["value"] = [100, 200, 300]
    segments[2] = modified_value

    # Verify that the original list remains unchanged
    assert lst[2]["value"] == [7, 8, 9]
    assert lst[2].key == "znsocket.Dict:test:data/2"
    # Verify that the modified segment reflects the change
    assert segments[2]["value"] == [100, 200, 300]
    assert segments[2].key == "znsocket.Dict:test:data/segments/2"

    assert list(segments) == [
        {"value": [1, 2, 3]},
        {"value": [4, 5, 6]},
        {"value": [100, 200, 300]},  # Modified value
        {"value": [10, 11, 12]},
    ]

    assert list(lst) == [
        {"value": [1, 2, 3]},
        {"value": [4, 5, 6]},
        {"value": [7, 8, 9]},  # Original value remains unchanged
        {"value": [10, 11, 12]},
    ]

    # Run the corresponding TypeScript test
    test_name = request.node.name.split("[")[0]  # Strip parameter part
    run_npm_test(test_name, client_url=c.address)


@pytest.mark.parametrize(
    "client",
    ["znsclient", "znsclient_w_redis"],
)
def test_list_adapter_with_segments(client, request, run_npm_test):
    """Test copy-on-write behavior using ListAdapter and Segments."""
    c = request.getfixturevalue(client)

    # Create original data as a regular Python list
    original_data = [
        {"name": "item_0", "score": 85, "category": "A"},
        {"name": "item_1", "score": 92, "category": "B"},
        {"name": "item_2", "score": 78, "category": "A"},
        {"name": "item_3", "score": 96, "category": "C"},
        {"name": "item_4", "score": 83, "category": "B"},
    ]

    # Use ListAdapter to expose the Python list via ZnSocket
    znsocket.ListAdapter(socket=c, key="test:adapter_data", object=original_data)

    # Create a List view of the adapted data
    lst = znsocket.List(r=c, key="test:adapter_data")

    # Verify adapter works correctly
    assert len(lst) == 5
    assert lst[0] == {"name": "item_0", "score": 85, "category": "A"}
    assert lst[2] == {"name": "item_2", "score": 78, "category": "A"}

    # Create copy-on-write view using Segments
    segments = znsocket.Segments(r=c, origin=lst, key="test:adapter_segments")

    # Verify segments can access the adapted data
    assert len(segments) == 5
    assert segments[1] == {"name": "item_1", "score": 92, "category": "B"}

    # Create a modified version of item 2 using copy-on-write
    # First create a Dict to store the modified data
    modified_dict = znsocket.Dict(r=c, key="test:adapter_segments/2")
    modified_dict.clear()
    modified_dict.update(
        {
            "name": "item_2_modified",
            "score": 95,  # Improved score
            "category": "A+",  # Upgraded category
            "modified": True,
            "source": "segments_copy",
        }
    )

    # Replace the element in segments with the modified version
    segments[2] = modified_dict

    # Verify copy-on-write behavior
    # Original data (via adapter) should be unchanged
    assert lst[2] == {"name": "item_2", "score": 78, "category": "A"}
    assert original_data[2] == {"name": "item_2", "score": 78, "category": "A"}

    # Segments should show the modification
    assert segments[2]["name"] == "item_2_modified"
    assert segments[2]["score"] == 95
    assert segments[2]["category"] == "A+"
    assert segments[2]["modified"] == True
    assert segments[2]["source"] == "segments_copy"

    # Other elements should remain unchanged in segments
    assert segments[0] == {"name": "item_0", "score": 85, "category": "A"}
    assert segments[1] == {"name": "item_1", "score": 92, "category": "B"}
    assert segments[3] == {"name": "item_3", "score": 96, "category": "C"}
    assert segments[4] == {"name": "item_4", "score": 83, "category": "B"}

    # Modify another element for more comprehensive testing
    modified_dict_4 = znsocket.Dict(r=c, key="test:adapter_segments/4")
    modified_dict_4.clear()
    modified_dict_4.update(
        {
            "name": "item_4_enhanced",
            "score": 99,
            "category": "S",  # Special category
            "enhanced": True,
            "multiplier": 1.2,
        }
    )
    segments[4] = modified_dict_4

    # Verify multiple modifications work correctly
    expected_segments = [
        {"name": "item_0", "score": 85, "category": "A"},  # Original
        {"name": "item_1", "score": 92, "category": "B"},  # Original
        {
            "name": "item_2_modified",
            "score": 95,
            "category": "A+",
            "modified": True,
            "source": "segments_copy",
        },  # Modified
        {"name": "item_3", "score": 96, "category": "C"},  # Original
        {
            "name": "item_4_enhanced",
            "score": 99,
            "category": "S",
            "enhanced": True,
            "multiplier": 1.2,
        },  # Modified
    ]

    for i, expected in enumerate(expected_segments):
        actual = dict(segments[i])
        assert actual == expected, f"Mismatch at index {i}: {actual} != {expected}"

    # Verify original list (adapter) remains completely unchanged
    expected_original = [
        {"name": "item_0", "score": 85, "category": "A"},
        {"name": "item_1", "score": 92, "category": "B"},
        {"name": "item_2", "score": 78, "category": "A"},  # Unchanged
        {"name": "item_3", "score": 96, "category": "C"},
        {"name": "item_4", "score": 83, "category": "B"},  # Unchanged
    ]

    for i, expected in enumerate(expected_original):
        assert lst[i] == expected, f"Original data changed at index {i}"
        assert original_data[i] == expected, f"Python list changed at index {i}"

    # Run the corresponding TypeScript test
    test_name = request.node.name.split("[")[0]  # Strip parameter part
    run_npm_test(test_name, client_url=c.address)
