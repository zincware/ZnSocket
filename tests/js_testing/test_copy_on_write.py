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

    segments = znsocket.Segments(
        r=c,
        origin=lst,
        key="test:data/segments"
    )

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
    test_name = request.node.name.split('[')[0]  # Strip parameter part
    run_npm_test(test_name, client_url=c.address)
