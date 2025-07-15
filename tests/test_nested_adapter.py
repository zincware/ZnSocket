import pytest

import znsocket
from znsocket.exceptions import FrozenStorageError


@pytest.fixture
def list_data():
    data = []
    p1 = {
        "name": "John Doe",
        "age": 30,
        "address": {"street": "123 Main St", "city": "Anytown", "state": "CA"},
        "contact": [
            {"type": "email", "value": "john.doe@example.com"},
            {"type": "phone", "value": "555-1234"},
        ],
    }
    p2 = {
        "name": "Jane Smith",
        "age": 25,
        "address": {"street": "456 Elm St", "city": "Othertown", "state": "NY"},
        "contact": [
            {"type": "email", "value": "jane.smith@example.com"},
            {"type": "phone", "value": "555-5678"},
        ],
    }
    data.extend([p1, p2])
    return data


@pytest.mark.parametrize("client", ["znsclient", "znsclient_w_redis"])
def test_nested_list_adapter(client, request, list_data):
    c = request.getfixturevalue(client)

    def transform_callback(item, key, socket, converter=None, convert_nan=False):
        return znsocket.DictAdapter(key, socket, item, converter, convert_nan)

    _ = znsocket.ListAdapter(
        key="nested_list",
        socket=c,
        object=list_data,
        item_transform_callback=transform_callback,
    )

    lst = znsocket.List(c, key="nested_list")
    assert len(lst) == 2
    assert isinstance(lst[0], znsocket.Dict)
    assert isinstance(lst[1], znsocket.Dict)
    assert lst[0]["name"] == "John Doe"
    assert lst[1]["name"] == "Jane Smith"

    with pytest.raises(FrozenStorageError):
        lst.append("new_item")

    with pytest.raises(FrozenStorageError):
        lst[0].update({"new_key": "new_value"})
