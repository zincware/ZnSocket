import pytest

import znsocket


@pytest.fixture
def empty() -> None:
    """Test against Python list implementation"""
    return None


@pytest.mark.parametrize("client", ["znsclient", "redisclient", "empty"])
def test_list_extend(client, request):
    c = request.getfixturevalue(client)
    if c is not None:
        lst = znsocket.List(r=c, key="list:test")
    else:
        lst = []

    lst.extend(["1", "2", "3", "4"])
    assert lst[0] == "1"
    assert lst[:] == ["1", "2", "3", "4"]
    assert lst[1::2] == ["2", "4"]
    assert lst[::-1] == ["4", "3", "2", "1"]
    assert len(lst) == 4
