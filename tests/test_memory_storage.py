import pytest


@pytest.mark.parametrize("r", ["memory_storage", "redisclient"])
def test_storage_set(r, request):
    c = request.getfixturevalue(r)
    c.set("name", "Alice")
    assert c.get("name") == "Alice"
