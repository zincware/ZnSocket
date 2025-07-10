import pytest

import znsocket


@pytest.mark.parametrize("client", ["znsclient", "znsclient_w_redis", "redisclient"])
def test_dct_pipeline(client, request):
    c = request.getfixturevalue(client)

    pipeline = c.pipeline()

    dct1 = znsocket.Dict(r=pipeline, key="dict:test")
    dct1["a"] = 1
    dct1["b"] = 2
    dct1["c"] = 3

    dct2 = znsocket.Dict(r=pipeline, key="dict:test2")
    dct2["x"] = 10
    dct2["y"] = 20
    dct2["z"] = 30

    pipeline.execute()

    lst = znsocket.List(r=c, key="list:test")
    lst.extend([dct1, dct2])

    assert lst[0] == {"a": 1, "b": 2, "c": 3}
    assert lst[1] == {"x": 10, "y": 20, "z": 30}

    # list appends are currently not pipelined, because
    # we need to fetch the length of the list to append
