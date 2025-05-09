from unittest.mock import MagicMock

import numpy as np
import numpy.testing as npt
import pytest
import znjson

import znsocket
import znsocket.client
from znsocket.abc import ZnSocketObject

SLEEP_TIME = 0.1


@pytest.mark.parametrize(
    "client", ["znsclient", "znsclient_w_redis", "redisclient"]
)
def test_list_extend(client, request):
    c = request.getfixturevalue(client)
    lst = znsocket.List(r=c, key="list:test")

    lst.extend(list(range(5)))
    segments = znsocket.Segments.from_list(lst, "segments:test")
    assert len(segments) == 5

    segments[2] = "x"
    assert list(segments) == [0, 1, "x", 3, 4]
    raw =  segments.get_raw()
    assert raw[0] == [0, 2, "list:test"]
    assert raw[1] == [0, 1, "segments:test"]
    assert raw[2] == [3, 5, "list:test"]
    assert len(raw) == 3

    segments[0] = "y"
    assert list(segments) == ["y", 1, "x", 3, 4]
    raw =  segments.get_raw()
    assert raw[0] == [1, 2, "segments:test"]
    assert raw[1] == [1, 2, "list:test"]
    assert raw[2] == [0, 1, "segments:test"]
    assert raw[3] == [3, 5, "list:test"]
    assert len(raw) == 4

    segments[4] = "z"
    assert list(segments) == ["y", 1, "x", 3, "z"]
    raw =  segments.get_raw()
    assert raw[0] == [1, 2, "segments:test"]
    assert raw[1] == [1, 2, "list:test"]
    assert raw[2] == [0, 1, "segments:test"]
    assert raw[3] == [3, 4, "list:test"]
    assert raw[4] == [2, 3, "segments:test"]
    assert len(raw) == 5

    segments[0] = "a"
    assert list(segments) == ["a", 1, "x", 3, "z"]
    raw =  segments.get_raw()
    assert raw[0] == [1, 2, "segments:test"] # did not change, becuase we modified "segments:test"
    assert raw[1] == [1, 2, "list:test"]
    assert raw[2] == [0, 1, "segments:test"]
    assert raw[3] == [3, 4, "list:test"]
    assert raw[4] == [2, 3, "segments:test"]
    assert len(raw) == 5

    segments[1] = "b"
    assert list(segments) == ["a", "b", "x", 3, "z"]
    raw =  segments.get_raw()
    # TODO: combine segments again!
    # assert raw[0] == [1, 2, "segments:test"] # did not change, becuase we modified "segments:test"
    # assert raw[1] == [1, 2, "list:test"]
    # assert raw[2] == [0, 1, "segments:test"]
    # assert raw[3] == [3, 4, "list:test"]
    # assert raw[4] == [2, 3, "segments:test"]
    assert len(raw) == 5

    segments[0] = "i"
    assert list(segments) == ["i", "b", "x", 3, "z"]

