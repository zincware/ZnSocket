import numpy as np
import numpy.testing as npt
import pytest
import znjson

import znsocket

SLEEP_TIME = 0.1


@pytest.mark.parametrize("client", ["znsclient", "znsclient_w_redis", "redisclient"])
def test_segments_getitem(client, request):
    c = request.getfixturevalue(client)
    lst = znsocket.List(r=c, key="list:test")

    lst.extend(list(range(5)))
    segments = znsocket.Segments.from_list(lst, "segments:test")
    assert len(segments) == 5

    list(segments) == [0, 1, 2, 3, 4]
    with pytest.raises(IndexError):
        _ = segments[10]


@pytest.mark.parametrize("client", ["znsclient"])
def test_segments_setup(client, request):
    c = request.getfixturevalue(client)
    dct = znsocket.Dict(r=c, key="list:test")

    with pytest.raises(TypeError):
        _ = znsocket.Segments.from_list(dct, "segments:test")
    with pytest.raises(TypeError):
        _ = znsocket.Segments.from_list(None, "segments:test")


@pytest.mark.parametrize("client", ["znsclient", "znsclient_w_redis", "redisclient"])
def test_segments_setitem(client, request):
    c = request.getfixturevalue(client)
    lst = znsocket.List(r=c, key="list:test")

    lst.extend(list(range(5)))
    segments = znsocket.Segments.from_list(lst, "segments:test")
    assert len(segments) == 5

    segments[2] = "x"
    assert list(segments) == [0, 1, "x", 3, 4]
    raw = segments.get_raw()
    assert raw[0] == [0, 2, "znsocket.List:list:test"]
    assert raw[1] == [0, 1, "znsocket.List:segments:test"]
    assert raw[2] == [3, 5, "znsocket.List:list:test"]
    assert len(raw) == 3

    segments[0] = "y"
    assert list(segments) == ["y", 1, "x", 3, 4]
    raw = segments.get_raw()
    assert raw[0] == [1, 2, "znsocket.List:segments:test"]
    assert raw[1] == [1, 2, "znsocket.List:list:test"]
    assert raw[2] == [0, 1, "znsocket.List:segments:test"]
    assert raw[3] == [3, 5, "znsocket.List:list:test"]
    assert len(raw) == 4

    segments[4] = "z"
    assert list(segments) == ["y", 1, "x", 3, "z"]
    raw = segments.get_raw()
    assert raw[0] == [1, 2, "znsocket.List:segments:test"]
    assert raw[1] == [1, 2, "znsocket.List:list:test"]
    assert raw[2] == [0, 1, "znsocket.List:segments:test"]
    assert raw[3] == [3, 4, "znsocket.List:list:test"]
    assert raw[4] == [2, 3, "znsocket.List:segments:test"]
    assert len(raw) == 5

    segments[0] = "a"
    assert list(segments) == ["a", 1, "x", 3, "z"]
    raw = segments.get_raw()
    assert raw[0] == [
        1,
        2,
        "znsocket.List:segments:test",
    ]  # did not change, because we modified "segments:test"
    assert raw[1] == [1, 2, "znsocket.List:list:test"]
    assert raw[2] == [0, 1, "znsocket.List:segments:test"]
    assert raw[3] == [3, 4, "znsocket.List:list:test"]
    assert raw[4] == [2, 3, "znsocket.List:segments:test"]
    assert len(raw) == 5

    segments[1] = "b"
    assert list(segments) == ["a", "b", "x", 3, "z"]
    raw = segments.get_raw()
    # TODO: combine segments again (1-3) / have a sanitze method.
    assert raw[0] == [
        1,
        2,
        "znsocket.List:segments:test",
    ]  # did not change, because we modified "segments:test"
    assert raw[1] == [4, 5, "znsocket.List:segments:test"]
    assert raw[2] == [0, 1, "znsocket.List:segments:test"]
    assert raw[3] == [3, 4, "znsocket.List:list:test"]
    assert raw[4] == [2, 3, "znsocket.List:segments:test"]
    assert len(raw) == 5

    segments[0] = "i"
    assert list(segments) == ["i", "b", "x", 3, "z"]

    with pytest.raises(IndexError):
        segments[10] = "x"


@pytest.mark.parametrize("client", ["znsclient", "znsclient_w_redis", "redisclient"])
def test_segments_delitem(client, request):
    c = request.getfixturevalue(client)
    lst = znsocket.List(r=c, key="list:test")

    lst.extend(list(range(5)))
    segments = znsocket.Segments.from_list(lst, "segments:test")
    assert len(segments) == 5

    del segments[2]
    assert list(segments) == [0, 1, 3, 4]
    assert len(segments) == 4
    raw = segments.get_raw()
    assert raw[0] == [0, 2, "znsocket.List:list:test"]
    assert raw[1] == [3, 5, "znsocket.List:list:test"]
    assert len(raw) == 2

    del segments[0]
    assert list(segments) == [1, 3, 4]
    assert len(segments) == 3
    raw = segments.get_raw()
    assert raw[0] == [1, 2, "znsocket.List:list:test"]
    assert raw[1] == [3, 5, "znsocket.List:list:test"]
    assert len(raw) == 2

    del segments[-1]
    assert list(segments) == [1, 3]
    assert len(segments) == 2
    raw = segments.get_raw()
    assert raw[0] == [1, 2, "znsocket.List:list:test"]
    assert raw[1] == [3, 4, "znsocket.List:list:test"]
    assert len(raw) == 2

    with pytest.raises(IndexError):
        del segments[10]


@pytest.mark.parametrize("client", ["znsclient", "znsclient_w_redis", "redisclient"])
def test_segments_insert(client, request):
    c = request.getfixturevalue(client)
    lst = znsocket.List(r=c, key="list:test")

    lst.extend(list(range(5)))
    segments = znsocket.Segments.from_list(lst, "segments:test")
    assert len(segments) == 5

    segments.insert(2, "x")
    assert list(segments) == [0, 1, "x", 2, 3, 4]
    raw = segments.get_raw()
    assert raw[0] == [0, 2, "znsocket.List:list:test"]
    assert raw[1] == [0, 1, "znsocket.List:segments:test"]
    assert raw[2] == [2, 5, "znsocket.List:list:test"]
    assert len(raw) == 3

    segments.insert(0, "y")
    assert list(segments) == ["y", 0, 1, "x", 2, 3, 4]
    segments.insert(-1, "z")
    assert list(segments) == ["y", 0, 1, "x", 2, 3, "z", 4]


@pytest.mark.parametrize("client", ["znsclient", "znsclient_w_redis", "redisclient"])
def test_segments_extend_append(client, request):
    c = request.getfixturevalue(client)
    lst = znsocket.List(r=c, key="list:test")

    lst.extend(list(range(5)))
    segments = znsocket.Segments.from_list(lst, "segments:test")

    segments.extend([5, 6, 7])
    assert list(segments) == list(range(8))

    segments.append(8)
    assert list(segments) == list(range(9))


@pytest.mark.parametrize("client", ["znsclient", "znsclient_w_redis", "redisclient"])
def test_segments_numpy(client, request):
    c = request.getfixturevalue(client)
    lst = znsocket.List(
        r=c, key="list:test", converter=[znjson.converter.NumpyConverter]
    )

    lst.extend(np.arange(9).reshape(3, 3))
    assert len(lst) == 3
    segments = znsocket.Segments.from_list(lst, "segments:test")
    assert len(segments) == 3
    npt.assert_array_equal(np.array(list(segments)), np.arange(9).reshape(3, 3))
