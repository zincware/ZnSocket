import numpy as np
import numpy.testing as npt
import pytest
import znjson

import znsocket

SLEEP_TIME = 0.1


@pytest.mark.parametrize("client", ["znsclient", "znsclient_w_redis"])  #  "redisclient"
def test_segments_numpy(client, request):
    c = request.getfixturevalue(client)
    c = request.getfixturevalue(client)
    key = "list:test"
    adapter = znsocket.ListAdapter(
        socket=c,
        key=key,
        object=np.arange(9).reshape(3, 3),
        converter=[znjson.converter.NumpyConverter],
    )
    lst = znsocket.List(r=c, key=key, converter=[znjson.converter.NumpyConverter])
    assert len(lst) == 3
    segments = znsocket.Segments.from_list(lst, "segments:test")
    assert len(segments) == 3
    npt.assert_array_equal(np.array(list(segments)), adapter.object)

    segments[1] = "x"

    npt.assert_array_equal(segments[0], [0, 1, 2])
    assert segments[1] == "x"
    npt.assert_array_equal(segments[2], [6, 7, 8])
    assert len(segments) == 3

    raw = segments.get_raw()
    assert raw[0] == [0, 1, "znsocket.List:list:test"]
    assert raw[1] == [0, 1, "znsocket.List:segments:test"]
    assert raw[2] == [2, 3, "znsocket.List:list:test"]
