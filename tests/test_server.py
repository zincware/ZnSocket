import pytest

import znsocket


def test_server_from_url():
    server = znsocket.Server.from_url("znsocket://127.0.0.1:5000")
    assert server.port == 5000

    with pytest.raises(ValueError):
        znsocket.Server.from_url("http://127.0.0.1")
