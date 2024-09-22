import znsocket


def test_dict_dual_clients(znsclient_w_redis, redisclient):
    # this checks access via the znsocket server through redis
    #  and redis directly yields the same results
    dct_znsocket_client = znsocket.Dict(r=znsclient_w_redis, key="dict:test")
    dct_redis = znsocket.Dict(r=redisclient, key="dict:test")

    dct_znsocket_client["a"] = "1"
    assert dct_znsocket_client == {"a": "1"}
    assert dct_redis == {"a": "1"}
    dct_redis["a"] = "2"
    assert dct_znsocket_client == {"a": "2"}
    assert dct_redis == {"a": "2"}
