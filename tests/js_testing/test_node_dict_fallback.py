import znsocket


def test_dict_fallback_frozen_get(znsclient, run_npm_test, request):
    fallback_key = "dict:test:fallback"
    key = "dict:test"

    # Create adapter with initial data
    initial_data = {"a": 1, "b": 2, "c": 3}
    znsocket.DictAdapter(
        socket=znsclient,
        key=fallback_key,
        object=initial_data,
    )

    run_npm_test(request.node.name, client_url=znsclient.address)

    dct = znsocket.Dict(
        r=znsclient,
        key=key,
        fallback=fallback_key,
        fallback_policy="frozen",
    )

    assert dct["a"] == 1
    assert dct["c"] == 3


def test_dict_fallback_frozen_len(znsclient, run_npm_test, request):
    fallback_key = "dict:test:fallback"
    key = "dict:test"

    # Create adapter with initial data
    initial_data = {"a": 1, "b": 2, "c": 3}
    znsocket.DictAdapter(
        socket=znsclient,
        key=fallback_key,
        object=initial_data,
    )

    run_npm_test(request.node.name, client_url=znsclient.address)

    dct = znsocket.Dict(
        r=znsclient,
        key=key,
        fallback=fallback_key,
        fallback_policy="frozen",
    )

    assert len(dct) == 3


def test_dict_fallback_frozen_keys(znsclient, run_npm_test, request):
    fallback_key = "dict:test:fallback"
    key = "dict:test"

    # Create adapter with initial data
    initial_data = {"a": 1, "b": 2, "c": 3}
    znsocket.DictAdapter(
        socket=znsclient,
        key=fallback_key,
        object=initial_data,
    )

    run_npm_test(request.node.name, client_url=znsclient.address)

    dct = znsocket.Dict(
        r=znsclient,
        key=key,
        fallback=fallback_key,
        fallback_policy="frozen",
    )

    assert sorted(dct.keys()) == ["a", "b", "c"]


def test_dict_fallback_frozen_values(znsclient, run_npm_test, request):
    fallback_key = "dict:test:fallback"
    key = "dict:test"

    # Create adapter with initial data
    initial_data = {"a": 1, "b": 2, "c": 3}
    znsocket.DictAdapter(
        socket=znsclient,
        key=fallback_key,
        object=initial_data,
    )

    run_npm_test(request.node.name, client_url=znsclient.address)

    dct = znsocket.Dict(
        r=znsclient,
        key=key,
        fallback=fallback_key,
        fallback_policy="frozen",
    )

    assert sorted(dct.values()) == [1, 2, 3]


def test_dict_fallback_frozen_items(znsclient, run_npm_test, request):
    fallback_key = "dict:test:fallback"
    key = "dict:test"

    # Create adapter with initial data
    initial_data = {"a": 1, "b": 2, "c": 3}
    znsocket.DictAdapter(
        socket=znsclient,
        key=fallback_key,
        object=initial_data,
    )

    run_npm_test(request.node.name, client_url=znsclient.address)

    dct = znsocket.Dict(
        r=znsclient,
        key=key,
        fallback=fallback_key,
        fallback_policy="frozen",
    )

    assert sorted(dct.items()) == [("a", 1), ("b", 2), ("c", 3)]
