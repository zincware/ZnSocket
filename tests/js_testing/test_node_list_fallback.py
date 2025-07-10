import znsocket


def test_empty_list_adapter_fallback(znsclient, run_npm_test, request):
    fallback_key = "list:test:fallback"
    key = "list:test"
    
    # Create adapter with initial data
    initial_data = [10, 20, 30, 40, 50]
    znsocket.ListAdapter(
        socket=znsclient,
        key=fallback_key,
        object=initial_data,
    )

    run_npm_test(request.node.name, client_url=znsclient.address)

    lst = znsocket.List(
        r=znsclient,
        key=key,
        fallback=fallback_key,
        fallback_policy="frozen",
    )

    assert len(lst) == 5

    lst = znsocket.List(
        r=znsclient,
        key=key,
        # fallback=fallback_key,
        fallback_policy="copy",
    )

    assert len(lst) == 0

    lst = znsocket.List(
        r=znsclient,
        key=fallback_key,
        # fallback=fallback_key,
        fallback_policy="copy",
    )
    assert len(lst) == 5