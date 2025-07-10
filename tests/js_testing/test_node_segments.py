import znsocket


def test_segments_len_znsocket(znsclient, run_npm_test, request):
    lst = znsocket.List(r=znsclient, key="list:test")
    lst.extend(list(range(5)))
    segments = znsocket.Segments.from_list(lst, key="segments:test")
    assert list(segments) == [0, 1, 2, 3, 4]
    run_npm_test(request.node.name, client_url=znsclient.address)


def test_segments_getitem_znsocket(znsclient, run_npm_test, request):
    lst = znsocket.List(r=znsclient, key="list:test")
    lst.extend(list(range(5)))
    segments = znsocket.Segments.from_list(lst, key="segments:test")
    assert len(segments.get_raw()) == 1
    assert list(segments) == [0, 1, 2, 3, 4]
    run_npm_test(request.node.name, client_url=znsclient.address)
    # now we insert the exact same items into segments but from different list
    segments[0] = 0
    segments[3] = 3
    segments[4] = 4
    assert len(segments.get_raw()) == 4
    run_npm_test(request.node.name, client_url=znsclient.address)
