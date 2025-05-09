import znsocket


def test_segments_len_znsocket(znsclient, run_npm_test, request):
    lst = znsocket.List(r=znsclient, key="list:test")
    lst.extend(list(range(5)))
    segments = znsocket.Segments.from_list(lst, key="segments:test")
    assert list(segments) == [0, 1, 2, 3, 4]
    run_npm_test(request.node.name, client_url=znsclient.address)
