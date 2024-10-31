from znsocket.utils import parse_url


def test_parse_url_with_path():
    url = "https://example.com/path/to/resource"
    base_url, path = parse_url(url)
    assert base_url == "https://example.com"
    assert path == "path/to/resource"


def test_parse_url_without_path():
    url = "https://example.com"
    base_url, path = parse_url(url)
    assert base_url == "https://example.com"
    assert path is None


def test_parse_url_http():
    url = "http://example.com"
    base_url, path = parse_url(url)
    assert base_url == "http://example.com"
    assert path is None
