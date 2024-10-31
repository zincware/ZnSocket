import typing as t
from urllib.parse import urlparse


def parse_url(input_url) -> t.Tuple[str, t.Optional[str]]:
    parsed = urlparse(input_url)
    base_url = f"{parsed.scheme}://{parsed.netloc}"
    path = parsed.path.strip("/") if parsed.path else None
    return base_url, path if path else None
