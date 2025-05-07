import json
import typing as t
from urllib.parse import urlparse

import znjson

from znsocket import exceptions


def handle_error(result):
    """Handle errors in the server response."""

    if not isinstance(result, dict):
        return

    if "error" not in result:
        return

    error_map = {
        "DataError": exceptions.DataError,
        "TypeError": TypeError,
        "IndexError": IndexError,
        "KeyError": KeyError,
        "UnknownEventError": exceptions.UnknownEventError,
        "ResponseError": exceptions.ResponseError,
    }

    error_type = result["error"].get("type")
    error_msg = result["error"].get("msg", "Unknown error")

    # Raise the mapped exception if it exists, else raise a generic ZnSocketError
    raise error_map.get(error_type, exceptions.ZnSocketError)(error_msg)


def parse_url(input_url) -> t.Tuple[str, t.Optional[str]]:
    parsed = urlparse(input_url)
    base_url = f"{parsed.scheme}://{parsed.netloc}"
    path = parsed.path.strip("/") if parsed.path else None
    return base_url, path if path else None


def encode(self, data: t.Any) -> str:
    if self.converter is not None:
        try:
            return json.dumps(
                data,
                cls=znjson.ZnEncoder.from_converters(self.converter),
                allow_nan=False,
            )
        except ValueError:
            if self.convert_nan:
                value = json.dumps(
                    data,
                    cls=znjson.ZnEncoder.from_converters(self.converter),
                    allow_nan=True,
                )
                return (
                    value.replace("NaN", "null")
                    .replace("-Infinity", "null")
                    .replace("Infinity", "null")
                )
            raise

    try:
        return json.dumps(data, allow_nan=False)
    except ValueError:
        if self.convert_nan:
            value = json.dumps(data, allow_nan=True)
            return (
                value.replace("NaN", "null")
                .replace("-Infinity", "null")
                .replace("Infinity", "null")
            )
        raise


def decode(self, data: str) -> t.Any:
    if self.converter is not None:
        data = json.loads(data, cls=znjson.ZnDecoder.from_converters(self.converter))
    else:
        data = json.loads(data)
    handle_error(data)
    return data
