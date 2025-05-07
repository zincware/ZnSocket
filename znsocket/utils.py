import typing as t
from urllib.parse import urlparse
import json
import znjson


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
        return json.loads(data, cls=znjson.ZnDecoder.from_converters(self.converter))
    return json.loads(data)