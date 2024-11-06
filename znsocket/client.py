import dataclasses
import functools
import json
import typing as t
import warnings

import socketio.exceptions
import typing_extensions as tyex
from redis import Redis

from znsocket import exceptions
from znsocket.abc import RefreshDataTypeDict
from znsocket.utils import parse_url


@dataclasses.dataclass(frozen=True)
class Client:
    address: str
    decode_responses: bool = True
    sio: socketio.Client = dataclasses.field(
        default_factory=socketio.Client, repr=False, init=False
    )
    namespace: str = "/znsocket"
    refresh_callbacks: dict = dataclasses.field(default_factory=dict)

    def pipeline(self, *args, **kwargs) -> "Pipeline":
        return Pipeline(self, *args, **kwargs)

    @classmethod
    def from_url(cls, url, namespace: str = "/znsocket", **kwargs) -> "Client":
        """Connect to a znsocket server using a URL.

        Parameters
        ----------
        url : str
            The URL of the znsocket server. Should be in the format
            "znsocket://127.0.0.1:5000".
        namespace : str
            The namespace to connect to. Default is "/znsocket".
        """
        return cls(
            address=url.replace("znsocket://", "http://"), namespace=namespace, **kwargs
        )

    def __post_init__(self):
        @self.sio.on("refresh", namespace=self.namespace)
        def refresh(data: RefreshDataTypeDict):
            for key in self.refresh_callbacks:
                if data["target"] == key:
                    self.refresh_callbacks[key](data["data"])

        _url, _path = parse_url(self.address)
        try:
            self.sio.connect(
                _url,
                namespaces=[self.namespace],
                wait=True,
                socketio_path=f"{_path}/socket.io" if _path else "socket.io",
            )

        except socketio.exceptions.ConnectionError as err:
            raise exceptions.ConnectionError(self.address) from err

        if not self.decode_responses:
            raise NotImplementedError("decode_responses=False is not supported yet")

    def _redis_command(self, command, *args, **kwargs):
        """Generic handler for Redis commands."""
        result = self.sio.call(command, [args, kwargs], namespace=self.namespace)
        if result is None:
            raise exceptions.ZnSocketError("No response from server")
        if "error" in result:
            if result["error"]["type"] == "DataError":
                raise exceptions.DataError(result["error"]["msg"])
            elif result["error"]["type"] == "TypeError":
                raise TypeError(result["error"]["msg"])
            elif result["error"]["type"] == "IndexError":
                raise IndexError(result["error"]["msg"])
            elif result["error"]["type"] == "KeyError":
                raise KeyError(result["error"]["msg"])
            elif result["error"]["type"] == "UnknownEventError":
                raise exceptions.UnknownEventError(result["error"]["msg"])
            elif result["error"]["type"] == "ResponseError":
                raise exceptions.ResponseError(result["error"]["msg"])
            else:
                raise exceptions.ZnSocketError(
                    f"{result['error']['type']}: {result['error']['msg']} -- for command {command}"
                )
            # raise exceptions.DataError(result["error"])
        if "type" in result and result["type"] == "set":
            return set(result["data"])
        return result["data"]

    def __getattr__(self, name):
        """Intercepts method calls to dynamically route Redis commands."""
        # Check if name corresponds to a Redis command
        if hasattr(Redis, name):
            return functools.partial(self._redis_command, name)
        raise AttributeError(
            f"'{type(self).__name__}' object has no attribute '{name}'"
        )

    @tyex.deprecated("hmset() is deprecated. Use hset() instead.")
    def hmset(self, name, mapping):
        return self.hset(name, mapping=mapping)


@dataclasses.dataclass
class Pipeline:
    client: Client
    max_message_size: t.Optional[int] = 10 * 1024 * 1024
    pipeline: list = dataclasses.field(default_factory=list)

    def _add_to_pipeline(self, command, *args, **kwargs):
        """Generic handler to add Redis commands to the pipeline."""
        self.pipeline.append((command, [args, kwargs]))
        return self

    def __getattr__(self, name):
        """Intercepts method calls to dynamically add Redis commands to the pipeline."""
        # Check if the name corresponds to a Redis command
        if hasattr(Redis, name):
            return functools.partial(self._add_to_pipeline, name)
        raise AttributeError(
            f"'{type(self).__name__}' object has no attribute '{name}'"
        )

    def execute(self):
        """Executes the pipeline of commands as a batch on the server."""
        # iterate over self.pipeline and keep adding until the size is greater than max_message_size
        # then send the message, collect the results and continue

        def _send_message(message) -> list:
            result = self.client.sio.call(
                "pipeline",
                {"pipeline": message},
                namespace=self.client.namespace,
            )
            if result is None:
                raise exceptions.ZnSocketError("No response from server")
            if "error" in result:
                if result["error"]["type"] == "DataError":
                    raise exceptions.DataError(result["error"]["msg"])
                elif result["error"]["type"] == "TypeError":
                    raise TypeError(result["error"]["msg"])
                elif result["error"]["type"] == "IndexError":
                    raise IndexError(result["error"]["msg"])
                elif result["error"]["type"] == "KeyError":
                    raise KeyError(result["error"]["msg"])
                elif result["error"]["type"] == "UnknownEventError":
                    raise exceptions.UnknownEventError(result["error"]["msg"])
                elif result["error"]["type"] == "ResponseError":
                    raise exceptions.ResponseError(result["error"]["msg"])
                else:
                    raise exceptions.ZnSocketError(
                        f"{result['error']['type']}: {result['error']['msg']} -- for command {command}"
                    )
            return result["data"]

        message = []
        results = []
        for idx, entry in enumerate(self.pipeline):
            message.append(entry)
            if self.max_message_size is not None:
                msg_size = json.dumps(message).__sizeof__()
                if msg_size > self.max_message_size:
                    warnings.warn(
                        f"Message size '{msg_size}' is greater than"
                        f" '{self.max_message_size = }'. Sending message"
                        f" at index {idx} and continuing."
                    )
                    results.extend(_send_message(message))
                    message = []
        if message:
            results.extend(_send_message(message))

        # TODO: test pipeline with smembers and test with errors
        return results
