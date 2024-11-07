import dataclasses
import functools
import json
import logging
import typing as t
import warnings

import socketio.exceptions
import typing_extensions as tyex
from redis import Redis

from znsocket import exceptions
from znsocket.abc import RefreshDataTypeDict
from znsocket.utils import parse_url

log = logging.getLogger(__name__)


def _handle_data(data: dict):
    if "type" in data:
        if data["type"] == "set":
            return set(data["data"])
        else:
            raise TypeError(f"Can not convert type '{data['type']}'")
    return data["data"]


def _handle_error(result):
    """Handle errors in the server response."""

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
        _handle_error(result)

        return _handle_data(result)

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
    """A pipeline of Redis commands to be executed as a batch on the server.

    Arguments
    ---------
    client : Client
        The client to send the pipeline to.
    max_commands_per_call : int
        The maximum number of commands to send in a single call to the server.
        Decrease this number for large commands to avoid hitting the message size limit.
        Increase it for small commands to reduce latency.
    """

    client: Client
    max_commands_per_call: int = 1_000_000
    pipeline: list = dataclasses.field(default_factory=list, init=False)

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

    def _send_message(self, message) -> list:
        """Send a message to the server and process the response."""
        result = self.client.sio.call(
            "pipeline",
            {"pipeline": message},
            namespace=self.client.namespace,
        )

        if result is None:
            raise exceptions.ZnSocketError("No response from server")
        _handle_error(result)

        return [_handle_data(res) for res in result["data"]]

    def execute(self):
        """Executes the pipeline of commands as a batch on the server."""
        # iterate over self.pipeline and keep adding until the size is greater than max_message_size
        # then send the message, collect the results and continue

        message = []
        results = []
        for idx, entry in enumerate(self.pipeline):
            message.append(entry)
            if len(message) > self.max_commands_per_call:
                log.debug(
                    f"splitting message at index {idx} due to max_message_chunk",
                )
                results.extend(self._send_message(message))
                message = []
        if message:
            results.extend(self._send_message(message))

        return results
