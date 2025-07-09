import dataclasses
import datetime
import functools
import logging
import typing as t

import socketio.exceptions
import typing_extensions as tyex
from redis import Redis

from znsocket import exceptions
from znsocket.abc import RefreshDataTypeDict
from znsocket.utils import handle_error, parse_url

log = logging.getLogger(__name__)


def _handle_data(data: dict):
    if "type" in data:
        if data["type"] == "set":
            return set(data["data"])
        else:
            raise TypeError(f"Can not convert type '{data['type']}'")
    return data["data"]


@dataclasses.dataclass
class Client:
    """Client to interact with a znsocket server.

    The Client class provides an interface to connect to and communicate with a znsocket server
    using websockets. It supports Redis-like commands and provides automatic reconnection
    capabilities.

    Parameters
    ----------
    address : str
        The address of the znsocket server (e.g., "http://localhost:5000").
    decode_responses : bool, optional
        Whether to decode responses from the server. Default is True.
    namespace : str, optional
        The namespace to connect to. Default is "/znsocket".
    refresh_callbacks : dict, optional
        A dictionary of callbacks to call when a refresh event is received.
    adapter_callback : callable, optional
        Callback function for adapter events. Default is None.
    delay_between_calls : datetime.timedelta, optional
        The time to wait between calls. Default is None.
    retry : int, optional
        The number of times to retry a failed call. Default is 1.
    connect_wait_timeout : int, optional
        Timeout in seconds for connection establishment. Default is 1.

    Attributes
    ----------
    sio : socketio.Client
        The underlying socket.io client instance.

    Examples
    --------
    >>> client = Client("http://localhost:5000")
    >>> client.hset("mykey", "field1", "value1")
    >>> client.hget("mykey", "field1")
    'value1'
    """

    address: str
    decode_responses: bool = True
    sio: socketio.Client = dataclasses.field(
        default_factory=socketio.Client, repr=False, init=False
    )
    namespace: str = "/znsocket"
    refresh_callbacks: dict = dataclasses.field(default_factory=dict)
    adapter_callback: t.Callable | None = None
    delay_between_calls: datetime.timedelta | None = None
    retry: int = 1
    connect_wait_timeout: int = 1

    _last_call: datetime.datetime = dataclasses.field(
        default_factory=datetime.datetime.now, init=False
    )

    def pipeline(self, *args, **kwargs) -> "Pipeline":
        """Create a pipeline for batching Redis commands.

        Parameters
        ----------
        *args
            Positional arguments to pass to the Pipeline constructor.
        **kwargs
            Keyword arguments to pass to the Pipeline constructor.

        Returns
        -------
        Pipeline
            A new Pipeline instance for batching commands.

        Examples
        --------
        >>> client = Client("http://localhost:5000")
        >>> pipe = client.pipeline()
        >>> pipe.hset("key1", "field1", "value1")
        >>> pipe.hset("key2", "field2", "value2")
        >>> results = pipe.execute()
        """
        return Pipeline(self, *args, **kwargs)

    @classmethod
    def from_url(cls, url, namespace: str = "/znsocket", **kwargs) -> "Client":
        """Connect to a znsocket server using a URL.

        Parameters
        ----------
        url : str
            The URL of the znsocket server. Should be in the format
            "znsocket://127.0.0.1:5000".
        namespace : str, optional
            The namespace to connect to. Default is "/znsocket".
        **kwargs
            Additional keyword arguments to pass to the Client constructor.

        Returns
        -------
        Client
            A new Client instance connected to the specified server.

        Examples
        --------
        >>> client = Client.from_url("znsocket://127.0.0.1:5000")
        >>> client.hset("key", "field", "value")
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

        @self.sio.on("adapter:get", namespace=self.namespace)
        def adapter(data: RefreshDataTypeDict):
            if self.adapter_callback is None:
                raise exceptions.ZnSocketError("No adapter callback set")
            return self.adapter_callback(data)

        _url, _path = parse_url(self.address)
        try:
            self.sio.connect(
                _url,
                namespaces=[self.namespace],
                wait=True,
                wait_timeout=self.connect_wait_timeout,
                socketio_path=f"{_path}/socket.io" if _path else "socket.io",
            )

        except socketio.exceptions.ConnectionError as err:
            raise exceptions.ConnectionError(self.address) from err

        if not self.decode_responses:
            raise NotImplementedError("decode_responses=False is not supported yet")

    def call(self, event: str, *args, **kwargs) -> t.Any:
        """Call an event on the server.

        Parameters
        ----------
        event : str
            The event name to call on the server.
        *args
            Positional arguments to pass to the event.
        **kwargs
            Keyword arguments to pass to the event.

        Returns
        -------
        Any
            The response from the server.

        Raises
        ------
        socketio.exceptions.TimeoutError
            If the server does not respond within the timeout period after all retries.
        """
        if self.delay_between_calls:
            time_since_last_call = datetime.datetime.now() - self._last_call
            delay_needed = self.delay_between_calls - time_since_last_call
            if delay_needed > datetime.timedelta(0):
                self.sio.sleep(delay_needed.total_seconds())
            self._last_call = datetime.datetime.now()

        for idx in reversed(range(self.retry + 1)):
            try:
                return self.sio.call(event, [args, kwargs], namespace=self.namespace)
            except socketio.exceptions.TimeoutError:
                if idx == 0:
                    raise
                log.warning(f"Connection error. Retrying... {idx} attempts left")
                self.sio.sleep(1)

    def _redis_command(self, command, *args, **kwargs):
        """Generic handler for Redis commands."""
        result = self.call(command, *args, **kwargs)

        if result is None:
            raise exceptions.ZnSocketError("No response from server")
        handle_error(result)

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

    The Pipeline class allows batching multiple Redis commands together to reduce
    network overhead and improve performance when executing multiple operations.

    Parameters
    ----------
    client : Client
        The client to send the pipeline to.
    max_commands_per_call : int, optional
        The maximum number of commands to send in a single call to the server.
        Decrease this number for large commands to avoid hitting the message size limit.
        Increase it for small commands to reduce latency. Default is 1,000,000.

    Attributes
    ----------
    pipeline : list
        Internal list storing the commands to be executed.

    Examples
    --------
    >>> client = Client("http://localhost:5000")
    >>> pipe = client.pipeline()
    >>> pipe.hset("key1", "field1", "value1")
    >>> pipe.hset("key2", "field2", "value2")
    >>> results = pipe.execute()
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
        result = self.client.call(
            "pipeline",
            message=message,
        )

        if result is None:
            raise exceptions.ZnSocketError("No response from server")
        handle_error(result)

        return [_handle_data(res) for res in result["data"]]

    def execute(self):
        """Execute the pipeline of commands as a batch on the server.

        Sends all queued commands to the server and returns the results in order.
        The commands are sent in batches if the total number exceeds max_commands_per_call.

        Returns
        -------
        list
            A list of results corresponding to each command in the pipeline, in order.

        Raises
        ------
        exceptions.ZnSocketError
            If the server returns no response or an error occurs.

        Examples
        --------
        >>> pipe = client.pipeline()
        >>> pipe.hset("key1", "field1", "value1")
        >>> pipe.hget("key1", "field1")
        >>> results = pipe.execute()
        >>> print(results)  # [1, 'value1']
        """
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
        if len(message) > 0:
            results.extend(self._send_message(message))

        return results
