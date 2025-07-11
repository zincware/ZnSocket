import base64
import dataclasses
import datetime
import functools
import gzip
import json
import logging
import typing as t
import uuid

import socketio.exceptions
import typing_extensions as tyex
from redis import Redis
from rich.progress import track

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
    max_message_size_bytes: int = 5 * 1024 * 1024  # 5MB (5% of 100MB limit)
    enable_compression: bool = True  # Enable gzip compression for large messages
    compression_threshold: int = 1024  # Compress messages larger than 1KB

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

    def _serialize_message(self, args: tuple, kwargs: dict) -> bytes:
        """Serialize message arguments to bytes with optional compression.

        Parameters
        ----------
        args : tuple
            Positional arguments.
        kwargs : dict
            Keyword arguments.

        Returns
        -------
        bytes
            Serialized (and optionally compressed) message as bytes.
        """
        payload = [args, kwargs]
        json_bytes = json.dumps(payload, separators=(",", ":"), default=str).encode("utf-8")
        
        # Apply compression if enabled and message is large enough
        if self.enable_compression and len(json_bytes) > self.compression_threshold:
            compressed_bytes = gzip.compress(json_bytes, compresslevel=6)
            # Only use compression if it actually reduces size
            if len(compressed_bytes) < len(json_bytes):
                log.debug(f"Compressed message from {len(json_bytes)} to {len(compressed_bytes)} bytes "
                         f"({len(compressed_bytes)/len(json_bytes)*100:.1f}%)")
                # Prepend compression flag (1 byte) + original size (4 bytes)
                return b'\x01' + len(json_bytes).to_bytes(4, 'big') + compressed_bytes
        
        # Return uncompressed with flag
        return b'\x00' + json_bytes

    def _split_message_bytes(
        self, message_bytes: bytes, max_chunk_size: int
    ) -> list[bytes]:
        """Split message bytes into chunks.

        Parameters
        ----------
        message_bytes : bytes
            The message to split.
        max_chunk_size : int
            Maximum size per chunk in bytes.

        Returns
        -------
        list[bytes]
            List of byte chunks.
        """
        chunks = []
        for i in range(0, len(message_bytes), max_chunk_size):
            chunks.append(message_bytes[i : i + max_chunk_size])
        return chunks

    def _deserialize_message(self, message_bytes: bytes) -> tuple[tuple, dict]:
        """Deserialize message bytes back to args and kwargs with compression support.

        Parameters
        ----------
        message_bytes : bytes
            The serialized (and optionally compressed) message.

        Returns
        -------
        tuple[tuple, dict]
            The original args and kwargs.
        """
        if len(message_bytes) == 0:
            raise ValueError("Empty message bytes")
        
        # Check compression flag
        compression_flag = message_bytes[0:1]
        
        if compression_flag == b'\x01':
            # Compressed message: flag (1) + original_size (4) + compressed_data
            if len(message_bytes) < 5:
                raise ValueError("Invalid compressed message format")
            original_size = int.from_bytes(message_bytes[1:5], 'big')
            compressed_data = message_bytes[5:]
            json_bytes = gzip.decompress(compressed_data)
            if len(json_bytes) != original_size:
                raise ValueError("Decompressed size mismatch")
            log.debug(f"Decompressed message from {len(compressed_data)} to {len(json_bytes)} bytes")
        elif compression_flag == b'\x00':
            # Uncompressed message: flag (1) + json_data
            json_bytes = message_bytes[1:]
        else:
            # Legacy format (no compression flag) - assume uncompressed
            json_bytes = message_bytes
        
        payload = json.loads(json_bytes.decode("utf-8"))
        return tuple(payload[0]), payload[1]

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

        Automatically handles chunking for large messages that exceed the size limit.

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

        # Check if message needs chunking
        message_bytes = self._serialize_message(args, kwargs)

        if len(message_bytes) > self.max_message_size_bytes:
            # Use chunked transmission
            log.debug(
                f"Message size ({len(message_bytes):,} bytes) exceeds limit "
                f"({self.max_message_size_bytes:,} bytes). Using chunked transmission."
            )
            return self._call_chunked(event, message_bytes)
        else:
            # Use normal transmission
            return self._call_normal(event, args, kwargs)

    def _call_normal(self, event: str, args: tuple, kwargs: dict) -> t.Any:
        """Send a normal (non-chunked) message to the server."""
        for idx in reversed(range(self.retry + 1)):
            try:
                return self.sio.call(event, [args, kwargs], namespace=self.namespace)
            except socketio.exceptions.TimeoutError:
                if idx == 0:
                    raise
                log.warning(f"Connection error. Retrying... {idx} attempts left")
                self.sio.sleep(1)

    def _call_chunked(self, event: str, message_bytes: bytes) -> t.Any:
        """Send a chunked message to the server with optimized encoding."""
        # Reserve space for chunk metadata (approximately 150 bytes for base64 encoding)
        chunk_size = self.max_message_size_bytes - 150
        chunks = self._split_message_bytes(message_bytes, chunk_size)
        chunk_id = str(uuid.uuid4())

        log.debug(f"Splitting message into {len(chunks)} chunks with ID {chunk_id} "
                 f"(original: {len(message_bytes)} bytes)")

        # Send all chunks
        chunk_iter = enumerate(chunks)
        if log.isEnabledFor(logging.DEBUG):
            chunk_iter = track(chunk_iter, description="Sending chunks")
        for chunk_index, chunk_data in chunk_iter:
            chunk_metadata = {
                "chunk_id": chunk_id,
                "chunk_index": chunk_index,
                "total_chunks": len(chunks),
                "event": event,
                "data": base64.b64encode(chunk_data).decode('ascii'),  # Binary safe encoding
                "size": len(chunk_data),  # Original chunk size for verification
            }

            for idx in reversed(range(self.retry + 1)):
                try:
                    response = self.sio.call(
                        "chunked_message", chunk_metadata, namespace=self.namespace
                    )
                    if response and response.get("error"):
                        raise exceptions.ZnSocketError(
                            f"Chunk {chunk_index} failed: {response['error']}"
                        )
                    break
                except socketio.exceptions.TimeoutError:
                    if idx == 0:
                        raise
                    log.warning(
                        f"Connection error on chunk {chunk_index}. Retrying... {idx} attempts left"
                    )
                    self.sio.sleep(1)

        # Wait for final response with assembled result
        for idx in reversed(range(self.retry + 1)):
            try:
                final_response = self.sio.call(
                    "get_chunked_result",
                    {"chunk_id": chunk_id},
                    namespace=self.namespace,
                )
                if final_response and final_response.get("error"):
                    raise exceptions.ZnSocketError(
                        f"Chunked message failed: {final_response['error']}"
                    )
                return final_response
            except socketio.exceptions.TimeoutError:
                if idx == 0:
                    raise
                log.warning(
                    f"Connection error getting chunked result. Retrying... {idx} attempts left"
                )
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
    Large pipelines are automatically chunked based on message size.

    Parameters
    ----------
    client : Client
        The client to send the pipeline to.

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
        Automatic chunking is handled by the client.call() method based on message size.

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
        # Send all commands at once - chunking is handled automatically by client.call()
        return self._send_message(self.pipeline)
