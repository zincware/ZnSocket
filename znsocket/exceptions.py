import redis.exceptions
import socketio.exceptions


class ZnSocketError(Exception):
    def __init__(self, message: str):
        self.message = message

    def __str__(self):
        return self.message


class ResponseError(ZnSocketError, redis.exceptions.ResponseError):
    """ResponseError."""


class ConnectionError(ZnSocketError, socketio.exceptions.ConnectionError):
    def __init__(self, address: str):
        self.address = address

    def __str__(self):
        response = f"Could not connect to {self.address}. "
        response += "Is the 'znsocket' server running? "
        response += "You can start it using the CLI 'znsocket'."
        return response


class DataError(ZnSocketError, redis.exceptions.DataError):
    """DataError."""


class UnknownEventError(ZnSocketError):
    """UnknownEventError."""
