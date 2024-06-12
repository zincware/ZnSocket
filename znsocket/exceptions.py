class ZnSocketError(Exception):
    pass


class ResponseError(ZnSocketError):
    def __init__(self, response: str):
        self.response = response

    def __str__(self):
        return self.response


class ConnectionError(ZnSocketError):
    def __init__(self, address: str):
        self.address = address

    def __str__(self):
        response = f"Could not connect to {self.address}. "
        response += "Is the 'znsocket' server running? "
        response += "You can start it using the CLI 'znsocket'."
        return response


class DataError(ZnSocketError):
    def __init__(self, message: str):
        self.message = message

    def __str__(self):
        return self.message
