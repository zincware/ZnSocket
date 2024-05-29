class ResponseError(Exception):
    def __init__(self, response: str):
        self.response = response

    def __str__(self):
        return self.response
