import typing as t

import eventlet.wsgi
import socketio
import typer

from znsocket.server import get_sio

app = typer.Typer()


@app.command()
def server(
    port: int = 5000,
    max_http_buffer_size: t.Optional[int] = None,
):
    """Run a znsocket server.

    Attributes
    ----------
    port : int
        The port to run the server on.
    max_http_buffer_size : int, optional
        The maximum size of the HTTP buffer.
        If you need to share large data, you may need to increase this value.
    """
    sio = get_sio(max_http_buffer_size=max_http_buffer_size)
    server_app = socketio.WSGIApp(sio)
    eventlet.wsgi.server(eventlet.listen(("localhost", port)), server_app)
