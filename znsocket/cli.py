import typer
from znsocket.server import get_sio
import socketio
import eventlet.wsgi
import typing as t

app = typer.Typer()


@app.command()
def server(port: int = 5000, max_http_buffer_size: t.Optional[int] = None):
    sio = get_sio(max_http_buffer_size=max_http_buffer_size)
    server_app = socketio.WSGIApp(sio)
    eventlet.wsgi.server(eventlet.listen(("localhost", port)), server_app)
