import typer
from znsocket.server import sio
import socketio
import eventlet.wsgi

app = typer.Typer()


@app.command()
def server(port: int = 5000):
    server_app = socketio.WSGIApp(sio)
    eventlet.wsgi.server(eventlet.listen(("localhost", port)), server_app)
