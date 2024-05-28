import datetime
import typing as t

import typer

from znsocket import Server

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
    typer.echo(
        f"{datetime.datetime.now().isoformat()}: Starting znsocket server on port {port}"
    )
    server = Server(port=port, max_http_buffer_size=max_http_buffer_size)
    server.run()
    typer.echo(
        f"{datetime.datetime.now().isoformat()}: Stopped znsocket server on port {port}"
    )
