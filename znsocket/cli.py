import datetime
import typing as t

import typer

from znsocket import Server

app = typer.Typer()


@app.command()
def server(
    port: int = typer.Argument(8080, help="The port to run the server on."),
    max_http_buffer_size: t.Optional[int] = typer.Option(
        100 * 1024 * 1024,
        help="The maximum size of the HTTP buffer. The default value is set to 100 MB. If a single data packet exceeds this size, the server will SILENTLY ignore the packet.",
        show_default=True,
    ),
):
    """Run a znsocket server.

    The server will provide a WebSocket interface to the clients.
    It enables keeping the data synchronized between the different clients.
    """
    typer.echo(
        f"{datetime.datetime.now().isoformat()}: Starting znsocket server on port {port}"
    )
    server = Server(port=port, max_http_buffer_size=max_http_buffer_size)
    server.run()
    typer.echo(
        f"{datetime.datetime.now().isoformat()}: Stopped znsocket server on port {port}"
    )
