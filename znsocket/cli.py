import logging
import typing as t

import typer

from znsocket import Server

app = typer.Typer()


@app.command()
def server(
    port: int = typer.Option(8080, help="The port to run the server on."),
    max_http_buffer_size: t.Optional[int] = typer.Option(
        5 * 1024 * 1024,
        help="The maximum size of the HTTP buffer. The default value is set to 5 MB. If a single data packet exceeds this size, the server will SILENTLY ignore the packet.",
        show_default=True,
    ),
    storage: str = typer.Option(
        "memory", help="The storage backend to use (memory or redis)."
    ),
    log_level: str = typer.Option(
        "INFO",
        help="The logging level for the server. Options are: DEBUG, INFO, WARNING, ERROR, CRITICAL.",
        show_default=True,
    ),
):
    """Run a znsocket server.

    The server will provide a WebSocket interface to the clients.
    It enables keeping the data synchronized between the different clients.
    """
    logging.basicConfig(
        level=log_level.upper(),
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )

    log = logging.getLogger(__name__)

    log.info(f"Starting znsocket server on port {port}")
    server = Server(
        port=port, max_http_buffer_size=max_http_buffer_size, storage=storage
    )
    server.run()
    log.info(f"Stopped znsocket server on port {port}")
