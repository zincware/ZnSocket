import argparse
import socket
import sys


def check_socketio_server(port: int):
    try:
        s = socket.create_connection(("localhost", port), timeout=5)
        s.close()
        sys.exit(0)  # success
    except Exception as e:
        print(f"Health check failed: {e}")
        sys.exit(1)  # failure


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Check if a socket.io server is running."
    )
    parser.add_argument("--port", type=int, default=4748, help="The port to check.")
    args = parser.parse_args()
    check_socketio_server(args.port)
