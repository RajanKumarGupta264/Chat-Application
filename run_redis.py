"""Standalone local Redis TCP server for development and multi-worker testing.

Runs a local Redis-compatible broker on 127.0.0.1:6379 without requiring
Docker or external Redis installations.
"""

import sys
import time
from fakeredis import TcpFakeServer


def main():
    host = "127.0.0.1"
    port = 6379
    print("=" * 70)
    print(f"Starting Local Redis TCP Broker on {host}:{port}...")
    print("=" * 70)
    try:
        server = TcpFakeServer((host, port))
        print(f"[READY] Redis server listening on {host}:{port}")
        print("Both Worker 1 (:8000) and Worker 2 (:8001) can now sync in real-time!")
        print("Press Ctrl+C to stop.")
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nStopping Redis server...")
    except Exception as exc:
        print(f"[ERROR] Could not start Redis server: {exc}")
        sys.exit(1)


if __name__ == "__main__":
    main()

