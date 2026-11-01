"""A preload parent with scripted children, for slot pool tests.

It runs the real fork path of ``preload_main``. Only the child's boot and task
execution come from ``scripted_child``.
"""

# pyright: reportPrivateUsage=false

from __future__ import annotations

import socket
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

import scripted_child  # type: ignore[import-not-found]  # noqa: E402

from horsies.core.worker import preload_main  # noqa: E402
from horsies.core.worker.child_main import BootFn, serve as real_serve  # noqa: E402
from horsies.core.worker.child_protocol import InitFrame  # noqa: E402


def _serve_with_socket(sock: socket.socket, *args: object) -> None:
    """Give scripted operations the socket, as the exec entry does."""
    scripted_child._sock = sock
    real_serve(sock, *args)  # type: ignore[arg-type]


def _prepare(_init: InitFrame) -> BootFn:
    return scripted_child._boot


def main() -> None:
    preload_main.serve = _serve_with_socket  # type: ignore[assignment]
    ctrl = socket.socket(fileno=int(sys.argv[1]))
    preload_main.run(ctrl, int(sys.argv[2]), _prepare, scripted_child._run)


if __name__ == '__main__':
    main()
