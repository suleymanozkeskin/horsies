"""A worker for tests of the preload program: it starts the real preload
program (`horsies.core.worker.preload_main`), asks it for children, boots
each child through the real child program, stops it, and writes a JSON
report.

Run as `python -m tests.fixtures.preload_worker <mode> <app locator>
<children> <report path>` from the repository root. Linux only: this
process holds the subreaper flag, so each forked child reparents to it, as
a child of the core's worker does.

The report: `{"preload": <first preload frame>, "children": [...]}`. Each
child: its pid, its first frame, the inodes of the sockets that it holds
(standard streams left out), the inode of the socket it was given, and its
exit code after the stop frame. The process exits 1 with the cause on
stderr when a step fails.
"""

from __future__ import annotations

import ctypes
import json
import os
import socket
import struct
import subprocess
import sys
from pathlib import Path
from typing import Final, cast

from horsies.core.worker.preload_main import SPAWN_REQUEST
from tests.fixtures.core_frames import framed, init_meta

PR_SET_CHILD_SUBREAPER: Final = 36
STEP_TIMEOUT_S: Final = 30.0
HEADER: Final = struct.Struct('>IQ')
STANDARD_STREAMS: Final = frozenset({0, 1, 2})
PRELOAD_PROGRAM: Final = 'horsies.core.worker.preload_main'


def _read_exact(sock: socket.socket, count: int) -> bytes:
    data = bytearray()
    while len(data) < count:
        chunk = sock.recv(count - len(data))
        if not chunk:
            raise EOFError(f'the peer closed after {len(data)} of {count} bytes')
        data.extend(chunk)
    return bytes(data)


def read_frame(sock: socket.socket) -> dict[str, object]:
    """The metadata of the next frame on `sock`; its body is read and
    dropped. Raises `EOFError` or `TimeoutError` when no whole frame comes."""
    meta_bytes, body_bytes = HEADER.unpack(_read_exact(sock, HEADER.size))
    meta = cast('dict[str, object]', json.loads(_read_exact(sock, meta_bytes)))
    _ = _read_exact(sock, body_bytes)
    return meta


def _socket_inodes(pid: int) -> list[int]:
    """The inodes of the sockets that process `pid` holds, standard streams
    left out, in descriptor order."""
    fd_dir = Path(f'/proc/{pid}/fd')
    found: list[int] = []
    for entry in sorted(fd_dir.iterdir(), key=lambda path: int(path.name)):
        if int(entry.name) in STANDARD_STREAMS:
            continue
        target = os.readlink(entry)
        if target.startswith('socket:['):
            found.append(int(target.removeprefix('socket:[').removesuffix(']')))
    return found


def _child(worker_end: socket.socket, init: bytes) -> dict[str, object]:
    """Ask for one child, boot it, record it, stop it, and reap it."""
    parent_end, child_end = socket.socketpair()
    given = os.fstat(child_end.fileno()).st_ino
    _ = socket.send_fds(worker_end, [SPAWN_REQUEST], [child_end.fileno()])
    child_end.close()
    forked = read_frame(worker_end)
    if forked.get('kind') != 'child_forked':
        raise RuntimeError(f'the preload program gave no child: {forked}')
    pid = cast('int', forked['pid'])
    parent_end.settimeout(STEP_TIMEOUT_S)
    parent_end.sendall(init)
    first = read_frame(parent_end)
    sockets = _socket_inodes(pid)
    parent_end.sendall(framed({'kind': 'stop'}))
    _, status = os.waitpid(pid, 0)
    parent_end.close()
    return {
        'pid': pid,
        'first_frame': first,
        'sockets': sockets,
        'given_socket': given,
        'exit_code': os.waitstatus_to_exitcode(status),
    }


def run(mode: str, app_locator: str, children: int) -> dict[str, object]:
    """Start the preload program in `mode`, and boot `children` children
    from it. Returns the report."""
    libc = ctypes.CDLL(None, use_errno=True)
    if libc.prctl(PR_SET_CHILD_SUBREAPER, 1, 0, 0, 0) != 0:
        raise OSError(ctypes.get_errno(), 'prctl(PR_SET_CHILD_SUBREAPER) is refused')
    worker_end, preload_end = socket.socketpair()
    worker_end.settimeout(STEP_TIMEOUT_S)
    process = subprocess.Popen(
        [
            sys.executable,
            '-m',
            PRELOAD_PROGRAM,
            str(preload_end.fileno()),
            str(os.getpid()),
            mode,
        ],
        pass_fds=(preload_end.fileno(),),
        stdin=subprocess.DEVNULL,
        start_new_session=True,
    )
    preload_end.close()
    init = framed(init_meta(app_locator=app_locator, parent_sys_path=tuple(sys.path)))
    worker_end.sendall(init)
    preload = read_frame(worker_end)
    booted: list[dict[str, object]] = []
    if preload.get('kind') == 'preload_ready':
        booted = [_child(worker_end, init) for _ in range(children)]
    worker_end.close()
    _ = process.wait(timeout=STEP_TIMEOUT_S)
    return {'preload': preload, 'children': booted}


def main(argv: list[str]) -> None:
    """Program entry: `<mode> <app locator> <children> <report path>`."""
    mode, app_locator, children, report = argv
    result = run(mode, app_locator, int(children))
    Path(report).write_text(json.dumps(result), encoding='utf-8')


if __name__ == '__main__':
    main(sys.argv[1:])
