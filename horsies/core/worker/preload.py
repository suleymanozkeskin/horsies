"""The worker's side of the preload parent. Linux only.

``PreloadParent`` owns one preload process and asks it for children. Only one
request runs at a time. ``ReparentedProcess`` watches a child that reparented
to this process, through a pidfd on the event loop.

The worker is the real parent of each forked child, so exit status comes from
``waitpid``, as it does for an ``exec`` child. The subreaper flag is on only
while a spawn runs, so the orphans of a task do not collect on the worker.
"""

from __future__ import annotations

import asyncio
import ctypes
import errno
import os
import socket
import sys
from collections.abc import Collection, Sequence
from dataclasses import dataclass
from typing import Final, Protocol, assert_never

from horsies.core.logging import get_logger
from horsies.core.types.result import Err, Ok, Result
from horsies.core.worker.child_protocol import (
    FRAME_HEADER,
    ChildForkedFrame,
    ForkFailedFrame,
    InitFrame,
    PreloadFrame,
    PreloadReadyFrame,
    PreloadRefusedFrame,
    decode_preload_frame,
    encode_parent_frame,
    parse_header,
)
from horsies.core.worker.config import ChildPreload
from horsies.core.worker.preload_protocol import SPAWN_REQUEST

logger = get_logger('worker')

PRELOAD_READY_TIMEOUT_S: Final = 120.0
PRELOAD_FORK_TIMEOUT_S: Final = 5.0
PRELOAD_STOP_TIMEOUT_S: Final = 5.0
ORPHAN_SWEEP_LIMIT: Final = 64
_PR_SET_CHILD_SUBREAPER: Final = 36


class ChildProcess(Protocol):
    """What a slot needs from a process, however it started."""

    @property
    def pid(self) -> int: ...

    @property
    def returncode(self) -> int | None: ...

    async def wait(self) -> int | None: ...


@dataclass(frozen=True, slots=True)
class PreloadSupported:
    """This host can run the preload parent."""


@dataclass(frozen=True, slots=True)
class PreloadUnsupported:
    """This host cannot run the preload parent. Children use ``exec``."""

    reason: str


@dataclass(frozen=True, slots=True)
class PreloadRefused:
    """The preload parent ran and refused: an import failed, or a fork is unsafe."""

    reason: str


@dataclass(frozen=True, slots=True)
class PreloadStartFailed:
    """The preload parent could not start, or it stopped before it was ready."""

    detail: str


@dataclass(frozen=True, slots=True)
class PreloadDown:
    """No child came from this request. The caller uses ``exec``."""

    detail: str


def _set_subreaper(enabled: bool) -> bool:
    """Set or clear the subreaper flag. Returns False when the kernel refuses."""
    return ctypes.CDLL(None).prctl(_PR_SET_CHILD_SUBREAPER, int(enabled), 0, 0, 0) == 0


def _open_pidfd(pid: int) -> int:
    """Open a pidfd. Raises ``OSError`` when the kernel has none."""
    if sys.platform == 'linux':
        return os.pidfd_open(pid)
    raise OSError(errno.ENOSYS, 'pidfd needs Linux')


def _next_exited_child() -> int | None:
    """Return an exited child without reaping it, or None if there is none."""
    if sys.platform != 'linux':
        return None
    try:
        found = os.waitid(os.P_ALL, 0, os.WEXITED | os.WNOHANG | os.WNOWAIT)
    except ChildProcessError:
        return None
    return None if found is None else found.si_pid


def preload_support() -> PreloadSupported | PreloadUnsupported:
    """Check what the preload parent needs: Linux, a pidfd, and the subreaper flag."""
    if sys.platform != 'linux':
        return PreloadUnsupported(f'platform {sys.platform} is not Linux')
    try:
        os.close(_open_pidfd(os.getpid()))
    except OSError as exc:
        return PreloadUnsupported(f'pidfd_open: {exc}')
    if not _set_subreaper(False):
        return PreloadUnsupported('prctl is refused')
    return PreloadSupported()


class ReparentedProcess:
    """A forked child whose real parent is this process."""

    def __init__(self, pid: int, loop: asyncio.AbstractEventLoop) -> None:
        """Start the watch. Raises ``OSError`` when the pidfd cannot open."""
        self.pid = pid
        self.returncode: int | None = None
        self._loop = loop
        self._done: asyncio.Future[int | None] = loop.create_future()
        self._pidfd = _open_pidfd(pid)
        loop.add_reader(self._pidfd, self._on_exit)

    def _on_exit(self) -> None:
        self._loop.remove_reader(self._pidfd)
        os.close(self._pidfd)
        try:
            _, status = os.waitpid(self.pid, 0)
        except ChildProcessError:
            # Only a defect reaches this: the sweep skips known pids.
            logger.critical('Exit status of child pid=%s was taken', self.pid)
        else:
            self.returncode = os.waitstatus_to_exitcode(status)
        if not self._done.done():
            self._done.set_result(self.returncode)

    async def wait(self) -> int | None:
        """Wait for the exit. Returns None only after the defect above."""
        return await asyncio.shield(self._done)


def reap_landed_orphans(known_pids: Collection[int]) -> None:
    """Reap task orphans that landed here while the subreaper flag was on.

    An orphan is a child that this worker did not start and that left the
    worker's session. Any other child keeps its exit status for its own waiter,
    and the sweep stops there, because ``waitid`` keeps returning that child.
    """
    own_session = os.getsid(0)
    for _ in range(ORPHAN_SWEEP_LIMIT):
        pid = _next_exited_child()
        if pid is None or pid in known_pids:
            return
        try:
            if os.getsid(pid) == own_session:
                return
            os.waitpid(pid, os.WNOHANG)
        except (ChildProcessError, ProcessLookupError):
            return


class PreloadParent:
    """One running preload process. Serves one spawn request at a time."""

    def __init__(
        self,
        process: asyncio.subprocess.Process,
        sock: socket.socket,
    ) -> None:
        self.process = process
        self._sock = sock
        self._loop = asyncio.get_running_loop()
        self._lock = asyncio.Lock()

    @classmethod
    async def start(
        cls,
        program: Sequence[str],
        mode: ChildPreload,
        init: InitFrame,
    ) -> Result['PreloadParent', PreloadRefused | PreloadStartFailed]:
        """Start the process and wait until it reports ready, bounded."""
        worker_sock, preload_sock = socket.socketpair()
        try:
            process = await asyncio.create_subprocess_exec(
                sys.executable,
                *program,
                str(preload_sock.fileno()),
                str(os.getpid()),
                mode.value,
                stdin=asyncio.subprocess.DEVNULL,
                pass_fds=(preload_sock.fileno(),),
                start_new_session=True,
            )
        except OSError as exc:
            worker_sock.close()
            return Err(PreloadStartFailed(f'{type(exc).__name__}: {exc}'))
        finally:
            preload_sock.close()
        worker_sock.setblocking(False)
        parent = cls(process, worker_sock)
        return await parent._await_ready(init)

    async def _await_ready(
        self,
        init: InitFrame,
    ) -> Result['PreloadParent', PreloadRefused | PreloadStartFailed]:
        encoded = encode_parent_frame(init)
        try:
            await self._loop.sock_sendall(self._sock, encoded.head + encoded.body)
            frame = await asyncio.wait_for(self._read_frame(), PRELOAD_READY_TIMEOUT_S)
        except (OSError, EOFError, TimeoutError) as exc:
            await self.stop()
            return Err(PreloadStartFailed(f'{type(exc).__name__}: {exc}'))
        match frame:
            case PreloadReadyFrame():
                return Ok(self)
            case PreloadRefusedFrame(reason=reason):
                await self.stop()
                return Err(PreloadRefused(reason))
            case ChildForkedFrame() | ForkFailedFrame():
                await self.stop()
                return Err(PreloadStartFailed(f'unexpected frame {frame!r}'))
            case _ as unreachable:
                assert_never(unreachable)

    async def _read_exact(self, count: int) -> bytes:
        data = bytearray()
        while len(data) < count:
            chunk = await self._loop.sock_recv(self._sock, count - len(data))
            if not chunk:
                raise EOFError
            data.extend(chunk)
        return bytes(data)

    async def _read_frame(self) -> PreloadFrame:
        """Read one frame. A bad frame is a defect, so this raises ``EOFError``."""
        match parse_header(await self._read_exact(FRAME_HEADER.size)):
            case Ok(sizes):
                pass
            case Err(over):
                raise EOFError(f'preload frame over the bounds: {over}')
        match decode_preload_frame(await self._read_exact(sizes.meta_bytes)):
            case Ok(frame):
                return frame
            case Err(malformed):
                raise EOFError(f'bad preload frame: {malformed.reason}')

    @property
    def is_running(self) -> bool:
        return self.process.returncode is None

    async def fork_child(
        self,
        child_sock: socket.socket,
        live_pids: set[int],
    ) -> Result[ReparentedProcess, PreloadDown]:
        """Ask for one child on ``child_sock``. The socket stays open on failure.

        ``live_pids`` is the pool's registry. A new pid enters it before any
        await, so a sweep can never take this child's exit status.

        A preload parent that fails mid-request is killed: the channel may hold
        a partial frame, so it cannot serve another request.
        """
        async with self._lock:
            if not self.is_running:
                return Err(PreloadDown('the preload parent exited'))
            if not _set_subreaper(True):
                return Err(PreloadDown('prctl is refused'))
            try:
                socket.send_fds(self._sock, [SPAWN_REQUEST], [child_sock.fileno()])
                frame = await asyncio.wait_for(
                    self._read_frame(),
                    PRELOAD_FORK_TIMEOUT_S,
                )
            except (OSError, EOFError, TimeoutError) as exc:
                self._kill()
                return Err(PreloadDown(f'{type(exc).__name__}: {exc}'))
            finally:
                _set_subreaper(False)
            match frame:
                case ChildForkedFrame(pid=pid):
                    live_pids.add(pid)
                case ForkFailedFrame() | PreloadReadyFrame() | PreloadRefusedFrame():
                    pass
                case _ as unreachable:
                    assert_never(unreachable)
            reap_landed_orphans({*live_pids, self.process.pid})
        return self._child_from(frame)

    def _child_from(self, frame: PreloadFrame) -> Result[ReparentedProcess, PreloadDown]:
        match frame:
            case ChildForkedFrame(pid=pid):
                try:
                    return Ok(ReparentedProcess(pid, self._loop))
                except OSError as exc:
                    return Err(PreloadDown(f'pidfd_open: {exc}'))
            case ForkFailedFrame(reason=reason):
                return Err(PreloadDown(reason))
            case PreloadReadyFrame() | PreloadRefusedFrame():
                self._kill()
                return Err(PreloadDown(f'unexpected frame {frame!r}'))
            case _ as unreachable:
                assert_never(unreachable)

    def _kill(self) -> None:
        if self.is_running:
            self.process.kill()

    async def stop(self) -> None:
        """Close the channel, then wait, bounded. It exits on the closed socket."""
        self._sock.close()
        try:
            await asyncio.wait_for(self.process.wait(), PRELOAD_STOP_TIMEOUT_S)
        except TimeoutError:
            logger.warning('Preload parent pid=%s ignored the close', self.process.pid)
            self._kill()
            await self.process.wait()
