"""The child program (`horsies.core.worker.child_main`) as the handler child,
run as a real process.

The test is the worker: it gives the child its socket, sends the init frame
of the handler kind, an `events` frame and the stop frame, and reads the
frames of the child. The app of the child has a hook and a handler that
write to a file, and the handler fails for one event. No database is needed:
the handler child opens no connection of its own.
"""

from __future__ import annotations

import json
import os
import socket
import subprocess
import sys
import time
from pathlib import Path
from typing import Final, cast

import pytest

from horsies.core.worker.child_runner import CHILD_HOOK_FAILURE_EXIT_CODE
from horsies.core.worker_manifest import CHILD_MAIN_MODULE
from tests.fixtures.core_frames import framed, handler_kind, init_meta
from tests.fixtures.preload_worker import read_frame

pytestmark = [pytest.mark.unit]

REPOSITORY_ROOT: Final = Path(__file__).resolve().parents[2]
_VECTORS: Final = (
    REPOSITORY_ROOT.parents[1]
    / 'core'
    / 'tests'
    / 'fixtures'
    / 'event_wire'
    / 'vectors.json'
)
STEP_TIMEOUT_S: Final = 30.0

HANDLER_APP: Final = """
from pathlib import Path

from horsies.core.app import Horsies
from horsies.core.models.app import AppConfig
from horsies.core.models.broker import PostgresConfig
from horsies.core.models.queues import QueueMode
from horsies.telemetry import AttemptEnded

OUT = Path({out!r})

app = Horsies(
    AppConfig(
        queue_mode=QueueMode.DEFAULT,
        broker=PostgresConfig(database_url='postgresql://u:p@localhost/none'),
    ),
)


@app.on_handler_child_start
def mark_start() -> None:
    {hook_body}


@app.on_event(AttemptEnded)
def record_end(event: AttemptEnded) -> None:
    if event.attempt == 2:
        raise RuntimeError('the sink refused attempt 2')
    with OUT.open('a', encoding='utf-8') as out:
        out.write(f'end {{event.attempt}}\\n')
"""

_WRITING_HOOK: Final = "OUT.write_text('started\\n', encoding='utf-8')"
_FAILING_HOOK: Final = "raise RuntimeError('no tracer provider')"


def _attempt_ended_text(attempt: int) -> str:
    document = cast(
        'dict[str, object]', json.loads(_VECTORS.read_text(encoding='utf-8'))
    )
    vectors = cast('list[dict[str, object]]', document['vectors'])
    for vector in vectors:
        wire = cast('list[object]', vector['wire'])
        if wire[0] == 'AttemptEnded':
            wire[6] = attempt
            return json.dumps(wire, separators=(',', ':'))
    raise AssertionError('the vectors hold an AttemptEnded event')


def _spawn(
    tmp_path: Path, hook_body: str
) -> tuple[subprocess.Popen[bytes], socket.socket, Path]:
    out = tmp_path / 'handler_lines.txt'
    app = tmp_path / 'handler_app.py'
    app.write_text(
        HANDLER_APP.format(out=str(out), hook_body=hook_body), encoding='utf-8'
    )
    worker_end, child_end = socket.socketpair()
    worker_end.settimeout(STEP_TIMEOUT_S)
    process = subprocess.Popen(
        [
            sys.executable,
            '-m',
            CHILD_MAIN_MODULE,
            str(child_end.fileno()),
            str(os.getpid()),
        ],
        pass_fds=(child_end.fileno(),),
        stdin=subprocess.DEVNULL,
        cwd=REPOSITORY_ROOT,
    )
    child_end.close()
    worker_end.sendall(
        framed(
            init_meta(
                child_kind=handler_kind(),
                app_locator=f'{app}:app',
                parent_sys_path=(str(REPOSITORY_ROOT), *sys.path),
                database_url='postgresql://unused@127.0.0.1:1/unused',
            ),
        ),
    )
    return process, worker_end, out


def _end(process: subprocess.Popen[bytes], worker_end: socket.socket) -> None:
    worker_end.close()
    if process.poll() is None:
        process.kill()
        _ = process.wait(timeout=STEP_TIMEOUT_S)


def _frames_until_exit(worker_end: socket.socket) -> list[dict[str, object]]:
    frames: list[dict[str, object]] = []
    while True:
        try:
            frames.append(read_frame(worker_end))
        except EOFError:
            return frames


def test_the_handler_child_runs_its_hook_and_handler_then_stops(tmp_path: Path) -> None:
    process, worker_end, out = _spawn(tmp_path, _WRITING_HOOK)
    try:
        ready = read_frame(worker_end)
        assert ready['kind'] == 'ready', ready
        assert out.read_text(encoding='utf-8') == 'started\n'
        texts = [_attempt_ended_text(attempt) for attempt in (1, 2, 3)]
        worker_end.sendall(
            framed({'kind': 'events'}, f'[{",".join(texts)}]'.encode('utf-8'))
        )
        deadline = time.monotonic() + STEP_TIMEOUT_S
        while out.read_text(encoding='utf-8').count('end ') < 2:
            assert time.monotonic() < deadline, out.read_text(encoding='utf-8')
            time.sleep(0.01)
        worker_end.sendall(framed({'kind': 'stop'}))
        frames = _frames_until_exit(worker_end)
        code = process.wait(timeout=STEP_TIMEOUT_S)
    finally:
        _end(process, worker_end)

    assert code == 0
    assert out.read_text(encoding='utf-8') == 'started\nend 1\nend 3\n'
    assert frames, 'the child sends a last handler_stats frame at the stop'
    assert all(frame['kind'] == 'handler_stats' for frame in frames), frames
    (counts,) = cast('list[dict[str, object]]', frames[-1]['handlers'])
    # A file app imports under the module name of its path
    # (`horsies._dynamic.<hash>`), in the worker as in the child.
    assert str(counts['handler']).startswith('horsies._dynamic.')
    assert str(counts['handler']).endswith('.record_end')
    assert (counts['errors'], counts['dropped']) == (1, 0)


def test_a_failing_hook_ends_the_child_with_the_hook_code_before_ready(
    tmp_path: Path,
) -> None:
    process, worker_end, _out = _spawn(tmp_path, _FAILING_HOOK)
    try:
        frames = _frames_until_exit(worker_end)
        code = process.wait(timeout=STEP_TIMEOUT_S)
    finally:
        _end(process, worker_end)

    assert frames == []
    assert code == CHILD_HOOK_FAILURE_EXIT_CODE
