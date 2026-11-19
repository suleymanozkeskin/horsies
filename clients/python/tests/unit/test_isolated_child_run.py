"""The end of a run in an isolated child (``child_main``): the order of the
finalizing marker and the frames, the refusal of a run on a child to retire,
the exits of a run that escapes the runner, the stop exit, the stop signals,
the restored parent state, and the bounds of free text and of the result.

The socket and ``os._exit`` are replaced; the task loop is a real
``TaskLoop``.
"""

# pyright: reportPrivateUsage=false

from __future__ import annotations

import asyncio
import json
import multiprocessing
import signal
import struct
import subprocess
import sys
import time
import uuid
from collections.abc import Coroutine, Iterator
from pathlib import Path
from typing import Any, Final, NoReturn

import pytest

import horsies._core as core
from horsies.core.worker import child_main
from horsies.core.worker import task_loop as task_loop_module
from horsies.core.worker.child_frames import MAX_BODY_BYTES
from horsies.core.worker.run_return import (
    MarkerDone,
    MarkerEnd,
    MarkerMetStoppedCore,
    ResultEnvelope,
    SetRunning,
    StoppedBeforeRunning,
)
from horsies.core.worker.task_loop import (
    ExecutorShutDown,
    ExecutorThreadsLeft,
    TaskLoop,
)
from tests.fixtures.core_frames import init_frame, run_frame

pytestmark = [pytest.mark.unit]

_HEADER: Final = struct.Struct('>IQ')
TASK_ID: Final = uuid.UUID('0190f2d8-7c1e-7a3b-9f00-0000000000a1')
ENVELOPE: Final = '{"__h_task_result__":true,"ok":"kept","err":null}'
SHORT_GRACE_S: Final = 0.2
SHORT_CLOSE_TIMEOUT_S: Final = 0.2
PROGRAM_TIMEOUT_S: Final = 30.0
MARKER: Final = 'marker'


class _Exited(Exception):
    def __init__(self, code: int) -> None:
        super().__init__(code)
        self.code = code


def _raise_exit(code: int) -> NoReturn:
    raise _Exited(code)


class _RecordingSocket:
    """Records the kind and the metadata of each frame that the child sends,
    in one list with the finalizing marker writes."""

    def __init__(self, events: list[str], metas: list[dict[str, Any]]) -> None:
        self._events = events
        self._metas = metas

    def sendall(self, data: bytes) -> None:
        meta_bytes, _body_bytes = _HEADER.unpack_from(data)
        meta = json.loads(data[_HEADER.size : _HEADER.size + meta_bytes])
        self._events.append(str(meta['kind']))
        self._metas.append(meta)


@pytest.fixture
def short_grace(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(task_loop_module, 'ISOLATED_LEFTOVER_GRACE_S', SHORT_GRACE_S)
    monkeypatch.setattr(task_loop_module, 'LOOP_CLOSE_TIMEOUT_S', SHORT_CLOSE_TIMEOUT_S)


@pytest.fixture
def task_loop() -> Iterator[TaskLoop]:
    """An open task loop, as the boot of a child with an async task leaves it."""
    loop = TaskLoop()
    loop.open().unwrap()
    yield loop
    loop.close()


def _leave(task_loop: TaskLoop, work: Coroutine[Any, Any, None]) -> asyncio.Task[None]:
    """Run a body that starts ``work`` as a task and returns without it."""
    kept: list[asyncio.Task[None]] = []

    async def body() -> None:
        kept.append(asyncio.get_running_loop().create_task(work))
        await asyncio.sleep(0)

    task_loop.run(body())
    return kept[0]


def _report(
    task_loop: TaskLoop,
    returned: SetRunning | StoppedBeforeRunning,
    marker_end: MarkerEnd = MarkerDone(),
    events: list[str] | None = None,
    metas: list[dict[str, Any]] | None = None,
) -> tuple[child_main._AfterRun, list[str], list[dict[str, Any]]]:
    """Report one run. The marker records itself in ``events`` and ends
    with ``marker_end``; pass ``events`` and ``metas`` to read them when the
    report ends the child."""
    events = [] if events is None else events
    metas = [] if metas is None else metas
    frame = run_frame(task_id=TASK_ID)

    def mark_finalizing(marked: object) -> MarkerEnd:
        assert marked is frame
        events.append(MARKER)
        return marker_end

    after = child_main._report_run(
        _RecordingSocket(events, metas),  # pyright: ignore[reportArgumentType]
        frame,
        returned,
        task_loop,
        mark_finalizing,
    )
    return after, events, metas


@pytest.fixture
def exits(monkeypatch: pytest.MonkeyPatch) -> None:
    """``os._exit`` raises ``_Exited``; ``logging.shutdown`` does nothing, so
    the handlers of the test stay open."""
    monkeypatch.setattr(child_main.os, '_exit', _raise_exit)
    monkeypatch.setattr(child_main.logging, 'shutdown', lambda: None)


class TestTheMarkerOfARun:
    """The finalizing marker is the last write of a run that set its row
    RUNNING; the frames around it tell the parent how the run ended."""

    def test_a_run_with_no_task_loop_writes_its_marker_before_its_result(self) -> None:
        after, events, metas = _report(TaskLoop(), SetRunning(ResultEnvelope(ENVELOPE)))

        assert events == [MARKER, 'result']
        assert metas[0]['task_loop'] == {'kind': 'none'}
        assert after is child_main._AfterRun.SERVES

    def test_an_async_run_that_leaves_no_work_writes_its_marker_then_reports_at_once(
        self,
        task_loop: TaskLoop,
    ) -> None:
        async def awaits_its_work() -> None:
            await asyncio.sleep(0)

        task_loop.run(awaits_its_work())
        started = time.monotonic()
        after, events, metas = _report(task_loop, SetRunning(ResultEnvelope(ENVELOPE)))
        elapsed = time.monotonic() - started

        assert events == [MARKER, 'result']
        assert metas[0]['task_loop'] == {'kind': 'empty'}
        assert elapsed < task_loop_module.ISOLATED_LEFTOVER_GRACE_S / 2
        assert after is child_main._AfterRun.SERVES

    def test_the_marker_follows_the_end_of_work_left_within_the_grace(
        self,
        short_grace: None,
        task_loop: TaskLoop,
    ) -> None:
        steps: list[str] = []

        async def flush() -> None:
            await asyncio.sleep(SHORT_GRACE_S / 4)
            steps.append('ended')

        _ = _leave(task_loop, flush())
        after, events, metas = _report(task_loop, SetRunning(ResultEnvelope(ENVELOPE)))

        assert steps == ['ended']
        assert events == ['body_returned', MARKER, 'left_work_ended']
        assert metas[-1]['end'] == 'in_grace'
        assert after is child_main._AfterRun.TO_RETIRE

    def test_the_marker_follows_the_cancel_of_work_left_past_the_grace(
        self,
        short_grace: None,
        task_loop: TaskLoop,
    ) -> None:
        async def wait_forever() -> None:
            await asyncio.Event().wait()

        left = _leave(task_loop, wait_forever())
        after, events, metas = _report(task_loop, SetRunning(ResultEnvelope(ENVELOPE)))

        assert left.cancelled()
        assert events == ['body_returned', MARKER, 'left_work_ended']
        assert metas[-1]['end'] == 'cancelled'
        assert after is child_main._AfterRun.TO_RETIRE

    def test_work_that_does_not_stop_gets_no_marker(
        self,
        short_grace: None,
        task_loop: TaskLoop,
    ) -> None:
        """The parent kills the child; its runner heartbeats cover the row
        until the exit."""

        async def ignore_cancel() -> None:
            while True:
                try:
                    await asyncio.sleep(60)
                except asyncio.CancelledError:
                    continue

        left = _leave(task_loop, ignore_cancel())
        after, events, _metas = _report(task_loop, SetRunning(ResultEnvelope(ENVELOPE)))

        assert events == ['body_returned', 'left_work_stuck']
        assert after is child_main._AfterRun.TO_RETIRE
        # The task never ends. Drop it quietly: it is pending on the loop.
        left._log_destroy_pending = False  # pyright: ignore[reportAttributeAccessIssue]

    def test_a_marker_that_met_the_stopped_core_reports_the_result_then_ends_the_child(
        self,
        exits: None,
    ) -> None:
        """The body finished: the parent gets its result and writes it, and
        the child ends after the frame."""
        events: list[str] = []
        metas: list[dict[str, Any]] = []
        stopped = core.CoreStopped('log forwarder', 'planned panic')

        with pytest.raises(_Exited) as exited:
            _report(
                TaskLoop(),
                SetRunning(ResultEnvelope(ENVELOPE)),
                MarkerMetStoppedCore(stopped),
                events,
                metas,
            )

        assert exited.value.code == child_main.CORE_STOPPED_EXIT_CODE
        assert events == [MARKER, 'result']
        assert metas[0]['end'] == {'kind': 'result'}

    def test_a_marker_after_left_work_that_met_the_stopped_core_ends_the_child_after_the_frame(
        self,
        exits: None,
        short_grace: None,
        task_loop: TaskLoop,
    ) -> None:
        events: list[str] = []
        metas: list[dict[str, Any]] = []
        stopped = core.CoreStopped('log forwarder', 'planned panic')

        async def flush() -> None:
            await asyncio.sleep(SHORT_GRACE_S / 4)

        _ = _leave(task_loop, flush())
        with pytest.raises(_Exited) as exited:
            _report(
                task_loop,
                SetRunning(ResultEnvelope(ENVELOPE)),
                MarkerMetStoppedCore(stopped),
                events,
                metas,
            )

        assert exited.value.code == child_main.CORE_STOPPED_EXIT_CODE
        assert events == ['body_returned', MARKER, 'left_work_ended']
        assert metas[-1]['end'] == 'in_grace'

    def test_a_run_that_stopped_before_running_writes_no_marker(self) -> None:
        after, events, metas = _report(TaskLoop(), StoppedBeforeRunning('CLAIM_LOST'))

        assert events == ['result']
        assert metas[0]['end'] == {
            'kind': 'stopped_before_running',
            'reason': 'CLAIM_LOST',
        }
        assert after is child_main._AfterRun.SERVES


class TestAChildToRetire:
    def test_a_run_frame_after_a_run_that_left_work_ends_the_child(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Cancelled code can leave broken state, so the parent replaces the
        child; a run frame here is a defect of the parent."""
        monkeypatch.setattr(child_main.os, '_exit', _raise_exit)

        with pytest.raises(_Exited) as exited:
            child_main._refuse_run_on_a_child_to_retire(
                child_main._AfterRun.TO_RETIRE,
                run_frame(task_id=TASK_ID),
            )

        assert exited.value.code == child_main.BAD_PARENT_FRAME_EXIT_CODE

    def test_a_child_that_serves_takes_the_next_run(self) -> None:
        child_main._refuse_run_on_a_child_to_retire(
            child_main._AfterRun.SERVES,
            run_frame(task_id=TASK_ID),
        )


class TestWhatEscapesARun:
    """A normal exit would wait for every non-daemon thread of the task, with
    no bound; the child ends at once and the parent recovers the row."""

    def test_an_escaped_exception_ends_the_child_with_the_defect_code(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.setattr(child_main.os, '_exit', _raise_exit)

        def escapes(_frame: object, _init: object, _loop: object) -> NoReturn:
            raise RuntimeError('escaped the runner')

        with pytest.raises(_Exited) as exited:
            child_main._run_or_exit(
                escapes, run_frame(task_id=TASK_ID), init_frame(), TaskLoop()
            )

        assert exited.value.code == child_main.CHILD_DEFECT_EXIT_CODE

    def test_a_system_exit_of_the_task_ends_the_child_with_its_code(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.setattr(child_main.os, '_exit', _raise_exit)

        def exits(_frame: object, _init: object, _loop: object) -> NoReturn:
            raise SystemExit(7)

        with pytest.raises(_Exited) as exited:
            child_main._run_or_exit(
                exits, run_frame(task_id=TASK_ID), init_frame(), TaskLoop()
            )

        assert exited.value.code == 7

    def test_a_system_exit_of_the_left_work_ends_the_child_at_once(
        self,
        monkeypatch: pytest.MonkeyPatch,
        short_grace: None,
        task_loop: TaskLoop,
    ) -> None:
        monkeypatch.setattr(child_main.os, '_exit', _raise_exit)

        async def exits() -> None:
            await asyncio.sleep(SHORT_GRACE_S / 4)
            raise SystemExit(5)

        _ = _leave(task_loop, exits())
        with pytest.raises(_Exited) as exited:
            child_main._end_left_work_or_exit(task_loop, run_frame(task_id=TASK_ID))

        assert exited.value.code == 5


class TestTheStopExit:
    def test_executor_threads_left_end_the_process_with_their_code(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """A normal exit would join executor threads that still run, with no
        bound."""
        monkeypatch.setattr(child_main.os, '_exit', _raise_exit)

        with pytest.raises(_Exited) as exited:
            child_main.exit_after_stop(ExecutorThreadsLeft())

        assert exited.value.code == child_main.STOPPED_WITH_EXECUTOR_THREADS_EXIT_CODE

    def test_an_executor_that_shut_down_ends_the_process_normally(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.setattr(child_main, 'end_without_teardown', _raise_exit)

        with pytest.raises(_Exited) as exited:
            child_main.exit_after_stop(ExecutorShutDown())

        assert exited.value.code == 0


# A stopped child: a non-daemon thread of its task, an ``atexit`` handler and
# an object that only the interpreter teardown frees each write a mark.
STOPPED_CHILD_PROGRAM: Final = """
import atexit
import os
import sys
import threading
import time

from horsies.core.worker.child_main import end_without_teardown

directory = sys.argv[1]
FLAGS = os.O_WRONLY | os.O_CREAT | os.O_TRUNC


def write_mark(name):
    os.close(os.open(os.path.join(directory, name), FLAGS, 0o600))


class MarkOnDelete:
    def __init__(self, path):
        self._path = path
        self._open = os.open
        self._close = os.close
        self._flags = FLAGS

    def __del__(self):
        self._close(self._open(self._path, self._flags, 0o600))


def thread_of_the_task():
    time.sleep(0.3)
    write_mark('thread')


atexit.register(write_mark, 'atexit')
KEPT_UNTIL_EXIT = MarkOnDelete(os.path.join(directory, 'teardown'))
threading.Thread(target=thread_of_the_task).start()
end_without_teardown(0)
"""


def test_a_stopped_child_ends_without_the_interpreter_teardown(tmp_path: Path) -> None:
    """It waits for the non-daemon thread of its task and runs its ``atexit``
    handler, as ``sys.exit`` does, then ends with ``os._exit``: the teardown
    that would free the module-level object does not run."""
    result = subprocess.run(
        [sys.executable, '-c', STOPPED_CHILD_PROGRAM, str(tmp_path)],
        capture_output=True,
        text=True,
        check=False,
        timeout=PROGRAM_TIMEOUT_S,
    )

    assert result.returncode == 0, result.stderr
    assert (tmp_path / 'thread').exists(), result.stderr
    assert (tmp_path / 'atexit').exists(), result.stderr
    assert not (tmp_path / 'teardown').exists(), result.stderr


# A child after ``ignore_stop_signals``: a SIGTERM to the child does not end
# it, and a subprocess of its task still ends on ``terminate``.
STOP_SIGNALS_PROGRAM: Final = """
import os
import signal
import subprocess
import sys

from horsies.core.worker.child_main import ignore_stop_signals

ignore_stop_signals()
os.kill(os.getpid(), signal.SIGTERM)
os.kill(os.getpid(), signal.SIGINT)
sub = subprocess.Popen([sys.executable, '-c', 'import time; time.sleep(60)'])
sub.terminate()
print(sub.wait(timeout=20))
"""


def test_a_stop_signal_to_the_child_does_not_stop_it_and_terminate_reaches_its_subprocess() -> (
    None
):
    result = subprocess.run(
        [sys.executable, '-c', STOP_SIGNALS_PROGRAM],
        capture_output=True,
        text=True,
        check=False,
        timeout=PROGRAM_TIMEOUT_S,
    )

    assert result.returncode == 0, result.stderr
    assert result.stdout.strip() == str(-signal.SIGTERM)


def test_the_child_restores_the_argv_the_path_and_the_process_name_of_the_worker(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(sys, 'argv', ['-m', 'horsies.core.worker.child_main'])
    monkeypatch.setattr(sys, 'path', list(sys.path))
    monkeypatch.setattr(multiprocessing.current_process(), 'name', 'MainProcess')

    child_main.restore_parent_state(init_frame())

    assert sys.argv == ['horsies', 'worker']
    assert sys.path == []
    assert multiprocessing.current_process().name == 'horsies-child'


class TestBounds:
    def test_long_text_is_cut_and_says_how_long_it_was(self) -> None:
        text = 'x' * 70_000

        bounded = child_main.bounded_text(text, 1_000)

        assert bounded.startswith('x' * 1_000)
        assert bounded.endswith(' [cut from 70000 characters]')

    def test_a_lone_surrogate_becomes_valid_text(self) -> None:
        bounded = child_main.bounded_text('bad \udc80 text', 1_000)

        assert bounded.encode('utf-8').decode('utf-8') == bounded
        assert bounded.startswith('bad ')

    def test_a_result_under_the_body_bound_is_kept(self) -> None:
        envelope = ResultEnvelope(ENVELOPE)

        assert child_main.bounded_envelope('app.tasks.t', envelope) is envelope

    def test_a_result_over_the_body_bound_becomes_a_serialization_error(self) -> None:
        text = json.dumps(
            {'__h_task_result__': True, 'ok': 'x' * MAX_BODY_BYTES, 'err': None}
        )

        bounded = child_main.bounded_envelope('app.tasks.t', ResultEnvelope(text))

        stored = json.loads(bounded.text)
        assert stored['ok'] is None
        assert stored['err']['error_code'] == {
            '__builtin_task_code__': 'WORKER_SERIALIZATION_ERROR'
        }
        assert stored['err']['data'] == {
            'task_name': 'app.tasks.t',
            'result_bytes': len(text.encode('utf-8')),
        }
