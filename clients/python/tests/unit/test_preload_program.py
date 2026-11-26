"""The preload program (`horsies.core.worker.preload_main`) on a real app
module: what it refuses before it forks, and the children that it forks.

The program runs as a real process. `TestAppGuard` is its worker; the
other tests run `tests/fixtures/preload_worker.py`, which holds the
subreaper flag, as the core's worker does, and boots each forked child
through the real child program. Linux only: the program counts the threads
of its process under `/proc`, and a forked child waits until the worker is
its parent.
"""

from __future__ import annotations

import json
import os
import socket
import subprocess
import sys
import textwrap
from pathlib import Path
from typing import Any, Final, cast

import pytest

from horsies.core.worker_manifest import PRELOAD_MAIN_MODULE
from tests.fixtures.core_frames import framed, init_meta
from tests.fixtures.preload_worker import read_frame

pytestmark = [
    pytest.mark.unit,
    pytest.mark.skipif(sys.platform != 'linux', reason='preload is Linux only'),
]

PROGRAM_TIMEOUT_S: Final = 30.0
WORKER_TIMEOUT_S: Final = 120.0
REPOSITORY_ROOT: Final = Path(__file__).resolve().parents[2]
CHILDREN: Final = 2

APP_HEAD: Final = """
from horsies.core.app import Horsies
from horsies.core.models.app import AppConfig
from horsies.core.models.broker import PostgresConfig
from horsies.core.models.queues import QueueMode

app = Horsies(
    AppConfig(
        queue_mode=QueueMode.DEFAULT,
        broker=PostgresConfig(database_url='postgresql://u:p@localhost/none'),
    ),
)
"""


class TestAppGuard:
    """`preload_main.run` in the `app` mode: an app whose import leaves a
    state that a fork would copy wrong is refused with the cause, and so is
    an app whose import raises (Python 0.7.0 `TestAppGuard`)."""

    @staticmethod
    def _answer(tmp_path: Path, body: str) -> tuple[dict[str, object], int]:
        """The first frame of the preload program on an app with `body`,
        and the exit code of the program after its socket closed."""
        app = tmp_path / 'guarded_app.py'
        app.write_text(APP_HEAD + textwrap.dedent(body), encoding='utf-8')
        worker_end, preload_end = socket.socketpair()
        with worker_end:
            process = subprocess.Popen(
                [
                    sys.executable,
                    '-m',
                    PRELOAD_MAIN_MODULE,
                    str(preload_end.fileno()),
                    str(os.getpid()),
                    'app',
                ],
                pass_fds=(preload_end.fileno(),),
                stdin=subprocess.DEVNULL,
                start_new_session=True,
            )
            preload_end.close()
            worker_end.settimeout(PROGRAM_TIMEOUT_S)
            worker_end.sendall(
                framed(
                    init_meta(app_locator=f'{app}:app', parent_sys_path=tuple(sys.path)),
                ),
            )
            first = read_frame(worker_end)
        return first, process.wait(timeout=PROGRAM_TIMEOUT_S)

    def test_a_clean_app_is_accepted(self, tmp_path: Path) -> None:
        first, exit_code = self._answer(tmp_path, '')
        assert first['kind'] == 'preload_ready', first
        assert exit_code == 0

    def test_a_thread_is_refused(self, tmp_path: Path) -> None:
        body = """
            import threading, time
            threading.Thread(target=time.sleep, args=(60,), daemon=True).start()
        """
        first, exit_code = self._answer(tmp_path, body)
        assert first['kind'] == 'preload_refused', first
        assert 'threads' in cast('str', first['reason'])
        assert exit_code == 1

    def test_a_socket_is_refused(self, tmp_path: Path) -> None:
        body = """
            import socket
            kept = socket.socket()
        """
        first, exit_code = self._answer(tmp_path, body)
        assert first['kind'] == 'preload_refused', first
        assert 'sockets' in cast('str', first['reason'])
        assert exit_code == 1

    def test_a_failed_import_names_its_cause(self, tmp_path: Path) -> None:
        first, exit_code = self._answer(tmp_path, 'raise RuntimeError("bad app")')
        assert first['kind'] == 'preload_refused', first
        assert 'bad app' in cast('str', first['reason'])
        assert exit_code == 1


def _run_worker(directory: Path, mode: str) -> dict[str, Any]:
    """Boot `CHILDREN` children from the preload program in `mode`, on an
    app that counts its imports in `imports` and runs a start hook that
    writes the freeze count of the child to `hooks`. Returns the report of
    the worker, with the counts."""
    imports = directory / 'imports'
    hooks = directory / 'hooks'
    app = directory / 'counted_app.py'
    app.write_text(
        APP_HEAD
        + textwrap.dedent(f"""
            import gc

            with open({str(imports)!r}, 'a') as out:
                out.write('x')

            @app.on_child_start
            def count_child() -> None:
                with open({str(hooks)!r}, 'a') as out:
                    out.write(f'{{gc.get_freeze_count()}}\\n')
        """),
        encoding='utf-8',
    )
    report = directory / 'report.json'
    done = subprocess.run(
        [
            sys.executable,
            '-m',
            'tests.fixtures.preload_worker',
            mode,
            f'{app}:app',
            str(CHILDREN),
            str(report),
        ],
        cwd=REPOSITORY_ROOT,
        capture_output=True,
        text=True,
        timeout=WORKER_TIMEOUT_S,
        check=False,
    )
    assert done.returncode == 0, done.stderr
    result = cast('dict[str, Any]', json.loads(report.read_text(encoding='utf-8')))
    result['imports'] = len(imports.read_text(encoding='utf-8'))
    result['freeze_counts'] = [
        int(line) for line in hooks.read_text(encoding='utf-8').splitlines()
    ]
    return result


@pytest.fixture(scope='module')
def app_run(tmp_path_factory: pytest.TempPathFactory) -> dict[str, Any]:
    return _run_worker(tmp_path_factory.mktemp('app_preload'), 'app')


@pytest.fixture(scope='module')
def library_run(tmp_path_factory: pytest.TempPathFactory) -> dict[str, Any]:
    return _run_worker(tmp_path_factory.mktemp('library_preload'), 'library')


def _children_served(run: dict[str, Any]) -> list[dict[str, Any]]:
    """The children of a run, after the check that each booted and ended
    at its stop frame."""
    assert run['preload']['kind'] == 'preload_ready', run['preload']
    children = cast('list[dict[str, Any]]', run['children'])
    assert len(children) == CHILDREN
    for child in children:
        assert child['first_frame']['kind'] == 'ready', child
        assert child['exit_code'] == 0, child
    return children


class TestRealChildren:
    """The real child program, booted through the real preload program
    (Python 0.7.0 `TestRealChildren`)."""

    def test_app_preload_imports_once_and_runs_hooks_per_child(
        self,
        app_run: dict[str, Any],
    ) -> None:
        _ = _children_served(app_run)
        assert (app_run['imports'], len(app_run['freeze_counts'])) == (1, CHILDREN)

    def test_library_preload_imports_the_app_per_child(
        self,
        library_run: dict[str, Any],
    ) -> None:
        _ = _children_served(library_run)
        assert (library_run['imports'], len(library_run['freeze_counts'])) == (
            CHILDREN,
            CHILDREN,
        )


class TestForkedChild:
    """What a forked child holds (Python 0.7.0 `TestForkedChild`)."""

    @pytest.mark.parametrize('run_name', ['library_run', 'app_run'])
    def test_the_child_shares_the_frozen_heap(
        self,
        run_name: str,
        request: pytest.FixtureRequest,
    ) -> None:
        """The preload program freezes its heap before it forks, so each
        child starts with a permanent generation that it shares."""
        run = cast('dict[str, Any]', request.getfixturevalue(run_name))
        _ = _children_served(run)
        freeze_counts = cast('list[int]', run['freeze_counts'])
        assert len(freeze_counts) == CHILDREN
        assert all(count > 0 for count in freeze_counts), freeze_counts

    @pytest.mark.parametrize('run_name', ['library_run', 'app_run'])
    def test_the_child_holds_only_its_own_socket(
        self,
        run_name: str,
        request: pytest.FixtureRequest,
    ) -> None:
        """No socket of another child or of the preload program. The app has
        no async task, so the child has no task loop and no socket pair of
        one."""
        run = cast('dict[str, Any]', request.getfixturevalue(run_name))
        for child in _children_served(run):
            assert child['sockets'] == [child['given_socket']], child
