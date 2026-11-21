"""Tests for the preload parent. Tests that fork need Linux."""

# pyright: reportPrivateUsage=false

from __future__ import annotations

import ast
import subprocess
import sys
from collections.abc import Callable
from pathlib import Path
from typing import NoReturn
from unittest.mock import MagicMock

import pytest

from horsies.core.worker import preload_main as kept_preload_main
from horsies.core.worker.config import ChildPreload
from horsies.core.worker.preload_main import REPARENT_FAILED_EXIT_CODE, parse_arguments

pytestmark = [pytest.mark.unit]


# The skip reads the platform and never preload_support(): a Linux host that
# cannot run the preload parent must fail these tests, not skip them.
linux_only = pytest.mark.skipif(
    sys.platform != 'linux',
    reason='preload is Linux only',
)


class TestArguments:
    def test_valid(self) -> None:
        assert parse_arguments(['5', '42', 'app']) == (5, 42, ChildPreload.APP)

    @pytest.mark.parametrize(
        'argv',
        [[], ['5', '42'], ['x', '42', 'app'], ['5', 'x', 'app'], ['5', '42', 'other']],
    )
    def test_invalid(self, argv: list[str]) -> None:
        assert parse_arguments(argv) is None


class TestChildSteps:
    def test_a_child_serves_only_after_it_reparents(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """``serve`` binds to the real parent, so the order is the contract."""
        steps: list[str] = []

        def record(step: str) -> Callable[..., None]:
            def note(*_args: object) -> None:
                steps.append(step)

            return note

        def serve_then_exit(*_args: object) -> NoReturn:
            """``serve`` ends the process after a stop frame."""
            steps.append('serve')
            raise SystemExit(0)

        monkeypatch.setattr(kept_preload_main.os, 'setsid', record('setsid'))
        monkeypatch.setattr(kept_preload_main, '_wait_for_reparent', record('reparent'))
        monkeypatch.setattr(kept_preload_main, 'serve', serve_then_exit)
        monkeypatch.setattr(kept_preload_main.socket, 'socket', MagicMock())

        with pytest.raises(SystemExit) as stopped:
            kept_preload_main._run_child(5, 42, MagicMock(), MagicMock())

        assert stopped.value.code == 0
        assert steps == ['setsid', 'reparent', 'serve']


def _cli_exit_literals() -> set[int]:
    """The codes that the CLI passes to ``sys.exit`` as literals."""
    from horsies.core import cli

    tree = ast.parse(Path(cli.__file__).read_text(encoding='utf-8'))
    return {
        node.args[0].value
        for node in ast.walk(tree)
        if isinstance(node, ast.Call)
        and ast.unparse(node.func) == 'sys.exit'
        and len(node.args) == 1
        and isinstance(node.args[0], ast.Constant)
        and isinstance(node.args[0].value, int)
    }


class TestChildExitCodes:
    def test_every_named_exit_code_is_distinct(self) -> None:
        """A forked child can exit through ``preload_main`` before the
        reparent and through ``child_main`` after it, each with a code of the
        generated protocol module. A reader tells the cause from the code, so
        no two causes share one, and none is a code the CLI exits with. The
        codes of the worker process itself (runner heartbeats lost, liveness
        lock lost) are core values; core tests keep them apart from these and
        from the CLI codes."""
        from horsies.core.worker.child_frames import PROTOCOL_NUMBERS

        codes = {
            name: getattr(PROTOCOL_NUMBERS, name)
            for name in dir(type(PROTOCOL_NUMBERS))
            if name.endswith('_exit_code')
        }
        cli_codes = _cli_exit_literals()

        assert codes['reparent_failed_exit_code'] == REPARENT_FAILED_EXIT_CODE
        assert 'stopped_with_executor_threads_exit_code' in codes
        assert 'child_hook_failed_exit_code' in codes
        assert all(isinstance(value, int) for value in codes.values()), codes
        assert {0, 1, 2} <= cli_codes
        assert len(set(codes.values())) == len(codes), codes
        assert not set(codes.values()) & cli_codes, (codes, cli_codes)


@linux_only
class TestReparentWait:
    def test_a_child_that_never_reparents_exits(self) -> None:
        code = (
            'from horsies.core.worker import preload_main as main\n'
            'main.REPARENT_TIMEOUT_S = 0.05\n'
            'main._wait_for_reparent(1 << 30)\n'
        )
        result = subprocess.run([sys.executable, '-c', code], check=False)
        assert result.returncode == REPARENT_FAILED_EXIT_CODE


@linux_only
class TestProcessState:
    """``fork_refusal`` reads this process, so each case runs in its own."""

    @staticmethod
    def _refusal(setup: str) -> str:
        code = f"""
import socket, threading, time
from horsies.core.worker.preload_main import fork_refusal
{setup}
print(fork_refusal(-1))
"""
        result = subprocess.run(
            [sys.executable, '-c', code],
            capture_output=True,
            text=True,
            check=True,
        )
        return result.stdout.strip()

    def test_a_clean_process_is_safe(self) -> None:
        assert self._refusal('') == 'None'

    def test_a_python_thread_is_refused(self) -> None:
        setup = 'threading.Thread(target=time.sleep, args=(5,), daemon=True).start()'
        assert 'threads' in self._refusal(setup)

    def test_a_socket_is_refused(self) -> None:
        assert 'sockets' in self._refusal('kept = socket.socket()')

    def test_an_os_thread_that_python_did_not_start_is_refused(self) -> None:
        # A thread started through _thread is not in threading.enumerate().
        setup = (
            'import _thread, os\n'
            '_thread.start_new_thread(time.sleep, (5,))\n'
            'while len(os.listdir("/proc/self/task")) < 2:\n'
            '    time.sleep(0.01)'
        )
        assert 'OS threads run in this process' in self._refusal(setup)

    def test_importing_the_core_starts_no_thread_and_opens_no_socket(self) -> None:
        # Executor risk 2: the preload imports horsies, and so horsies._core.
        assert self._refusal('import horsies, horsies._core') == 'None'

    def test_a_core_that_started_in_this_process_is_refused(self) -> None:
        setup = (
            'import horsies.core.worker.preload_main as preload_main\n'
            "preload_main.core_state = lambda: 'running'"
        )
        assert self._refusal(setup) == 'the core of horsies._core is running in this process'
