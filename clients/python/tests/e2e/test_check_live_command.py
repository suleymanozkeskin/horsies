"""E2E: ``horsies check <app> --live`` as a process.

Each test runs the ``horsies`` command on
``tests.e2e.tasks.instance_check_live`` with a database URL of its own and
checks the exit status and the output. The Rust client has the same tests in
``clients/rust/tests/worker/tests/check_live_command.rs``.
"""

from __future__ import annotations

import os
import subprocess
from typing import Final

import pytest
from sqlalchemy.engine import make_url

from tests.e2e.conftest import DB_URL
from tests.e2e.helpers.env import CHECK_LIVE_URL_ENV_NAME
from tests.e2e.helpers.worker import REPO_ROOT

pytestmark = [pytest.mark.e2e]

CHECK_INSTANCE: Final = 'tests.e2e.tasks.instance_check_live:app'

CHECK_TIMEOUT_S: Final = 60.0
"""The longest run of one ``horsies check --live`` process: the start of
``uv run``, the import of the app, one connect and the reads of the live
phases. One connect attempt waits at most 10 s."""

UNREACHABLE_DATABASE_URL: Final = 'postgresql://postgres:unused@127.0.0.1:1/horsies'
"""A URL whose port has no server (port 1). It holds a password, so the
connect reads no password source of the environment."""

COLOR_VARIABLES: Final = ('HORSIES_FORCE_COLOR', 'FORCE_COLOR')
"""The variables that turn on ANSI color in the report and the log lines.
The process gets none of them, and ``NO_COLOR``, so the output is plain
text."""


def _run_check_live(*, database_url: str) -> subprocess.CompletedProcess[str]:
    """Run ``horsies check <instance> --live`` with ``database_url`` as the
    database URL of the app and wait for its end.

    Raises:
        subprocess.TimeoutExpired: the process did not end within
            ``CHECK_TIMEOUT_S``; ``subprocess.run`` killed it.
    """
    environment = os.environ.copy()
    for name in COLOR_VARIABLES:
        _ = environment.pop(name, None)
    environment['NO_COLOR'] = '1'
    environment['PYTHONPATH'] = REPO_ROOT
    environment[CHECK_LIVE_URL_ENV_NAME] = database_url
    return subprocess.run(
        ['uv', 'run', 'horsies', 'check', CHECK_INSTANCE, '--live'],
        env=environment,
        capture_output=True,
        text=True,
        timeout=CHECK_TIMEOUT_S,
        check=False,
    )


def _report(run: subprocess.CompletedProcess[str]) -> str:
    """The exit status and both streams of ``run``, for a failure message."""
    return f'exit {run.returncode}\nstdout:\n{run.stdout}\nstderr:\n{run.stderr}'


def test_check_live_on_the_test_database_passes() -> None:
    """The live phases connect, read the connection limit and compare each
    worker profile of the instance with it."""
    run = _run_check_live(database_url=DB_URL)

    assert run.returncode == 0, _report(run)
    assert 'ok: all validations passed' in run.stdout, _report(run)
    assert 'error[' not in run.stdout + run.stderr, _report(run)


def test_check_live_refuses_a_url_that_names_a_driver_before_any_connect() -> None:
    """``PostgresConfig`` refuses the URL when the app module is imported, so
    the check does not start: the command reports the config refusal and
    exits 1."""
    driver_named_url = (
        make_url(DB_URL)
        .set(drivername='postgresql+psycopg')
        .render_as_string(hide_password=False)
    )

    run = _run_check_live(database_url=driver_named_url)

    output = run.stdout + run.stderr
    assert run.returncode == 1, _report(run)
    assert (
        "error[HRS-203]: invalid database_url scheme 'postgresql+psycopg'" in output
    ), _report(run)
    assert 'broker connectivity check failed' not in output, _report(run)
    assert 'ok: all validations passed' not in output, _report(run)


def test_check_live_reports_a_port_with_no_server_as_a_connectivity_failure() -> None:
    """The static check passes; the connect of the live phase is refused."""
    run = _run_check_live(database_url=UNREACHABLE_DATABASE_URL)

    output = run.stdout + run.stderr
    assert run.returncode == 1, _report(run)
    assert 'error[HRS-203]: broker connectivity check failed' in output, _report(run)
    assert 'ok: all validations passed' not in output, _report(run)
