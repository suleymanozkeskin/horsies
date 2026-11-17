"""`horsies worker` on the core (`horsies.core.worker.core_run`): the startup
errors before the run and the host that the core calls. No database: each
test ends before `PreparedWorker.run`, and `set_panic_rule` is replaced so
the test process keeps its panic rule.
"""

from __future__ import annotations

import logging
from collections.abc import Generator, Sequence
from contextlib import contextmanager
from unittest.mock import MagicMock, patch

import pytest
from pydantic import SecretStr

from horsies import _core
from horsies.core.app import Horsies
from horsies.core.errors import ConfigurationError, ErrorCode, HorsiesError
from horsies.core.models.app import AppConfig
from horsies.core.models.broker import PostgresConfig
from horsies.core.models.worker_profile import IsolatedChildren, WorkerProfile
from horsies.core.worker.core_run import (
    EXIT_FAILED,
    CoreWorkerHost,
    WorkerLaunch,
    run_core_worker,
)

LOGGER = logging.getLogger('tests.core_run')


def _app() -> Horsies:
    return Horsies(
        AppConfig(
            broker=PostgresConfig(
                database_url=SecretStr('postgresql://user:secret@localhost:5432/db'),
            ),
            worker_profiles={
                'small': WorkerProfile(isolated=IsolatedChildren(count=1)),
            },
        ),
    )


def _launch(profile: str) -> WorkerLaunch:
    return WorkerLaunch(
        app_locator='app.main:app',
        imports=(),
        sys_path_roots=(),
        profile=profile,
        loglevel='INFO',
    )


class _Reported:
    """The startup errors that `run_core_worker` reported."""

    def __init__(self) -> None:
        self.errors: list[HorsiesError] = []

    def __call__(self, errors: Sequence[HorsiesError]) -> None:
        self.errors.extend(errors)


@contextmanager
def _no_panic_rule() -> Generator[MagicMock, None, None]:
    with patch.object(_core, 'set_panic_rule') as set_panic_rule:
        yield set_panic_rule


def test_a_broker_that_does_not_build_is_a_startup_error() -> None:
    reported = _Reported()
    app = _app()
    refused = HorsiesError(message='broker refused', code=ErrorCode.BROKER_INIT_FAILED)
    with (
        _no_panic_rule() as set_panic_rule,
        patch.object(Horsies, 'get_broker', side_effect=refused),
    ):
        exit_code = run_core_worker(app, _launch('small'), LOGGER, reported)
    assert exit_code == EXIT_FAILED
    assert reported.errors == [refused]
    set_panic_rule.assert_called_once_with('ABORT_PROCESS')


def test_a_profile_that_the_core_refuses_is_reported_with_its_lines() -> None:
    reported = _Reported()
    app = _app()
    with (
        _no_panic_rule(),
        patch.object(Horsies, 'get_broker', return_value=MagicMock()),
    ):
        exit_code = run_core_worker(app, _launch('missing'), LOGGER, reported)
    assert exit_code == EXIT_FAILED
    assert reported.errors
    for error in reported.errors:
        assert isinstance(error, ConfigurationError)
        assert error.code is ErrorCode.CONFIG_INVALID_WORKER_PROFILE
    assert any('missing' in error.message for error in reported.errors)


def test_the_host_builds_each_child_with_the_app_of_its_broker() -> None:
    broker = MagicMock()
    request = MagicMock()
    built = MagicMock()
    with patch(
        'horsies.core.worker.core_run.build_child_spec', return_value=built,
    ) as build:
        answer = CoreWorkerHost(broker, LOGGER).build_child_spec(request)
    assert answer is built
    build.assert_called_once_with(broker, request)


def test_the_start_hook_freezes_the_objects_of_the_start(
    caplog: pytest.LogCaptureFixture,
) -> None:
    with (
        patch('horsies.core.worker.core_run.freeze_startup_objects') as freeze,
        caplog.at_level(logging.INFO, logger='tests.core_run'),
    ):
        CoreWorkerHost(MagicMock(), LOGGER).on_started()
    freeze.assert_called_once_with()
    assert 'Worker started' in caplog.messages
