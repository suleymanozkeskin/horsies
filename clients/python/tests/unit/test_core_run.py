"""`horsies worker` on the core (`horsies.core.worker.core_run`): the startup
errors before the run, the manifest of the worker process, and the host that
the core calls. No database: each test ends before `PreparedWorker.run`, and
`set_panic_rule` is replaced so the test process keeps its panic rule.
"""

from __future__ import annotations

import dataclasses
import json
import logging
import sys
from collections.abc import Generator, Sequence
from contextlib import contextmanager
from pathlib import Path
from typing import Any, cast
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
    worker_manifest_json,
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


def test_a_locator_in_main_is_a_startup_error_before_any_core_call() -> None:
    """The Python worker of 0.7.0 refused `__main__:app` at its start: a
    child is a new program with its own `__main__`."""
    reported = _Reported()
    launch = dataclasses.replace(_launch('small'), app_locator='__main__:app')
    with (
        _no_panic_rule() as set_panic_rule,
        patch.object(_core, 'prepare_worker') as prepare_worker,
        patch.object(Horsies, 'get_broker') as get_broker,
    ):
        exit_code = run_core_worker(_app(), launch, LOGGER, reported)
    assert exit_code == EXIT_FAILED
    assert len(reported.errors) == 1
    error = reported.errors[0]
    assert isinstance(error, ConfigurationError)
    assert error.code is ErrorCode.WORKER_INVALID_LOCATOR
    assert "'__main__:app'" in error.message
    set_panic_rule.assert_not_called()
    get_broker.assert_not_called()
    prepare_worker.assert_not_called()


def test_the_core_takes_the_manifest_of_entries_that_python_skips(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Regression of #1950: a `Path` on `sys.path` stopped the dump, a
    number reached the manifest where the core reads text, and a lone
    surrogate on `sys.path` or `sys.argv` reached it as an escape that the
    core's JSON parser refuses."""
    kept = list(sys.path)
    monkeypatch.setattr(sys, 'path', [Path('/srv/app'), 7, 'bad\udcff', '/srv/lib', *kept])
    monkeypatch.setattr(sys, 'argv', ['horsies', 'worker', 'bad\udcff'])
    with caplog.at_level(logging.WARNING, logger='tests.core_run'):
        manifest_json = worker_manifest_json(_app(), _launch('small'), LOGGER).unwrap()
    monkeypatch.undo()

    prepared = _core.prepare_worker(manifest_json, host=CoreWorkerHost(MagicMock(), LOGGER))
    assert isinstance(prepared, _core.PreparedWorker)
    program = cast('dict[str, Any]', json.loads(manifest_json))['child_launch']['program']
    assert program['parent_sys_path'][:2] == ['/srv/app', '/srv/lib']
    assert program['parent_argv'] == ['horsies', 'worker', 'bad\\udcff']
    assert [
        record.getMessage() for record in caplog.records if record.name == 'tests.core_run'
    ] == [
        'Leaving sys.path entry 7 out of worker children: '
        'it is not a str, bytes or os.PathLike path',
        "Leaving sys.path entry 'bad\\udcff' out of worker children: "
        'it is not valid UTF-8',
    ]


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
