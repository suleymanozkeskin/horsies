"""`prepare_worker` and `PreparedWorker` of `horsies._core`: the worker of a
worker manifest, before any run. No database: `prepare_worker` is pure, and
a stop request never connects.
"""

from __future__ import annotations

import threading
from datetime import timedelta

import pytest
from pydantic import SecretStr

from horsies import _core
from horsies._catalog import worker_manifest as wire
from horsies.core.models.app import AppConfig
from horsies.core.models.broker import PostgresConfig
from horsies.core.models.tasks import Execution
from horsies.core.models.worker_profile import IsolatedChildren, WorkerProfile
from horsies.core.types.result import Err, Ok
from horsies.core.worker_manifest import python_child_launch, to_worker_manifest


class _Host:
    """A host whose start hook counts its calls."""

    def __init__(self) -> None:
        self.started = 0

    def on_started(self) -> None:
        self.started += 1


def _manifest_json(profile: str | None) -> str:
    config = AppConfig(
        broker=PostgresConfig(
            database_url=SecretStr('postgresql://user:secret@localhost:5432/db'),
        ),
        worker_profiles={
            'small': WorkerProfile(isolated=IsolatedChildren(count=1)),
        },
    )
    launch = python_child_launch(
        executable='/usr/bin/python3',
        interpreter_flags=(),
        app_locator='app.main:app',
        imports=('app.tasks',),
        sys_path_roots=(),
        parent_sys_path=(),
        parent_argv=('horsies', 'worker'),
        log_level='INFO',
    )
    match to_worker_manifest(
        config,
        profile=profile,
        tasks={'app.tasks.add': Execution.ISOLATED},
        child_launch=launch,
        idempotency_reservation_window=timedelta(minutes=1),
    ):
        case Ok(manifest):
            return wire.dump_worker_manifest(manifest)
        case Err(above):
            raise AssertionError(f'the test config is inside the bounds: {above}')


def test_a_known_profile_gives_a_prepared_worker() -> None:
    prepared = _core.prepare_worker(_manifest_json('small'), host=_Host())
    assert isinstance(prepared, _core.PreparedWorker)


def test_an_unknown_profile_is_refused_with_the_report_lines() -> None:
    refused = _core.prepare_worker(_manifest_json('missing'), host=_Host())
    assert isinstance(refused, _core.WorkerProfileRefused)
    assert any('missing' in line for line in refused.refusals)


def test_no_profile_is_refused() -> None:
    refused = _core.prepare_worker(_manifest_json(None), host=_Host())
    assert isinstance(refused, _core.WorkerProfileRefused)
    assert refused.refusals


def test_a_manifest_the_core_refuses_raises_manifest_refused() -> None:
    with pytest.raises(_core.ManifestRefused):
        _core.prepare_worker('{"manifest_version": 1}', host=_Host())


def test_a_stop_request_returns_at_once_and_repeats() -> None:
    prepared = _core.prepare_worker(_manifest_json('small'), host=_Host())
    assert isinstance(prepared, _core.PreparedWorker)
    assert isinstance(prepared.request_stop(), _core.StopRequested)
    assert isinstance(prepared.request_stop(), _core.StopRequested)


def test_a_run_off_the_main_thread_is_refused_and_does_not_start() -> None:
    prepared = _core.prepare_worker(_manifest_json('small'), host=_Host())
    assert isinstance(prepared, _core.PreparedWorker)
    raised: list[BaseException] = []

    def _run() -> None:
        try:
            prepared.run()
        except BaseException as exc:  # noqa: BLE001 - the test reads the raised type
            raised.append(exc)

    thread = threading.Thread(target=_run)
    thread.start()
    thread.join(timeout=30)
    assert len(raised) == 1
    assert isinstance(raised[0], _core.NotMainThread)
