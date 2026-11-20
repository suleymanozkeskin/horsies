"""`prepare_worker` and `PreparedWorker` of `horsies._core`: the worker of a
worker manifest, before any run. No database: `prepare_worker` is pure, and
a stop request never connects.
"""

from __future__ import annotations

import json
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
from horsies.core.worker.config import ChildPreload
from horsies.core.worker_manifest import (
    CurrentDirectory,
    TaskFacts,
    child_argv,
    child_sys_path,
    python_child_launch,
    to_worker_manifest,
)


class _Host:
    """A host whose start hook counts its calls. No workflow step runs in
    these tests, so no child build is asked for."""

    def __init__(self) -> None:
        self.started = 0

    def on_started(self) -> None:
        self.started += 1

    def build_child_spec(
        self, request: _core.ChildSpecRequest,
    ) -> _core.ChildSpecBuilt | _core.ChildSpecRefused:
        raise AssertionError(f'no workflow step runs here: {request!r}')


class _HostWithoutChildSpecSource:
    """A host with a start hook and no `build_child_spec`."""

    def on_started(self) -> None:
        return None


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
        parent_sys_path=child_sys_path((), CurrentDirectory('/srv')),
        parent_argv=child_argv(('horsies', 'worker')),
        log_level='INFO',
    ).unwrap()
    manifest = to_worker_manifest(
        config,
        profile=profile,
        metrics_endpoint=wire.MetricsEndpointNoMetricsEndpoint(),
        tasks={'app.tasks.add': TaskFacts(execution=Execution.ISOLATED, error_codes=())},
        child_launch=launch,
        idempotency_reservation_window=timedelta(minutes=1),
    )
    return wire.dump_worker_manifest(manifest)


def test_a_known_profile_gives_a_prepared_worker() -> None:
    prepared = _core.prepare_worker(_manifest_json('small'), host=_Host())
    assert isinstance(prepared, _core.PreparedWorker)


def test_a_default_python_profile_reaches_the_core_as_library() -> None:
    """Python's default `preload` is `LIBRARY`, and the manifest carries it.
    The core's own default (`NoPreload`, for a Rust profile) never applies to
    a Python worker: the core takes the manifest with the key, and refuses the
    same manifest without it."""
    default_profile = WorkerProfile(isolated=IsolatedChildren(count=1))
    assert default_profile.preload is ChildPreload.LIBRARY
    manifest_json = _manifest_json('small')
    document = json.loads(manifest_json)
    assert document['profiles']['small']['preload'] == 'LIBRARY'
    prepared = _core.prepare_worker(manifest_json, host=_Host())
    assert isinstance(prepared, _core.PreparedWorker)

    del document['profiles']['small']['preload']
    with pytest.raises(_core.ManifestRefused) as refused:
        _core.prepare_worker(json.dumps(document), host=_Host())
    assert str(refused.value.args[0]).startswith('profiles.small')


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


def test_a_host_with_no_child_spec_source_raises_attribute_error() -> None:
    with pytest.raises(AttributeError, match='build_child_spec'):
        _core.prepare_worker(
            _manifest_json('small'), host=_HostWithoutChildSpecSource(),
        )


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
