"""The live check of the Postgres connections of a worker profile against the
server limit (``horsies check --live``), which the core runs."""

from __future__ import annotations

import json
from contextlib import contextmanager
from dataclasses import dataclass
from collections.abc import Generator
from typing import Any
from unittest import mock

import pytest
from pydantic import SecretStr

from horsies._core import CoreStartFailed
from horsies.core.app import Horsies
from horsies.core.errors import ErrorCode
from horsies.core.models.app import AppConfig
from horsies.core.models.broker import PostgresConfig
from horsies.core.models.worker_profile import (
    IsolatedChildren,
    WorkerProfile,
)

pytestmark = [pytest.mark.unit]

_URL = 'postgresql://user:pass@localhost/db'


def _broker(**fields: Any) -> PostgresConfig:
    return PostgresConfig(database_url=SecretStr(_URL), **fields)


def _app(
    profiles: dict[str, WorkerProfile],
    **broker_fields: Any,
) -> Horsies:
    return Horsies(
        AppConfig(broker=_broker(**broker_fields), worker_profiles=profiles),
    )


@dataclass(frozen=True)
class _Passed:
    """Stands for ``horsies._core.ConnectivityPassed``."""


@dataclass(frozen=True)
class _Refused:
    """Stands for ``horsies._core.ConnectivityRefused``."""

    step: str
    message: str


@dataclass(frozen=True)
class _CoreRefusal:
    """Stands for ``horsies._core.WorkerConnectionError``."""

    message: str
    code: str | None
    notes: list[str]
    help_text: str | None


class _LiveHandle:
    """A core handle of the live check: passes or refuses the connectivity
    step, answers the worker connection step with ``refusals`` (or raises
    it), keeps the profile text it got, and counts its closes."""

    built: list[_LiveHandle] = []
    connectivity: object = _Passed()
    refusals: object = []

    def __init__(self, manifest_json: str, *, pool_profile: str) -> None:
        self.manifest = json.loads(manifest_json)
        self.pool_profile = pool_profile
        self.profiles: list[dict[str, Any]] = []
        self.closes = 0
        _LiveHandle.built.append(self)

    def check_connectivity(self) -> object:
        return _LiveHandle.connectivity

    def check_worker_connections(self, profiles_json: str) -> object:
        self.profiles.append(json.loads(profiles_json))
        match _LiveHandle.refusals:
            case BaseException() as raised:
                raise raised
            case refusals:
                return refusals

    def close(self) -> None:
        self.closes += 1


@contextmanager
def _live_core(
    *,
    refusals: object,
    connectivity: object | None = None,
) -> Generator[list[_LiveHandle], None, None]:
    """Run the live check against `_LiveHandle`. Yields the handles that the
    check built."""
    _LiveHandle.built = []
    _LiveHandle.connectivity = _Passed() if connectivity is None else connectivity
    _LiveHandle.refusals = refusals
    with (
        mock.patch('horsies.core.app.CoreHandle', _LiveHandle),
        mock.patch('horsies.core.app.ConnectivityPassed', _Passed),
        mock.patch('horsies.core.app.ConnectivityRefused', _Refused),
    ):
        yield _LiveHandle.built


class TestCheckLiveWorkerConnections:
    """``app.check(live=True)``: the core's check of each profile against the
    server limit (``CoreHandle.check_worker_connections``), on the handle of
    the connectivity step."""

    def test_each_core_refusal_is_a_python_error_with_its_parts(self) -> None:
        app = _app({'big': WorkerProfile(isolated=IsolatedChildren(count=20))})
        refusal = _CoreRefusal(
            message="worker_profiles['big']: one worker opens up to 27 Postgres connections",
            code='HRS-219',
            notes=['parent pool max_size 5 + listener 1 + runner heartbeat 1', 'not counted'],
            help_text='lower the child counts',
        )

        with _live_core(refusals=[refusal]):
            errors = app.check(live=True)

        assert [error.code for error in errors] == [ErrorCode.CHECK_WORKER_CONNECTIONS]
        assert errors[0].message == refusal.message
        assert errors[0].notes == refusal.notes
        assert errors[0].help_text == 'lower the child counts'
        assert errors[0].location is None

    def test_the_profiles_reach_the_core_in_the_worker_manifest_form(self) -> None:
        app = _app(
            {
                'big': WorkerProfile(isolated=IsolatedChildren(count=20)),
                'small': WorkerProfile(isolated=IsolatedChildren(count=1)),
            }
        )

        with _live_core(refusals=[]) as built:
            assert app.check(live=True) == []

        [handle] = built
        [profiles] = handle.profiles
        assert sorted(profiles) == ['big', 'small']
        assert profiles['big']['isolated']['kind'] == 'CHILDREN'
        assert profiles['big']['isolated']['count'] == 20
        assert handle.manifest['schema'] == 'OBSERVE_ONLY'
        assert handle.closes == 1

    def test_no_profiles_sends_an_empty_set_for_the_core_to_skip(self) -> None:
        with _live_core(refusals=[]) as built:
            assert _app({}).check(live=True) == []
        assert built[0].profiles == [{}]

    def test_a_transaction_pooler_is_in_the_manifest_that_the_core_reads(self) -> None:
        app = _app(
            {'small': WorkerProfile(isolated=IsolatedChildren(count=1))},
            pgbouncer_transaction_mode=True,
            session_database_url=SecretStr(
                'postgresql://user:pass@localhost:5433/db',
            ),
        )
        with _live_core(refusals=[]) as built:
            assert app.check(live=True) == []
        assert built[0].manifest['database']['pgbouncer_transaction_mode'] is True

    def test_a_core_that_cannot_run_the_call_is_one_unread_limit_error(self) -> None:
        app = _app({'small': WorkerProfile(isolated=IsolatedChildren(count=1))})

        with _live_core(
            refusals=CoreStartFailed('the core runtime could not start'),
        ) as built:
            errors = app.check(live=True)

        assert [(error.code, error.notes) for error in errors] == [
            (ErrorCode.CHECK_WORKER_CONNECTIONS, ['the core runtime could not start']),
        ]
        assert errors[0].message == 'the server connection limit could not be read'
        assert built[0].closes == 1

    def test_the_phase_is_skipped_when_connectivity_fails(self) -> None:
        app = _app({'big': WorkerProfile(isolated=IsolatedChildren(count=20))})

        with _live_core(
            refusals=AssertionError('the worker phase must not run'),
            connectivity=_Refused(step='MAIN_URL', message='db down'),
        ) as built:
            errors = app.check(live=True)

        assert [error.code for error in errors] == [ErrorCode.BROKER_INVALID_URL]
        assert built[0].profiles == []
        assert built[0].closes == 1

    def test_check_without_live_builds_no_handle(self) -> None:
        app = _app({'big': WorkerProfile(isolated=IsolatedChildren(count=20))})
        with _live_core(refusals=AssertionError('no live phase')) as built:
            assert app.check(live=False) == []
        assert built == []
