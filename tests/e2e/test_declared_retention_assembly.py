"""A real worker registers the retention class its config declares.

The integration wiring test builds `WorkerConfig` itself, so it proves
the loop hands retention to the pass — and nothing about how a deployed
worker gets that config in the first place. If the CLI stopped supplying
`retention_config`, the loop's `or RetentionConfig()` binding would make
every real worker run on defaults: declared classes silently inert, and
every existing test still green.

So this boots a worker the way a deployment does — `horsies worker
<instance>` through the CLI — and asserts the declared class reaches the
registry. That single assertion spans the whole chain: CLI →
`AppConfig.retention` → `WorkerConfig` → reaper loop → gate → breaker →
maintenance pass → registrar.

It also covers the loop-to-pass seam, which no in-process test can: the
integration tests call `_run_reaper_pass` directly and therefore skip the
interval, the cluster-wide gate and the breaker that stand between the
loop and the pass.
"""

from __future__ import annotations

import os

import pytest
from sqlalchemy import create_engine, text

from tests.e2e.helpers.retention import forget_retention_class
from tests.e2e.helpers.worker import run_worker
from tests.e2e.tasks.instance_declared_retention import (
    E2E_CLASS_DURATION,
    E2E_CLASS_KEY,
)

pytestmark = [pytest.mark.e2e]

INSTANCE = 'tests.e2e.tasks.instance_declared_retention:app'

from tests.e2e.helpers.env import e2e_database_url

DB_URL = e2e_database_url('HORSES_E2E_DB_URL')


def _registered_duration() -> object | None:
    """The class's duration as the database holds it, or None."""
    engine = create_engine(DB_URL)
    try:
        with engine.connect() as connection:
            return (
                connection.execute(
                    text(
                        'SELECT duration FROM horsies_retention_classes '
                        'WHERE class_key = :key'
                    ),
                    {'key': E2E_CLASS_KEY},
                )
            ).scalar_one_or_none()
    finally:
        engine.dispose()


def _forget_class() -> None:
    """Drop the class, its leaves, and the readers naming them."""
    forget_retention_class(DB_URL, E2E_CLASS_KEY)


def test_a_booted_worker_registers_its_declared_class() -> None:
    """The declaration survives every link from config to registrar."""
    _forget_class()
    try:
        assert _registered_duration() is None, (
            'setup failed: the class was already registered'
        )

        # The readiness condition IS the assertion's subject: poll the
        # registry rather than a log line, so the test cannot pass on a
        # worker that merely started.
        def _class_is_registered() -> bool:
            return _registered_duration() is not None

        with run_worker(
            INSTANCE,
            processes=1,
            timeout=60.0,
            ready_check=_class_is_registered,
        ):
            registered = _registered_duration()

        assert registered == E2E_CLASS_DURATION, (
            f'a booted worker did not register {E2E_CLASS_KEY!r} with its '
            'declared duration — the declaration did not survive the path '
            'from AppConfig.retention to the registrar'
        )
    finally:
        _forget_class()
