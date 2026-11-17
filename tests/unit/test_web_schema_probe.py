# pyright: reportPrivateUsage=false
# These tests deliberately exercise module-private rules and seams.
"""Unit tests for schema compatibility probing and the no-DDL construction flag.

The monitoring tool must never migrate a database it is pointed at, and must
never write through a schema it does not recognize. These cover the probe's
verdicts, its caching, what it does when the database cannot answer, and
that the broker flag genuinely suppresses the DDL seam.
"""

from __future__ import annotations

from typing import Any

from unittest.mock import AsyncMock, patch

import psycopg
import pytest
from pydantic import SecretStr

from horsies.core.app import Horsies
from horsies.core.brokers.postgres import PostgresBroker
from horsies.core.models.app import AppConfig
from horsies.core.models.broker import PostgresConfig
from horsies.core.baseline.migration import BASELINE_VERSION
from horsies.web.schema import (
    EXPECTED_SCHEMA_VERSION,
    SCHEMA_INCOMPATIBLE,
    SCHEMA_UNKNOWN,
    SchemaIncompatible,
    SchemaProbe,
    SchemaState,
    SchemaStatus,
    classify_stored_version,
    classify_without_migrations,
)
from tests.broker_resources_support import resources_of
from tests.dualrun.reference.web_guards import schema_guard

pytestmark = [pytest.mark.unit]

asyncio_tests = pytest.mark.asyncio(loop_scope='function')

DB_URL = 'postgresql://user:pw@127.0.0.1:1/none'


class StubProbe(SchemaProbe):
    """A probe whose database answer is scripted."""

    def __init__(self, answers: list[SchemaStatus | None]) -> None:
        self.answers = answers
        self.reads = 0
        self._cached: SchemaStatus | None = None
        self._expires_at = 0.0
        self._ttl_seconds = 60.0

    async def _read(self) -> SchemaStatus | None:
        self.reads += 1
        return self.answers[min(self.reads - 1, len(self.answers) - 1)]

    def expire(self) -> None:
        """Age the cache out so the next call re-reads."""
        self._expires_at = 0.0


def status(state: SchemaState, version: int | None) -> SchemaStatus:
    """A probe result for the given state."""
    return SchemaStatus(state=state, version=version, expected_version=EXPECTED_SCHEMA_VERSION)


class TestSchemaStatus:
    """Only an exact version match permits writing."""

    def test_only_match_is_compatible(self) -> None:
        assert status(SchemaState.MATCH, EXPECTED_SCHEMA_VERSION).compatible is True
        assert status(SchemaState.MISMATCH, EXPECTED_SCHEMA_VERSION - 1).compatible is False
        assert status(SchemaState.ABSENT, None).compatible is False
        assert status(SchemaState.ZERO_X, None).compatible is False
        assert status(SchemaState.UNKNOWN, None).compatible is False


class TestClassification:
    """What each set of catalog facts means."""

    def test_no_bookkeeping_at_all_is_absent(self) -> None:
        assert classify_without_migrations(schema_version_table=False) == status(
            SchemaState.ABSENT, None,
        )

    def test_the_0x_python_bookkeeping_alone_is_a_0x_schema(self) -> None:
        """horsies_schema_version without horsies_migrations is horsies 0.x."""
        assert classify_without_migrations(schema_version_table=True) == status(
            SchemaState.ZERO_X, None,
        )

    def test_no_successful_migration_is_absent(self) -> None:
        assert classify_stored_version(0) == status(SchemaState.ABSENT, None)

    @pytest.mark.parametrize('stored', [1, 65, BASELINE_VERSION - 1])
    def test_a_version_below_the_baseline_is_a_0x_schema(self, stored: int) -> None:
        """horsies-rust 0.x records versions 1 to 65 in horsies_migrations."""
        assert classify_stored_version(stored) == status(SchemaState.ZERO_X, stored)

    def test_the_expected_version_matches(self) -> None:
        assert classify_stored_version(EXPECTED_SCHEMA_VERSION) == status(
            SchemaState.MATCH, EXPECTED_SCHEMA_VERSION,
        )

    def test_a_newer_version_is_a_mismatch(self) -> None:
        assert classify_stored_version(EXPECTED_SCHEMA_VERSION + 1) == status(
            SchemaState.MISMATCH, EXPECTED_SCHEMA_VERSION + 1,
        )


class TestProbeCaching:
    """The version is read once per window, not once per request."""

    pytestmark = [asyncio_tests]

    async def test_repeated_calls_reuse_one_read(self) -> None:
        probe = StubProbe([status(SchemaState.MATCH, EXPECTED_SCHEMA_VERSION)])

        for _ in range(5):
            assert (await probe.status()).state is SchemaState.MATCH

        assert probe.reads == 1

    async def test_an_expired_cache_reads_again(self) -> None:
        probe = StubProbe(
            [
                status(SchemaState.MISMATCH, 13),
                status(SchemaState.MATCH, EXPECTED_SCHEMA_VERSION),
            ]
        )
        assert (await probe.status()).state is SchemaState.MISMATCH
        probe.expire()

        assert (await probe.status()).state is SchemaState.MATCH
        assert probe.reads == 2


class TestProbeFailureHandling:
    """A database that cannot answer never becomes a verdict of its own."""

    pytestmark = [asyncio_tests]

    async def test_a_cold_start_failure_is_unknown_not_absent(self) -> None:
        """ "No schema" and "cannot reach it" are different instructions.

        Reporting ABSENT here would tell an operator to initialize a database
        that may simply be down.
        """
        probe = StubProbe([None])

        observed = await probe.status()

        assert observed.state is SchemaState.UNKNOWN
        assert observed.version is None
        assert observed.compatible is False

    async def test_absent_only_comes_from_a_successful_observation(self) -> None:
        probe = StubProbe([status(SchemaState.ABSENT, None)])

        assert (await probe.status()).state is SchemaState.ABSENT

    async def test_a_blip_keeps_the_last_known_answer(self) -> None:
        """A transient outage must not report the schema as missing."""
        probe = StubProbe([status(SchemaState.MATCH, EXPECTED_SCHEMA_VERSION), None])
        assert (await probe.status()).state is SchemaState.MATCH
        probe.expire()

        assert (await probe.status()).state is SchemaState.MATCH

    async def test_a_failed_read_is_not_cached(self) -> None:
        probe = StubProbe([status(SchemaState.MATCH, EXPECTED_SCHEMA_VERSION), None])
        await probe.status()
        probe.expire()
        await probe.status()
        probe.expire()
        await probe.status()

        assert probe.reads == 3


class TestSchemaGuard:
    """The dependency every action route of the Python app inherited before
    the switch (``tests/dualrun/reference/web_guards.py``)."""

    pytestmark = [asyncio_tests]

    async def test_match_passes(self) -> None:
        guard = schema_guard(StubProbe([status(SchemaState.MATCH, EXPECTED_SCHEMA_VERSION)]))

        assert await guard() is None

    async def test_mismatch_raises_with_both_versions_named(self) -> None:
        guard = schema_guard(StubProbe([status(SchemaState.MISMATCH, 11)]))

        with pytest.raises(SchemaIncompatible) as raised:
            await guard()

        assert 'v11' in raised.value.detail
        assert f'v{EXPECTED_SCHEMA_VERSION}' in raised.value.detail

    async def test_unknown_reports_reachability_not_a_missing_schema(self) -> None:
        """The copy must never send an operator to initialize a live database."""
        guard = schema_guard(StubProbe([status(SchemaState.UNKNOWN, None)]))

        with pytest.raises(SchemaIncompatible) as raised:
            await guard()

        assert raised.value.code == SCHEMA_UNKNOWN
        assert 'Cannot reach the database' in raised.value.detail
        assert 'no horsies schema' not in raised.value.detail

    async def test_absent_says_the_tool_will_not_create_it(self) -> None:
        guard = schema_guard(StubProbe([status(SchemaState.ABSENT, None)]))

        with pytest.raises(SchemaIncompatible) as raised:
            await guard()

        assert raised.value.code == SCHEMA_INCOMPATIBLE
        assert 'no horsies schema' in raised.value.detail
        assert 'never modifies the database schema' in raised.value.detail

    async def test_a_0x_schema_names_the_new_database_remedy(self) -> None:
        """A worker start cannot fix a 0.x database: the copy must not say so."""
        guard = schema_guard(StubProbe([status(SchemaState.ZERO_X, None)]))

        with pytest.raises(SchemaIncompatible) as raised:
            await guard()

        assert raised.value.code == SCHEMA_INCOMPATIBLE
        assert 'horsies 0.x schema' in raised.value.detail
        assert 'new database' in raised.value.detail
        assert 'Start a horsies app or worker' not in raised.value.detail


class TestNoDdlConstructionFlag:
    """The flag that makes a broker structurally incapable of migrating."""

    def test_default_preserves_existing_behaviour(self) -> None:
        broker = PostgresBroker(
            PostgresConfig(database_url=SecretStr(DB_URL)), assume_initialized=True
        )

        assert broker.run_schema_migrations is True

    def test_flag_propagates_from_app_to_broker(self) -> None:
        app = Horsies(
            AppConfig(broker=PostgresConfig(database_url=SecretStr(DB_URL))),
            run_schema_migrations=False,
        )

        assert app.get_broker().run_schema_migrations is False

    def test_app_default_is_unchanged(self) -> None:
        app = Horsies(AppConfig(broker=PostgresConfig(database_url=SecretStr(DB_URL))))

        assert app.get_broker().run_schema_migrations is True

    @asyncio_tests
    async def test_disabled_broker_runs_no_ddl_at_the_seam(self) -> None:
        """The seam returns before opening a transaction, so nothing executes."""
        broker = PostgresBroker(
            PostgresConfig(database_url=SecretStr(DB_URL)),
            assume_initialized=True,
            run_schema_migrations=False,
        )

        class ExplodingEngine:
            def begin(self) -> Any:
                raise AssertionError('the install seam opened a transaction')

        with patch(
            'horsies.core.brokers.postgres.psycopg.AsyncConnection.connect',
            new=AsyncMock(side_effect=AssertionError('the install seam connected')),
        ):
            await broker._maybe_run_schema_init(ExplodingEngine())  # type: ignore[arg-type]

    @asyncio_tests
    async def test_enabled_broker_still_reaches_the_database(self) -> None:
        """Control: with the flag on, the same call does try to connect."""
        broker = PostgresBroker(
            PostgresConfig(database_url=SecretStr(DB_URL)), assume_initialized=True
        )

        with pytest.raises(psycopg.OperationalError):
            await broker._maybe_run_schema_init((await resources_of(broker)).engine)
