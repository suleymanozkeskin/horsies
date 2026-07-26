# pyright: reportPrivateUsage=false
# These tests deliberately exercise module-private rules and seams.
"""Unit tests for schema compatibility probing and the no-DDL construction flag.

The monitoring tool must never migrate a database it is pointed at, and must
never write through a schema it does not recognize. These cover the probe's
three verdicts, its caching, what it does when the database cannot answer, and
that the broker flag genuinely suppresses the DDL seam.
"""

from __future__ import annotations

from typing import Any

import pytest
from pydantic import SecretStr
from sqlalchemy.exc import OperationalError

from horsies.core.app import Horsies
from horsies.core.brokers.postgres import PostgresBroker
from horsies.core.models.app import AppConfig
from horsies.core.models.broker import PostgresConfig
from horsies.core.schemas.migrations import SCHEMA_VERSION
from horsies.web.schema import (
    SCHEMA_INCOMPATIBLE,
    SCHEMA_UNKNOWN,
    SchemaIncompatible,
    SchemaProbe,
    SchemaState,
    SchemaStatus,
    schema_guard,
)

pytestmark = [pytest.mark.unit]

asyncio_tests = pytest.mark.asyncio(loop_scope='function')

DB_URL = 'postgresql+psycopg://user:pw@127.0.0.1:1/none'


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
    return SchemaStatus(state=state, version=version, expected_version=SCHEMA_VERSION)


class TestSchemaStatus:
    """Only an exact version match permits writing."""

    def test_only_match_is_compatible(self) -> None:
        assert status(SchemaState.MATCH, SCHEMA_VERSION).compatible is True
        assert status(SchemaState.MISMATCH, SCHEMA_VERSION - 1).compatible is False
        assert status(SchemaState.ABSENT, None).compatible is False
        assert status(SchemaState.UNKNOWN, None).compatible is False


class TestProbeCaching:
    """The version is read once per window, not once per request."""

    pytestmark = [asyncio_tests]

    async def test_repeated_calls_reuse_one_read(self) -> None:
        probe = StubProbe([status(SchemaState.MATCH, SCHEMA_VERSION)])

        for _ in range(5):
            assert (await probe.status()).state is SchemaState.MATCH

        assert probe.reads == 1

    async def test_an_expired_cache_reads_again(self) -> None:
        probe = StubProbe(
            [
                status(SchemaState.MISMATCH, 13),
                status(SchemaState.MATCH, SCHEMA_VERSION),
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
        probe = StubProbe([status(SchemaState.MATCH, SCHEMA_VERSION), None])
        assert (await probe.status()).state is SchemaState.MATCH
        probe.expire()

        assert (await probe.status()).state is SchemaState.MATCH

    async def test_a_failed_read_is_not_cached(self) -> None:
        probe = StubProbe([status(SchemaState.MATCH, SCHEMA_VERSION), None])
        await probe.status()
        probe.expire()
        await probe.status()
        probe.expire()
        await probe.status()

        assert probe.reads == 3


class TestSchemaGuard:
    """The dependency every action route inherits."""

    pytestmark = [asyncio_tests]

    async def test_match_passes(self) -> None:
        guard = schema_guard(StubProbe([status(SchemaState.MATCH, SCHEMA_VERSION)]))

        assert await guard() is None

    async def test_mismatch_raises_with_both_versions_named(self) -> None:
        guard = schema_guard(StubProbe([status(SchemaState.MISMATCH, 11)]))

        with pytest.raises(SchemaIncompatible) as raised:
            await guard()

        assert 'v11' in raised.value.detail
        assert f'v{SCHEMA_VERSION}' in raised.value.detail

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
                raise AssertionError('the migration seam opened a transaction')

        await broker._run_schema_migrations(ExplodingEngine())  # type: ignore[arg-type]

    @asyncio_tests
    async def test_enabled_broker_still_reaches_the_database(self) -> None:
        """Control: with the flag on, the same call does try to connect."""
        broker = PostgresBroker(
            PostgresConfig(database_url=SecretStr(DB_URL)), assume_initialized=True
        )

        with pytest.raises(OperationalError):
            await broker._run_schema_migrations(broker.async_engine)
