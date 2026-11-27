# pyright: reportPrivateUsage=false
# These tests deliberately exercise module-private rules and seams.
"""Unit tests for schema compatibility probing and the no-DDL construction flag.

The monitoring tool must never migrate a database it is pointed at, and must
never write through a schema it does not recognize. These cover the probe's
verdicts, its caching, what it does when the database cannot answer, and
that the broker flag genuinely suppresses the DDL seam.
"""

from __future__ import annotations

import logging

from unittest.mock import AsyncMock, MagicMock

import pytest
from pydantic import SecretStr

from horsies._core import SchemaStatusRecord, TooManyPendingCalls
from horsies.core.brokers.postgres import PostgresBroker
from horsies.core.models.broker import PostgresConfig
from horsies.core.models.schema_mode import SchemaMode
from horsies.web.schema import (
    EXPECTED_SCHEMA_VERSION,
    SchemaProbe,
    SchemaState,
    SchemaStatus,
)

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


class TestProbeRead:
    """The read is one core call (``CoreHandle.schema_status_async``); its
    record decides the observation."""

    pytestmark = [asyncio_tests]

    @staticmethod
    def _probe(answer: object) -> tuple[SchemaProbe, PostgresBroker, MagicMock]:
        """A probe, its broker, and the broker's core handle, which answers
        ``answer``: a record, or an exception that the call raises."""
        broker = PostgresBroker(
            PostgresConfig(database_url=SecretStr(DB_URL)),
            schema_mode=SchemaMode.ASSUME_INITIALIZED,
        )
        handle = MagicMock()
        match answer:
            case BaseException():
                handle.schema_status_async = AsyncMock(side_effect=answer)
            case _:
                handle.schema_status_async = AsyncMock(return_value=answer)
        broker._core_handle = handle
        return SchemaProbe(broker), broker, handle

    @staticmethod
    def _record(state: str, version: int | None, expected_version: int) -> MagicMock:
        return MagicMock(
            spec=SchemaStatusRecord,
            state=state,
            version=version,
            expected_version=expected_version,
        )

    @pytest.mark.parametrize(
        ('state', 'version'),
        [
            (SchemaState.MATCH, EXPECTED_SCHEMA_VERSION),
            (SchemaState.MISMATCH, EXPECTED_SCHEMA_VERSION + 1),
            (SchemaState.ABSENT, None),
            (SchemaState.ZERO_X, 65),
            (SchemaState.ZERO_X, None),
        ],
    )
    async def test_an_observed_state_is_the_observation_of_the_record(
        self, state: SchemaState, version: int | None,
    ) -> None:
        """The state, the version and the expected version come from the core."""
        expected_version = EXPECTED_SCHEMA_VERSION + 7
        probe, _, handle = self._probe(
            self._record(state.value, version, expected_version),
        )

        observed = await probe._read()

        assert observed == SchemaStatus(
            state=state, version=version, expected_version=expected_version,
        )
        handle.schema_status_async.assert_awaited_once_with()

    async def test_unknown_is_a_failed_read_not_a_verdict(self) -> None:
        """The core's UNKNOWN is a read that failed: None, and status() keeps
        its rule for a failed read."""
        probe, _, _ = self._probe(
            self._record(SchemaState.UNKNOWN.value, None, EXPECTED_SCHEMA_VERSION),
        )

        assert await probe._read() is None
        assert (await probe.status()).state is SchemaState.UNKNOWN

    async def test_a_core_that_cannot_run_the_read_is_a_failed_read(
        self, caplog: pytest.LogCaptureFixture,
    ) -> None:
        """A core boundary error is a failed read, logged with its type."""
        refused = TooManyPendingCalls('CoreHandle.schema_status_async', 65_536)
        probe, _, _ = self._probe(refused)

        with caplog.at_level(logging.WARNING):
            observed = await probe._read()

        assert observed is None
        assert any(
            'Schema version probe failed: TooManyPendingCalls' in record.getMessage()
            for record in caplog.records
        )

    async def test_a_defect_of_the_call_propagates(self) -> None:
        """An exception that is not a CoreError is not a failed read."""
        defect = TypeError('stub defect')
        probe, _, _ = self._probe(defect)

        with pytest.raises(TypeError) as raised:
            await probe._read()

        assert raised.value is defect


