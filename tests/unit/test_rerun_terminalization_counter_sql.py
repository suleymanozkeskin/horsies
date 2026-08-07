"""The restated counter statements must not drift from the ones they reuse.

The paired terminalization collector counts statements, write transactions and
WAL the same way the performance harness does, but asynchronously, so it
restates three statements rather than importing a synchronous probe. Restated
SQL that nothing pins is SQL that quietly stops matching: a filter added to the
original's exclusion list would leave the collector counting its own bookkeeping
as measured work, and every WAL number it reported would be wrong in a way no
verdict would show.

These tests import the originals solely to compare them.
"""

from __future__ import annotations

from tests.perf import counters
from tests.task_history_prototypes.rerun_terminalization_evidence import (
    COUNTER_READ_SQL,
    COUNTER_RESET_SQL,
    COUNTER_WRITE_TRANSACTIONS_SQL,
)


class TestCounterStatementsMatchTheHarnessTheyReuse:
    def test_reset_statement_matches(self) -> None:
        # Reaching into the harness's private statements is the point of a
        # drift test: the copy is only honest if it is compared with the
        # exact text it copied, not with a public restatement of it.
        assert COUNTER_RESET_SQL == counters._RESET_SQL.text  # pyright: ignore[reportPrivateUsage]

    def test_read_statement_matches(self) -> None:
        assert COUNTER_READ_SQL == counters._READ_SQL.text  # pyright: ignore[reportPrivateUsage]

    def test_write_transaction_statement_matches(self) -> None:
        assert (
            COUNTER_WRITE_TRANSACTIONS_SQL
            == counters._WRITE_TRANSACTIONS_SQL.text  # pyright: ignore[reportPrivateUsage]
        )

    def test_read_statement_still_excludes_harness_bookkeeping(self) -> None:
        # The exclusions are the reason the counts mean anything: without them
        # the probe's own reads land in the numbers it reports.
        assert "query NOT LIKE '%pg_stat%'" in COUNTER_READ_SQL
        assert "query NOT LIKE '%pg_snapshot%'" in COUNTER_READ_SQL

    def test_wal_is_attributed_to_top_level_statements_only(self) -> None:
        # Summing both layers would report a PL/pgSQL wrapper as writing its
        # WAL twice, which is exactly the candidate's shape here.
        assert (
            'COALESCE(SUM(wal_bytes) FILTER (WHERE toplevel), 0)'
            in COUNTER_READ_SQL
        )
        assert 'FILTER (WHERE NOT toplevel)' not in COUNTER_READ_SQL.split(
            'wal_bytes'
        )[1]
