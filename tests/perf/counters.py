"""Server-side counts, which are exact where wall-clock is only estimated.

Statement counts, commit counts and WAL volume are not sampled — the server
records every one — so they need no confidence interval and admit no
inconclusive verdict. A change in any of them is a fact about the
implementation rather than a fact about the machine it ran on, which makes
them the load-bearing half of a comparison and the wall-clock the noisy half.

Client and nested statements are counted apart. A statement issued inside a
PL/pgSQL function is real work, but comparing it to a client round trip would
report an implementation that collapsed five round trips into one function as
having grown its statement count.
"""

from __future__ import annotations

from dataclasses import dataclass

from sqlalchemy import text
from sqlalchemy.engine import Connection

_RESET_SQL = text('SELECT pg_stat_statements_reset()')

# Bookkeeping the harness itself issues is excluded by name. Without this the
# reset call and these very reads would be counted as work under measurement.
_READ_SQL = text("""
    SELECT
        COALESCE(SUM(calls) FILTER (WHERE toplevel), 0)      AS client_statements,
        COALESCE(SUM(calls) FILTER (WHERE NOT toplevel), 0)  AS nested_statements,
        COALESCE(SUM(rows), 0)                               AS rows_affected,
        COALESCE(SUM(wal_records), 0)                        AS wal_records,
        COALESCE(SUM(wal_bytes), 0)                          AS wal_bytes,
        COALESCE(SUM(wal_fpi), 0)                            AS wal_fpi
    FROM pg_stat_statements
    WHERE query NOT LIKE '%pg_stat%'
      AND query NOT LIKE '%pg_snapshot%'
""")

# Committed write transactions, counted by transaction-id consumption rather
# than by the database's commit counter. Two reasons, and the first is fatal to
# the alternative: cumulative statistics are flushed on a timer, so a counter
# read straight after a block reports zero and the work turns up attributed to
# whatever ran a second later. Transaction ids are assigned as work happens and
# are exact immediately, on every supported version. The second reason is that
# this counts exactly the transactions that wrote — a read-only transaction
# consumes no id — and a terminal transition that grew an extra write is the
# regression the count exists to catch.
#
# The count is server-wide, not per-session: ids are a global sequence, so
# autovacuum waking during a block is counted too. That makes it exact for
# "how many write transactions happened here" and approximate for "how many
# were mine", which is why equality between two sides of a control run is
# asserted on the attributable counters and this one is allowed background
# drift.
_WRITE_TRANSACTIONS_SQL = text(
    'SELECT pg_snapshot_xmax(pg_current_snapshot())::text::bigint'
)


@dataclass(frozen=True, slots=True)
class Counts:
    """What the server recorded for one measured block."""

    client_statements: int
    nested_statements: int
    rows_affected: int
    wal_records: int
    wal_bytes: int
    wal_fpi: int
    write_transactions: int

    def per_row(self, total: int) -> float:
        return total / self.rows_affected if self.rows_affected else 0.0

    @property
    def wal_records_per_row(self) -> float:
        return self.per_row(self.wal_records)

    @property
    def wal_bytes_per_row(self) -> float:
        return self.per_row(self.wal_bytes)


class CounterProbe:
    """Brackets a block of operations and reports what the server counted."""

    def __init__(self, connection: Connection) -> None:
        self._connection = connection
        self._transactions_at_start: int | None = None

    def begin(self) -> None:
        self._connection.execute(_RESET_SQL)
        self._transactions_at_start = self._read_write_transactions()

    def finish(self) -> Counts:
        if self._transactions_at_start is None:
            raise RuntimeError('probe finished before it began')
        write_transactions = (
            self._read_write_transactions() - self._transactions_at_start
        )
        row = self._connection.execute(_READ_SQL).one()
        self._transactions_at_start = None
        return Counts(
            client_statements=int(row.client_statements),
            nested_statements=int(row.nested_statements),
            rows_affected=int(row.rows_affected),
            wal_records=int(row.wal_records),
            wal_bytes=int(row.wal_bytes),
            wal_fpi=int(row.wal_fpi),
            write_transactions=write_transactions,
        )

    def _read_write_transactions(self) -> int:
        return int(
            self._connection.execute(_WRITE_TRANSACTIONS_SQL).scalar_one()
        )


def install_extension(connection: Connection) -> None:
    """Make the statement view available in this database.

    Preloading the library is a server setting; creating the extension is
    per-database, and both are required before anything can be counted.
    """
    connection.execute(text('CREATE EXTENSION IF NOT EXISTS pg_stat_statements'))
    connection.commit()
