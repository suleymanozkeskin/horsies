"""Operation dual-runs: the statements one operation sent before its switch, against the caller after it.

The redesign of an operation moves its Python caller from its own statements
to one baseline SQL function. The dual-run of that switch runs one seed state
and one operation twice, each time on a fresh database with the baseline:

- reference side: the statements the caller sent before the switch, kept as
  a copy in ``tests/dualrun/reference/``;
- candidate side: the Python caller after the switch.

The two normalized snapshots must compare equal. Each side also returns a
report (what the caller learned), which the test compares in its own terms.
Where the function's rule deliberately differs from the old statements, the
test names the case and pins the difference.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from typing import Final

import psycopg
from sqlalchemy.exc import SQLAlchemyError

from horsies.core.types.result import Err, Ok, Result
from tests.dualrun.capture import Connection
from tests.dualrun.compare import SnapshotDiff, compare_snapshots
from tests.dualrun.databases import DualRunDatabase, install_baseline_schema
from tests.dualrun.selfcheck import (
    SelfCheckRun,
    SelfCheckStepFailed,
    capture_run,
    read_run_start,
    straddles,
)
from tests.dualrun.values import CalendarUnit

BOUNDARY_RETRY_LIMIT: Final = 1
"""A pair whose run window crosses an hour boundary runs again once: heartbeat
leaves are hourly, so a beat written on either side of the boundary lands in
a leaf of another name."""


@dataclass(frozen=True, slots=True)
class OperationSide[S, R]:
    """One implementation of an operation.

    ``act`` runs the operation on the database after the seed state was
    written, with the seed's facts. It returns its report, or raises what the
    driver raises.
    """

    name: str
    act: Callable[[DualRunDatabase, S], R]


@dataclass(frozen=True, slots=True)
class SideRun[R]:
    """The snapshot of one side and the report of its operation."""

    run: SelfCheckRun
    report: R


@dataclass(frozen=True, slots=True)
class OperationDualRun[RR, CR]:
    """Both sides of one dual-run and the diff of their snapshots."""

    reference: SideRun[RR]
    candidate: SideRun[CR]
    diff: SnapshotDiff


def _failed(step: str, cause: psycopg.Error | SQLAlchemyError) -> Err[SelfCheckStepFailed]:
    return Err(SelfCheckStepFailed(step=step, cause=str(cause)))


def run_side[S, R](
    database: DualRunDatabase,
    seed: Callable[[Connection], S],
    side: OperationSide[S, R],
) -> Result[SideRun[R], SelfCheckStepFailed]:
    """Install the baseline, write the seed in one transaction, run ``side``, capture.

    ``seed`` raises ``psycopg.Error`` on a database error; its transaction then
    rolls back. A failed step leaves the database as the step left it.
    """
    with psycopg.connect(database.url, autocommit=True) as connection:
        match read_run_start(connection):
            case Err(failed):
                return Err(failed)
            case Ok(start):
                pass
        match install_baseline_schema(database):
            case Err(failed):
                return Err(SelfCheckStepFailed(step='install the baseline', cause=str(failed)))
            case Ok():
                pass
        try:
            with connection.transaction():
                facts = seed(connection)
        except psycopg.Error as exc:
            return _failed('write the seed state', exc)
        try:
            report = side.act(database, facts)
        except (psycopg.Error, SQLAlchemyError) as exc:
            return _failed(f'run the {side.name} side', exc)
        match capture_run(connection, start):
            case Err(failed):
                return Err(failed)
            case Ok(run):
                return Ok(SideRun(run=run, report=report))


def _either_straddles(*runs: SelfCheckRun) -> bool:
    return any(straddles(run.window, CalendarUnit.HOUR) for run in runs)


def dual_run[S, RR, CR](
    make_database: Callable[[], DualRunDatabase],
    seed: Callable[[Connection], S],
    reference: OperationSide[S, RR],
    candidate: OperationSide[S, CR],
) -> Result[OperationDualRun[RR, CR], SelfCheckStepFailed]:
    """Run ``reference`` and ``candidate`` on two fresh databases with the same seed, and compare.

    A pair whose windows cross an hour boundary runs again, at most
    ``BOUNDARY_RETRY_LIMIT`` times; the last pair is returned either way.
    """
    attempts = BOUNDARY_RETRY_LIMIT + 1
    for attempt in range(1, attempts + 1):
        match run_side(make_database(), seed, reference):
            case Err(failed):
                return Err(failed)
            case Ok(reference_run):
                pass
        match run_side(make_database(), seed, candidate):
            case Err(failed):
                return Err(failed)
            case Ok(candidate_run):
                pass
        if attempt < attempts and _either_straddles(reference_run.run, candidate_run.run):
            continue
        return Ok(
            OperationDualRun(
                reference=reference_run,
                candidate=candidate_run,
                diff=compare_snapshots(
                    reference_run.run.snapshot, candidate_run.run.snapshot
                ),
            )
        )
    raise AssertionError('the loop returns on its last attempt')
