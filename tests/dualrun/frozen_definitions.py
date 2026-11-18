"""Dual-runs of a switch inside the baseline SQL: the definitions before it against the baseline.

Some switches change the bodies of baseline functions and not the calls that
a client sends. Such a switch is checked with the function-call script
(``crates/horsies-core/src/sql_tests/cases/baseline_function_calls.sql``),
which calls every function of the baseline. The script runs twice, each time
on a fresh database with the baseline:

- reference side: the definitions before the switch, from SQL files in
  ``tests/dualrun/reference/``, installed over the baseline in order;
- candidate side: the baseline as it is.

The two normalized snapshots must compare equal.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from pathlib import Path
from typing import Final, LiteralString

import psycopg

from horsies.core.types.result import Err, Ok, Result
from tests.dualrun.capture import Connection
from tests.dualrun.databases import DualRunDatabase, install_baseline_schema
from tests.dualrun.selfcheck import (
    FUNCTION_CALLS_SQL,
    SelfCheckRun,
    SelfCheckStepFailed,
    capture_run,
    function_calls_script,
    read_run_start,
)

REFERENCE_FOLDER: Final = Path(__file__).parent / 'reference'

_SCRIPT_ID: Final = re.compile(r"'(00000000-0000-7000-8000-[0-9a-f]{12})'")
"""The form of a fixed id in the function-call script. Each run replaces every
fixed id with a new UUIDv7, so a run never meets the ids of an earlier run."""

_NEW_IDS_SQL: Final[LiteralString] = (
    'SELECT horsies_uuidv7() FROM generate_series(1, %(count)s)'
)


@dataclass(frozen=True, slots=True)
class BaselineAsInstalled:
    """The candidate side: the baseline install, with no change."""


@dataclass(frozen=True, slots=True)
class FrozenDefinitions:
    """The reference side: the SQL files of ``paths`` run in order after the baseline install, in one transaction."""

    paths: tuple[Path, ...]


type Definitions = BaselineAsInstalled | FrozenDefinitions


def _failed(step: str, cause: object) -> Err[SelfCheckStepFailed]:
    return Err(SelfCheckStepFailed(step=step, cause=str(cause)))


def _install_definitions(
    connection: Connection, definitions: Definitions
) -> Result[None, SelfCheckStepFailed]:
    match definitions:
        case BaselineAsInstalled():
            return Ok(None)
        case FrozenDefinitions(paths=paths):
            texts: list[tuple[str, str]] = []
            for path in paths:
                try:
                    texts.append((path.name, path.read_text(encoding='utf-8')))
                except OSError as exc:
                    return _failed(f'read the definitions {path.name}', exc)
            installing = ''
            try:
                with connection.transaction():
                    for installing, text in texts:
                        _ = connection.execute(text.encode('utf-8'))
            except psycopg.Error as exc:
                return _failed(f'install the definitions {installing}', exc)
            return Ok(None)


def _script_with_new_ids(connection: Connection) -> str:
    """The function-call script with each fixed id replaced by a new UUIDv7.

    Raises ``psycopg.Error`` when the id read fails.
    """
    fixed = sorted(
        set(_SCRIPT_ID.findall(FUNCTION_CALLS_SQL.read_text(encoding='utf-8')))
    )
    records = connection.execute(_NEW_IDS_SQL, {'count': len(fixed)}).fetchall()
    fresh = [str(record[0]) for record in records]
    return function_calls_script(dict(zip(fixed, fresh, strict=True)))


def run_function_calls_with(
    database: DualRunDatabase, definitions: Definitions
) -> Result[SelfCheckRun, SelfCheckStepFailed]:
    """Install the baseline and ``definitions``, run the function-call script with new ids, commit, capture.

    A failed step leaves the database as the step left it.
    """
    with psycopg.connect(database.url, autocommit=True) as connection:
        match read_run_start(connection):
            case Err(failed):
                return Err(failed)
            case Ok(start):
                pass
        match install_baseline_schema(database):
            case Err(failed):
                return _failed('install the baseline', failed)
            case Ok():
                pass
        match _install_definitions(connection, definitions):
            case Err(failed):
                return Err(failed)
            case Ok():
                pass
        try:
            script = _script_with_new_ids(connection)
            with connection.transaction():
                _ = connection.execute(script.encode('utf-8'))
        except psycopg.Error as exc:
            return _failed('run the function-call script', exc)
        return capture_run(connection, start)
