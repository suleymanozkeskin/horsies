"""Read the baseline SQL files of this package as one migration.

The migration text is the files of ``BASELINE_SQL_FILES``, read as UTF-8 and
concatenated in that order with no separator. Its checksum is SHA-384 of the
UTF-8 bytes of that text, the form that the ``horsies_migrations`` bookkeeping
row stores.

``load_baseline_migration`` reads package data and does nothing else: it
opens no database connection and changes no state.
"""

from __future__ import annotations

import hashlib
from dataclasses import dataclass
from importlib import resources
from typing import Final

from horsies.core.types.result import Err, Ok, Result

BASELINE_VERSION: Final = 1000
"""Version of the baseline migration in ``horsies_migrations``."""

BASELINE_DESCRIPTION: Final = 'baseline'
"""Description of the baseline migration in ``horsies_migrations``."""

BASELINE_SQL_FILES: Final = (
    '0001_tables.sql',
    '0002_indexes.sql',
    '0003_interim_functions.sql',
    '0006_history_functions.sql',
    '0007_scheduler_monitoring_functions.sql',
    '0008_history_maintenance_functions.sql',
)
"""The SQL files of the migration, in the order they run."""

_SQL_PACKAGE: Final = 'horsies.core.baseline'
_SQL_FOLDER: Final = 'sql'


@dataclass(frozen=True, slots=True)
class BaselineMigration:
    """The baseline migration text with the facts of its bookkeeping row."""

    version: int
    description: str
    sql: str
    checksum: bytes


@dataclass(frozen=True, slots=True)
class BaselineSqlUnreadable:
    """A SQL file of the package could not be read as UTF-8 text.

    This means a broken installation of the package. Reading again does not
    help.
    """

    file_name: str
    cause: str


def baseline_checksum(sql: str) -> bytes:
    """SHA-384 of the UTF-8 bytes of a migration text."""
    return hashlib.sha384(sql.encode('utf-8')).digest()


def _read_sql_file(file_name: str) -> Result[str, BaselineSqlUnreadable]:
    resource = resources.files(_SQL_PACKAGE).joinpath(_SQL_FOLDER, file_name)
    try:
        return Ok(resource.read_text(encoding='utf-8'))
    except (OSError, UnicodeDecodeError) as exc:
        return Err(
            BaselineSqlUnreadable(
                file_name=file_name,
                cause=f'{type(exc).__name__}: {exc}',
            ),
        )


def load_baseline_migration() -> Result[BaselineMigration, BaselineSqlUnreadable]:
    """Read the baseline SQL files and return them as one migration.

    Returns ``Err(BaselineSqlUnreadable)`` for the first file that cannot be
    read; no partial text is returned.
    """
    parts: list[str] = []
    for file_name in BASELINE_SQL_FILES:
        match _read_sql_file(file_name):
            case Err(unreadable):
                return Err(unreadable)
            case Ok(text):
                parts.append(text)
    sql = ''.join(parts)
    return Ok(
        BaselineMigration(
            version=BASELINE_VERSION,
            description=BASELINE_DESCRIPTION,
            sql=sql,
            checksum=baseline_checksum(sql),
        ),
    )
