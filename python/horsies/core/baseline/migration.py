"""Read the baseline migration from ``horsies._core``.

The baseline SQL files are in the core crate (``crates/horsies-core/baseline/``)
and are compiled into ``horsies._core``. The migration text is those files,
concatenated in their run order with no separator. Its checksum is SHA-384 of
the UTF-8 bytes of that text, the form that the ``horsies_migrations``
bookkeeping row stores.

``load_baseline_migration`` reads the texts from ``horsies._core`` and does
nothing else: it opens no database connection, starts no core runtime, and
changes no state.
"""

from __future__ import annotations

import hashlib
from dataclasses import dataclass
from typing import Final

import horsies._core as _core

BASELINE_VERSION: Final = 1000
"""Version of the baseline migration in ``horsies_migrations``."""

BASELINE_DESCRIPTION: Final = 'baseline'
"""Description of the baseline migration in ``horsies_migrations``."""


@dataclass(frozen=True, slots=True)
class BaselineMigration:
    """The baseline migration text with the facts of its bookkeeping row."""

    version: int
    description: str
    sql: str
    checksum: bytes


def baseline_checksum(sql: str) -> bytes:
    """SHA-384 of the UTF-8 bytes of a migration text."""
    return hashlib.sha384(sql.encode('utf-8')).digest()


def load_baseline_migration() -> BaselineMigration:
    """The baseline SQL files of ``horsies._core`` as one migration."""
    sql = ''.join(text for _, text in _core.baseline_sql_files())
    return BaselineMigration(
        version=BASELINE_VERSION,
        description=BASELINE_DESCRIPTION,
        sql=sql,
        checksum=baseline_checksum(sql),
    )
