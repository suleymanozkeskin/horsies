"""The install of ``interim_terminal_writes.sql`` on a reference database.

``cancel.py`` calls ``horsies_cancel_locked_task`` and ``stale_running.py``
calls ``horsies_fail_stale_task``. The baseline no longer defines either, so
the reference side installs their record before it runs.
"""

from __future__ import annotations

from pathlib import Path
from typing import Final

from tests.dualrun.capture import Connection

INTERIM_TERMINAL_WRITES_SQL: Final = Path(__file__).with_name(
    'interim_terminal_writes.sql'
)


def install_interim_terminal_writes(connection: Connection) -> None:
    """Create the two interim terminal writes from their record, in one transaction.

    Raises ``psycopg.Error`` on a database error; the transaction then rolls
    back. Raises ``OSError`` when the record cannot be read.
    """
    text = INTERIM_TERMINAL_WRITES_SQL.read_text(encoding='utf-8')
    with connection.transaction():
        _ = connection.execute(text.encode('utf-8'))
