"""Name of the archive maintenance sessions table.

The baseline schema does not create this table. The forever sweep in
``pruning`` reads the name, and queries the table only after
``cutover_complete`` returns ``True``, which a baseline database never
does.
"""

from __future__ import annotations

from typing import Final

ARCHIVE_MAINTENANCE_SESSIONS: Final = 'horsies_archive_maintenance_sessions'
