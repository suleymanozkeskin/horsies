"""The clock-skew exceptions of the duplicate-identity guard.

The guard (`horsies_task_history_duplicate_staged`) is rendered by the staged
loader publisher from the published manifest. For a UUIDv7 id it probes only
the leaves that can hold a row born at that time, minus the clock bound.
A producer whose clock ran ahead mints an id whose birth is later than the
row's terminal time, so the row lands in a leaf the guard skips. Every move
records such an id in the clock-skew exception table, and the guard consults
the table when its probes find nothing. Producers are never refused for a
wrong clock; only the guard's lookup changes. The table and the moves are
SQL in the baseline migration.

An exception row lives as long as the history row it stands for: dropping a
leaf deletes the exceptions of the rows that leaf held.
"""

from __future__ import annotations

from typing import Final

from ..names import CLOCK_SKEW_EXCEPTIONS

DELETE_LEAF_EXCEPTIONS_SQL: Final = f"""
DELETE FROM {CLOCK_SKEW_EXCEPTIONS}
WHERE retention_class_key = :class_key
  AND terminal_at >= :lower
  AND terminal_at < :upper
"""
"""Delete the exceptions of the rows a dropped leaf held."""
