"""The history contract error: the database state violates an invariant that
the history reads rely on. It is never a task or maintenance outcome. The
class is generated from the core (``horsies._catalog.history_errors``).
"""

from __future__ import annotations

from horsies._catalog.history_errors import HistoryContractError as HistoryContractError
