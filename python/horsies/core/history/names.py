"""Relation and function names shared across history modules.

One module owns each object's DDL; every other module refers to it through
this vocabulary instead of a string literal, so a rename is a one-line change
and a typo is an import error rather than a runtime probe of a missing
relation.

These names are internal. They are not a public SQL contract and may change
between releases without notice.
"""

from __future__ import annotations

from typing import Final

TASK_PROVENANCE_FUNCTION: Final = 'horsies_task_provenance_staged'
TASK_DETAIL_FUNCTION: Final = 'horsies_task_detail_staged'
TASK_RESULT_FUNCTION: Final = 'horsies_task_result_staged'
TASK_DUPLICATE_GUARD_FUNCTION: Final = 'horsies_task_history_duplicate_staged'

