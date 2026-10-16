"""The literal nonterminal node predicate follows the node status enum.

Recovery statements and the partial recovery index spell the nonterminal
statuses as a literal IN-list so the planner can match the index predicate.
A status added to ``WorkflowTaskStatus`` must reach every copy of the list.
"""

from __future__ import annotations

import pytest
from sqlalchemy import TextClause

from horsies.core.models.workflow.enums import WorkflowTaskStatus
from horsies.core.schemas.recovery import (
    RECOVERY_INDEXES,
    VALIDATE_RECOVERY_INDEXES_SQL,
)
from horsies.core.workflows import recovery

NONTERMINAL_NODE_INDEX_NAME = 'idx_horsies_workflow_tasks_nonterminal'

PREDICATE_STATEMENTS: dict[str, TextClause] = {
    'GET_PENDING_WITH_TERMINAL_DEPS_SQL': recovery.GET_PENDING_WITH_TERMINAL_DEPS_SQL,
    'GLOBAL_GET_PENDING_WITH_TERMINAL_DEPS_SQL': (
        recovery.GLOBAL_GET_PENDING_WITH_TERMINAL_DEPS_SQL
    ),
    'GET_TERMINAL_WORKFLOW_CANDIDATES_SQL': (
        recovery.GET_TERMINAL_WORKFLOW_CANDIDATES_SQL
    ),
    'GLOBAL_GET_TERMINAL_WORKFLOW_CANDIDATES_SQL': (
        recovery.GLOBAL_GET_TERMINAL_WORKFLOW_CANDIDATES_SQL
    ),
    'GLOBAL_WORKFLOW_AUDIT_SQL': recovery.GLOBAL_WORKFLOW_AUDIT_SQL,
    'VALIDATE_RECOVERY_INDEXES_SQL': VALIDATE_RECOVERY_INDEXES_SQL,
}


def _nonterminal_in_list() -> str:
    statuses = [status.value for status in WorkflowTaskStatus if not status.is_terminal]
    return 'IN (' + ', '.join(f"'{value}'" for value in statuses) + ')'


@pytest.mark.unit
@pytest.mark.parametrize('statement_name', sorted(PREDICATE_STATEMENTS))
def test_statement_spells_the_nonterminal_statuses_of_the_enum(
    statement_name: str,
) -> None:
    statement_text = PREDICATE_STATEMENTS[statement_name].text
    assert f'status {_nonterminal_in_list()}' in statement_text


@pytest.mark.unit
def test_partial_index_spells_the_nonterminal_statuses_of_the_enum() -> None:
    index = next(
        index for index in RECOVERY_INDEXES if index.name == NONTERMINAL_NODE_INDEX_NAME
    )
    assert index.create_sql.endswith(f'WHERE status {_nonterminal_in_list()}')
