"""Test support for code that sends ``horsies_workflow_start`` with no database.

``start_workflow_async`` sends one statement on an autocommit connection
(``autocommit_connection(session)``) when the tree has no failed-node
record. ``start_session`` gives a session whose connection answers that
statement with the given rows; ``start_parameters`` reads the parameters
that the start sent, and ``node_rows`` turns their node arrays into one
mapping per node.
"""

from __future__ import annotations

import uuid
from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock, MagicMock

from horsies.core.workflows.steps import START_WORKFLOW_FUNCTION_SQL

_STEP_COLUMNS = (
    'row_kind', 'disposition', 'workflow_id', 'workflow_status', 'advance_child_workflow_id',
    'detail', 'task_index', 'node_input', 'args_from', 'sub_workflow_name',
    'sub_definition_key', 'sub_workflow_params', 'parent_depth', 'root_workflow_id',
    'source_task_index', 'source_node_id', 'source_status', 'source_task_name',
    'source_definition_key', 'source_result', 'source_summary', 'workflow_name',
)


def outcome_row(disposition: str, detail: str | None = None) -> SimpleNamespace:
    """The OUTCOME row of a start: ``STARTED``, or ``EXISTS`` with the stored name."""
    values: dict[str, Any] = {column: None for column in _STEP_COLUMNS}
    values.update(
        row_kind='OUTCOME',
        disposition=disposition,
        workflow_id=uuid.uuid4(),
        workflow_status='RUNNING',
        detail=detail,
    )
    return SimpleNamespace(**values)


def start_session(rows: list[SimpleNamespace]) -> tuple[MagicMock, MagicMock, MagicMock]:
    """A session, its context manager and its autocommit connection.

    The connection answers every statement with ``rows``. The session sends
    nothing itself.
    """
    result = MagicMock()
    result.fetchall.return_value = rows
    connection = MagicMock()
    connection.execute = AsyncMock(return_value=result)
    session = MagicMock()
    session.info = {}
    session.execute = AsyncMock()
    session.commit = AsyncMock()
    session.connection = AsyncMock(return_value=connection)
    session_ctx = MagicMock()
    session_ctx.__aenter__ = AsyncMock(return_value=session)
    session_ctx.__aexit__ = AsyncMock(return_value=False)
    return session, session_ctx, connection


def start_parameters(connection: MagicMock) -> dict[str, Any]:
    """The parameters of the one ``horsies_workflow_start`` call on ``connection``."""
    calls = [
        call for call in connection.execute.await_args_list
        if call.args[0] is START_WORKFLOW_FUNCTION_SQL
    ]
    assert len(calls) == 1, calls
    parameters: dict[str, Any] = calls[0].args[1]
    return parameters


def node_rows(parameters: dict[str, Any]) -> list[dict[str, Any]]:
    """One mapping per node of the start parameters, keyed by the array name
    without its ``node_`` prefix (``queue_names`` becomes ``queue_name``)."""
    singular = {
        'workflow_ids': 'workflow_id', 'task_indexes': 'task_index', 'ids': 'node_id',
        'task_names': 'task_name', 'inputs': 'input', 'queue_names': 'queue_name',
        'priorities': 'priority', 'dependencies': 'dependencies', 'args_from': 'args_from',
        'workflow_ctx_from': 'workflow_ctx_from', 'allow_failed_deps': 'allow_failed_deps',
        'join_types': 'join_type', 'min_successes': 'min_success',
        'task_options': 'task_options', 'max_retries': 'max_retries',
        'executions': 'execution', 'good_untils': 'good_until',
        'retention_class_keys': 'retention_class_key', 'is_subworkflow': 'is_subworkflow',
        'sub_workflow_names': 'sub_workflow_name', 'sub_definition_keys': 'sub_definition_key',
        'sub_workflow_params': 'sub_workflow_params',
    }
    arrays = {
        singular[key.removeprefix('node_')]: value
        for key, value in parameters.items()
        if key.startswith('node_')
    }
    count = len(arrays['task_index'])
    return [{name: values[position] for name, values in arrays.items()} for position in range(count)]
