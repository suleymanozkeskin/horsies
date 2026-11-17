"""Workflow trees for the workflow operation dual-runs.

``start_tree`` writes one tree with ``horsies_workflow_start``: the top-level
workflow first, then each child that a sub-workflow node of the tree starts
at once. The start mints the node and task ids and enqueues the root nodes.
The other helpers move one task, node or workflow into the state a test
needs, with the columns that the claim, the start of a task and a terminal
workflow write.

Every helper raises ``psycopg.Error`` on a database error and
``AssertionError`` when its row is not in the state it expects; the seed's
transaction then rolls back (``tests/dualrun/operation.py``).
"""

from __future__ import annotations

import json
import uuid
from collections.abc import Sequence
from dataclasses import dataclass
from enum import Enum
from typing import Final, LiteralString

from horsies.core.types.result import Err, Ok, Result
from tests.dualrun.capture import Connection
from tests.dualrun.databases import DualRunDatabase
from tests.dualrun.operation import OperationDualRun
from tests.dualrun.selfcheck import SelfCheckStepFailed, seed_history

_TASK_OPTIONS: Final = '{"retry":"none"}'
_QUEUE: Final = 'default'
_PRIORITY: Final = 50
_MAX_RETRIES: Final = 0
_EXECUTION: Final = 'ISOLATED'
_RETENTION: Final = 'standard_30d'
"""The class that a default app config resolves for the default queue, as a
start writes it: a promotion copies the node's class onto the task row."""
_HOST: Final = 'dualrun-host'
_PID: Final = 4242
_PROCESS: Final = 'dualrun-worker-process'


class NodeKind(Enum):
    """What a node runs."""

    TASK = 'TASK'
    SUBWORKFLOW = 'SUBWORKFLOW'


class OnError(Enum):
    """The failure policy of a workflow."""

    FAIL = 'fail'
    PAUSE = 'pause'


@dataclass(frozen=True, slots=True)
class NodeSeed:
    """One node. ``name`` is the task name, or the child workflow name of a sub-workflow node.

    ``definition_key`` is the key that loads the child of a sub-workflow node;
    ``None`` on a task node, and on a sub-workflow node whose child no test
    builds. ``args_from`` maps a kwarg of the task to the index of the
    dependency whose result it receives.
    """

    index: int
    name: str
    kind: NodeKind
    dependencies: tuple[int, ...]
    definition_key: str | None = None
    args_from: tuple[tuple[str, int], ...] = ()


@dataclass(frozen=True, slots=True)
class TopLevel:
    """The workflow has no parent."""


@dataclass(frozen=True, slots=True)
class ChildOf:
    """The workflow is the child of node ``parent_task_index`` of ``parent``."""

    parent: uuid.UUID
    parent_task_index: int
    depth: int


type Parent = TopLevel | ChildOf


@dataclass(frozen=True, slots=True)
class NoOutput:
    """The workflow names no output node: its result is outputless."""


@dataclass(frozen=True, slots=True)
class OutputNode:
    """The result of node ``index`` is the result of the workflow."""

    index: int


type Output = NoOutput | OutputNode


@dataclass(frozen=True, slots=True)
class WorkflowSeed:
    """One workflow row and its nodes."""

    workflow_id: uuid.UUID
    name: str
    on_error: OnError
    parent: Parent
    nodes: tuple[NodeSeed, ...]
    output: Output = NoOutput()


def task_node(index: int, name: str, *dependencies: int) -> NodeSeed:
    """A task node of ``name`` that waits for ``dependencies``."""
    return NodeSeed(index=index, name=name, kind=NodeKind.TASK, dependencies=dependencies)


def args_from_node(
    index: int, name: str, args_from: tuple[tuple[str, int], ...], *dependencies: int,
) -> NodeSeed:
    """A task node of ``name`` that waits for ``dependencies`` and receives the
    results that ``args_from`` names."""
    return NodeSeed(
        index=index,
        name=name,
        kind=NodeKind.TASK,
        dependencies=dependencies,
        args_from=args_from,
    )


def subworkflow_node(
    index: int, child_name: str, *dependencies: int, definition_key: str | None = None,
) -> NodeSeed:
    """A sub-workflow node whose child workflow is named ``child_name``."""
    return NodeSeed(
        index=index,
        name=child_name,
        kind=NodeKind.SUBWORKFLOW,
        dependencies=dependencies,
        definition_key=definition_key,
    )


_START_SQL: Final[LiteralString] = """
    SELECT row_kind, disposition FROM horsies_workflow_start(
        %(workflow_ids)s, %(names)s, %(definition_keys)s, %(on_errors)s,
        %(output_task_indexes)s, %(success_policies)s, %(parent_workflow_ids)s,
        %(parent_task_indexes)s, %(depths)s, now() - interval '1 second',
        %(node_workflow_ids)s, %(node_task_indexes)s, %(node_ids)s, %(node_task_names)s,
        %(node_inputs)s, %(node_queue_names)s, %(node_priorities)s, %(node_dependencies)s,
        %(node_args_from)s, %(node_workflow_ctx_from)s, %(node_allow_failed_deps)s,
        %(node_join_types)s, %(node_min_successes)s, %(node_task_options)s,
        %(node_max_retries)s, %(node_executions)s,
        CAST(%(node_good_untils)s AS timestamptz[]), %(node_retention_class_keys)s,
        %(node_is_subworkflow)s, %(node_sub_workflow_names)s, %(node_sub_definition_keys)s,
        %(node_sub_workflow_params)s,
        CAST(%(failed_workflow_ids)s AS uuid[]), CAST(%(failed_task_indexes)s AS integer[]),
        CAST(%(failed_results)s AS text[])
    )
"""


def _parent_columns(parent: Parent) -> tuple[uuid.UUID | None, int | None, int]:
    match parent:
        case TopLevel():
            return None, None, 0
        case ChildOf(parent=parent_id, parent_task_index=index, depth=depth):
            return parent_id, index, depth


def _output_task_index(output: Output) -> int | None:
    match output:
        case NoOutput():
            return None
        case OutputNode(index=index):
            return index


def _is_task(node: NodeSeed) -> bool:
    match node.kind:
        case NodeKind.TASK:
            return True
        case NodeKind.SUBWORKFLOW:
            return False


def _dependencies_text(node: NodeSeed) -> str:
    return '[' + ','.join(str(index) for index in node.dependencies) + ']'


def _args_from_text(node: NodeSeed) -> str | None:
    match node.args_from:
        case ():
            return None
        case pairs:
            return json.dumps(dict(pairs), separators=(',', ':'))


def start_parameters(workflows: Sequence[WorkflowSeed]) -> dict[str, object]:
    """The arrays of ``horsies_workflow_start`` for ``workflows`` (top-level row first). Pure."""
    parents = [_parent_columns(workflow.parent) for workflow in workflows]
    nodes = [(workflow, node) for workflow in workflows for node in workflow.nodes]
    return {
        'workflow_ids': [workflow.workflow_id for workflow in workflows],
        'names': [workflow.name for workflow in workflows],
        'definition_keys': [None for _ in workflows],
        'on_errors': [workflow.on_error.value for workflow in workflows],
        'output_task_indexes': [_output_task_index(workflow.output) for workflow in workflows],
        'success_policies': [None for _ in workflows],
        'parent_workflow_ids': [parent for parent, _, _ in parents],
        'parent_task_indexes': [index for _, index, _ in parents],
        'depths': [depth for _, _, depth in parents],
        'node_workflow_ids': [workflow.workflow_id for workflow, _ in nodes],
        'node_task_indexes': [node.index for _, node in nodes],
        'node_ids': [f'{workflow.name}.n{node.index}' for workflow, node in nodes],
        'node_task_names': [node.name for _, node in nodes],
        'node_inputs': ['{}' for _ in nodes],
        'node_queue_names': [_QUEUE for _ in nodes],
        'node_priorities': [_PRIORITY for _ in nodes],
        'node_dependencies': [_dependencies_text(node) for _, node in nodes],
        'node_args_from': [_args_from_text(node) for _, node in nodes],
        'node_workflow_ctx_from': [None for _ in nodes],
        'node_allow_failed_deps': [False for _ in nodes],
        'node_join_types': ['all' for _ in nodes],
        'node_min_successes': [None for _ in nodes],
        'node_task_options': [_TASK_OPTIONS if _is_task(node) else None for _, node in nodes],
        'node_max_retries': [_MAX_RETRIES if _is_task(node) else None for _, node in nodes],
        'node_executions': [_EXECUTION if _is_task(node) else None for _, node in nodes],
        'node_good_untils': [None for _ in nodes],
        'node_retention_class_keys': [_RETENTION if _is_task(node) else None for _, node in nodes],
        'node_is_subworkflow': [not _is_task(node) for _, node in nodes],
        'node_sub_workflow_names': [None if _is_task(node) else node.name for _, node in nodes],
        'node_sub_definition_keys': [node.definition_key for _, node in nodes],
        'node_sub_workflow_params': [None if _is_task(node) else 'NOT_GIVEN' for _, node in nodes],
        'failed_workflow_ids': [],
        'failed_task_indexes': [],
        'failed_results': [],
    }


def start_tree(connection: Connection, workflows: Sequence[WorkflowSeed]) -> None:
    """Start the tree of ``workflows`` in one statement; the outcome must be STARTED with no owed work."""
    rows = connection.execute(_START_SQL, start_parameters(workflows)).fetchall()
    kinds = sorted((str(row[0]), str(row[1])) for row in rows)
    assert kinds == [('OUTCOME', 'STARTED')], f'the seeded start returned {kinds}'


_NODE_TASK_SQL: Final[LiteralString] = """
    SELECT task_id FROM horsies_workflow_tasks WHERE workflow_id = %(wf)s AND task_index = %(idx)s
"""

_CLAIM_SQL: Final[LiteralString] = """
    UPDATE horsies_tasks
    SET status = 'CLAIMED', claimed_at = now(), claimed_by_worker_id = %(worker)s,
        claim_expires_at = now() + interval '1 minute', updated_at = now()
    WHERE id = %(task)s AND status = 'PENDING'
"""

_RUN_TASK_SQL: Final[LiteralString] = """
    UPDATE horsies_tasks
    SET status = 'RUNNING', claimed_at = now(), claimed_by_worker_id = %(worker)s,
        claim_expires_at = NULL, started_at = now(), worker_pid = %(pid)s,
        worker_hostname = %(host)s, worker_process_name = %(process)s, updated_at = now()
    WHERE id = %(task)s AND status = 'PENDING'
"""

_RUN_NODE_SQL: Final[LiteralString] = """
    UPDATE horsies_workflow_tasks SET status = 'RUNNING', started_at = now()
    WHERE workflow_id = %(wf)s AND task_index = %(idx)s AND status = 'ENQUEUED'
"""

_END_WORKFLOW_SQL: Final[LiteralString] = """
    UPDATE horsies_workflows SET status = %(status)s, completed_at = now(), updated_at = now()
    WHERE id = %(wf)s
"""

_PAUSE_WORKFLOW_SQL: Final[LiteralString] = """
    UPDATE horsies_workflows SET status = 'PAUSED', updated_at = now()
    WHERE id = %(wf)s AND status = 'RUNNING'
"""


_FINISH_SUCCESS_SQL: Final[LiteralString] = """
    SELECT outcome FROM horsies_finish_workflow_success(
        %(task)s, %(worker)s,
        (SELECT claimed_at FROM horsies_tasks WHERE id = %(task)s), %(result)s
    )
"""

_NODE_FINISH_SQL: Final[LiteralString] = """
    SELECT row_kind, disposition FROM horsies_workflow_node_finish(%(task)s, FALSE, 'READ')
"""

_PAUSE_TREE_SQL: Final[LiteralString] = """
    SELECT outcome FROM horsies_workflow_pause(%(wf)s)
"""


def finish_node_task(
    connection: Connection,
    workflow_id: uuid.UUID,
    index: int,
    worker_id: str,
    result_json: str,
) -> uuid.UUID:
    """Finish the RUNNING task of node ``index`` with ``result_json``: the
    success of phase 1, then phase 2 with no wake. The node must owe no
    child start (its workflow is PAUSED, or no sub-workflow node is below it)."""
    task_id = node_task_id(connection, workflow_id, index)
    finished = connection.execute(
        _FINISH_SUCCESS_SQL, {'task': task_id, 'worker': worker_id, 'result': result_json},
    ).fetchone()
    assert finished is not None and finished[0] == 'APPLIED', (
        f'the success of node {index} of {workflow_id} returned {finished}'
    )
    rows = connection.execute(_NODE_FINISH_SQL, {'task': task_id}).fetchall()
    kinds = sorted((str(row[0]), str(row[1])) for row in rows)
    assert kinds == [('OUTCOME', 'APPLIED')], f'the phase 2 of node {index} returned {kinds}'
    return task_id


def pause_tree(connection: Connection, workflow_id: uuid.UUID) -> None:
    """Pause a RUNNING workflow and its RUNNING descendants, as the pause does."""
    paused = connection.execute(_PAUSE_TREE_SQL, {'wf': workflow_id}).fetchone()
    assert paused is not None and paused[0] == 'PAUSED', f'the pause returned {paused}'


def node_task_id(connection: Connection, workflow_id: uuid.UUID, index: int) -> uuid.UUID:
    """The task of node ``index`` of ``workflow_id``; the node must have one."""
    record = connection.execute(_NODE_TASK_SQL, {'wf': workflow_id, 'idx': index}).fetchone()
    assert record is not None and record[0] is not None, (
        f'node {index} of workflow {workflow_id} has no task'
    )
    task_id = record[0]
    assert isinstance(task_id, uuid.UUID)
    return task_id


def claim_node_task(
    connection: Connection, workflow_id: uuid.UUID, index: int, worker_id: str,
) -> uuid.UUID:
    """Claim the PENDING task of node ``index`` for ``worker_id``, with a live lease."""
    task_id = node_task_id(connection, workflow_id, index)
    cursor = connection.execute(_CLAIM_SQL, {'task': task_id, 'worker': worker_id})
    assert cursor.rowcount == 1, f'the task of node {index} of {workflow_id} is not PENDING'
    return task_id


def run_node_task(
    connection: Connection, workflow_id: uuid.UUID, index: int, worker_id: str,
) -> uuid.UUID:
    """Move the PENDING task of node ``index`` to RUNNING and its ENQUEUED node to RUNNING."""
    task_id = node_task_id(connection, workflow_id, index)
    task = connection.execute(
        _RUN_TASK_SQL,
        {'task': task_id, 'worker': worker_id, 'pid': _PID, 'host': _HOST, 'process': _PROCESS},
    )
    assert task.rowcount == 1, f'the task of node {index} of {workflow_id} is not PENDING'
    node = connection.execute(_RUN_NODE_SQL, {'wf': workflow_id, 'idx': index})
    assert node.rowcount == 1, f'node {index} of {workflow_id} is not ENQUEUED'
    return task_id


def end_workflow(connection: Connection, workflow_id: uuid.UUID, status: str) -> None:
    """Set a terminal ``status`` with ``completed_at`` on one workflow row; its nodes stay."""
    cursor = connection.execute(_END_WORKFLOW_SQL, {'wf': workflow_id, 'status': status})
    assert cursor.rowcount == 1, f'workflow {workflow_id} is absent'


def pause_workflow_row(connection: Connection, workflow_id: uuid.UUID) -> None:
    """Set one RUNNING workflow row PAUSED; its descendants and tasks stay."""
    cursor = connection.execute(_PAUSE_WORKFLOW_SQL, {'wf': workflow_id})
    assert cursor.rowcount == 1, f'workflow {workflow_id} is not RUNNING'


_PENDING_SQL: Final[LiteralString] = """
    INSERT INTO horsies_workflow_phase2_pending (
        task_id, workflow_id, workflow_node_row_id, terminalization_kind, recovery_source,
        history_class, history_anchor, history_schema_version, result_digest,
        quarantine_task_id, created_at, attempt_count, last_attempt_at, last_failure_class
    )
    SELECT n.task_id, n.workflow_id, n.id, 'COMPLETE_LOCKED', 'HISTORY',
           'forever', now(), 1, sha256(convert_to('seed result', 'UTF8')),
           NULL, now() - interval '1 minute', %(attempts)s,
           CASE WHEN %(attempts)s = 0 THEN NULL ELSE now() END,
           CASE WHEN %(attempts)s = 0 THEN NULL ELSE 'RAISED' END
    FROM horsies_workflow_tasks n
    WHERE n.workflow_id = %(wf)s AND n.task_index = %(idx)s AND n.task_id IS NOT NULL
"""


def insert_phase2_pending(
    connection: Connection, workflow_id: uuid.UUID, index: int, attempts: int,
) -> uuid.UUID:
    """A phase-2 record of the task of node ``index``, with ``attempts`` raised passes.

    The record points at a history row that the seed does not write; the
    quarantine of the record then reports that the source is absent.
    """
    task_id = node_task_id(connection, workflow_id, index)
    cursor = connection.execute(
        _PENDING_SQL, {'wf': workflow_id, 'idx': index, 'attempts': attempts},
    )
    assert cursor.rowcount == 1, f'node {index} of {workflow_id} has no task'
    return task_id


def cover_history(database: DualRunDatabase) -> None:
    """The production coverage pass, which publishes the staged history readers.

    A side runs it before its operation: a task cancel or abandon moves the
    task to history. Raises ``AssertionError`` when the pass fails.
    """
    match seed_history(database):
        case Err(failed):
            raise AssertionError(f'{failed.step}: {failed.cause}')
        case Ok():
            pass


def run_or_fail[RR, CR](
    result: Result[OperationDualRun[RR, CR], SelfCheckStepFailed],
) -> OperationDualRun[RR, CR]:
    """The dual-run, or ``AssertionError`` naming the failed step."""
    match result:
        case Ok(value):
            return value
        case Err(failed):
            raise AssertionError(f'{failed.step}: {failed.cause}')


_PHASE_ONE_SQL: Final[LiteralString] = """
    SELECT outcome FROM horsies_finish_workflow_success(
        %(task)s, %(worker)s,
        (SELECT claimed_at FROM horsies_tasks WHERE id = %(task)s), %(result)s
    )
"""


def finish_phase_one(
    connection: Connection,
    workflow_id: uuid.UUID,
    index: int,
    worker_id: str,
    result_json: str,
) -> uuid.UUID:
    """The success of phase 1 alone for the RUNNING task of node ``index``: the
    task moves to history and the phase-2 record waits for recovery, as after
    a worker crash between the two phases."""
    task_id = node_task_id(connection, workflow_id, index)
    finished = connection.execute(
        _PHASE_ONE_SQL, {'task': task_id, 'worker': worker_id, 'result': result_json},
    ).fetchone()
    assert finished is not None and finished[0] == 'APPLIED', (
        f'the success of node {index} of {workflow_id} returned {finished}'
    )
    return task_id
