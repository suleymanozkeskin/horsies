"""Workflow recovery: the reaper's global pass and the tree pass of a resume.

Each candidate is one step in its own transaction, on a function of
``0005_workflow_functions.sql``:

- case 0, a PENDING node whose dependencies are all terminal:
  ``horsies_workflow_node_reevaluate`` with its owed child starts;
- case 1, a READY task node with no task, and case 1.5, a READY sub-workflow
  node with no child: ``horsies_workflow_nodes_enqueue_ready`` with its owed
  child starts;
- case 1.6, a terminal child whose parent node did not advance:
  ``horsies_workflow_child_finish`` with its owed child starts;
- cases 2/3 (a RUNNING workflow whose nodes are all terminal) and 4 (a RUNNING
  workflow with no node) of the global pass: one audit page
  (``horsies_workflow_recovery_audit_page``), then
  ``horsies_workflow_recover_completion`` or ``horsies_workflow_fail_orphaned``
  per candidate, each one statement in autocommit that renews the page claim
  with its work;
- cases 2/3 and 4 of the tree pass: the completion check under the workflow
  row lock (``horsies_workflow_try_complete``), and the orphan write.

A candidate commits or rolls back as one transaction. The parent nodes of the
child workflows that a candidate ended advance after its commit, one
transaction per level (``steps.advance_parents``). A failure there is logged:
the child stays terminal under a live parent node, so case 1.6 selects the
pair next pass and counts a persistent failure on the parent workflow.

A persistent raise of a candidate of the global pass counts toward the failure
limit of its workflow (``recovery_limit.count_recovery_raise``); the tree pass
of a resume only logs, because the reaper counts the same candidates.

The crashed-worker case — a task terminal, its node not advanced — is
NOT here: terminalization records that progression as it moves the task
off the live table, and ``phase2_recovery`` consumes those records.
"""

from __future__ import annotations

import time
import uuid
from collections.abc import Sequence
from dataclasses import asdict, dataclass, field
from enum import Enum
from typing import TYPE_CHECKING, Any, Final, assert_never, cast

from sqlalchemy import text

from horsies.core.codec.json_io import SerializationError, dumps_json
from horsies.core.codec.typed import encode_task_error
from horsies.core.lifecycle.outcomes import OutcomeDecodeError
from horsies.core.logging import get_logger
from horsies.core.models.recovery import DEFAULT_WORKFLOW_RECOVERY_FAILURE_LIMIT
from horsies.core.models.tasks import OperationalErrorCode, TaskError
from horsies.core.types.result import Err, Ok
from horsies.core.utils.db import autocommit_connection
from horsies.core.workflows.recovery_limit import (
    MAX_RAISE_TEXT_LENGTH,
    CountFailures,
    LogFailuresOnly,
    RaiseRecord,
    RecoveryCandidateOutcome,
    RecoveryCase,
    RecoveryFailurePolicy,
    RecoveryRaise,
    RecoveryRaiseClass,
    classify_recovery_raise,
    count_over_failure_limit,
    count_recovery_raise,
)
from horsies.core.workflows.steps import (
    CHILD_FINISH_FUNCTION_SQL,
    NODE_REEVALUATE_FUNCTION_SQL,
    NODES_ENQUEUE_READY_FUNCTION_SQL,
    ChildFinishDisposition,
    CommittedStep,
    EnqueueReadyDisposition,
    NoParentAdvance,
    ParentAdvance,
    ParentAdvanceOwed,
    ReevaluateDisposition,
    advance_parents,
    run_in_transaction,
)

if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker
    from horsies.core.brokers.postgres import PostgresBroker

logger = get_logger('workflow.recovery')


GLOBAL_SCAN_ROW_CAP: Final = 200
"""Rows that one global case query returns, and the size of one audit page.
Later passes and later cursor pages cover the rest."""

GLOBAL_WORKFLOW_AUDIT_CLAIM_TTL_MS: Final = 300_000
"""The life of the audit page claim. Each candidate statement renews it with
its work; a stopped pass releases the page by expiry within this time."""


# ---------------------------------------------------------------------------
# Discovery statements. A scoped statement reads the workflows ``scope_ids``
# (a resumed tree) with no row cap; its global form reads every workflow with
# ``max_rows``.
# ---------------------------------------------------------------------------

_SCOPE_FILTER_WORKFLOW: Final = '      AND wt.workflow_id = ANY(CAST(:scope_ids AS uuid[]))\n'
_SCOPE_FILTER_CHILD: Final = '      AND child.id = ANY(CAST(:scope_ids AS uuid[]))\n'


def _global_form(scoped: Any, scope_filter: str) -> Any:
    """The global form of a scoped discovery statement: the statement without
    its scope filter line. A filter line that is not in the statement is a
    defect of this module."""
    assert scope_filter in scoped.text, f'no scope filter in {scoped.text!r}'
    return text(scoped.text.replace(scope_filter, ''))

GET_PENDING_WITH_TERMINAL_DEPS_SQL: Final = text("""
    SELECT wt.workflow_id, wt.task_index
    FROM horsies_workflow_tasks wt
    JOIN horsies_workflows w ON w.id = wt.workflow_id
    WHERE wt.status = 'PENDING'
      AND w.status = 'RUNNING'
      AND wt.workflow_id = ANY(CAST(:scope_ids AS uuid[]))
      AND NOT EXISTS (
          SELECT 1 FROM horsies_workflow_tasks dep
          WHERE dep.workflow_id = wt.workflow_id
            AND wt.dependencies @> ARRAY[dep.task_index]
            AND dep.status IN ('PENDING', 'READY', 'ENQUEUED', 'RUNNING')
      )
    LIMIT CAST(:max_rows AS bigint)
""")
GLOBAL_GET_PENDING_WITH_TERMINAL_DEPS_SQL: Final = _global_form(
    GET_PENDING_WITH_TERMINAL_DEPS_SQL, _SCOPE_FILTER_WORKFLOW,
)

GET_READY_NOT_ENQUEUED_SQL: Final = text("""
    SELECT wt.workflow_id, wt.task_index
    FROM horsies_workflow_tasks wt
    JOIN horsies_workflows w ON w.id = wt.workflow_id
    WHERE wt.status = 'READY'
      AND wt.task_id IS NULL
      AND wt.is_subworkflow = FALSE
      AND w.status = 'RUNNING'
      AND wt.workflow_id = ANY(CAST(:scope_ids AS uuid[]))
    LIMIT CAST(:max_rows AS bigint)
""")
GLOBAL_GET_READY_NOT_ENQUEUED_SQL: Final = _global_form(
    GET_READY_NOT_ENQUEUED_SQL, _SCOPE_FILTER_WORKFLOW,
)

GET_READY_SUBWORKFLOWS_NOT_STARTED_SQL: Final = text("""
    SELECT wt.workflow_id, wt.task_index
    FROM horsies_workflow_tasks wt
    JOIN horsies_workflows w ON w.id = wt.workflow_id
    WHERE wt.status = 'READY'
      AND wt.is_subworkflow = TRUE
      AND wt.sub_workflow_id IS NULL
      AND w.status = 'RUNNING'
      AND wt.workflow_id = ANY(CAST(:scope_ids AS uuid[]))
    LIMIT CAST(:max_rows AS bigint)
""")
GLOBAL_GET_READY_SUBWORKFLOWS_NOT_STARTED_SQL: Final = _global_form(
    GET_READY_SUBWORKFLOWS_NOT_STARTED_SQL, _SCOPE_FILTER_WORKFLOW,
)

GET_COMPLETED_CHILDREN_NOT_UPDATED_SQL: Final = text("""
    SELECT child.id, child.parent_workflow_id, child.parent_task_index
    FROM horsies_workflows child
    JOIN horsies_workflows parent ON parent.id = child.parent_workflow_id
    JOIN horsies_workflow_tasks wt ON wt.workflow_id = parent.id AND wt.task_index = child.parent_task_index
    WHERE child.status IN ('COMPLETED', 'FAILED', 'CANCELLED', 'EXPIRED')
      AND wt.status = 'RUNNING'
      AND parent.status = 'RUNNING'
      AND child.id = ANY(CAST(:scope_ids AS uuid[]))
    LIMIT CAST(:max_rows AS bigint)
""")
GLOBAL_GET_COMPLETED_CHILDREN_NOT_UPDATED_SQL: Final = _global_form(
    GET_COMPLETED_CHILDREN_NOT_UPDATED_SQL, _SCOPE_FILTER_CHILD,
)

GET_TERMINAL_WORKFLOW_CANDIDATES_SQL: Final = text("""
    SELECT w.id
    FROM horsies_workflows w
    WHERE w.status = 'RUNNING'
      AND w.id = ANY(CAST(:scope_ids AS uuid[]))
      AND EXISTS (
          SELECT 1 FROM horsies_workflow_tasks present
          WHERE present.workflow_id = w.id
      )
      AND NOT EXISTS (
          SELECT 1 FROM horsies_workflow_tasks wt2
          WHERE wt2.workflow_id = w.id
            AND wt2.status IN ('PENDING', 'READY', 'ENQUEUED', 'RUNNING')
      )
    LIMIT CAST(:max_rows AS bigint)
""")
"""Cases 2/3 of the tree pass. The global pass reads them from the audit page."""

GET_ORPHANED_WORKFLOW_CANDIDATES_SQL: Final = text("""
    SELECT w.id, w.name
    FROM horsies_workflows w
    WHERE w.status = 'RUNNING'
      AND w.id = ANY(CAST(:scope_ids AS uuid[]))
      AND NOT EXISTS (
          SELECT 1 FROM horsies_workflow_tasks wt
          WHERE wt.workflow_id = w.id
      )
    LIMIT CAST(:max_rows AS bigint)
""")
"""Case 4 of the tree pass. The global pass reads it from the audit page."""

GET_WORKFLOW_TREE_IDS_SQL: Final = text("""
    WITH RECURSIVE tree AS (
        SELECT id FROM horsies_workflows WHERE id = :wf_id
        UNION ALL
        SELECT child.id
        FROM horsies_workflows child
        JOIN tree parent ON child.parent_workflow_id = parent.id
    )
    SELECT id FROM tree
""")
"""A workflow and its descendants: the scope of the tree pass of a resume."""


# ---------------------------------------------------------------------------
# Candidate statements of cases 2/3 and 4.
# ---------------------------------------------------------------------------

AUDIT_PAGE_FUNCTION_SQL: Final = text("""
    SELECT outcome, scanned_count, completion_ids, orphan_ids, orphan_names, claim_token
    FROM horsies_workflow_recovery_audit_page(
        CAST(:max_rows AS integer), CAST(:claim_token AS uuid), CAST(:claim_ttl_ms AS integer)
    )
""")
"""One page of the audit of RUNNING workflows, with the page claim when the
page has candidates."""

RECOVER_COMPLETION_FUNCTION_SQL: Final = text("""
    SELECT outcome, workflow_status, advance_child_workflow_id
    FROM horsies_workflow_recover_completion(
        CAST(:wf_id AS uuid), CAST(:claim_token AS uuid), CAST(:claim_ttl_ms AS integer)
    )
""")
"""Cases 2/3 of one audit candidate: the claim renewal and the completion check."""

FAIL_ORPHANED_FUNCTION_SQL: Final = text("""
    SELECT outcome, workflow_status, advance_child_workflow_id
    FROM horsies_workflow_fail_orphaned(
        CAST(:wf_id AS uuid), CAST(:claim_token AS uuid), CAST(:claim_ttl_ms AS integer),
        CAST(:error AS text)
    )
""")
"""Case 4 of one audit candidate: the claim renewal and the FAILED write."""

RELEASE_GLOBAL_WORKFLOW_AUDIT_CLAIM_SQL: Final = text("""
    UPDATE horsies_recovery_scan_cursors
    SET claim_token = NULL, claim_expires_at = NULL
    WHERE scan_name = 'running_workflows'
      AND claim_token = CAST(:claim_token AS uuid)
""")
"""Ends the page claim of this pass."""

LOCK_TREE_CANDIDATE_SQL: Final = text("""
    SELECT parent_workflow_id FROM horsies_workflows
    WHERE id = CAST(:wf_id AS uuid)
    FOR UPDATE
""")
"""The tree pass's lock of one completion candidate, the precondition of
``horsies_workflow_try_complete``, and its parent."""

TRY_COMPLETE_SQL: Final = text("""
    SELECT horsies_workflow_try_complete(CAST(:wf_id AS uuid)) AS written
""")
"""The completion check of a RUNNING workflow with no live node, under the
caller's row lock: the status it wrote, or NULL."""

FAIL_TREE_ORPHAN_SQL: Final = text("""
    UPDATE horsies_workflows w
    SET status = 'FAILED', error = :error, completed_at = NOW(), updated_at = NOW()
    WHERE w.id = CAST(:wf_id AS uuid)
      AND w.status = 'RUNNING'
      AND NOT EXISTS (SELECT 1 FROM horsies_workflow_tasks n WHERE n.workflow_id = w.id)
    RETURNING w.parent_workflow_id
""")
"""Case 4 of the tree pass: a RUNNING workflow with no node goes FAILED, while
it is RUNNING and has no node."""


# ---------------------------------------------------------------------------
# Report
# ---------------------------------------------------------------------------


@dataclass
class RecoveryCaseMetrics:
    """Bounded work and outcome counts for one recovery case."""

    rows_selected: int = 0
    candidates_returned: int = 0
    duration_ms: int = 0
    refusals: int = 0
    errors: int = 0
    failures_counted: int = 0
    limit_reached: int = 0


@dataclass(frozen=True, slots=True)
class AuditPage:
    """The global audit returned a page.

    `completion_candidates` counts the page's completion candidates as the
    audit returned them, before any is processed.
    """

    scanned: int
    completion_candidates: int


@dataclass(frozen=True, slots=True)
class NoAuditPage:
    """The global audit returned no page: it failed, or another worker
    holds the audit cursor."""


type AuditResult = AuditPage | NoAuditPage


def completion_candidate_yield(audit: AuditResult) -> float | None:
    """Completion candidates per scanned workflow, or None when undefined.

    Undefined when the audit returned no page or an empty page. A
    populated page without completion candidates yields 0.0.
    """
    match audit:
        case NoAuditPage():
            return None
        case AuditPage(scanned=0):
            return None
        case AuditPage(scanned=scanned, completion_candidates=candidates):
            return candidates / scanned


@dataclass
class RecoveryReport:
    """Health result for one recovery pass.

    `errors` holds only the failures of the pass that no case counted. The
    published `errors` adds the errors of every case, so each failure counts
    once. A step that serves two cases, such as the audit, counts in each.
    """

    recovered: int = 0
    errors: int = 0
    metrics: dict[str, RecoveryCaseMetrics] = field(default_factory=lambda: {
        name: RecoveryCaseMetrics()
        for name in ('case_0', 'case_1', 'case_1_5', 'case_1_6', 'case_2_3', 'case_4')
    })
    audit: AuditResult = field(default_factory=NoAuditPage)
    over_failure_limit: int = 0

    def health(self) -> dict[str, object]:
        return {
            'recovered': self.recovered,
            'errors': self.errors + sum(
                metrics.errors for metrics in self.metrics.values()
            ),
            'over_failure_limit': self.over_failure_limit,
            'completion_candidate_yield': completion_candidate_yield(self.audit),
            'cases': {
                name: asdict(metrics)
                for name, metrics in self.metrics.items()
            },
        }


class RecoveryPassFailure(Exception):
    """A failed global pass with its partial health report."""

    def __init__(self, report: RecoveryReport, error: Exception) -> None:
        super().__init__(str(error))
        self.report = report
        self.error = error

    def health(self) -> dict[str, object]:
        snapshot = self.report.health()
        snapshot['state'] = 'error'
        snapshot['error'] = str(self.error)
        return snapshot


class _CaseCountedFailure(Exception):
    """A raise that ends the pass after a case put it in its errors.

    The global pass unwraps it, so that one failure counts once.
    """

    def __init__(self, error: Exception) -> None:
        super().__init__(str(error))
        self.error = error


# ---------------------------------------------------------------------------
# The rows of the audit functions
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class _AuditClaim:
    """The page has candidates; this pass holds the page claim."""

    token: str


@dataclass(frozen=True, slots=True)
class _NoAuditClaim:
    """The page has no candidate, so it carries no claim."""


@dataclass(frozen=True, slots=True)
class _AuditPageRead:
    """A ``PAGE`` row of the audit."""

    scanned_count: int
    completion_ids: tuple[str, ...]
    orphans: tuple[tuple[str, str], ...]
    """The orphan candidates, each with its workflow name, in page order."""
    claim: _AuditClaim | _NoAuditClaim


@dataclass(frozen=True, slots=True)
class _AuditCursorHeld:
    """Another reaper holds the cursor row or its live claim; nothing was written."""


@dataclass(frozen=True, slots=True)
class _AuditWritten:
    """The candidate statement wrote the workflow COMPLETED or FAILED."""

    parent_advance: ParentAdvance


@dataclass(frozen=True, slots=True)
class _AuditUnchanged:
    """The workflow did not change: not RUNNING, gone, a live node, or (case
    4) a node."""


@dataclass(frozen=True, slots=True)
class _AuditClaimLost:
    """The page claim is gone; nothing was done."""


type _AuditAction = _AuditWritten | _AuditUnchanged | _AuditClaimLost


def _decode_failure(function: str, reason: str) -> OutcomeDecodeError:
    return OutcomeDecodeError(f'{function} returned a row outside its contract: {reason}')


def _uuid_text(value: object, function: str, column: str) -> str:
    match value:
        case uuid.UUID():
            return str(value)
        case str():
            try:
                return str(uuid.UUID(value))
            except ValueError:
                raise _decode_failure(function, f'{column} is {value!r}; expected a uuid') from None
        case _:
            raise _decode_failure(function, f'{column} is {value!r}; expected a uuid')


def _array_items(value: object, function: str, column: str) -> Sequence[object]:
    """The items of an array column. The driver gives a list."""
    match value:
        case list():
            return cast(list[object], value)
        case _:
            raise _decode_failure(function, f'{column} is {value!r}; expected an array')


def _uuid_texts(value: object, function: str, column: str) -> tuple[str, ...]:
    return tuple(
        _uuid_text(item, function, column) for item in _array_items(value, function, column)
    )


def _texts(value: object, function: str, column: str) -> tuple[str, ...]:
    texts: list[str] = []
    for item in _array_items(value, function, column):
        match item:
            case str():
                texts.append(item)
            case _:
                raise _decode_failure(function, f'{column} holds {item!r}; expected text')
    return tuple(texts)


def _one_row(rows: Sequence[Any], function: str) -> Any:
    match rows:
        case [row]:
            return row
        case _:
            raise _decode_failure(function, f'{len(rows)} rows; expected one row')


def decode_audit_page(rows: Sequence[Any]) -> _AuditPageRead | _AuditCursorHeld:
    """The row of ``horsies_workflow_recovery_audit_page``. Pure.

    Raises:
        OutcomeDecodeError: a row outside the function's contract.
    """
    function = 'horsies_workflow_recovery_audit_page'
    row = _one_row(rows, function)
    match row.outcome:
        case 'CURSOR_HELD':
            return _AuditCursorHeld()
        case 'PAGE':
            pass
        case other:
            raise _decode_failure(function, f'outcome {other!r}')
    match row.scanned_count:
        case bool():
            raise _decode_failure(function, f'scanned_count is {row.scanned_count!r}')
        case int() as scanned:
            pass
        case other:
            raise _decode_failure(function, f'scanned_count is {other!r}')
    orphan_ids = _uuid_texts(row.orphan_ids, function, 'orphan_ids')
    orphan_names = _texts(row.orphan_names, function, 'orphan_names')
    if len(orphan_ids) != len(orphan_names):
        raise _decode_failure(
            function,
            f'{len(orphan_ids)} orphan ids and {len(orphan_names)} orphan names',
        )
    claim: _AuditClaim | _NoAuditClaim = (
        _NoAuditClaim()
        if row.claim_token is None
        else _AuditClaim(token=_uuid_text(row.claim_token, function, 'claim_token'))
    )
    return _AuditPageRead(
        scanned_count=scanned,
        completion_ids=_uuid_texts(row.completion_ids, function, 'completion_ids'),
        orphans=tuple(zip(orphan_ids, orphan_names, strict=True)),
        claim=claim,
    )


def decode_audit_action(rows: Sequence[Any], function: str) -> _AuditAction:
    """The row of ``horsies_workflow_recover_completion`` or
    ``horsies_workflow_fail_orphaned``. Pure.

    Raises:
        OutcomeDecodeError: a row outside the function's contract.
    """
    row = _one_row(rows, function)
    match row.outcome:
        case 'COMPLETED' | 'FAILED':
            advance = row.advance_child_workflow_id
            return _AuditWritten(
                parent_advance=(
                    NoParentAdvance()
                    if advance is None
                    else ParentAdvanceOwed(
                        child_workflow_id=_uuid_text(
                            advance, function, 'advance_child_workflow_id',
                        ),
                    )
                ),
            )
        case 'UNCHANGED':
            return _AuditUnchanged()
        case 'AUDIT_CLAIM_LOST':
            return _AuditClaimLost()
        case other:
            raise _decode_failure(function, f'outcome {other!r}')


def orphaned_workflow_error(workflow_id: str, name: str) -> str:
    """The stored error of a RUNNING workflow with no node (case 4): its start
    did not finish. Pure apart from serialization.

    Raises:
        SerializationError: the error does not serialize (a defect).
    """
    error = TaskError(
        error_code=OperationalErrorCode.WORKFLOW_START_INCOMPLETE,
        message=(
            f"Orphaned workflow '{name}': no workflow_tasks found. "
            'Workflow task insertion did not complete.'
        ),
        data={'workflow_id': workflow_id, 'recovery': 'case_4'},
    )
    match dumps_json(encode_task_error(error)):
        case Ok(error_json):
            return error_json
        case Err(cause):
            raise SerializationError(f'case 4 error of workflow {workflow_id}: {cause}')


# ---------------------------------------------------------------------------
# One pass
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class _GlobalScope:
    """Every workflow; each case query returns at most ``max_rows`` rows."""

    max_rows: int


@dataclass(frozen=True, slots=True)
class _TreeScope:
    """One workflow tree (the root and its descendants), with no row cap."""

    workflow_ids: tuple[str, ...]


type _Scope = _GlobalScope | _TreeScope


@dataclass(frozen=True, slots=True)
class _Pass:
    """The facts every case of one pass reads."""

    session_factory: 'async_sessionmaker[AsyncSession]'
    broker: 'PostgresBroker'
    scope: _Scope
    failure_policy: RecoveryFailurePolicy


@dataclass(frozen=True, slots=True)
class _CandidateFailed:
    """The candidate raised; its transaction rolled back."""

    record: RaiseRecord


class _ClaimHold(Enum):
    """Whether the pass still holds the audit page claim after a candidate."""

    HELD = 'held'
    LOST = 'lost'


def _elapsed_ms(started: float) -> int:
    return int((time.monotonic() - started) * 1000)


async def _discover(
    pass_: _Pass,
    global_statement: Any,
    scoped_statement: Any,
    metrics: RecoveryCaseMetrics,
    started: float,
) -> list[Any]:
    """The candidate rows of one case, in their own session.

    Raises:
        _CaseCountedFailure: the statement failed; ``metrics`` notes it.
    """
    match pass_.scope:
        case _GlobalScope(max_rows=max_rows):
            statement: Any = global_statement
            parameters: dict[str, object] = {'max_rows': max_rows}
        case _TreeScope(workflow_ids=workflow_ids):
            statement = scoped_statement
            parameters = {'scope_ids': list(workflow_ids), 'max_rows': None}
    try:
        async with pass_.session_factory() as session:
            rows = list((await session.execute(statement, parameters)).fetchall())
    except Exception as error:
        metrics.duration_ms = _elapsed_ms(started)
        metrics.errors += 1
        raise _CaseCountedFailure(error) from error
    metrics.rows_selected = len(rows)
    metrics.candidates_returned = len(rows)
    return rows


async def _record_candidate_raise(
    pass_: _Pass,
    exc: Exception,
    *,
    case: RecoveryCase,
    workflow_id: str,
    task_index: int | None,
) -> RaiseRecord:
    """Decide whether a raise counts, and count it.

    A transient raise and a ``LogFailuresOnly`` pass write nothing.
    """
    match pass_.failure_policy:
        case LogFailuresOnly():
            return RecoveryCandidateOutcome.NOT_COUNTED
        case CountFailures() as counting:
            pass
        case _ as unreachable:
            assert_never(unreachable)
    match classify_recovery_raise(exc):
        case RecoveryRaiseClass.TRANSIENT:
            return RecoveryCandidateOutcome.NOT_COUNTED
        case RecoveryRaiseClass.PERSISTENT:
            return await count_recovery_raise(
                pass_.session_factory,
                counting,
                RecoveryRaise.of(exc, workflow_id=workflow_id, task_index=task_index, case=case),
            )
        case _ as unreachable:
            assert_never(unreachable)


def _note_raise_record(metrics: RecoveryCaseMetrics, record: RaiseRecord) -> None:
    match record:
        case RecoveryCandidateOutcome.NOT_COUNTED:
            pass
        case RecoveryCandidateOutcome.COUNTED:
            metrics.failures_counted += 1
        case RecoveryCandidateOutcome.LIMIT_REACHED:
            metrics.failures_counted += 1
            metrics.limit_reached += 1
        case _ as unreachable:
            assert_never(unreachable)


async def _candidate_failed(
    pass_: _Pass,
    exc: Exception,
    *,
    case: RecoveryCase,
    workflow_id: str,
    task_index: int | None,
    metrics: RecoveryCaseMetrics,
) -> _CandidateFailed:
    """Log a raised candidate with its subject and cause, and apply the failure bound."""
    metrics.errors += 1
    logger.error(
        'Workflow recovery candidate failed: case=%s workflow_id=%s '
        'task_index=%s exception_type=%s exception=%s',
        case.value,
        workflow_id,
        task_index,
        type(exc).__name__,
        str(exc)[:MAX_RAISE_TEXT_LENGTH],
    )
    record = await _record_candidate_raise(
        pass_, exc, case=case, workflow_id=workflow_id, task_index=task_index,
    )
    _note_raise_record(metrics, record)
    return _CandidateFailed(record=record)


async def _run_step_candidate[D: Enum](
    pass_: _Pass,
    *,
    case: RecoveryCase,
    workflow_id: str,
    task_index: int,
    statement: Any,
    parameters: dict[str, object],
    function: str,
    dispositions: type[D],
    metrics: RecoveryCaseMetrics,
) -> CommittedStep[D] | _CandidateFailed:
    """One step candidate in its own transaction with its owed child starts."""
    try:
        async with pass_.session_factory() as session:
            return await run_in_transaction(
                session, pass_.broker, statement, parameters, function, dispositions,
            )
    except Exception as exc:
        return await _candidate_failed(
            pass_, exc, case=case, workflow_id=workflow_id, task_index=task_index,
            metrics=metrics,
        )


async def _advance_after_candidate(
    pass_: _Pass, workflow_id: str, owed: Sequence[str],
) -> None:
    """Advance the parent nodes that a committed candidate owes, one
    transaction per level. A failure is logged and counted nowhere: recovery
    case 1.6 advances the pair next pass and counts it on the parent."""
    if not owed:
        return
    failure = await advance_parents(pass_.broker, pass_.session_factory, owed)
    if failure is not None:
        logger.error(
            'Parent advance after a workflow recovery candidate failed: '
            'workflow_id=%s child_workflow_id=%s exception_type=%s exception=%s; '
            'recovery case 1.6 advances it',
            workflow_id,
            failure.failed_child_workflow_id,
            type(failure.cause).__name__,
            str(failure.cause)[:MAX_RAISE_TEXT_LENGTH],
        )


def _owed_of(advance: ParentAdvance) -> tuple[str, ...]:
    match advance:
        case NoParentAdvance():
            return ()
        case ParentAdvanceOwed(child_workflow_id=child_workflow_id):
            return (child_workflow_id,)


async def _recover_case0(pass_: _Pass, report: RecoveryReport) -> None:
    """Case 0: each PENDING node whose dependencies are all terminal."""
    metrics = report.metrics['case_0']
    started = time.monotonic()
    rows = await _discover(
        pass_,
        GLOBAL_GET_PENDING_WITH_TERMINAL_DEPS_SQL,
        GET_PENDING_WITH_TERMINAL_DEPS_SQL,
        metrics,
        started,
    )
    for row in rows:
        workflow_id = str(row.workflow_id)
        task_index = int(row.task_index)
        step = await _run_step_candidate(
            pass_,
            case=RecoveryCase.PENDING_TERMINAL_DEPS,
            workflow_id=workflow_id,
            task_index=task_index,
            statement=NODE_REEVALUATE_FUNCTION_SQL,
            parameters={'wf_id': workflow_id, 'task_index': task_index},
            function='horsies_workflow_node_reevaluate',
            dispositions=ReevaluateDisposition,
            metrics=metrics,
        )
        match step:
            case _CandidateFailed():
                continue
            case CommittedStep(outcome=outcome, parent_advances=owed):
                pass
        match outcome.disposition:
            case ReevaluateDisposition.APPLIED:
                report.recovered += 1
                logger.info(
                    f'Recovery evaluated stuck PENDING task: workflow={workflow_id}, '
                    f'task_index={task_index}, status={outcome.detail}',
                )
            case (
                ReevaluateDisposition.WAITING
                | ReevaluateDisposition.STALE
                | ReevaluateDisposition.NOT_RUNNING
                | ReevaluateDisposition.NOT_FOUND
            ):
                metrics.refusals += 1
            case _ as unreachable:
                assert_never(unreachable)
        await _advance_after_candidate(pass_, workflow_id, owed)
    metrics.duration_ms = _elapsed_ms(started)


class _ReadyCase(Enum):
    """The kind of READY node that a case 1 or 1.5 candidate repairs."""

    TASK_NODE = 'case_1'
    SUBWORKFLOW_NODE = 'case_1_5'


async def _recover_ready_nodes(
    pass_: _Pass, ready: _ReadyCase, report: RecoveryReport,
) -> None:
    """Cases 1 and 1.5: each READY node with no work, as its own transaction.

    A node whose build fails is written FAILED by the step, and its
    workflow's ``on_error`` applies.
    """
    metrics = report.metrics[ready.value]
    match ready:
        case _ReadyCase.TASK_NODE:
            statements = (GLOBAL_GET_READY_NOT_ENQUEUED_SQL, GET_READY_NOT_ENQUEUED_SQL)
            case = RecoveryCase.READY_NOT_ENQUEUED
        case _ReadyCase.SUBWORKFLOW_NODE:
            statements = (
                GLOBAL_GET_READY_SUBWORKFLOWS_NOT_STARTED_SQL,
                GET_READY_SUBWORKFLOWS_NOT_STARTED_SQL,
            )
            case = RecoveryCase.READY_SUBWORKFLOW_NOT_STARTED
    started = time.monotonic()
    rows = await _discover(pass_, statements[0], statements[1], metrics, started)
    for row in rows:
        workflow_id = str(row.workflow_id)
        task_index = int(row.task_index)
        step = await _run_step_candidate(
            pass_,
            case=case,
            workflow_id=workflow_id,
            task_index=task_index,
            statement=NODES_ENQUEUE_READY_FUNCTION_SQL,
            parameters={'wf_id': workflow_id, 'task_indexes': [task_index]},
            function='horsies_workflow_nodes_enqueue_ready',
            dispositions=EnqueueReadyDisposition,
            metrics=metrics,
        )
        match step:
            case _CandidateFailed():
                continue
            case CommittedStep(outcome=outcome, parent_advances=owed):
                pass
        match outcome.disposition:
            case EnqueueReadyDisposition.APPLIED:
                report.recovered += 1
                logger.info(
                    f'Recovered stuck READY node ({case.value}): '
                    f'workflow={workflow_id}, task_index={task_index}',
                )
            case (
                EnqueueReadyDisposition.STALE
                | EnqueueReadyDisposition.NOT_RUNNING
                | EnqueueReadyDisposition.NOT_FOUND
            ):
                metrics.refusals += 1
            case _ as unreachable:
                assert_never(unreachable)
        await _advance_after_candidate(pass_, workflow_id, owed)
    metrics.duration_ms = _elapsed_ms(started)


async def _recover_case1_6(pass_: _Pass, report: RecoveryReport) -> None:
    """Case 1.6: each terminal child whose RUNNING parent node did not advance.

    A failure counts on the parent workflow.
    """
    metrics = report.metrics['case_1_6']
    started = time.monotonic()
    rows = await _discover(
        pass_,
        GLOBAL_GET_COMPLETED_CHILDREN_NOT_UPDATED_SQL,
        GET_COMPLETED_CHILDREN_NOT_UPDATED_SQL,
        metrics,
        started,
    )
    for row in rows:
        child_id = str(row.id)
        parent_workflow_id = str(row.parent_workflow_id)
        parent_task_index = int(row.parent_task_index)
        step = await _run_step_candidate(
            pass_,
            case=RecoveryCase.COMPLETED_CHILD_PARENT_NOT_UPDATED,
            workflow_id=parent_workflow_id,
            task_index=parent_task_index,
            statement=CHILD_FINISH_FUNCTION_SQL,
            parameters={'child_workflow_id': child_id},
            function='horsies_workflow_child_finish',
            dispositions=ChildFinishDisposition,
            metrics=metrics,
        )
        match step:
            case _CandidateFailed():
                continue
            case CommittedStep(outcome=outcome, parent_advances=owed):
                pass
        match outcome.disposition:
            case ChildFinishDisposition.APPLIED:
                report.recovered += 1
                logger.info(
                    f'Recovered stuck child workflow completion: child={child_id}, '
                    f'parent={parent_workflow_id}:{parent_task_index}',
                )
            case (
                ChildFinishDisposition.ALREADY_APPLIED
                | ChildFinishDisposition.CHILD_ABSENT
                | ChildFinishDisposition.NOT_A_CHILD
                | ChildFinishDisposition.CHILD_NOT_TERMINAL
                | ChildFinishDisposition.PARENT_ABSENT
            ):
                metrics.refusals += 1
            case _ as unreachable:
                assert_never(unreachable)
        await _advance_after_candidate(pass_, parent_workflow_id, owed)
    metrics.duration_ms = _elapsed_ms(started)


async def _run_alone(pass_: _Pass, statement: Any, parameters: dict[str, object]) -> list[Any]:
    """One statement in autocommit on its own session.

    Raises:
        Exception: what the pool and the driver raise; after a lost reply the
            statement may have committed.
    """
    async with pass_.session_factory() as session:
        connection = await autocommit_connection(session)
        return list((await connection.execute(statement, parameters)).fetchall())


async def _recover_completion_candidate(
    pass_: _Pass, workflow_id: str, claim_token: str, report: RecoveryReport,
) -> _ClaimHold:
    """Cases 2/3 of one audit candidate, then the parent advance it owes.

    A raise counts toward the limit on the workflow.
    """
    metrics = report.metrics['case_2_3']
    function = 'horsies_workflow_recover_completion'
    try:
        action = decode_audit_action(
            await _run_alone(
                pass_,
                RECOVER_COMPLETION_FUNCTION_SQL,
                {
                    'wf_id': workflow_id,
                    'claim_token': claim_token,
                    'claim_ttl_ms': GLOBAL_WORKFLOW_AUDIT_CLAIM_TTL_MS,
                },
            ),
            function,
        )
    except Exception as exc:
        _ = await _candidate_failed(
            pass_,
            exc,
            case=RecoveryCase.TERMINAL_WORKFLOW_RUNNING,
            workflow_id=workflow_id,
            task_index=None,
            metrics=metrics,
        )
        return _ClaimHold.HELD
    match action:
        case _AuditWritten(parent_advance=advance):
            report.recovered += 1
            logger.info(f'Recovered terminal workflow via completion check: {workflow_id}')
            await _advance_after_candidate(pass_, workflow_id, _owed_of(advance))
            return _ClaimHold.HELD
        case _AuditUnchanged():
            metrics.refusals += 1
            return _ClaimHold.HELD
        case _AuditClaimLost():
            metrics.refusals += 1
            return _ClaimHold.LOST


async def _recover_orphan_candidate(
    pass_: _Pass, workflow_id: str, name: str, claim_token: str, report: RecoveryReport,
) -> _ClaimHold:
    """Case 4 of one audit candidate, then the parent advance it owes.

    A raise is logged and noted as an error of the case; it does not count
    toward the limit.
    """
    metrics = report.metrics['case_4']
    function = 'horsies_workflow_fail_orphaned'
    try:
        action = decode_audit_action(
            await _run_alone(
                pass_,
                FAIL_ORPHANED_FUNCTION_SQL,
                {
                    'wf_id': workflow_id,
                    'claim_token': claim_token,
                    'claim_ttl_ms': GLOBAL_WORKFLOW_AUDIT_CLAIM_TTL_MS,
                    'error': orphaned_workflow_error(workflow_id, name),
                },
            ),
            function,
        )
    except Exception as failure:
        metrics.errors += 1
        logger.error(
            'Global orphan workflow recovery failed: workflow_id=%s '
            'exception_type=%s exception=%s',
            workflow_id,
            type(failure).__name__,
            str(failure)[:MAX_RAISE_TEXT_LENGTH],
        )
        return _ClaimHold.HELD
    match action:
        case _AuditWritten(parent_advance=advance):
            report.recovered += 1
            logger.warning(
                f"Recovery failed orphaned workflow {workflow_id} ('{name}'): no node rows",
            )
            await _advance_after_candidate(pass_, workflow_id, _owed_of(advance))
            return _ClaimHold.HELD
        case _AuditUnchanged():
            metrics.refusals += 1
            return _ClaimHold.HELD
        case _AuditClaimLost():
            metrics.refusals += 1
            return _ClaimHold.LOST


async def _recover_global_end_states(
    pass_: _Pass, max_rows: int, report: RecoveryReport,
) -> None:
    """Cases 2/3 and 4 of the global pass: one audit page, then one statement
    per candidate that renews the page claim with its work. The claim is
    released at the end while this pass still owns it.

    Raises:
        _CaseCountedFailure: the audit statement failed (both cases note it).
    """
    case_2_3 = report.metrics['case_2_3']
    case_4 = report.metrics['case_4']
    started = time.monotonic()
    try:
        page = decode_audit_page(
            await _run_alone(
                pass_,
                AUDIT_PAGE_FUNCTION_SQL,
                {
                    'max_rows': max_rows,
                    'claim_token': str(uuid.uuid4()),
                    'claim_ttl_ms': GLOBAL_WORKFLOW_AUDIT_CLAIM_TTL_MS,
                },
            ),
        )
    except Exception as error:
        duration_ms = _elapsed_ms(started)
        case_2_3.duration_ms = duration_ms
        case_2_3.errors += 1
        case_4.duration_ms = duration_ms
        case_4.errors += 1
        raise _CaseCountedFailure(error) from error
    duration_ms = _elapsed_ms(started)
    case_2_3.duration_ms = duration_ms
    case_4.duration_ms = duration_ms
    match page:
        case _AuditCursorHeld():
            case_2_3.refusals += 1
            case_4.refusals += 1
            return
        case _AuditPageRead():
            pass
    report.audit = AuditPage(
        scanned=page.scanned_count,
        completion_candidates=len(page.completion_ids),
    )
    case_2_3.rows_selected = page.scanned_count
    case_2_3.candidates_returned = len(page.completion_ids)
    case_4.rows_selected = page.scanned_count
    case_4.candidates_returned = len(page.orphans)
    match page.claim:
        case _NoAuditClaim():
            if page.completion_ids:
                case_2_3.refusals += 1
            if page.orphans:
                case_4.refusals += 1
            return
        case _AuditClaim(token=claim_token):
            pass

    hold = _ClaimHold.HELD
    for workflow_id in page.completion_ids:
        hold = await _recover_completion_candidate(pass_, workflow_id, claim_token, report)
        if hold is _ClaimHold.LOST:
            break
    if hold is _ClaimHold.HELD:
        for workflow_id, name in page.orphans:
            hold = await _recover_orphan_candidate(
                pass_, workflow_id, name, claim_token, report,
            )
            if hold is _ClaimHold.LOST:
                break
    match hold:
        case _ClaimHold.HELD:
            try:
                _ = await _run_alone(
                    pass_,
                    RELEASE_GLOBAL_WORKFLOW_AUDIT_CLAIM_SQL,
                    {'claim_token': claim_token},
                )
            except Exception as error:
                logger.error(f'Workflow audit claim release failed: {error}')
                case_2_3.errors += 1
                case_4.errors += 1
        case _ClaimHold.LOST:
            pass
    duration_ms = _elapsed_ms(started)
    case_2_3.duration_ms = duration_ms
    case_4.duration_ms = duration_ms


@dataclass(frozen=True, slots=True)
class _TreeWritten:
    """The completion check or the orphan write ended the workflow."""

    parent_advance: ParentAdvance


@dataclass(frozen=True, slots=True)
class _TreeUnchanged:
    """The workflow is gone, not RUNNING, or not a candidate any more; nothing written."""


def _tree_written(workflow_id: str, parent: object) -> _TreeWritten:
    return _TreeWritten(
        parent_advance=(
            NoParentAdvance() if parent is None else ParentAdvanceOwed(child_workflow_id=workflow_id)
        ),
    )


async def _complete_in_tree(pass_: _Pass, workflow_id: str) -> _TreeWritten | _TreeUnchanged:
    """The completion check of one workflow in one transaction: the row lock,
    ``horsies_workflow_try_complete``, ``COMMIT``.

    Raises:
        Exception: what the pool and the driver raise; nothing committed, or
            the outcome of the COMMIT is unknown.
    """
    async with pass_.session_factory() as session:
        locked = (
            await session.execute(LOCK_TREE_CANDIDATE_SQL, {'wf_id': workflow_id})
        ).fetchone()
        if locked is None:
            await session.rollback()
            return _TreeUnchanged()
        written = (await session.execute(TRY_COMPLETE_SQL, {'wf_id': workflow_id})).scalar_one()
        await session.commit()
    match written:
        case None:
            return _TreeUnchanged()
        case str():
            return _tree_written(workflow_id, locked.parent_workflow_id)
        case other:
            raise _decode_failure('horsies_workflow_try_complete', f'written is {other!r}')


async def _recover_tree_case2_3(pass_: _Pass, report: RecoveryReport) -> None:
    """Cases 2/3 of the tree pass: each RUNNING workflow of the tree whose
    nodes are all terminal, as its own transaction, then the parent advance
    it owes."""
    metrics = report.metrics['case_2_3']
    started = time.monotonic()
    rows = await _discover(
        pass_,
        GET_TERMINAL_WORKFLOW_CANDIDATES_SQL,
        GET_TERMINAL_WORKFLOW_CANDIDATES_SQL,
        metrics,
        started,
    )
    for row in rows:
        workflow_id = str(row.id)
        try:
            completion = await _complete_in_tree(pass_, workflow_id)
        except Exception as exc:
            _ = await _candidate_failed(
                pass_,
                exc,
                case=RecoveryCase.TERMINAL_WORKFLOW_RUNNING,
                workflow_id=workflow_id,
                task_index=None,
                metrics=metrics,
            )
            continue
        match completion:
            case _TreeWritten(parent_advance=advance):
                report.recovered += 1
                logger.info(f'Recovered terminal workflow via completion check: {workflow_id}')
                await _advance_after_candidate(pass_, workflow_id, _owed_of(advance))
            case _TreeUnchanged():
                metrics.refusals += 1
    metrics.duration_ms = _elapsed_ms(started)


async def _recover_tree_case4(pass_: _Pass, report: RecoveryReport) -> None:
    """Case 4 of the tree pass: each RUNNING workflow of the tree with no node
    goes FAILED, one statement in autocommit, then the parent advance it owes."""
    metrics = report.metrics['case_4']
    started = time.monotonic()
    rows = await _discover(
        pass_,
        GET_ORPHANED_WORKFLOW_CANDIDATES_SQL,
        GET_ORPHANED_WORKFLOW_CANDIDATES_SQL,
        metrics,
        started,
    )
    for row in rows:
        workflow_id = str(row.id)
        name = str(row.name)
        try:
            written = await _run_alone(
                pass_,
                FAIL_TREE_ORPHAN_SQL,
                {'wf_id': workflow_id, 'error': orphaned_workflow_error(workflow_id, name)},
            )
        except Exception as failure:
            metrics.errors += 1
            logger.error(
                'Orphan workflow recovery failed: workflow_id=%s exception_type=%s exception=%s',
                workflow_id,
                type(failure).__name__,
                str(failure)[:MAX_RAISE_TEXT_LENGTH],
            )
            continue
        match written:
            case []:
                metrics.refusals += 1
            case [failed_row]:
                report.recovered += 1
                logger.warning(
                    f"Recovery failed orphaned workflow {workflow_id} ('{name}'): no node rows",
                )
                advance = _tree_written(workflow_id, failed_row.parent_workflow_id).parent_advance
                await _advance_after_candidate(pass_, workflow_id, _owed_of(advance))
            case _:
                raise AssertionError(
                    f'the orphan write of workflow {workflow_id} returned {len(written)} rows',
                )
    metrics.duration_ms = _elapsed_ms(started)


async def _recover_stuck_nodes(pass_: _Pass, report: RecoveryReport) -> None:
    """Cases 0, 1, 1.5 and 1.6, in this order.

    Raises:
        _CaseCountedFailure: a discovery statement failed; the cases before it ran.
    """
    await _recover_case0(pass_, report)
    await _recover_ready_nodes(pass_, _ReadyCase.TASK_NODE, report)
    await _recover_ready_nodes(pass_, _ReadyCase.SUBWORKFLOW_NODE, report)
    await _recover_case1_6(pass_, report)


async def recover_stuck_workflows_global(
    session_factory: 'async_sessionmaker[AsyncSession]',
    broker: 'PostgresBroker',
    *,
    failure_limit: int = DEFAULT_WORKFLOW_RECOVERY_FAILURE_LIMIT,
) -> RecoveryReport:
    """The reaper's global pass, with ``GLOBAL_SCAN_ROW_CAP`` rows per case
    query and per audit page.

    ``failure_limit`` is how many passes with a persistent raise a workflow
    may see before it fails closed. This function makes the one pass id of
    the pass: every case gets the same value, so a workflow counts once per
    pass.

    A candidate's own raise is not a failure of the pass: it is logged, noted
    in its case, and counted toward the limit when it is persistent.

    Raises:
        RecoveryPassFailure: a discovery statement, the audit page or the
            count of workflows over the limit failed, with the partial
            report; the cases before it ran.
    """
    report = RecoveryReport()
    pass_ = _Pass(
        session_factory=session_factory,
        broker=broker,
        scope=_GlobalScope(max_rows=GLOBAL_SCAN_ROW_CAP),
        failure_policy=CountFailures(pass_id=str(uuid.uuid4()), limit=failure_limit),
    )
    try:
        await _recover_stuck_nodes(pass_, report)
        await _recover_global_end_states(pass_, GLOBAL_SCAN_ROW_CAP, report)
        report.over_failure_limit = await count_over_failure_limit(
            session_factory,
            failure_limit,
        )
        return report
    except _CaseCountedFailure as counted:
        raise RecoveryPassFailure(report, counted.error) from counted.error
    except Exception as error:
        report.errors += 1
        raise RecoveryPassFailure(report, error) from error


async def recover_stuck_workflow_tree(
    session_factory: 'async_sessionmaker[AsyncSession]',
    broker: 'PostgresBroker',
    workflow_id: str,
) -> RecoveryReport:
    """The recovery pass of one workflow tree (the workflow and its
    descendants), with no row cap. A resume runs it after its commit. It logs
    candidate raises and counts none: the reaper's global pass counts the
    same candidates.

    Raises:
        Exception: the tree read or a discovery statement raised (the
            driver's exception); the cases before it ran.
    """
    async with session_factory() as session:
        tree = (await session.execute(GET_WORKFLOW_TREE_IDS_SQL, {'wf_id': workflow_id})).fetchall()
    report = RecoveryReport()
    pass_ = _Pass(
        session_factory=session_factory,
        broker=broker,
        scope=_TreeScope(workflow_ids=tuple(str(row.id) for row in tree)),
        failure_policy=LogFailuresOnly(),
    )
    try:
        await _recover_stuck_nodes(pass_, report)
        await _recover_tree_case2_3(pass_, report)
        await _recover_tree_case4(pass_, report)
    except _CaseCountedFailure as counted:
        raise counted.error from counted.error
    return report
