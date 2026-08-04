"""What a terminalization operation reports back, decoded once for both drivers.

Outcomes are data. A guarded transition that matches nothing has not failed —
it has learned something about the row, and the caller needs to know which
thing. Encoding that as an exception would make the async SQLAlchemy adapter
and the sync psycopg adapter decode through their different exception
hierarchies, which is the drift this boundary exists to prevent.

Every operation returns the same row shape, so one decoder serves all of them.
The row carries what the caller could not already know: what the database
assigned, and what it saw under the lock at the moment it decided.

Decoding fails closed. An unknown discriminant, a missing column, an unknown
key in a diagnostic payload — each means the database and this code disagree
about the contract, which is infrastructure failure rather than a task
outcome, and it raises.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from typing import Any, assert_never, cast

from ..types.status import TaskStatus
from .operations import TerminalizationKind


class OutcomeDecodeError(Exception):
    """The returned row does not satisfy the wire contract.

    Never raised for a task outcome — only when the database returned
    something this code cannot interpret, which means one side is running
    against a contract the other does not implement.
    """


class GuardKind(Enum):
    """Which guard's evidence a diagnostic payload carries.

    Absent (a NULL column) means the evidence is claim-shaped and lives in the
    uniform observed columns rather than in a payload.
    """

    DEADLINE = 'DEADLINE'
    STALENESS = 'STALENESS'
    WORKFLOW_STATUS = 'WORKFLOW_STATUS'
    WORKFLOW_LINK_ABSENT = 'WORKFLOW_LINK_ABSENT'
    WORKFLOW_LINK_STATE = 'WORKFLOW_LINK_STATE'
    FOREIGN_TERMINALIZATION = 'FOREIGN_TERMINALIZATION'


# ---------------------------------------------------------------------------
# Observed evidence
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class ObservedTaskState:
    """The locked pre-transition image, for every outcome alike.

    On an applied transition this is what the guarded update matched; on a
    refusal it is what it found instead. One rule rather than a per-outcome
    convention, so a log line means the same thing wherever it came from.
    """

    status: TaskStatus | None
    worker_id: str | None
    claimed_at: datetime | None


@dataclass(frozen=True, slots=True)
class ObservedClaim:
    """Claim-shaped evidence: the fence could not match this row."""

    worker_id: str | None
    claimed_at: datetime | None


@dataclass(frozen=True, slots=True)
class ObservedDeadline:
    """A deadline guard's evidence, as the database evaluated it."""

    good_until: datetime | None
    evaluated_at: datetime


@dataclass(frozen=True, slots=True)
class ObservedStaleness:
    """A staleness guard's evidence, captured in the snapshot that judged it.

    Every value the two arms compared travels together with the instant they
    were compared at, so both comparisons are reconstructible from the log
    exactly — an active finalizer's refusal is explained by `finalizing_at`,
    a live runner's by `last_heartbeat_at`, and `evaluated_at` is the NOW()
    the guard actually used.
    """

    last_heartbeat_at: datetime | None
    started_at: datetime | None
    finalizing_at: datetime | None
    stale_after_seconds: int
    finalizing_stale_after_seconds: int
    evaluated_at: datetime


@dataclass(frozen=True, slots=True)
class ObservedWorkflowState:
    """A workflow-status guard's evidence."""

    workflow_id: str
    workflow_status: str


@dataclass(frozen=True, slots=True)
class ObservedWorkflowLink:
    """A workflow-link guard's evidence; None means the link is gone."""

    node_status: str | None


@dataclass(frozen=True, slots=True)
class ObservedForeignTerminalization:
    """The row is terminal, but another operation put it there.

    Claim-shaped evidence would be all-NULL on a terminal row — precisely
    where the log has to name who won. A committed kind of None is a row
    written before the kind column existed: unknown provenance, never
    inferred.
    """

    observed_status: TaskStatus
    committed_kind: TerminalizationKind | None
    terminal_at: datetime | None


type GuardEvidence = (
    ObservedClaim
    | ObservedDeadline
    | ObservedStaleness
    | ObservedWorkflowState
    | ObservedWorkflowLink
    | ObservedForeignTerminalization
)


# ---------------------------------------------------------------------------
# Outcomes
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class Applied:
    """The transition committed. `terminal_at` and `kind` are the row's now."""

    task_id: str
    ordinality: int | None
    terminal_at: datetime
    kind: TerminalizationKind
    observed: ObservedTaskState


@dataclass(frozen=True, slots=True)
class AlreadyApplied:
    """This operation's own effect was already committed.

    Equivalent kind, not merely equal status: five operations write CANCELLED,
    and only a kind in the same class proves the coupled workflow-node write
    committed too.
    """

    task_id: str
    ordinality: int | None
    terminal_at: datetime
    kind: TerminalizationKind
    observed: ObservedTaskState


@dataclass(frozen=True, slots=True)
class LostClaim:
    """The row is live but this caller's fence cannot match it.

    Includes a row requeued to PENDING, whose claim fields are cleared: the
    generation that held it is gone, which is what the caller must act on.
    """

    task_id: str
    ordinality: int | None
    observed: ObservedTaskState


@dataclass(frozen=True, slots=True)
class SourceStateConflict:
    """The row exists and is not this operation's to end.

    Carries the guard's own evidence, so a refusal is diagnosable from the log
    without re-reading the row — by which time it has moved on.
    """

    task_id: str
    ordinality: int | None
    observed: ObservedTaskState
    evidence: GuardEvidence


@dataclass(frozen=True, slots=True)
class TaskAbsent:
    """No such row. Observed columns are empty because there was nothing to see."""

    task_id: str
    ordinality: int | None


type TerminalizationOutcome = (
    Applied | AlreadyApplied | LostClaim | SourceStateConflict | TaskAbsent
)


# ---------------------------------------------------------------------------
# Decoding
# ---------------------------------------------------------------------------

_ROW_COLUMNS: frozenset[str] = frozenset({
    'task_id',
    'ordinality',
    'outcome',
    'terminal_at',
    'terminalization_kind',
    'observed_status',
    'observed_worker_id',
    'observed_claimed_at',
    'guard_kind',
    'observed_guard',
})

_GUARD_PAYLOAD_KEYS: dict[GuardKind, frozenset[str]] = {
    GuardKind.DEADLINE: frozenset({'good_until', 'evaluated_at'}),
    GuardKind.STALENESS: frozenset({
        'last_heartbeat_at',
        'started_at',
        'finalizing_at',
        'stale_after_seconds',
        'finalizing_stale_after_seconds',
        'evaluated_at',
    }),
    GuardKind.WORKFLOW_STATUS: frozenset({'workflow_id', 'workflow_status'}),
    GuardKind.WORKFLOW_LINK_ABSENT: frozenset(),
    GuardKind.WORKFLOW_LINK_STATE: frozenset({'node_status'}),
    GuardKind.FOREIGN_TERMINALIZATION: frozenset(),
}


def decode_outcome_row(row: Mapping[str, Any]) -> TerminalizationOutcome:
    """Decode one returned row into its typed outcome.

    Both adapters import this. Neither interprets a column itself: a driver
    that decides what a row means is a driver that can decide differently.

    Raises:
        OutcomeDecodeError: the row does not satisfy the wire contract.
    """
    _require_exact_columns(row)
    task_id = _require_str(row, 'task_id')
    ordinality = _optional_int(row, 'ordinality')
    observed = ObservedTaskState(
        status=_optional_status(row, 'observed_status'),
        worker_id=_optional_str(row, 'observed_worker_id'),
        claimed_at=_optional_datetime(row, 'observed_claimed_at'),
    )

    match row['outcome']:
        case 'APPLIED':
            return Applied(
                task_id=task_id,
                ordinality=ordinality,
                terminal_at=_require_datetime(row, 'terminal_at'),
                kind=_require_kind(row),
                observed=observed,
            )
        case 'ALREADY_APPLIED':
            return AlreadyApplied(
                task_id=task_id,
                ordinality=ordinality,
                terminal_at=_require_datetime(row, 'terminal_at'),
                kind=_require_kind(row),
                observed=observed,
            )
        case 'LOST_CLAIM':
            return LostClaim(
                task_id=task_id, ordinality=ordinality, observed=observed,
            )
        case 'SOURCE_STATE_CONFLICT':
            return SourceStateConflict(
                task_id=task_id,
                ordinality=ordinality,
                observed=observed,
                evidence=_decode_evidence(row, observed),
            )
        case 'TASK_ABSENT':
            _require_absent_row_is_empty(row)
            return TaskAbsent(task_id=task_id, ordinality=ordinality)
        case unknown:
            raise OutcomeDecodeError(
                f'unknown outcome {unknown!r}. The database implements an '
                f'outcome this driver does not; refusing to guess which of '
                f'the known ones it resembles.'
            )


def _decode_evidence(
    row: Mapping[str, Any],
    observed: ObservedTaskState,
) -> GuardEvidence:
    """Build the typed diagnostic variant for a conflict."""
    raw_guard_kind = row['guard_kind']
    if raw_guard_kind is None:
        if row['observed_guard'] is not None:
            raise OutcomeDecodeError(
                'observed_guard is populated with no guard_kind to interpret '
                'it by. A payload without its discriminant cannot be decoded.'
            )
        return ObservedClaim(
            worker_id=observed.worker_id, claimed_at=observed.claimed_at,
        )

    guard_kind = _require_guard_kind(raw_guard_kind)
    payload = _require_payload(row, guard_kind)

    match guard_kind:
        case GuardKind.DEADLINE:
            return ObservedDeadline(
                good_until=_optional_datetime(payload, 'good_until'),
                evaluated_at=_require_datetime(payload, 'evaluated_at'),
            )
        case GuardKind.STALENESS:
            return ObservedStaleness(
                last_heartbeat_at=_optional_datetime(payload, 'last_heartbeat_at'),
                started_at=_optional_datetime(payload, 'started_at'),
                finalizing_at=_optional_datetime(payload, 'finalizing_at'),
                stale_after_seconds=_require_int(payload, 'stale_after_seconds'),
                finalizing_stale_after_seconds=_require_int(
                    payload, 'finalizing_stale_after_seconds',
                ),
                evaluated_at=_require_datetime(payload, 'evaluated_at'),
            )
        case GuardKind.WORKFLOW_STATUS:
            return ObservedWorkflowState(
                workflow_id=_require_str(payload, 'workflow_id'),
                workflow_status=_require_str(payload, 'workflow_status'),
            )
        case GuardKind.WORKFLOW_LINK_ABSENT:
            return ObservedWorkflowLink(node_status=None)
        case GuardKind.WORKFLOW_LINK_STATE:
            return ObservedWorkflowLink(
                node_status=_require_str(payload, 'node_status'),
            )
        case GuardKind.FOREIGN_TERMINALIZATION:
            if observed.status is None:
                raise OutcomeDecodeError(
                    'foreign terminalization without an observed status. The '
                    'evidence that another operation won is the row it left.'
                )
            return ObservedForeignTerminalization(
                observed_status=observed.status,
                committed_kind=_optional_kind(row),
                terminal_at=_optional_datetime(row, 'terminal_at'),
            )
        case _ as unreachable:
            assert_never(unreachable)


# ---------------------------------------------------------------------------
# Column and key readers. Each states what it required, so a decode failure
# names the column rather than the type that rejected it.
# ---------------------------------------------------------------------------


def _require_exact_columns(row: Mapping[str, Any]) -> None:
    present = frozenset(row.keys())
    missing = _ROW_COLUMNS - present
    unexpected = present - _ROW_COLUMNS
    if missing or unexpected:
        raise OutcomeDecodeError(
            f'row shape does not match the wire contract. '
            f'missing={sorted(missing)} unexpected={sorted(unexpected)}'
        )


def _require_absent_row_is_empty(row: Mapping[str, Any]) -> None:
    populated = [
        column
        for column in (
            'terminal_at',
            'terminalization_kind',
            'observed_status',
            'observed_worker_id',
            'observed_claimed_at',
            'guard_kind',
            'observed_guard',
        )
        if row[column] is not None
    ]
    if populated:
        raise OutcomeDecodeError(
            f'task-absent row carries observations of a row that does not '
            f'exist: {sorted(populated)}'
        )


def _require_str(source: Mapping[str, Any], key: str) -> str:
    value = source.get(key)
    if not isinstance(value, str):
        raise OutcomeDecodeError(f'{key} must be a string, got {type(value).__name__}')
    return value


def _optional_str(source: Mapping[str, Any], key: str) -> str | None:
    value = source.get(key)
    if value is None:
        return None
    if not isinstance(value, str):
        raise OutcomeDecodeError(f'{key} must be a string, got {type(value).__name__}')
    return value


def _require_int(source: Mapping[str, Any], key: str) -> int:
    value = source.get(key)
    if isinstance(value, bool) or not isinstance(value, int):
        raise OutcomeDecodeError(f'{key} must be an integer, got {type(value).__name__}')
    return value


def _optional_int(source: Mapping[str, Any], key: str) -> int | None:
    if source.get(key) is None:
        return None
    return _require_int(source, key)


def _require_datetime(source: Mapping[str, Any], key: str) -> datetime:
    value = _optional_datetime(source, key)
    if value is None:
        raise OutcomeDecodeError(f'{key} is required and was NULL')
    return value


def _optional_datetime(source: Mapping[str, Any], key: str) -> datetime | None:
    value = source.get(key)
    match value:
        case None:
            return None
        case datetime():
            return value
        case str():
            # jsonb payloads carry timestamps as text; columns arrive typed.
            try:
                return datetime.fromisoformat(value)
            except ValueError as exc:
                raise OutcomeDecodeError(
                    f'{key} is not an ISO-8601 timestamp: {value!r}'
                ) from exc
        case _:
            raise OutcomeDecodeError(
                f'{key} must be a timestamp, got {type(value).__name__}'
            )


def _optional_status(source: Mapping[str, Any], key: str) -> TaskStatus | None:
    raw = _optional_str(source, key)
    if raw is None:
        return None
    try:
        return TaskStatus(raw)
    except ValueError as exc:
        raise OutcomeDecodeError(f'{key} is not a task status: {raw!r}') from exc


def _optional_kind(row: Mapping[str, Any]) -> TerminalizationKind | None:
    raw = _optional_str(row, 'terminalization_kind')
    if raw is None:
        return None
    try:
        return TerminalizationKind(raw)
    except ValueError as exc:
        raise OutcomeDecodeError(
            f'unknown terminalization kind {raw!r}. A kind this driver does '
            f'not know cannot be placed in an equivalence class, so its '
            f'provenance cannot be judged.'
        ) from exc


def _require_kind(row: Mapping[str, Any]) -> TerminalizationKind:
    kind = _optional_kind(row)
    if kind is None:
        raise OutcomeDecodeError(
            'an applied transition returned no terminalization kind. Every '
            'function writes its own; a NULL here means the row was not '
            'written by one of them.'
        )
    return kind


def _require_guard_kind(raw: Any) -> GuardKind:
    if not isinstance(raw, str):
        raise OutcomeDecodeError(
            f'guard_kind must be a string, got {type(raw).__name__}'
        )
    try:
        return GuardKind(raw)
    except ValueError as exc:
        raise OutcomeDecodeError(f'unknown guard_kind {raw!r}') from exc


def _require_payload(
    row: Mapping[str, Any],
    guard_kind: GuardKind,
) -> Mapping[str, Any]:
    """The diagnostic payload, checked against exactly the keys it must carry.

    Both directions are errors: a missing key leaves the variant unbuildable,
    and an unknown key means the function is sending evidence this decoder
    would silently drop.
    """
    required = _GUARD_PAYLOAD_KEYS[guard_kind]
    raw = row['observed_guard']
    if not required:
        if raw is not None:
            raise OutcomeDecodeError(
                f'{guard_kind.value} carries its evidence in the uniform '
                f'columns and must not send a payload'
            )
        return {}
    if not isinstance(raw, dict):
        raise OutcomeDecodeError(
            f'{guard_kind.value} requires a payload object, got '
            f'{type(raw).__name__}'
        )
    # jsonb arrives untyped from both drivers; the key check below is what
    # makes it safe to read, so the annotation states the shape it must have.
    payload: dict[str, Any] = cast('dict[str, Any]', raw)
    present = frozenset(str(key) for key in payload)
    missing = required - present
    unexpected = present - required
    if missing or unexpected:
        raise OutcomeDecodeError(
            f'{guard_kind.value} payload does not match its documented keys. '
            f'missing={sorted(missing)} unexpected={sorted(unexpected)}'
        )
    return payload
