"""The partition health contract: fail before terminalization does.

A terminal transition that finds no covering leaf fails closed and loses no
row, but it is already an incident. Health exists to make that unreachable:
coverage goes red while at least two complete future daily intervals still
remain, which is the window in which create-ahead can run calmly. The same
pass surfaces what else would eventually bite maintenance — nonconformant
leaves, an interrupted detach pinning the parent, a role that cannot run the
DDL its deployment mode expects it to run.

The pass is read-only over the leaf catalog, `pg_catalog`, and the pending
relation. It takes no relation locks on task storage and stays two batched
statements regardless of leaf count, which is what keeps it inside its
2-second budget at 512 attached leaves.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncConnection

from ..commands import CollectPartitionHealth
from ..errors import HistoryContractError, HistoryParentAbsent
from ..names import LEAF_CATALOG, WORKFLOW_PHASE2_PENDING
from ..outcomes import (
    CatalogConflictKind,
    ClassCoverage,
    CoverageBelowFloor,
    DetachAwaitingFinalize,
    HealthFault,
    LeafNonconformant,
    MissingDdlPrivilege,
    PartitionHealthReport,
    RetentionClassAbsent,
)
from .catalog import RetentionClassRow, database_now, read_retention_class

COVERAGE_FLOOR_INTERVALS = 2
"""Complete future daily intervals below which coverage health is red."""


@dataclass(frozen=True, slots=True)
class _LeafSurvey:
    leaf_name: str
    lower_anchor: datetime
    upper_anchor: datetime
    cataloged_bound: str
    relation_exists: bool
    actual_bound: str | None
    id_index_exists: bool
    detach_pending: bool | None
    blocker_count: int


async def collect_partition_health(
    connection: AsyncConnection,
    command: CollectPartitionHealth,
) -> PartitionHealthReport:
    """One health pass over one retention class."""
    now = await database_now(connection)
    retention_class = await read_retention_class(connection, command.class_key)
    match retention_class:
        case None:
            return PartitionHealthReport(
                class_key=command.class_key,
                checked_at=now,
                coverage=None,
                faults=(RetentionClassAbsent(class_key=command.class_key),),
            )
        case RetentionClassRow(duration=None):
            return PartitionHealthReport(
                class_key=command.class_key,
                checked_at=now,
                coverage=None,
                faults=(),
            )
        case RetentionClassRow():
            pass
    duration = retention_class.duration
    parent_name = retention_class.finite_parent_name
    if duration is None or parent_name is None:
        raise AssertionError('finite class invariant enforced by the catalog reader')

    survey = await _survey_attached_leaves(
        connection, class_key=command.class_key, parent_name=parent_name
    )

    faults: list[HealthFault] = []
    attached_count = 0
    coverage_until: datetime | None = None
    complete_future = 0
    detachable = 0
    blocked = 0

    for leaf in survey:
        if not leaf.relation_exists:
            faults.append(
                LeafNonconformant(
                    leaf_name=leaf.leaf_name,
                    kind=CatalogConflictKind.PHYSICAL_NONCONFORMANT,
                    detail='cataloged attached leaf has no relation',
                )
            )
            continue
        match leaf.detach_pending:
            case None:
                faults.append(
                    LeafNonconformant(
                        leaf_name=leaf.leaf_name,
                        kind=CatalogConflictKind.METADATA_MISMATCH,
                        detail=(
                            'catalog believes the leaf is attached but the '
                            'relation is not a partition of its parent'
                        ),
                    )
                )
                continue
            case True:
                faults.append(DetachAwaitingFinalize(leaf_name=leaf.leaf_name))
                continue
            case False:
                pass
        if leaf.actual_bound != leaf.cataloged_bound or not leaf.id_index_exists:
            faults.append(
                LeafNonconformant(
                    leaf_name=leaf.leaf_name,
                    kind=CatalogConflictKind.PHYSICAL_NONCONFORMANT,
                    detail=(
                        'attached leaf bound or task-ID index disagrees '
                        'with catalog'
                    ),
                )
            )
            continue

        attached_count += 1
        if coverage_until is None or leaf.upper_anchor > coverage_until:
            coverage_until = leaf.upper_anchor
        if leaf.lower_anchor >= now:
            complete_future += 1
        if leaf.blocker_count > 0:
            blocked += 1
        elif leaf.upper_anchor + duration <= now:
            detachable += 1

    if complete_future < COVERAGE_FLOOR_INTERVALS:
        faults.append(
            CoverageBelowFloor(
                class_key=command.class_key,
                complete_future_intervals=complete_future,
                coverage_until=coverage_until,
            )
        )

    if command.application_managed:
        privilege_fault = await _check_ddl_privileges(
            connection, parent_name=parent_name
        )
        if privilege_fault is not None:
            faults.append(privilege_fault)

    return PartitionHealthReport(
        class_key=command.class_key,
        checked_at=now,
        coverage=ClassCoverage(
            class_key=command.class_key,
            attached_leaf_count=attached_count,
            coverage_until=coverage_until,
            complete_future_intervals=complete_future,
            detachable_leaf_count=detachable,
            pending_blocked_leaf_count=blocked,
        ),
        faults=tuple(faults),
    )


async def _survey_attached_leaves(
    connection: AsyncConnection,
    *,
    class_key: str,
    parent_name: str,
) -> tuple[_LeafSurvey, ...]:
    """Catalog-vs-database survey of every leaf the catalog calls attached.

    One statement joins the catalog against `pg_catalog` and aggregates
    pending blockers per leaf, so the pass stays flat in statement count as
    the leaf set grows.
    """
    rows = (
        await connection.execute(
            text(
                f"""
                SELECT
                    c.leaf_name,
                    c.lower_anchor,
                    c.upper_anchor,
                    c.partition_bound AS cataloged_bound,
                    to_regclass(c.leaf_name) IS NOT NULL AS relation_exists,
                    (SELECT pg_get_expr(pc.relpartbound, pc.oid)
                     FROM pg_class AS pc
                     WHERE pc.oid = to_regclass(c.leaf_name)) AS actual_bound,
                    to_regclass(c.id_index_name) IS NOT NULL AS id_index_exists,
                    (SELECT i.inhdetachpending
                     FROM pg_inherits AS i
                     WHERE i.inhparent = to_regclass(:parent_name)
                       AND i.inhrelid = to_regclass(c.leaf_name))
                        AS detach_pending,
                    (SELECT count(*)
                     FROM {WORKFLOW_PHASE2_PENDING} AS p
                     WHERE p.recovery_source = 'HISTORY'
                       AND p.history_class = c.class_key
                       AND p.history_anchor >= c.lower_anchor
                       AND p.history_anchor < c.upper_anchor) AS blocker_count
                FROM {LEAF_CATALOG} AS c
                WHERE c.class_key = :class_key
                  AND c.detached_at IS NULL
                  AND c.dropped_at IS NULL
                ORDER BY c.lower_anchor
                """
            ),
            {'class_key': class_key, 'parent_name': parent_name},
        )
    ).all()
    survey: list[_LeafSurvey] = []
    for row in rows:
        detach_pending = row.detach_pending
        if detach_pending is not None and not isinstance(detach_pending, bool):
            raise HistoryContractError('inhdetachpending did not decode as boolean')
        blocker_count = row.blocker_count
        if isinstance(blocker_count, bool) or not isinstance(blocker_count, int):
            raise HistoryContractError('blocker count did not decode as integer')
        survey.append(
            _LeafSurvey(
                leaf_name=str(row.leaf_name),
                lower_anchor=row.lower_anchor,
                upper_anchor=row.upper_anchor,
                cataloged_bound=str(row.cataloged_bound),
                relation_exists=bool(row.relation_exists),
                actual_bound=(
                    str(row.actual_bound) if row.actual_bound is not None else None
                ),
                id_index_exists=bool(row.id_index_exists),
                detach_pending=detach_pending,
                blocker_count=blocker_count,
            )
        )
    return tuple(survey)


async def _check_ddl_privileges(
    connection: AsyncConnection,
    *,
    parent_name: str,
) -> MissingDdlPrivilege | None:
    row = (
        await connection.execute(
            text(
                """
                SELECT
                    has_schema_privilege(
                        current_user, current_schema, 'CREATE'
                    ) AS schema_create,
                    to_regclass(:parent_name) IS NOT NULL AS parent_exists,
                    pg_has_role(
                        current_user,
                        (SELECT relowner FROM pg_class
                         WHERE oid = to_regclass(:parent_name)),
                        'USAGE'
                    ) AS owns_parent
                """
            ),
            {'parent_name': parent_name},
        )
    ).one()
    if not bool(row.parent_exists):
        raise HistoryParentAbsent(
            f'finite history parent {parent_name!r} does not exist'
        )
    schema_create = bool(row.schema_create)
    owns_parent = bool(row.owns_parent)
    if schema_create and owns_parent:
        return None
    return MissingDdlPrivilege(schema_create=schema_create, owns_parent=owns_parent)
