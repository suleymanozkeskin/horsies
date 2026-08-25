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
from datetime import datetime, timedelta

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncConnection

from ..commands import CollectPartitionHealth
from ..errors import HistoryContractError, HistoryParentAbsent
from ..ddl.tables import FOREVER_CLASS_KEY
from ..names import (
    HEARTBEAT_CLASS_KEY,
    LEAF_CATALOG,
    TASK_HISTORY_FOREVER,
    WORKFLOW_PHASE2_PENDING,
)
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
from .catalog import (
    RetentionClassRow,
    database_now,
    execute_with_utc_rendering,
    read_retention_class,
)
from .forever import FOREVER_LEGACY_LEAF

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
    requested_bound_matches: bool
    id_index_conformant: bool
    ordering_index_conformant: bool
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
        case RetentionClassRow(duration=None) if (
            command.class_key == FOREVER_CLASS_KEY
        ):
            duration = None
            parent_name = TASK_HISTORY_FOREVER
        case RetentionClassRow(duration=None):
            return PartitionHealthReport(
                class_key=command.class_key,
                checked_at=now,
                coverage=None,
                faults=(),
            )
        case RetentionClassRow(
            duration=timedelta() as duration,
            finite_parent_name=str() as parent_name,
        ):
            pass
        case RetentionClassRow():
            raise AssertionError(
                'finite class invariant enforced by the catalog reader'
            )
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
        if (
            leaf.actual_bound != leaf.cataloged_bound
            or not leaf.requested_bound_matches
            or not leaf.id_index_conformant
            or not leaf.ordering_index_conformant
        ):
            faults.append(
                LeafNonconformant(
                    leaf_name=leaf.leaf_name,
                    kind=CatalogConflictKind.PHYSICAL_NONCONFORMANT,
                    detail=(
                        'attached leaf bound or required index '
                        'is not conformant'
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
        elif duration is not None and leaf.upper_anchor + duration <= now:
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
    the leaf set grows. The bound capture rides the UTC rendering
    convention, like every other capture that compares against the
    catalog's stored text.
    """
    rows = (
        await execute_with_utc_rendering(
            connection,
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
                    COALESCE(
                        (
                            c.leaf_name = :forever_legacy_leaf
                            AND c.class_key = :forever_class
                        )
                        OR (SELECT pg_get_expr(pc.relpartbound, pc.oid) = format(
                                'FOR VALUES FROM (%L) TO (%L)',
                                c.lower_anchor,
                                c.upper_anchor
                            )
                            FROM pg_class AS pc
                            WHERE pc.oid = to_regclass(c.leaf_name)),
                        FALSE
                    ) AS requested_bound_matches,
                    EXISTS (
                        SELECT 1
                        FROM pg_index AS task_index
                        JOIN pg_class AS index_relation
                          ON index_relation.oid = task_index.indexrelid
                        JOIN pg_am AS access_method
                          ON access_method.oid = index_relation.relam
                        WHERE task_index.indexrelid = to_regclass(c.id_index_name)
                          AND task_index.indrelid = to_regclass(c.leaf_name)
                          AND access_method.amname = 'btree'
                          AND task_index.indisvalid
                          AND task_index.indisready
                          AND task_index.indislive
                          AND NOT task_index.indisunique
                          AND NOT task_index.indisprimary
                          AND NOT task_index.indisexclusion
                          AND task_index.indpred IS NULL
                          AND task_index.indexprs IS NULL
                          AND (
                              (
                                  NOT CAST(:heartbeat_index AS boolean)
                                  AND task_index.indnkeyatts = 1
                                  AND task_index.indnatts = 1
                                  AND task_index.indkey[0] = (
                                      SELECT attribute.attnum
                                      FROM pg_attribute AS attribute
                                      WHERE attribute.attrelid
                                          = to_regclass(c.leaf_name)
                                        AND attribute.attname = 'task_id'
                                  )
                                  AND task_index.indcollation[0] = (
                                      SELECT attribute.attcollation
                                      FROM pg_attribute AS attribute
                                      WHERE attribute.attrelid
                                          = to_regclass(c.leaf_name)
                                        AND attribute.attname = 'task_id'
                                  )
                                  AND pg_get_indexdef(
                                      task_index.indexrelid, 1, false
                                  ) = 'task_id'
                                  AND pg_get_indexdef(task_index.indexrelid)
                                      LIKE '% USING btree (task_id)'
                                  AND task_index.indoption[0] = 0
                              )
                              OR (
                                  CAST(:heartbeat_index AS boolean)
                                  AND task_index.indnkeyatts = 3
                                  AND task_index.indnatts = 3
                                  AND task_index.indkey[0] = (
                                      SELECT attribute.attnum
                                      FROM pg_attribute AS attribute
                                      WHERE attribute.attrelid
                                          = to_regclass(c.leaf_name)
                                        AND attribute.attname = 'task_id'
                                  )
                                  AND task_index.indkey[1] = (
                                      SELECT attribute.attnum
                                      FROM pg_attribute AS attribute
                                      WHERE attribute.attrelid
                                          = to_regclass(c.leaf_name)
                                        AND attribute.attname = 'role'
                                  )
                                  AND task_index.indkey[2] = (
                                      SELECT attribute.attnum
                                      FROM pg_attribute AS attribute
                                      WHERE attribute.attrelid
                                          = to_regclass(c.leaf_name)
                                        AND attribute.attname = 'sent_at'
                                  )
                                  AND task_index.indcollation[0] = (
                                      SELECT attribute.attcollation
                                      FROM pg_attribute AS attribute
                                      WHERE attribute.attrelid
                                          = to_regclass(c.leaf_name)
                                        AND attribute.attname = 'task_id'
                                  )
                                  AND task_index.indcollation[1] = (
                                      SELECT attribute.attcollation
                                      FROM pg_attribute AS attribute
                                      WHERE attribute.attrelid
                                          = to_regclass(c.leaf_name)
                                        AND attribute.attname = 'role'
                                  )
                                  AND task_index.indcollation[2] = (
                                      SELECT attribute.attcollation
                                      FROM pg_attribute AS attribute
                                      WHERE attribute.attrelid
                                          = to_regclass(c.leaf_name)
                                        AND attribute.attname = 'sent_at'
                                  )
                                  AND pg_get_indexdef(
                                      task_index.indexrelid, 1, false
                                  ) = 'task_id'
                                  AND pg_get_indexdef(
                                      task_index.indexrelid, 2, false
                                  ) = 'role'
                                  AND pg_get_indexdef(
                                      task_index.indexrelid, 3, false
                                  ) = 'sent_at'
                                  AND pg_get_indexdef(task_index.indexrelid)
                                      LIKE '% USING btree '
                                           '(task_id, role, sent_at DESC)'
                                  AND task_index.indoption[0] = 0
                                  AND task_index.indoption[1] = 0
                                  AND task_index.indoption[2] = 3
                              )
                          )
                    ) AS id_index_conformant,
                    CAST(:heartbeat_index AS boolean) OR EXISTS (
                        SELECT 1
                        FROM pg_index AS ordering_index
                        JOIN pg_class AS ordering_relation
                          ON ordering_relation.oid = ordering_index.indexrelid
                        JOIN pg_am AS ordering_method
                          ON ordering_method.oid = ordering_relation.relam
                        JOIN pg_attribute AS ordering_attribute
                          ON ordering_attribute.attrelid = ordering_index.indrelid
                         AND ordering_attribute.attnum = ordering_index.indkey[0]
                        WHERE ordering_index.indrelid = to_regclass(c.leaf_name)
                          AND ordering_method.amname = 'btree'
                          AND ordering_index.indisvalid
                          AND ordering_index.indisready
                          AND ordering_index.indislive
                          AND NOT ordering_index.indisunique
                          AND NOT ordering_index.indisprimary
                          AND NOT ordering_index.indisexclusion
                          AND ordering_index.indpred IS NULL
                          AND ordering_index.indexprs IS NULL
                          AND ordering_index.indnkeyatts = 1
                          AND ordering_index.indnatts = 1
                          AND ordering_attribute.attname = 'enqueued_at'
                          AND ordering_index.indcollation[0]
                              = ordering_attribute.attcollation
                          AND pg_get_indexdef(
                              ordering_index.indexrelid, 1, false
                          ) = 'enqueued_at'
                          AND pg_get_indexdef(ordering_index.indexrelid)
                              LIKE '% USING btree (enqueued_at)'
                          AND ordering_index.indoption[0] = 0
                    ) AS ordering_index_conformant,
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
            {
                'class_key': class_key,
                'parent_name': parent_name,
                'forever_legacy_leaf': FOREVER_LEGACY_LEAF,
                'forever_class': FOREVER_CLASS_KEY,
                'heartbeat_index': class_key == HEARTBEAT_CLASS_KEY,
            },
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
                requested_bound_matches=bool(row.requested_bound_matches),
                id_index_conformant=bool(row.id_index_conformant),
                ordering_index_conformant=bool(row.ordering_index_conformant),
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
