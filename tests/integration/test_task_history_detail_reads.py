"""M10 read primitives against real PostgreSQL.

Detail resolves through the generated staged triple: a live task answers
LIVE, a moved task answers with the full frozen row and its attempt
snapshot decoded, and absence classifies against the published birth
floor — the purged path proven end to end by dropping the leaf a row
lived in. The page, facet, and aggregate surfaces prove window scoping,
and the aggregate's pruning claim is proven from the plan itself: an
out-of-range leaf never appears in EXPLAIN output.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from uuid import UUID

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncConnection

from horsies.core.history.commands import (
    CreateDailyHistoryLeaf,
    DetachExpiredHistoryLeaf,
    DropDetachedHistoryLeaf,
    LeafBounds,
    LeafRef,
)
from horsies.core.history.outcomes import LeafCreated, LeafDropped
from horsies.core.history.partitions.catalog import daily_leaf_name
from horsies.core.history.partitions.manager import (
    create_daily_leaf,
    detach_expired_leaf,
    drop_detached_leaf,
)
from horsies.core.history.reads.aggregates import (
    HistoryStatusAggregate,
    history_status_aggregate_statement,
)
from horsies.core.history.reads.detail import (
    HistoryTaskDetail,
    LiveTaskLocation,
    TaskDetailAbsent,
    read_task_detail,
)
from horsies.core.history.reads.pages import (
    HistoryFacet,
    HistoryFacetQuery,
    HistoryPageQuery,
    HistoryWindow,
    history_facet_statement,
    history_page_statement,
)
from horsies.core.history.reads.publisher import StagedLoaderPublisher

from tests.integration.task_history_harness import (
    HistorySchema,
    insert_live_task,
    prepare_move_storage,
    terminalization_schema_fixture,
)

pytestmark = [pytest.mark.integration]

UTC = timezone.utc
CLASS_KEY = 'it_reads'
WORKER = 'worker-reads-1'
FAILURE_RESULT = '{"error":{"code":"BOOM"}}'

terminalization_schema = terminalization_schema_fixture(
    'task_history_it_detail_reads'
)


def v7_with_birth(birth: datetime) -> str:
    """Craft a v7 identifier whose embedded birth is `birth`."""
    milliseconds = int(birth.timestamp() * 1000)
    raw = bytearray(16)
    raw[0:6] = milliseconds.to_bytes(6, 'big')
    raw[6] = 0x70
    raw[8] = 0x80
    raw[15] = 0x01
    return str(UUID(bytes=bytes(raw)))


async def fail_task(connection: AsyncConnection, task_id: str) -> None:
    outcome = (
        await connection.execute(
            text(
                'SELECT outcome FROM horsies_fail_locked_task('
                'CAST(:task_id AS uuid), :worker, :result, '
                "'BOOM', 'exploded')"
            ),
            {'task_id': task_id, 'worker': WORKER, 'result': FAILURE_RESULT},
        )
    ).one()
    assert outcome.outcome == 'APPLIED'


async def complete_task(connection: AsyncConnection, task_id: str) -> None:
    outcome = (
        await connection.execute(
            text(
                'SELECT outcome FROM horsies_complete_task_fused('
                'CAST(:task_id AS uuid), :worker, NULL, :result, '
                "'task_done', CAST(:task_id AS text))"
            ),
            {
                'task_id': task_id,
                'worker': WORKER,
                'result': '{"ok":true}',
            },
        )
    ).one()
    assert outcome.outcome == 'APPLIED'


def today_window() -> HistoryWindow:
    lower = datetime.now(UTC).replace(hour=0, minute=0, second=0, microsecond=0)
    return HistoryWindow(lower=lower, upper=lower + timedelta(days=1))


class TestDetail:
    @pytest.mark.asyncio
    async def test_moved_task_answers_with_decoded_history_detail(
        self, terminalization_schema: HistorySchema
    ) -> None:
        async with terminalization_schema.engine.begin() as connection:
            await prepare_move_storage(connection, CLASS_KEY)
            task_id = await insert_live_task(
                connection, class_key=CLASS_KEY, worker=WORKER
            )
            # A prior attempt row, as the child runner would have written
            # it; the move must carry it into the snapshot.
            await connection.execute(
                text(
                    'INSERT INTO horsies_task_attempts '
                    '(task_id, attempt, outcome, will_retry, started_at, '
                    'finished_at, error_code, error_message, failed_reason, '
                    'worker_id) VALUES '
                    "(CAST(:task_id AS uuid), 1, 'FAILED', TRUE, "
                    'statement_timestamp(), statement_timestamp(), '
                    "'BOOM', 'exploded once', 'TASK_ERROR', :worker)"
                ),
                {'task_id': task_id, 'worker': WORKER},
            )
            await fail_task(connection, task_id)
            detail = await read_task_detail(connection, task_id=task_id)
            assert isinstance(detail, HistoryTaskDetail)
            assert detail.task_id == task_id
            assert detail.status == 'FAILED'
            assert detail.terminalization_kind == 'FAIL_RUNNING'
            assert detail.retention_class_key == CLASS_KEY
            assert detail.result_payload is not None
            assert detail.result_payload.decode() == FAILURE_RESULT
            assert detail.error_code == 'BOOM'
            assert detail.final_failed_reason == 'exploded'
            assert detail.rerun_input_disposition == 'DECLINED_BY_POLICY'
            assert len(detail.attempts) == 1
            assert detail.attempts[0].outcome == 'FAILED'
            assert detail.attempts[0].error_code == 'BOOM'

    @pytest.mark.asyncio
    async def test_live_task_answers_location_only(
        self, terminalization_schema: HistorySchema
    ) -> None:
        async with terminalization_schema.engine.begin() as connection:
            await prepare_move_storage(connection, CLASS_KEY)
            task_id = await insert_live_task(
                connection, class_key=CLASS_KEY, worker=WORKER
            )
            detail = await read_task_detail(connection, task_id=task_id)
            assert detail == LiveTaskLocation(task_id=task_id)

    @pytest.mark.asyncio
    async def test_unknown_task_with_no_published_floor_is_unclassified(
        self, terminalization_schema: HistorySchema
    ) -> None:
        async with terminalization_schema.engine.begin() as connection:
            await prepare_move_storage(connection, CLASS_KEY)
            ghost = v7_with_birth(datetime.now(UTC))
            detail = await read_task_detail(connection, task_id=ghost)
            assert detail == TaskDetailAbsent(
                task_id=ghost, predates_retained_floor=None
            )

    @pytest.mark.asyncio
    async def test_purged_task_classifies_before_the_retained_floor(
        self, terminalization_schema: HistorySchema
    ) -> None:
        """The full purged path: a history row's leaf is dropped, the
        loader republishes without it, and the same identifier that
        resolved before now answers the typed expired presentation."""
        publisher = StagedLoaderPublisher()
        old_lower = (
            datetime.now(UTC) - timedelta(days=40)
        ).replace(hour=0, minute=0, second=0, microsecond=0)
        async with terminalization_schema.engine.begin() as connection:
            await prepare_move_storage(connection, CLASS_KEY)
            parent = (
                await connection.execute(
                    text(
                        'SELECT finite_parent_name '
                        'FROM horsies_retention_classes '
                        'WHERE class_key = :key'
                    ),
                    {'key': CLASS_KEY},
                )
            ).scalar_one()
            ref = LeafRef(
                leaf_name=daily_leaf_name(parent, old_lower),
                class_key=CLASS_KEY,
                bounds=LeafBounds(
                    lower=old_lower, upper=old_lower + timedelta(days=1)
                ),
            )
            created = await create_daily_leaf(
                connection, CreateDailyHistoryLeaf(leaf=ref), publisher
            )
            assert isinstance(created, LeafCreated)
            # A real moved row, re-anchored into the old leaf: the
            # partition-key UPDATE migrates the row with every gated
            # column intact, so the aged history is genuine.
            seed = await insert_live_task(
                connection, class_key=CLASS_KEY, worker=WORKER
            )
            await fail_task(connection, seed)
            old_task = v7_with_birth(old_lower + timedelta(hours=1))
            await connection.execute(
                text(
                    'UPDATE horsies_task_history '
                    'SET task_id = CAST(:old_task AS uuid), '
                    '    terminal_at = :old_terminal, '
                    '    retention_anchor_at = :old_terminal '
                    'WHERE task_id = CAST(:seed AS uuid)'
                ),
                {
                    'old_task': old_task,
                    'old_terminal': old_lower + timedelta(hours=2),
                    'seed': seed,
                },
            )
            resolved = await read_task_detail(connection, task_id=old_task)
            assert isinstance(resolved, HistoryTaskDetail)

        detached = await detach_expired_leaf(
            terminalization_schema.engine,
            DetachExpiredHistoryLeaf(leaf=ref, quarantine_horizon=None),
            publisher,
        )
        async with terminalization_schema.engine.begin() as connection:
            dropped = await drop_detached_leaf(
                connection, DropDetachedHistoryLeaf(leaf=ref), publisher
            )
            assert dropped == LeafDropped(leaf_name=ref.leaf_name)
            # Retirement recomputes the retained floor; stamp the
            # surviving leaves' birth metadata and republish so the
            # manifest carries a floor the classifier can compare to.
            await connection.execute(
                text(
                    'UPDATE horsies_task_history_leaf_catalog '
                    'SET min_birth_at = statement_timestamp() '
                    'WHERE dropped_at IS NULL'
                )
            )
            await publisher.republish(connection)
            absent = await read_task_detail(connection, task_id=old_task)
            assert absent == TaskDetailAbsent(
                task_id=old_task, predates_retained_floor=True
            )
        assert detached is not None


class TestPagesFacetsAggregates:
    @pytest.mark.asyncio
    async def test_window_scoped_page_facets_and_aggregate(
        self, terminalization_schema: HistorySchema
    ) -> None:
        async with terminalization_schema.engine.begin() as connection:
            await prepare_move_storage(connection, CLASS_KEY)
            failed = await insert_live_task(
                connection, class_key=CLASS_KEY, worker=WORKER
            )
            await fail_task(connection, failed)
            for _ in range(2):
                completed = await insert_live_task(
                    connection, class_key=CLASS_KEY, worker=WORKER
                )
                await complete_task(connection, completed)

            window = today_window()
            page_sql, page_params = history_page_statement(
                HistoryPageQuery(window=window, limit=100)
            )
            rows = (
                await connection.execute(text(page_sql), page_params)
            ).all()
            assert len(rows) == 3
            assert {row.status for row in rows} == {'COMPLETED', 'FAILED'}
            assert not hasattr(rows[0], 'result_payload')

            facet_sql, facet_params = history_facet_statement(
                HistoryFacetQuery(window=window, facet=HistoryFacet.STATUS)
            )
            facets = {
                row.facet_value: row.facet_count
                for row in (
                    await connection.execute(text(facet_sql), facet_params)
                ).all()
            }
            assert facets == {'COMPLETED': 2, 'FAILED': 1}

            aggregate_sql, aggregate_params = (
                history_status_aggregate_statement(
                    HistoryStatusAggregate(window=window)
                )
            )
            counts = {
                (row.status, row.terminalization_kind): row.terminal_count
                for row in (
                    await connection.execute(
                        text(aggregate_sql), aggregate_params
                    )
                ).all()
            }
            assert counts == {
                ('COMPLETED', 'COMPLETE_FUSED'): 2,
                ('FAILED', 'FAIL_RUNNING'): 1,
            }

            yesterday = HistoryWindow(
                lower=window.lower - timedelta(days=1), upper=window.lower
            )
            empty_sql, empty_params = history_page_statement(
                HistoryPageQuery(window=yesterday, limit=100)
            )
            assert (
                await connection.execute(text(empty_sql), empty_params)
            ).all() == []

    @pytest.mark.asyncio
    async def test_aggregate_plan_visits_no_out_of_range_leaf(
        self, terminalization_schema: HistorySchema
    ) -> None:
        """The pruning proof from the plan itself: with literal window
        bounds, plan-time pruning must exclude an out-of-range leaf and
        keep an in-range one."""
        async with terminalization_schema.engine.begin() as connection:
            await prepare_move_storage(connection, CLASS_KEY)
            parent = (
                await connection.execute(
                    text(
                        'SELECT finite_parent_name '
                        'FROM horsies_retention_classes '
                        'WHERE class_key = :key'
                    ),
                    {'key': CLASS_KEY},
                )
            ).scalar_one()
            old_lower = (
                datetime.now(UTC) - timedelta(days=10)
            ).replace(hour=0, minute=0, second=0, microsecond=0)
            out_of_range = LeafRef(
                leaf_name=daily_leaf_name(parent, old_lower),
                class_key=CLASS_KEY,
                bounds=LeafBounds(
                    lower=old_lower, upper=old_lower + timedelta(days=1)
                ),
            )
            created = await create_daily_leaf(
                connection,
                CreateDailyHistoryLeaf(leaf=out_of_range),
                StagedLoaderPublisher(),
            )
            assert isinstance(created, LeafCreated)

            window = today_window()
            sql, _ = history_status_aggregate_statement(
                HistoryStatusAggregate(window=window)
            )
            literal_sql = sql.replace(
                ':window_lower', f"TIMESTAMPTZ '{window.lower.isoformat()}'"
            ).replace(
                ':window_upper', f"TIMESTAMPTZ '{window.upper.isoformat()}'"
            )
            plan = '\n'.join(
                str(row[0])
                for row in (
                    await connection.execute(text(f'EXPLAIN {literal_sql}'))
                ).all()
            )
            assert out_of_range.leaf_name not in plan
            in_range = daily_leaf_name(parent, window.lower)
            assert in_range in plan
