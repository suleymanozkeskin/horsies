"""Type pin: a monitoring read takes a page value, not plain integers, and a
``MonitoringWindow``, not the unbounded ``HistoryWindow``.

pyright checks this file through ``pyrightconfig.clean.json``. pytest does not
collect it (no ``test_`` prefix), and nothing calls its functions. Each line
that pyright must refuse carries an ignore comment for its rule. The config
makes an unneeded ignore comment an error, so a read that accepts a plain
``int``, or a limit of another read, fails the check.
"""

from __future__ import annotations

from horsies.core.brokers.postgres import PostgresBroker
from horsies.core.history.reads.pages import HistoryWindow
from horsies.monitoring import (
    BreakdownLimit,
    MonitoringWindow,
    TaskPage,
    WorkflowRunsLimit,
    list_tasks,
    list_workflow_runs,
    task_breakdown,
)


async def a_plain_integer_is_not_a_limit(broker: PostgresBroker) -> None:
    await list_workflow_runs(
        broker,
        name=None,
        status=None,
        limit=30,  # pyright: ignore[reportArgumentType]
    )


async def one_read_refuses_the_limit_of_another(
    broker: PostgresBroker, window: MonitoringWindow,
) -> None:
    await list_workflow_runs(
        broker,
        name=None,
        status=None,
        limit=BreakdownLimit(value=1),  # pyright: ignore[reportArgumentType]
    )
    await task_breakdown(
        broker,
        window=window,
        group_by='queue',
        statuses=[],
        task_names=[],
        queues=[],
        workers=[],
        error_codes=[],
        error_categories=[],
        retried_only=False,
        limit=WorkflowRunsLimit(value=1),  # pyright: ignore[reportArgumentType]
    )


async def a_plain_integer_is_not_a_page(
    broker: PostgresBroker, window: MonitoringWindow,
) -> None:
    await list_tasks(
        broker,
        window=window,
        statuses=[],
        task_names=[],
        queues=[],
        workers=[],
        error_codes=[],
        error_categories=[],
        retried_only=False,
        sort_by='enqueued_at',
        sort_dir='desc',
        page=50,  # pyright: ignore[reportArgumentType]
    )


async def a_page_value_is_accepted(
    broker: PostgresBroker, window: MonitoringWindow,
) -> None:
    await list_tasks(
        broker,
        window=window,
        statuses=[],
        task_names=[],
        queues=[],
        workers=[],
        error_codes=[],
        error_categories=[],
        retried_only=False,
        sort_by='enqueued_at',
        sort_dir='desc',
        page=TaskPage(offset=0, limit=50),
    )


async def a_history_window_is_not_a_monitoring_window(
    broker: PostgresBroker, unbounded: HistoryWindow,
) -> None:
    await list_tasks(
        broker,
        window=unbounded,  # pyright: ignore[reportArgumentType]
        statuses=[],
        task_names=[],
        queues=[],
        workers=[],
        error_codes=[],
        error_categories=[],
        retried_only=False,
        sort_by='enqueued_at',
        sort_dir='desc',
        page=TaskPage(offset=0, limit=50),
    )
