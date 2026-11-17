"""The operation table of the monitoring and web switch.

One row per Python entry point of ``horsies.monitoring`` and ``horsies.web``
that calls the core. Every read runs in pool mode: one statement in
autocommit on the core's pool. Run:
``pytest tests/integration --dual-run=monitoring-and-web``.
"""

from __future__ import annotations

from typing import Final

from tests.dualrun.operations.table import DualRunOperation, PoolMode

MONITORING_AND_WEB_OPERATIONS: Final[tuple[DualRunOperation, ...]] = (
    DualRunOperation(
        name='monitoring window',
        production_path='horsies.monitoring.history_window:resolve_monitoring_window',
        reference_path='tests.dualrun.reference.monitoring_window:resolve_monitoring_window',
        mode=PoolMode(),
        core_methods=(),
        core_functions=('resolve_monitoring_window',),
    ),
    DualRunOperation(
        name='monitoring task list',
        production_path='horsies.monitoring.queries:list_tasks',
        reference_path='tests.dualrun.reference.monitoring_queries:list_tasks',
        mode=PoolMode(),
        core_methods=('monitoring_list_tasks_async',),
    ),
    DualRunOperation(
        name='monitoring task detail',
        production_path='horsies.monitoring.queries:get_task_detail',
        reference_path='tests.dualrun.reference.monitoring_queries:get_task_detail',
        mode=PoolMode(),
        core_methods=('monitoring_task_detail_async',),
    ),
    DualRunOperation(
        name='monitoring task facets',
        production_path='horsies.monitoring.queries:task_facets',
        reference_path='tests.dualrun.reference.monitoring_queries:task_facets',
        mode=PoolMode(),
        core_methods=('monitoring_task_facets_async',),
    ),
    DualRunOperation(
        name='monitoring task stats',
        production_path='horsies.monitoring.queries:task_stats',
        reference_path='tests.dualrun.reference.monitoring_queries:task_stats',
        mode=PoolMode(),
        core_methods=('monitoring_task_stats_async',),
    ),
    DualRunOperation(
        name='monitoring task breakdown',
        production_path='horsies.monitoring.queries:task_breakdown',
        reference_path='tests.dualrun.reference.monitoring_queries:task_breakdown',
        mode=PoolMode(),
        core_methods=('monitoring_task_breakdown_async',),
    ),
    DualRunOperation(
        name='monitoring workflow names',
        production_path='horsies.monitoring.queries:list_workflow_names',
        reference_path='tests.dualrun.reference.monitoring_queries:list_workflow_names',
        mode=PoolMode(),
        core_methods=('monitoring_workflow_names_async',),
    ),
    DualRunOperation(
        name='monitoring workflow runs',
        production_path='horsies.monitoring.queries:list_workflow_runs',
        reference_path='tests.dualrun.reference.monitoring_queries:list_workflow_runs',
        mode=PoolMode(),
        core_methods=('monitoring_workflow_runs_async',),
    ),
    DualRunOperation(
        name='monitoring workflow run',
        production_path='horsies.monitoring.queries:get_workflow_run',
        reference_path='tests.dualrun.reference.monitoring_queries:get_workflow_run',
        mode=PoolMode(),
        core_methods=('monitoring_workflow_run_async',),
    ),
    DualRunOperation(
        name='monitoring workflow node',
        production_path='horsies.monitoring.queries:get_workflow_node',
        reference_path='tests.dualrun.reference.monitoring_queries:get_workflow_node',
        mode=PoolMode(),
        core_methods=('monitoring_workflow_node_async',),
    ),
    DualRunOperation(
        name='monitoring schedules',
        production_path='horsies.monitoring.queries:list_schedules',
        reference_path='tests.dualrun.reference.monitoring_queries:list_schedules',
        mode=PoolMode(),
        core_methods=('monitoring_schedules_async',),
    ),
    DualRunOperation(
        name='stale task list',
        production_path='horsies.core.brokers.postgres:PostgresBroker.get_stale_tasks',
        reference_path='tests.dualrun.reference.broker_monitoring:get_stale_tasks',
        mode=PoolMode(),
        core_methods=('stale_tasks_async',),
    ),
    DualRunOperation(
        name='expired task list',
        production_path='horsies.core.brokers.postgres:PostgresBroker.get_expired_tasks',
        reference_path='tests.dualrun.reference.broker_monitoring:get_expired_tasks',
        mode=PoolMode(),
        core_methods=('expired_tasks_async',),
    ),
)
