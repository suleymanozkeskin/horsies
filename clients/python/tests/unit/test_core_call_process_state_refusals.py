# pyright: reportPrivateUsage=false
"""A process-state refusal of the core raises from every core call.

``ForkedAfterCoreStart`` (this process was forked without exec after the
core started) and ``CoreStopped`` (a core background task panicked) are
states of the process, not of one call. Each call family below raises them
unchanged after one call of the core handle; none answers them as its
failure code. The sends, the retry methods and ``PostgresBroker.enqueue``
are in ``test_send_process_state_refusals.py``. The core handle is a
stand-in; touches no database.
"""

from __future__ import annotations

import asyncio
import uuid
from collections.abc import Callable, Generator, Iterator
from contextlib import contextmanager
from datetime import UTC, datetime, timedelta
from typing import Any, Final
from unittest import mock
from unittest.mock import AsyncMock, MagicMock

import pytest
from pydantic import SecretStr

from horsies._core import CoreError, CoreStopped, ForkedAfterCoreStart
from horsies.core.app import Horsies
from horsies.core.brokers.postgres import PostgresBroker
from horsies.core.models.app import AppConfig
from horsies.core.models.broker import PostgresConfig
from horsies.core.models.queues import QueueMode
from horsies.core.models.tasks import TaskError
from horsies.core.models.workflow import NodeKey, TaskNode, WorkflowHandle, WorkflowSpec
from horsies.core.types.result import Ok, Result
from horsies.core.workflows.lifecycle import (
    pause_workflow,
    pause_workflow_sync,
    resume_workflow,
    resume_workflow_sync,
    start_workflow,
    start_workflow_async,
)
from horsies.monitoring import (
    BreakdownLimit,
    MonitoringWindow,
    TaskPage,
    WorkflowRunsLimit,
    cancel_task,
    get_task_detail,
    get_workflow_node,
    get_workflow_run,
    list_schedules,
    list_tasks,
    list_workflow_names,
    list_workflow_runs,
    task_breakdown,
    task_facets,
    task_stats,
)
from horsies.web.schema import SchemaProbe

pytestmark = [pytest.mark.unit]

_URL: Final = 'postgresql://u:p@127.0.0.1:1/unused'
_TASK_ID: Final = uuid.UUID('0198c0de-0000-7000-8000-0000000019b1')
_WORKFLOW_ID: Final = '0198c0de-0000-7000-8000-0000000019b2'
_WORKER_ID: Final = 'worker-1'
_UPPER: Final = datetime(2026, 11, 18, 12, 0, tzinfo=UTC)
_WINDOW: Final = MonitoringWindow(lower=_UPPER - timedelta(days=1), upper=_UPPER)

_APP: Final = Horsies(
    AppConfig(
        queue_mode=QueueMode.DEFAULT,
        broker=PostgresConfig(database_url=SecretStr(_URL)),
    ),
)


@_APP.task(task_name='refusals.value')
def value_task(*, value: int) -> Result[int, TaskError]:
    return Ok(value)


def _refusals() -> list[Any]:
    """One fresh instance of each process-state refusal, per parametrize."""
    return [
        pytest.param(ForkedAfterCoreStart(100, 101, 'use spawn or forkserver'), id='forked'),
        pytest.param(CoreStopped('log forwarder', 'planned panic'), id='stopped'),
    ]


def _core_call(method: str, refusal: CoreError) -> MagicMock:
    """A core handle method that raises ``refusal``: awaitable for a name
    that ends with ``_async``."""
    match method.endswith('_async'):
        case True:
            return AsyncMock(side_effect=refusal)
        case False:
            return MagicMock(side_effect=refusal)


def _calls_of(call: MagicMock) -> int:
    """How often the code under test called or awaited ``call``."""
    match call:
        case AsyncMock():
            return call.await_count
        case _:
            return call.call_count


@pytest.fixture
def broker() -> Iterator[PostgresBroker]:
    """A broker with a stand-in core handle."""
    built = PostgresBroker(PostgresConfig(database_url=SecretStr(_URL)))
    stand_in: Any = MagicMock()
    built._core_handle = stand_in
    yield built


def _raising(broker: PostgresBroker, method: str, refusal: CoreError) -> MagicMock:
    """Make ``method`` of the broker's core handle raise ``refusal``."""
    call = _core_call(method, refusal)
    setattr(broker._core_handle, method, call)
    return call


def _assert_raises_once(
    refusal: CoreError, call: MagicMock, run: Callable[[], object],
) -> None:
    """``run`` raises ``refusal`` itself, after one call of ``call``."""
    with pytest.raises(type(refusal)) as raised:
        _ = run()

    assert raised.value is refusal
    assert _calls_of(call) == 1


BrokerCall = Callable[[PostgresBroker], object]

_TASK_READS: Final[dict[str, tuple[str, BrokerCall]]] = {
    'get_task_info_async': (
        'task_info_async', lambda b: asyncio.run(b.get_task_info_async(_TASK_ID)),
    ),
    'get_task_info': ('task_info', lambda b: b.get_task_info(_TASK_ID)),
    'get_raw_result_record_async': (
        'raw_result_record_async', lambda b: asyncio.run(b.get_raw_result_record_async(_TASK_ID)),
    ),
    'get_raw_result_record': ('raw_result_record', lambda b: b.get_raw_result_record(_TASK_ID)),
}

_HEALTH_READS: Final[dict[str, tuple[str, BrokerCall]]] = {
    'ping_database_async': ('ping_database_async', lambda b: asyncio.run(b.ping_database_async())),
    'ping_database_blocking': ('ping_database', lambda b: b._ping_database_blocking()),
    'ping_workers_async': ('ping_workers_async', lambda b: asyncio.run(b.ping_workers_async())),
    'list_worker_states_async': (
        'worker_states_async', lambda b: asyncio.run(b.list_worker_states_async()),
    ),
    'get_worker_state_async': (
        'worker_state_async', lambda b: asyncio.run(b.get_worker_state_async(_WORKER_ID)),
    ),
    'get_worker_state_history_async': (
        'worker_state_history_async',
        lambda b: asyncio.run(b.get_worker_state_history_async(_WORKER_ID)),
    ),
    'get_class_coverage_async': (
        'class_coverage_async', lambda b: asyncio.run(b.get_class_coverage_async()),
    ),
    'get_class_report_async': (
        'class_report_async', lambda b: asyncio.run(b.get_class_report_async()),
    ),
    'ping_workers_blocking': (
        'ping_workers',
        lambda b: b._ping_workers_blocking(
            target_worker_id=None, timeout_seconds=2.0, min_responses=None,
        ),
    ),
    'list_worker_states_blocking': ('worker_states', lambda b: b._list_worker_states_blocking()),
    'get_worker_state_blocking': (
        'worker_state', lambda b: b._get_worker_state_blocking(_WORKER_ID),
    ),
    'get_worker_state_history_blocking': (
        'worker_state_history',
        lambda b: b._get_worker_state_history_blocking(_WORKER_ID, limit=None),
    ),
    'get_class_coverage_blocking': ('class_coverage', lambda b: b._get_class_coverage_blocking()),
    'get_class_report_blocking': ('class_report', lambda b: b._get_class_report_blocking()),
}

_TASK_LISTS_AND_PASSES: Final[dict[str, tuple[str, BrokerCall]]] = {
    'get_stale_tasks': ('stale_tasks_async', lambda b: asyncio.run(b.get_stale_tasks())),
    'get_expired_tasks': ('expired_tasks_async', lambda b: asyncio.run(b.get_expired_tasks())),
    'mark_stale_tasks_as_failed': (
        'mark_stale_running_async', lambda b: asyncio.run(b.mark_stale_tasks_as_failed()),
    ),
    'expire_pending_tasks': (
        'expire_pending_tasks_async', lambda b: asyncio.run(b.expire_pending_tasks()),
    ),
    'requeue_stale_claimed': (
        'requeue_stale_claimed_async', lambda b: asyncio.run(b.requeue_stale_claimed()),
    ),
    'audit_orphaned_workflow_tasks': (
        'audit_orphaned_workflow_tasks_async',
        lambda b: asyncio.run(b.audit_orphaned_workflow_tasks()),
    ),
    'terminate_orphaned_workflow_tasks': (
        'audit_orphaned_workflow_tasks_async',
        lambda b: asyncio.run(b.terminate_orphaned_workflow_tasks()),
    ),
}


class TestBrokerTaskReads:
    @pytest.mark.parametrize('form', list(_TASK_READS))
    @pytest.mark.parametrize('refusal', _refusals())
    def test_a_task_read_raises_the_refusal(
        self, broker: PostgresBroker, form: str, refusal: CoreError,
    ) -> None:
        method, run = _TASK_READS[form]
        call = _raising(broker, method, refusal)

        _assert_raises_once(refusal, call, lambda: run(broker))


class TestBrokerHealthReads:
    @pytest.mark.parametrize('form', list(_HEALTH_READS))
    @pytest.mark.parametrize('refusal', _refusals())
    def test_a_health_read_raises_the_refusal(
        self, broker: PostgresBroker, form: str, refusal: CoreError,
    ) -> None:
        method, run = _HEALTH_READS[form]
        call = _raising(broker, method, refusal)

        _assert_raises_once(refusal, call, lambda: run(broker))


class TestBrokerTaskListsAndPasses:
    @pytest.mark.parametrize('form', list(_TASK_LISTS_AND_PASSES))
    @pytest.mark.parametrize('refusal', _refusals())
    def test_a_task_list_or_a_pass_raises_the_refusal(
        self, broker: PostgresBroker, form: str, refusal: CoreError,
    ) -> None:
        method, run = _TASK_LISTS_AND_PASSES[form]
        call = _raising(broker, method, refusal)

        _assert_raises_once(refusal, call, lambda: run(broker))


class TestBrokerSchemaInstall:
    @pytest.mark.parametrize('refusal', _refusals())
    def test_the_schema_install_raises_the_refusal(
        self, broker: PostgresBroker, refusal: CoreError,
    ) -> None:
        call = _raising(broker, 'install_schema_async', refusal)

        _assert_raises_once(
            refusal, call, lambda: asyncio.run(broker.ensure_schema_initialized()),
        )
        assert broker._initialized is False


class TestBrokerClose:
    @pytest.mark.parametrize('form', ['close', 'close_async'])
    @pytest.mark.parametrize('refusal', _refusals())
    def test_the_close_raises_the_refusal(
        self, broker: PostgresBroker, form: str, refusal: CoreError,
    ) -> None:
        call = _raising(broker, form, refusal)

        match form:
            case 'close':
                _assert_raises_once(refusal, call, broker.close)
            case 'close_async':
                _assert_raises_once(refusal, call, lambda: asyncio.run(broker.close_async()))
            case _:
                raise AssertionError(f'unknown form: {form}')


class _LiveCheckHandle:
    """The core handle of a live check: the connectivity step passes, and
    ``raising`` names the step that raises ``refusal``."""

    built: list[_LiveCheckHandle] = []
    raising: str = 'check_connectivity'
    refusal: BaseException = RuntimeError('set by the test')

    def __init__(self, manifest_json: str, *, pool_profile: str) -> None:
        _ = (manifest_json, pool_profile)
        self.calls: list[str] = []
        _LiveCheckHandle.built.append(self)

    def _step(self, step: str, answer: object) -> object:
        self.calls.append(step)
        match step == _LiveCheckHandle.raising:
            case True:
                raise _LiveCheckHandle.refusal
            case False:
                return answer

    def check_connectivity(self) -> object:
        return self._step('check_connectivity', _ConnectivityPassed())

    def check_worker_connections(self, profiles_json: str) -> object:
        _ = profiles_json
        return self._step('check_worker_connections', [])

    def close(self) -> object:
        return self._step('close', None)


class _ConnectivityPassed:
    """Stands for ``horsies._core.ConnectivityPassed``."""


@contextmanager
def _live_check(
    raising: str, refusal: CoreError,
) -> Generator[list[_LiveCheckHandle]]:
    _LiveCheckHandle.built = []
    _LiveCheckHandle.raising = raising
    _LiveCheckHandle.refusal = refusal
    with (
        mock.patch('horsies.core.app.CoreHandle', _LiveCheckHandle),
        mock.patch('horsies.core.app.ConnectivityPassed', _ConnectivityPassed),
    ):
        yield _LiveCheckHandle.built


class TestAppLiveCheck:
    @pytest.mark.parametrize(
        'step', ['check_connectivity', 'check_worker_connections', 'close'],
    )
    @pytest.mark.parametrize('refusal', _refusals())
    def test_a_live_step_raises_the_refusal(self, step: str, refusal: CoreError) -> None:
        app = Horsies(AppConfig(broker=PostgresConfig(database_url=SecretStr(_URL))))

        with _live_check(step, refusal) as built:
            with pytest.raises(type(refusal)) as raised:
                _ = app.check(live=True)

        assert raised.value is refusal
        [handle] = built
        assert handle.calls.count(step) == 1


def _spec() -> WorkflowSpec[int]:
    node = TaskNode(fn=value_task, kwargs={'value': 1})
    return _APP.workflow('refusals.flow', [node], output=node, definition_key='refusals.flow.v1')


class TestWorkflowStart:
    @pytest.mark.parametrize('form', ['start_workflow_async', 'start_workflow'])
    @pytest.mark.parametrize('refusal', _refusals())
    def test_a_start_raises_the_refusal(
        self, broker: PostgresBroker, form: str, refusal: CoreError,
    ) -> None:
        call = _raising(broker, form, refusal)

        match form:
            case 'start_workflow_async':
                _assert_raises_once(
                    refusal,
                    call,
                    lambda: asyncio.run(start_workflow_async(_spec(), broker, _WORKFLOW_ID)),
                )
            case 'start_workflow':
                _assert_raises_once(
                    refusal, call, lambda: start_workflow(_spec(), broker, _WORKFLOW_ID),
                )
            case _:
                raise AssertionError(f'unknown form: {form}')


HandleCall = Callable[[WorkflowHandle[Any]], object]

_HANDLE_CALLS: Final[dict[str, tuple[str, HandleCall]]] = {
    'status_async': ('workflow_status_async', lambda h: asyncio.run(h.status_async())),
    'status': ('workflow_status', lambda h: h.status()),
    'get_async': ('wait_workflow_result_async', lambda h: asyncio.run(h.get_async())),
    'get': ('wait_workflow_result', lambda h: h.get()),
    'result_for_async': (
        'workflow_node_result_async',
        lambda h: asyncio.run(h.result_for_async(NodeKey[int](node_id='first'))),
    ),
    'results_async': ('workflow_node_results_async', lambda h: asyncio.run(h.results_async())),
    'tasks_async': ('workflow_nodes_async', lambda h: asyncio.run(h.tasks_async())),
    'cancel_async': ('cancel_workflow_async', lambda h: asyncio.run(h.cancel_async())),
    'pause_async': ('pause_workflow_async', lambda h: asyncio.run(h.pause_async())),
    'resume_async': ('resume_workflow_async', lambda h: asyncio.run(h.resume_async())),
    'result_for': (
        'workflow_node_result', lambda h: h.result_for(NodeKey[int](node_id='first')),
    ),
    'results': ('workflow_node_results', lambda h: h.results()),
    'tasks': ('workflow_nodes', lambda h: h.tasks()),
    'cancel': ('cancel_workflow', lambda h: h.cancel()),
    'pause': ('pause_workflow', lambda h: h.pause()),
    'resume': ('resume_workflow', lambda h: h.resume()),
}


class TestWorkflowHandle:
    @pytest.mark.parametrize('form', list(_HANDLE_CALLS))
    @pytest.mark.parametrize('refusal', _refusals())
    def test_a_handle_call_raises_the_refusal(
        self, broker: PostgresBroker, form: str, refusal: CoreError,
    ) -> None:
        method, run = _HANDLE_CALLS[form]
        call = _raising(broker, method, refusal)
        handle: WorkflowHandle[Any] = WorkflowHandle(workflow_id=uuid.UUID(_WORKFLOW_ID), broker=broker)

        _assert_raises_once(refusal, call, lambda: run(handle))


_LIFECYCLE_CALLS: Final[dict[str, tuple[str, BrokerCall]]] = {
    'pause_workflow': (
        'pause_workflow_async', lambda b: asyncio.run(pause_workflow(b, uuid.UUID(_WORKFLOW_ID))),
    ),
    'pause_workflow_sync': ('pause_workflow', lambda b: pause_workflow_sync(b, uuid.UUID(_WORKFLOW_ID))),
    'resume_workflow': (
        'resume_workflow_async', lambda b: asyncio.run(resume_workflow(b, uuid.UUID(_WORKFLOW_ID))),
    ),
    'resume_workflow_sync': ('resume_workflow', lambda b: resume_workflow_sync(b, uuid.UUID(_WORKFLOW_ID))),
}


class TestWorkflowLifecycle:
    @pytest.mark.parametrize('form', list(_LIFECYCLE_CALLS))
    @pytest.mark.parametrize('refusal', _refusals())
    def test_a_pause_or_resume_raises_the_refusal(
        self, broker: PostgresBroker, form: str, refusal: CoreError,
    ) -> None:
        method, run = _LIFECYCLE_CALLS[form]
        call = _raising(broker, method, refusal)

        _assert_raises_once(refusal, call, lambda: run(broker))


_MONITORING_READS: Final[dict[str, tuple[str, BrokerCall]]] = {
    'task_stats': (
        'monitoring_task_stats_async',
        lambda b: asyncio.run(task_stats(
            b,
            window=_WINDOW,
            task_names=[],
            queues=[],
            workers=[],
            error_codes=[],
            error_categories=[],
            retried_only=False,
        )),
    ),
    'task_facets': (
        'monitoring_task_facets_async',
        lambda b: asyncio.run(task_facets(
            b, window=_WINDOW, statuses=[], error_categories=[], retried_only=False,
        )),
    ),
    'task_breakdown': (
        'monitoring_task_breakdown_async',
        lambda b: asyncio.run(task_breakdown(
            b,
            window=_WINDOW,
            group_by='worker',
            statuses=[],
            task_names=[],
            queues=[],
            workers=[],
            error_codes=[],
            error_categories=[],
            retried_only=False,
            limit=BreakdownLimit(value=50),
        )),
    ),
    'list_tasks': (
        'monitoring_list_tasks_async',
        lambda b: asyncio.run(list_tasks(
            b,
            window=_WINDOW,
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
        )),
    ),
    'get_task_detail': (
        'monitoring_task_detail_async', lambda b: asyncio.run(get_task_detail(b, _TASK_ID)),
    ),
    'list_workflow_names': (
        'monitoring_workflow_names_async', lambda b: asyncio.run(list_workflow_names(b)),
    ),
    'list_workflow_runs': (
        'monitoring_workflow_runs_async',
        lambda b: asyncio.run(list_workflow_runs(
            b, name=None, status=None, limit=WorkflowRunsLimit(value=30),
        )),
    ),
    'get_workflow_run': (
        'monitoring_workflow_run_async', lambda b: asyncio.run(get_workflow_run(b, _WORKFLOW_ID)),
    ),
    'get_workflow_node': (
        'monitoring_workflow_node_async',
        lambda b: asyncio.run(get_workflow_node(b, _WORKFLOW_ID, 0)),
    ),
    'list_schedules': ('monitoring_schedules_async', lambda b: asyncio.run(list_schedules(b))),
}


class TestMonitoringReads:
    @pytest.mark.parametrize('form', list(_MONITORING_READS))
    @pytest.mark.parametrize('refusal', _refusals())
    def test_a_monitoring_read_raises_the_refusal(
        self, broker: PostgresBroker, form: str, refusal: CoreError,
    ) -> None:
        method, run = _MONITORING_READS[form]
        call = _raising(broker, method, refusal)

        _assert_raises_once(refusal, call, lambda: run(broker))


class TestTaskActions:
    @pytest.mark.parametrize('refusal', _refusals())
    def test_the_cancel_raises_the_refusal(
        self, broker: PostgresBroker, refusal: CoreError,
    ) -> None:
        call = _raising(broker, 'cancel_task_async', refusal)

        _assert_raises_once(
            refusal, call, lambda: asyncio.run(cancel_task(broker, _TASK_ID)),
        )


class TestWebSchemaProbe:
    @pytest.mark.parametrize('refusal', _refusals())
    def test_the_probe_raises_the_refusal(
        self, broker: PostgresBroker, refusal: CoreError,
    ) -> None:
        call = _raising(broker, 'schema_status_async', refusal)
        probe = SchemaProbe(broker)

        _assert_raises_once(refusal, call, lambda: asyncio.run(probe.status()))
