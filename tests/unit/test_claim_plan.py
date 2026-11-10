"""The claim plan, the capacity of each budget, and the class check of a row.

- The plan routes each class from the classes that a worker runs children
  for: own budget, the ISOLATED budget, or not served.
- The CLI builds the plan from each profile shape.
- The claim pass passes the capacity of each budget and the routes, and
  publishes both in the worker state.
- A claimed row whose shared class does not fit its registered task goes back
  to the queue as ISOLATED, fenced to its claim generation, and is not
  dispatched.
"""

from __future__ import annotations

# pyright: reportPrivateUsage=false

import json
import logging
from itertools import combinations
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest

from horsies.core.app import Horsies
from horsies.core.cli import claim_plan_for_profile, execution_settings
from horsies.core.models.app import AppConfig
from horsies.core.models.broker import PostgresConfig
from horsies.core.models.tasks import Execution, TaskError, TaskResult
from horsies.core.models.worker_profile import (
    IsolatedChildren,
    NoIsolatedChildren,
    SharedAsyncChildren,
    WorkerProfile,
)
from horsies.core.worker.claim_plan import (
    BudgetCapacity,
    ClaimPlan,
    ClaimRoute,
    NotServed,
    ServedBy,
    budgets_json,
    class_capacity_json,
    describe_claim_plan,
    isolated_children_plan,
    routes_json,
)
from horsies.core.worker.claiming import ClaimPass
from horsies.core.worker.config import WorkerConfig
from horsies.core.worker.row_class import (
    RowClassFits,
    RowClassMismatch,
    TaskNotRegistered,
    check_row_class,
    parse_row_class,
    registered_task_classes,
)
from horsies.core.worker.slots import SlotPool
from horsies.core.worker.sql import (
    HORSIES_CLAIM_SQL,
    NOTIFY_TASK_QUEUE_SQL,
    RETURN_CLAIMED_TASK_AS_ISOLATED_SQL,
)
from horsies.core.worker.worker import Worker, _log_claim_plan
from pydantic import SecretStr

pytestmark = [pytest.mark.unit]

ISOLATED = Execution.ISOLATED
SHARED_ASYNC = Execution.SHARED_ASYNC
SHARED_THREAD = Execution.SHARED_THREAD


def _all_budget_sets() -> list[frozenset[Execution]]:
    members = list(Execution)
    return [
        frozenset(subset)
        for size in range(len(members) + 1)
        for subset in combinations(members, size)
    ]


def _expected_route(
    budgets: frozenset[Execution],
    execution: Execution,
) -> ClaimRoute:
    """The routing rules, written out per case."""
    match execution in budgets, execution:
        case True, _:
            return ServedBy(budget=execution)
        case False, Execution.ISOLATED:
            return NotServed()
        case False, _ if ISOLATED in budgets:
            return ServedBy(budget=ISOLATED)
        case _:
            return NotServed()


class TestRoutes:
    @pytest.mark.parametrize(
        'budgets',
        _all_budget_sets(),
        ids=lambda b: '+'.join(sorted(m.value for m in b)) or 'none',
    )
    def test_each_class_routes_by_the_rules(
        self,
        budgets: frozenset[Execution],
    ) -> None:
        plan = ClaimPlan(budget_classes=budgets)
        for member in Execution:
            assert plan.route(member) == _expected_route(budgets, member)

    def test_isolated_children_claim_every_class_into_their_budget(self) -> None:
        plan = isolated_children_plan()
        assert [plan.route(m) for m in Execution] == [ServedBy(ISOLATED)] * 3
        assert plan.unserved() == ()
        assert plan.budgets() == (ISOLATED,)
        assert plan.served_by(ISOLATED) == (ISOLATED, SHARED_ASYNC, SHARED_THREAD)

    def test_isolated_children_never_take_a_class_with_its_own_children(
        self,
    ) -> None:
        plan = ClaimPlan(budget_classes=frozenset({ISOLATED, SHARED_ASYNC}))
        assert plan.route(SHARED_ASYNC) == ServedBy(SHARED_ASYNC)
        assert plan.served_by(ISOLATED) == (ISOLATED, SHARED_THREAD)
        assert plan.served_by(SHARED_ASYNC) == (SHARED_ASYNC,)

    def test_a_worker_without_isolated_children_claims_no_isolated_rows(
        self,
    ) -> None:
        plan = ClaimPlan(budget_classes=frozenset({SHARED_ASYNC}))
        assert plan.route(ISOLATED) == NotServed()
        assert plan.route(SHARED_THREAD) == NotServed()
        assert plan.unserved() == (ISOLATED, SHARED_THREAD)
        assert plan.budgets() == (SHARED_ASYNC,)

    def test_a_worker_without_children_serves_nothing(self) -> None:
        plan = ClaimPlan(budget_classes=frozenset())
        assert plan.unserved() == tuple(Execution)
        assert plan.budgets() == ()
        assert routes_json(plan) == {}


class TestPlanFromProfile:
    def test_isolated_children_give_the_isolated_plan(self) -> None:
        profile = WorkerProfile(isolated=IsolatedChildren(count=4))
        assert claim_plan_for_profile(profile) == isolated_children_plan()
        assert execution_settings(profile).claim_plan == isolated_children_plan()

    def test_no_isolated_children_give_no_budget(self) -> None:
        profile = WorkerProfile(isolated=NoIsolatedChildren())
        assert claim_plan_for_profile(profile) == ClaimPlan(
            budget_classes=frozenset(),
        )

    def test_shared_async_children_give_a_shared_async_budget(self) -> None:
        profile = WorkerProfile(
            isolated=IsolatedChildren(count=1),
            shared_async=SharedAsyncChildren(count=2, seats=10),
        )
        assert claim_plan_for_profile(profile) == ClaimPlan(
            budget_classes=frozenset({ISOLATED, SHARED_ASYNC}),
        )

    def test_only_shared_async_children_serve_no_isolated_row(self) -> None:
        profile = WorkerProfile(shared_async=SharedAsyncChildren(count=1, seats=4))
        plan = claim_plan_for_profile(profile)
        assert plan.budgets() == (SHARED_ASYNC,)
        assert plan.unserved() == (ISOLATED, SHARED_THREAD)

    def test_the_worker_config_default_is_the_isolated_plan(self) -> None:
        cfg = WorkerConfig(dsn='x', psycopg_dsn='y', queues=['default'])
        assert cfg.claim_plan == isolated_children_plan()


class TestEncodings:
    def test_routes_name_only_served_classes(self) -> None:
        plan = ClaimPlan(budget_classes=frozenset({SHARED_ASYNC}))
        assert routes_json(plan) == {'SHARED_ASYNC': 'SHARED_ASYNC'}
        assert routes_json(isolated_children_plan()) == {
            'ISOLATED': 'ISOLATED',
            'SHARED_ASYNC': 'ISOLATED',
            'SHARED_THREAD': 'ISOLATED',
        }

    def test_budgets_and_worker_state_capacity(self) -> None:
        plan = ClaimPlan(budget_classes=frozenset({ISOLATED, SHARED_ASYNC}))
        capacity = (
            BudgetCapacity(budget=ISOLATED, in_service=3),
            BudgetCapacity(budget=SHARED_ASYNC, in_service=40),
        )
        assert budgets_json(capacity) == {'ISOLATED': 3, 'SHARED_ASYNC': 40}
        assert class_capacity_json(plan, capacity) == {
            'ISOLATED': {'capacity': 3, 'serves': ['ISOLATED', 'SHARED_THREAD']},
            'SHARED_ASYNC': {'capacity': 40, 'serves': ['SHARED_ASYNC']},
        }

    def test_the_log_line_names_each_route(self) -> None:
        plan = ClaimPlan(budget_classes=frozenset({SHARED_ASYNC}))
        assert describe_claim_plan(plan) == (
            'ISOLATED -> not served, SHARED_ASYNC -> SHARED_ASYNC budget, '
            'SHARED_THREAD -> not served'
        )


class TestStartLog:
    def test_unserved_classes_get_one_line(
        self,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        with caplog.at_level(logging.INFO, logger='horsies.worker'):
            _log_claim_plan(ClaimPlan(budget_classes=frozenset({SHARED_ASYNC})))
        unserved = [
            r.getMessage() for r in caplog.records if 'claims no rows' in r.getMessage()
        ]
        assert unserved == [
            'This worker claims no rows of class ISOLATED, SHARED_THREAD: it '
            'runs no children that can run them',
        ]

    def test_a_worker_that_serves_every_class_logs_no_such_line(
        self,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        with caplog.at_level(logging.INFO, logger='horsies.worker'):
            _log_claim_plan(isolated_children_plan())
        assert 'claims no rows' not in caplog.text
        assert 'Claim plan: ISOLATED -> ISOLATED budget' in caplog.text


class TestRowClass:
    REGISTERED = {
        'iso': ISOLATED,
        'async_task': SHARED_ASYNC,
        'thread_task': SHARED_THREAD,
    }

    @pytest.mark.parametrize('task_name', ['iso', 'async_task', 'thread_task'])
    def test_an_isolated_row_fits_any_function(self, task_name: str) -> None:
        assert check_row_class(ISOLATED, task_name, self.REGISTERED) == RowClassFits()

    @pytest.mark.parametrize(
        ('row_class', 'task_name'),
        [(SHARED_ASYNC, 'async_task'), (SHARED_THREAD, 'thread_task')],
    )
    def test_a_shared_row_fits_a_function_of_its_class(
        self,
        row_class: Execution,
        task_name: str,
    ) -> None:
        assert check_row_class(row_class, task_name, self.REGISTERED) == RowClassFits()

    @pytest.mark.parametrize(
        ('row_class', 'task_name', 'registered_class'),
        [
            (SHARED_ASYNC, 'iso', ISOLATED),
            (SHARED_ASYNC, 'thread_task', SHARED_THREAD),
            (SHARED_THREAD, 'iso', ISOLATED),
            (SHARED_THREAD, 'async_task', SHARED_ASYNC),
        ],
    )
    def test_a_shared_row_of_another_class_is_a_mismatch(
        self,
        row_class: Execution,
        task_name: str,
        registered_class: Execution,
    ) -> None:
        assert check_row_class(row_class, task_name, self.REGISTERED) == (
            RowClassMismatch(row_class=row_class, registered_class=registered_class)
        )

    @pytest.mark.parametrize('row_class', list(Execution))
    def test_an_unknown_task_is_not_registered(self, row_class: Execution) -> None:
        assert check_row_class(row_class, 'other', self.REGISTERED) == (
            TaskNotRegistered()
        )

    def test_the_stored_class_parses(self) -> None:
        assert parse_row_class('t1', 'SHARED_THREAD') is SHARED_THREAD

    def test_a_value_outside_the_enum_is_a_defect(self) -> None:
        with pytest.raises(AssertionError, match="t1 has execution 'SHARED_GPU'"):
            parse_row_class('t1', 'SHARED_GPU')


class TestRegisteredClasses:
    def test_the_stored_options_give_the_class(self) -> None:
        classes = registered_task_classes(
            [
                ('no_options', None),
                ('before_classes', '{"timeout_ms": 1000}'),
                (
                    'async_task',
                    '{"execution": "SHARED_ASYNC", "cancel_grace_ms": 5000}',
                ),
                ('thread_task', '{"execution": "SHARED_THREAD"}'),
            ],
        )
        assert classes == {
            'no_options': ISOLATED,
            'before_classes': ISOLATED,
            'async_task': SHARED_ASYNC,
            'thread_task': SHARED_THREAD,
        }

    def test_options_that_are_not_json_are_a_defect(self) -> None:
        with pytest.raises(AssertionError, match="'broken' has task options"):
            registered_task_classes([('broken', '{not json')])

    def test_options_without_a_valid_class_are_a_defect(self) -> None:
        with pytest.raises(AssertionError, match='without a valid execution class'):
            registered_task_classes([('broken', '{"execution": "GPU"}')])

    def test_a_real_app_registers_each_declared_class(self) -> None:
        app = Horsies(
            AppConfig(
                broker=PostgresConfig(
                    database_url=SecretStr('postgresql+psycopg://u:p@localhost/db'),
                ),
            ),
        )

        @app.task('plain')
        def plain() -> TaskResult[int, TaskError]:  # pyright: ignore[reportUnusedFunction]
            return TaskResult(ok=1)

        @app.task('awaited', execution=Execution.SHARED_ASYNC, timeout_ms=1000)
        async def awaited() -> TaskResult[int, TaskError]:  # pyright: ignore[reportUnusedFunction]
            return TaskResult(ok=1)

        @app.task('threaded', execution=Execution.SHARED_THREAD, timeout_ms=1000)
        def threaded() -> TaskResult[int, TaskError]:  # pyright: ignore[reportUnusedFunction]
            return TaskResult(ok=1)

        classes = registered_task_classes(
            (name, task.task_options_json) for name, task in app.tasks.items()
        )
        assert classes == {
            'plain': ISOLATED,
            'awaited': SHARED_ASYNC,
            'threaded': SHARED_THREAD,
        }


# --- the claim pass of a Worker, with the database mocked ---

_COLUMNS = [
    'id',
    'task_name',
    'args',
    'kwargs',
    'queue_name',
    'is_workflow_task',
    'task_options',
    'claimed_at',
    'execution',
]


def _worker(**cfg: Any) -> Worker:
    worker = Worker(
        session_factory=MagicMock(),
        listener=MagicMock(),
        cfg=WorkerConfig(dsn='x', psycopg_dsn='y', queues=['default'], **cfg),
    )
    pool = SlotPool(worker._slot_pool_config(), pending_limit=1)
    worker._slots = pool
    return worker


def _session(
    claimed: list[tuple[object, ...]],
    returned_rowcount: int,
) -> AsyncMock:
    """A session whose claim returns ``claimed`` and whose class return
    updates ``returned_rowcount`` rows."""
    session = AsyncMock()
    session.__aenter__ = AsyncMock(return_value=session)
    session.__aexit__ = AsyncMock(return_value=False)

    async def _execute(statement: Any, params: Any = None) -> MagicMock:
        result = MagicMock()
        if statement is HORSIES_CLAIM_SQL:
            result.keys.return_value = _COLUMNS
            result.fetchall.return_value = claimed
        elif statement is RETURN_CLAIMED_TASK_AS_ISOLATED_SQL:
            result.rowcount = returned_rowcount
        return result

    session.execute = AsyncMock(side_effect=_execute)
    return session


def _row(task_id: str, task_name: str, execution: str) -> tuple[object, ...]:
    return (
        task_id,
        task_name,
        '[]',
        '{}',
        'q',
        False,
        None,
        'GEN-' + task_id,
        execution,
    )


def _statements(session: AsyncMock) -> list[Any]:
    return [call.args[0] for call in session.execute.await_args_list]


class TestClaimPassWithClasses:
    @pytest.mark.asyncio
    async def test_the_pass_sends_the_budgets_and_the_routes(self) -> None:
        worker = _worker(processes=1)
        session = _session([], returned_rowcount=0)
        worker.sf = MagicMock(return_value=session)

        await worker._claim_and_dispatch_all()

        params = session.execute.await_args_list[0].args[1]
        assert json.loads(params['p_budgets']) == {'ISOLATED': 1}
        assert json.loads(params['p_routes']) == {
            'ISOLATED': 'ISOLATED',
            'SHARED_ASYNC': 'ISOLATED',
            'SHARED_THREAD': 'ISOLATED',
        }
        assert 'p_processes' not in params

    @pytest.mark.asyncio
    async def test_a_mismatched_row_goes_back_as_isolated(
        self,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        worker = _worker()
        worker._registered_classes = {'now_def': ISOLATED, 'iso': ISOLATED}
        session = _session(
            [_row('t1', 'now_def', 'SHARED_ASYNC'), _row('t2', 'iso', 'ISOLATED')],
            returned_rowcount=1,
        )
        worker.sf = MagicMock(return_value=session)
        worker._dispatch_one = AsyncMock()  # type: ignore[method-assign]

        with caplog.at_level(logging.WARNING, logger='horsies.worker'):
            claim_pass = await worker._claim_and_dispatch_all()

        assert claim_pass == ClaimPass(claimed=2, dropped=1)
        dispatched = [call.args[0] for call in worker._dispatch_one.await_args_list]
        assert dispatched == ['t2']
        returns = [
            call.args[1]
            for call in session.execute.await_args_list
            if call.args[0] is RETURN_CLAIMED_TASK_AS_ISOLATED_SQL
        ]
        assert returns == [
            {'id': 't1', 'wid': worker.worker_instance_id, 'claimed_at': 'GEN-t1'},
        ]
        notifies = [
            call.args[1]
            for call in session.execute.await_args_list
            if call.args[0] is NOTIFY_TASK_QUEUE_SQL
        ]
        assert notifies == [{'c2': 'task_queue_q', 'p': 'class:t1'}]
        assert (
            'Task t1 (now_def) stores execution SHARED_ASYNC, and the registered '
            'function is ISOLATED on this worker; wrote ISOLATED on the row and '
            'returned it to the queue'
        ) in caplog.text

    @pytest.mark.asyncio
    async def test_a_lost_claim_is_neither_returned_nor_dispatched(
        self,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        worker = _worker()
        worker._registered_classes = {'now_def': ISOLATED}
        session = _session(
            [_row('t1', 'now_def', 'SHARED_THREAD')],
            returned_rowcount=0,
        )
        worker.sf = MagicMock(return_value=session)
        worker._dispatch_one = AsyncMock()  # type: ignore[method-assign]

        with caplog.at_level(logging.WARNING, logger='horsies.worker'):
            claim_pass = await worker._claim_and_dispatch_all()

        assert claim_pass == ClaimPass(claimed=1, dropped=1)
        worker._dispatch_one.assert_not_awaited()
        assert NOTIFY_TASK_QUEUE_SQL not in _statements(session)
        assert 'the claim was lost, so the row was left as it is' in caplog.text

    @pytest.mark.asyncio
    async def test_rows_that_fit_or_are_unknown_are_dispatched(self) -> None:
        worker = _worker()
        worker._registered_classes = {'awaited': SHARED_ASYNC}
        session = _session(
            [
                _row('t1', 'awaited', 'SHARED_ASYNC'),
                _row('t2', 'awaited', 'ISOLATED'),
                _row('t3', 'not_here', 'SHARED_THREAD'),
            ],
            returned_rowcount=1,
        )
        worker.sf = MagicMock(return_value=session)
        worker._dispatch_one = AsyncMock()  # type: ignore[method-assign]

        claim_pass = await worker._claim_and_dispatch_all()

        assert claim_pass == ClaimPass(claimed=3, dropped=0)
        assert RETURN_CLAIMED_TASK_AS_ISOLATED_SQL not in _statements(session)
        dispatched = [call.args[0] for call in worker._dispatch_one.await_args_list]
        assert dispatched == ['t1', 't2', 't3']


class TestCapacityHook:
    @pytest.mark.asyncio
    async def test_a_shared_thread_budget_is_a_defect(self) -> None:
        """This worker starts no SHARED_THREAD children."""
        worker = _worker(
            claim_plan=ClaimPlan(budget_classes=frozenset({SHARED_THREAD})),
        )
        with pytest.raises(AssertionError, match='SHARED_THREAD budget'):
            worker._claim_capacity()

    @pytest.mark.asyncio
    async def test_the_shared_async_budget_counts_the_seats_in_service(self) -> None:
        """Before boot every shared slot is starting, so every seat counts."""
        worker = _worker(
            processes=1,
            shared_async=SharedAsyncChildren(count=2, seats=6),
            claim_plan=ClaimPlan(budget_classes=frozenset({ISOLATED, SHARED_ASYNC})),
        )
        assert worker._claim_capacity() == (
            BudgetCapacity(budget=ISOLATED, in_service=1),
            BudgetCapacity(budget=SHARED_ASYNC, in_service=12),
        )

    @pytest.mark.asyncio
    async def test_the_isolated_budget_counts_the_slots_in_service(self) -> None:
        worker = _worker(processes=3)
        assert worker._claim_capacity() == (
            BudgetCapacity(budget=ISOLATED, in_service=3),
        )
