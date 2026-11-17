"""The operation table of the scheduler switch.

One row per entry point of the production ``Scheduler``
(``horsies.core.scheduler.service``) that the integration tests call: the
start and the tick. Both run in pool mode: the core opens and commits its own
transactions on its pools. The reference of each row is a function of
``tests/dualrun/reference/scheduler_loop_caller.py`` that runs the Python loop
of the facade instance (one reference loop per facade).

Tripwire: the facade builds its ``horsies._core.CoreScheduler`` only in
``Scheduler._core_scheduler`` (called by ``start`` and ``run_forever``), and
every core call of the facade (``start_async``, ``tick_async``,
``run_async``, ``request_stop``) is a method of that object. The start row
trips the ``CoreScheduler`` constructor, so in the Python run a facade path
that would reach the core raises ``TripwireHit`` when it builds the object.
The tick row has no tripwire of its own: with no core scheduler built, the
facade's tick raises ``RuntimeError`` before any core call.

Run: ``pytest tests/integration/test_schedule_state_rows.py
tests/integration/test_scheduler_single_connection_pool.py
tests/integration/test_scheduler_task_options.py --dual-run=scheduler``.
"""

from __future__ import annotations

from typing import Final

from tests.dualrun.operations.table import DualRunOperation, PoolMode

SCHEDULER_OPERATIONS: Final[tuple[DualRunOperation, ...]] = (
    DualRunOperation(
        name='scheduler start',
        production_path='horsies.core.scheduler.service:Scheduler.start',
        reference_path='tests.dualrun.reference.scheduler_loop_caller:facade_start',
        mode=PoolMode(),
        core_methods=(),
        core_functions=('CoreScheduler',),
    ),
    DualRunOperation(
        name='scheduler tick',
        production_path='horsies.core.scheduler.service:Scheduler._check_and_run_schedules',
        reference_path=(
            'tests.dualrun.reference.scheduler_loop_caller:facade_check_and_run_schedules'
        ),
        mode=PoolMode(),
        core_methods=(),
    ),
)
