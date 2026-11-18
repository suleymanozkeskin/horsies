"""The types of an operation table of a module switch.

During a switch, each production entry point of the module calls the core. Its
Python body before the switch stays as a reference under
``tests/dualrun/reference/``. A table names each pair, so that the integration
tests can run once with the references in the place of the production entry
points (the Python run) and once as shipped (the core run).

A path is ``'<module>:<attribute path>'``, for example
``'horsies.core.brokers.postgres:PostgresBroker.requeue_stale_claimed'``.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum


@dataclass(frozen=True, slots=True)
class PoolMode:
    """The core opens and commits its own transactions on a core pool for the
    same database. The test's own sessions read the committed state."""


@dataclass(frozen=True, slots=True)
class StatementMode:
    """The core builds each statement and decides the outcome; the caller's
    connection runs the statements (``rerun_task(connection, ...)``). The
    core run uses the test's connection as given, as the Python run does, so
    it needs no core connection and no test dialect."""


type OperationMode = PoolMode | StatementMode
"""The modes that a table uses. The connection mode comes with the first
module whose core operations run on a connection the core does not own."""


class SwitchModule(Enum):
    """A module whose switch has an operation table."""

    LIFECYCLE = 'lifecycle'
    HISTORY = 'history'
    BROKER = 'broker'
    MONITORING_AND_WEB = 'monitoring-and-web'
    WORKFLOW_ENGINE = 'workflow-engine'
    SCHEDULER = 'scheduler'


class Implementation(Enum):
    """The implementation that one run of a test uses."""

    PYTHON_REFERENCE = 'python'
    """The reference of each operation is in the place of its production entry point."""

    CORE = 'core'
    """Production as shipped: the entry points call the core."""


@dataclass(frozen=True, slots=True)
class DualRunOperation:
    """One switched entry point and its Python reference.

    ``production_path`` and ``reference_path`` name functions with the same
    signature. ``core_methods`` names the ``CoreHandle`` methods that the
    production entry point calls, and ``core_functions`` the functions of
    ``horsies._core`` that it calls (a pure call needs no handle): in the
    Python run each one raises ``TripwireHit``.
    """

    name: str
    production_path: str
    reference_path: str
    mode: OperationMode
    core_methods: tuple[str, ...]
    core_functions: tuple[str, ...] = ()


@dataclass(frozen=True, slots=True)
class CoreOnlyTest:
    """An integration test that asserts a fact of the core's path of a
    switched operation (a statement of the core, a direct call of a core
    function, a core exception class), so the reference cannot pass it. In
    the module's dual-run it runs once, on the core. ``node_id`` is the
    pytest id without parameters."""

    node_id: str
    reason: str


@dataclass(frozen=True, slots=True)
class ReferenceOnlyTest:
    """An integration test that observes the Python reference of a switched
    operation itself (it calls the reference, or counts the reference's
    statements), so the core run trips on it. In the module's dual-run it
    runs once, on the reference. ``node_id`` is the pytest id without
    parameters."""

    node_id: str
    reason: str


@dataclass(frozen=True, slots=True)
class LargeSeedTest:
    """An integration test that seeds more rows into one table than the
    capture reads by default (``tests.dualrun.capture.TABLE_ROW_LIMIT``), on
    purpose: a list bound or a query plan needs them. The captures of its
    pair read up to ``plugin.LARGE_SEED_ROW_LIMIT`` rows per table.
    ``node_id`` is the pytest id without parameters."""

    node_id: str
    reason: str


class PairFact(Enum):
    """A fact that both runs of one test fix, while the stored state that
    holds it follows the run timing. The value states the fact that the
    pair compares; ``tests.dualrun.pair_rules.project`` reduces a snapshot
    to it."""

    ONE_RACE_WINNER = (
        'exactly one of the claim and the cancel took the PENDING row, and the '
        'history row and the status events agree with it; not which one'
    )
    """The cancel first: the history row has no claim time and no claim
    worker. The claim first: the history row has both, and one more
    ``horsies_task_status`` event of the task (the status change of the
    claim)."""

    HEARTBEAT_SENDERS = (
        'per task and role, that heartbeat rows exist, and their senders '
        '(sender_id, hostname, pid); not their count, send times or leaves'
    )
    """A leaf (``tableoid``) follows the send time."""

    ONE_KEYED_SEND_WINNER = (
        'exactly one of the concurrent sends of one idempotency key committed its '
        'task, and the task holds a command as its input; not which command'
    )
    """The sends carry two commands. The input of a task that a keyed send
    committed is the command of the send that won."""

    ONE_MOVER_PER_ROUND = (
        'per round, the finalize or the stale recovery moved the task to history, '
        'and the history row and the capacity wakes agree with it; not which one, '
        'and not what the move wrote'
    )
    """The finalize writes ``FAIL_RUNNING`` (a failure; a plain task also gets
    the capacity wake of its queue) or ``COMPLETE_LOCKED`` (the success of a
    workflow task). The stale recovery writes ``FAIL_STALE`` and no wake."""


@dataclass(frozen=True, slots=True)
class PairRule:
    """The pair of one integration test compares ``fact`` in the place of
    the stored values that hold it, and every other captured value by its
    column rule. ``reason`` states why the stored values follow the run
    timing. ``node_id`` is the pytest id without parameters."""

    node_id: str
    fact: PairFact
    reason: str


@dataclass(frozen=True, slots=True)
class WholeSnapshot:
    """The pair compares every captured value by its column rule."""


type PairCompare = WholeSnapshot | PairRule
"""How the pair of one test compares its two snapshots."""


class TripwireHit(BaseException):
    """A run called the implementation of the other run: the rebinding missed
    a call path.

    A ``BaseException``, so that a production ``except Exception`` does not
    hide it. ``args`` is ``(operation, implementation, caller)``.
    """
