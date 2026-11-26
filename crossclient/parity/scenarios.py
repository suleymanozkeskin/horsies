"""The scenarios of the parity runs.

`clients/rust/tests/parity/src/lib.rs` (`scenarios`) holds the same scenarios with
the same names, sends, workflows and actions: each side runs its own copy,
and the snapshot compare shows any difference between the two copies too.

A scenario runs in this order: the sends, the reruns, the workflow starts,
the actions on the started workflows, then the pings. The workers start
before the sends, or after the actions (`WorkerStart`). A scenario with
`ScheduleSlots` also runs a scheduler process. Each scenario ends the same
way on every run: no step depends on the timing of a worker.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import timedelta
from enum import Enum
from typing import Final

from crossclient.parity import app as parity


class SendForm(Enum):
    """How a send is written."""

    DIRECT = 'direct'
    """One send."""

    RETRY_OF_ASYNC_CONTEXT = 'retry_of_async_context'
    """A blocking send on the thread of the running event loop, refused with
    `ASYNC_CONTEXT` before any statement; then two `resend` calls with
    its error, which write one task under the prepared id."""


@dataclass(frozen=True, slots=True)
class Send:
    """One send: the task name, its keyword input, its idempotency key and
    its form."""

    task_name: str
    kwargs: tuple[tuple[str, int], ...]
    idempotency_key: str | None = None
    form: SendForm = SendForm.DIRECT


class ActionKind(Enum):
    PAUSE = 'pause'
    RESUME = 'resume'
    CANCEL = 'cancel'


@dataclass(frozen=True, slots=True)
class WorkflowAction:
    """An action on the started workflow at `index`. A pause and a resume
    must answer true."""

    kind: ActionKind
    index: int


@dataclass(frozen=True, slots=True)
class Rerun:
    """The rerun of the task of task line `source`: a send, or an earlier
    rerun. The send process waits for the end of that task, then reruns it
    with the default retention class, the input retained and a 24 h key
    reservation. The new task is a task line after the lines of the sends,
    in the order of the reruns."""

    source: int


class Health(Enum):
    """Whether the send process pings."""

    NOT_PINGED = 'not_pinged'
    PINGED = 'pinged'
    """After the actions: `ping_database`; `ping_workers` until each worker
    of the scenario answered, at most `PING_ATTEMPTS` pings of
    `PING_WINDOW_S` (`send.py`); then a ping of the first worker that
    answered, by its id. The send process prints one ping line with the
    counts of the answers."""


@dataclass(frozen=True, slots=True)
class NoScheduler:
    """The scenario runs no scheduler process."""


@dataclass(frozen=True, slots=True)
class ScheduleSlots:
    """A scheduler process of the side runs the schedule of the app
    (`app.SCHEDULE_NAME`: every `app.SCHEDULE_INTERVAL`, missed runs caught
    up, at most `app.SCHEDULE_MAX_CATCH_UP_RUNS` slots per fire).

    Its start writes the state row with the first run one interval after
    the database time. The runner then moves `next_run_at` to `backdate`
    before the database time. The slots from there to the database time
    are due: `backdate // SCHEDULE_INTERVAL + 1` of them. The scheduler
    fires them over several fires, and the next run is then in the future.
    The interval is longer than any run, so no other slot is due: the slot
    count does not follow the run time. `backdate` is not a multiple of the
    interval, so no slot is at the database time of the move.

    The scenario has no sends and no workflow starts: every task of the
    slot task name in its database is a slot task."""

    backdate: timedelta


type SchedulerRun = NoScheduler | ScheduleSlots


class WorkerStart(Enum):
    BEFORE_SENDS = 'before_sends'
    AFTER_ACTIONS = 'after_actions'
    """The steps run on a database with no worker: no claim can race a
    pause, a resume or a cancel."""


@dataclass(frozen=True, slots=True)
class Scenario:
    """One worker per profile, the sends in order, the reruns in order, the
    workflow starts in order, the actions in order, then the pings."""

    name: str
    profiles: tuple[str, ...]
    sends: tuple[Send, ...]
    workflows: tuple[str, ...]
    actions: tuple[WorkflowAction, ...] = ()
    worker_start: WorkerStart = WorkerStart.BEFORE_SENDS
    reruns: tuple[Rerun, ...] = ()
    health: Health = Health.NOT_PINGED
    scheduler: SchedulerRun = NoScheduler()


def _send(task_name: str, *kwargs: tuple[str, int], key: str | None = None) -> Send:
    return Send(task_name=task_name, kwargs=kwargs, idempotency_key=key)


SCENARIOS: Final[tuple[Scenario, ...]] = (
    Scenario(
        name='isolated_outcomes',
        profiles=(parity.ISOLATED,),
        sends=(
            _send('parity.add', ('a', 2), ('b', 3)),
            _send('parity.refuse', ('value', 7)),
            _send('parity.transient', ('value', 1)),
        ),
        workflows=(),
    ),
    Scenario(
        name='shared_async_outcomes',
        profiles=(parity.SHARED_ASYNC,),
        sends=(
            _send('parity.add_async', ('a', 2), ('b', 3)),
            _send('parity.refuse_async', ('value', 7)),
            _send('parity.transient_async', ('value', 1)),
        ),
        workflows=(),
    ),
    Scenario(
        name='shared_thread_outcomes',
        profiles=(parity.SHARED_THREAD,),
        sends=(
            _send('parity.add_thread', ('a', 2), ('b', 3)),
            _send('parity.refuse_thread', ('value', 7)),
            _send('parity.transient_thread', ('value', 1)),
        ),
        workflows=(),
    ),
    Scenario(
        name='isolated_timeout',
        profiles=(parity.ISOLATED,),
        sends=(_send('parity.sleep_past_deadline'),),
        workflows=(),
    ),
    Scenario(
        name='shared_async_timeout_cancel_grace',
        profiles=(parity.SHARED_ASYNC,),
        sends=(_send('parity.async_past_deadline'),),
        workflows=(),
    ),
    Scenario(
        name='shared_thread_timeout',
        profiles=(parity.SHARED_THREAD,),
        sends=(_send('parity.thread_past_deadline'),),
        workflows=(),
    ),
    Scenario(
        name='idempotency_key',
        profiles=(parity.ISOLATED,),
        sends=(
            _send('parity.add', ('a', 1), ('b', 1), key='parity-key'),
            _send('parity.add', ('a', 1), ('b', 1), key='parity-key'),
        ),
        workflows=(),
    ),
    Scenario(
        name='workflow_chain',
        profiles=(parity.ISOLATED,),
        sends=(),
        workflows=(parity.CHAIN_NAME,),
    ),
    # Layer 2: two workers, and each task has one possible worker. A worker
    # with isolated children also serves a shared class that it has no
    # children for, through its ISOLATED budget; a worker with no isolated
    # children serves only the classes of its own children (the core's
    # `ClaimPlan::route`). So neither worker here serves the class of the
    # other.
    Scenario(
        name='several_workers',
        profiles=(parity.SHARED_ASYNC, parity.SHARED_THREAD),
        sends=(
            _send('parity.add_async', ('a', 2), ('b', 3)),
            _send('parity.add_thread', ('a', 2), ('b', 3)),
        ),
        workflows=(),
    ),
    Scenario(
        name='send_retry',
        profiles=(parity.ISOLATED,),
        sends=(
            Send(
                task_name='parity.add',
                kwargs=(('a', 4), ('b', 5)),
                form=SendForm.RETRY_OF_ASYNC_CONTEXT,
            ),
        ),
        workflows=(),
    ),
    # Layer 6: the first chain is paused and resumed, the second cancelled,
    # before any worker runs.
    Scenario(
        name='workflow_pause_resume_cancel',
        profiles=(parity.ISOLATED,),
        sends=(),
        workflows=(parity.CHAIN_NAME, parity.CHAIN_NAME),
        actions=(
            WorkflowAction(kind=ActionKind.PAUSE, index=0),
            WorkflowAction(kind=ActionKind.RESUME, index=0),
            WorkflowAction(kind=ActionKind.CANCEL, index=1),
        ),
        worker_start=WorkerStart.AFTER_ACTIONS,
    ),
    # Layer 7: a child workflow whose output a later node of the parent
    # takes.
    Scenario(
        name='sub_workflow',
        profiles=(parity.ISOLATED,),
        sends=(),
        workflows=(parity.PARENT_NAME,),
    ),
    # Layer 3: the slots of 150 minutes before the database time are due
    # (minus 150, minus 90 and minus 30 minutes): two fires, three tasks.
    Scenario(
        name='schedule_slots',
        profiles=(parity.ISOLATED,),
        sends=(),
        workflows=(),
        scheduler=ScheduleSlots(backdate=timedelta(minutes=150)),
    ),
    # Layer 8: the database and the two workers answer.
    Scenario(
        name='worker_health',
        profiles=(parity.ISOLATED, parity.SHARED_ASYNC),
        sends=(),
        workflows=(),
        health=Health.PINGED,
    ),
    # A failed task, then its rerun: a new task with the source as its
    # rerun parent and root.
    Scenario(
        name='rerun',
        profiles=(parity.ISOLATED,),
        sends=(_send('parity.refuse', ('value', 7)),),
        workflows=(),
        reruns=(Rerun(source=0),),
    ),
)


def scenario_named(name: str) -> Scenario:
    """The scenario of `name`. Raises `KeyError` for an unknown name."""
    for scenario in SCENARIOS:
        if scenario.name == name:
            return scenario
    raise KeyError(f'no parity scenario {name!r}')
