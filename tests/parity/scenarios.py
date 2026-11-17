"""The scenarios of the parity runs.

`rust/tests/parity/src/lib.rs` (`scenarios`) holds the same scenarios with
the same names, sends, workflows and actions: each side runs its own copy,
and the snapshot compare shows any difference between the two copies too.

A scenario runs in this order: the sends, the workflow starts, then the
actions on the started workflows. The workers start before the sends, or
after the actions (`WorkerStart`). Each scenario ends the same way on every
run: no step depends on the timing of a worker.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Final

from tests.parity import app as parity


class SendForm(Enum):
    """How a send is written."""

    DIRECT = 'direct'
    """One send."""

    RETRY_OF_ASYNC_CONTEXT = 'retry_of_async_context'
    """A blocking send on the thread of the running event loop, refused with
    `ASYNC_CONTEXT` before any statement; then two `retry_send` calls with
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


class WorkerStart(Enum):
    BEFORE_SENDS = 'before_sends'
    AFTER_ACTIONS = 'after_actions'
    """The steps run on a database with no worker: no claim can race a
    pause, a resume or a cancel."""


@dataclass(frozen=True, slots=True)
class Scenario:
    """One worker per profile, the sends in order, the workflow starts in
    order, then the actions in order."""

    name: str
    profiles: tuple[str, ...]
    sends: tuple[Send, ...]
    workflows: tuple[str, ...]
    actions: tuple[WorkflowAction, ...] = ()
    worker_start: WorkerStart = WorkerStart.BEFORE_SENDS


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
    # Layer 2: two workers. A worker claims only the classes that its
    # profile has children for, so each task has one possible worker.
    Scenario(
        name='several_workers',
        profiles=(parity.ISOLATED, parity.SHARED_ASYNC),
        sends=(
            _send('parity.add', ('a', 2), ('b', 3)),
            _send('parity.add_async', ('a', 2), ('b', 3)),
            _send('parity.refuse', ('value', 7)),
            _send('parity.refuse_async', ('value', 7)),
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
)


def scenario_named(name: str) -> Scenario:
    """The scenario of `name`. Raises `KeyError` for an unknown name."""
    for scenario in SCENARIOS:
        if scenario.name == name:
            return scenario
    raise KeyError(f'no parity scenario {name!r}')
