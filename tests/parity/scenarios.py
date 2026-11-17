"""The scenarios of the parity runs.

`rust/tests/parity/src/lib.rs` (`scenarios`) holds the same scenarios with
the same names, sends and workflows: each side runs its own copy, and the
snapshot compare shows any difference between the two copies too.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Final

from tests.parity import app as parity


@dataclass(frozen=True, slots=True)
class Send:
    """One send: the task name, its keyword input, and its idempotency key."""

    task_name: str
    kwargs: tuple[tuple[str, int], ...]
    idempotency_key: str | None = None


@dataclass(frozen=True, slots=True)
class Scenario:
    """The profile of the worker, the sends in order, then the workflow
    starts in order."""

    name: str
    profile: str
    sends: tuple[Send, ...]
    workflows: tuple[str, ...]


def _send(task_name: str, *kwargs: tuple[str, int], key: str | None = None) -> Send:
    return Send(task_name=task_name, kwargs=kwargs, idempotency_key=key)


SCENARIOS: Final[tuple[Scenario, ...]] = (
    Scenario(
        name='isolated_outcomes',
        profile=parity.ISOLATED,
        sends=(
            _send('parity.add', ('a', 2), ('b', 3)),
            _send('parity.refuse', ('value', 7)),
            _send('parity.transient', ('value', 1)),
        ),
        workflows=(),
    ),
    Scenario(
        name='shared_async_outcomes',
        profile=parity.SHARED_ASYNC,
        sends=(
            _send('parity.add_async', ('a', 2), ('b', 3)),
            _send('parity.refuse_async', ('value', 7)),
            _send('parity.transient_async', ('value', 1)),
        ),
        workflows=(),
    ),
    Scenario(
        name='shared_thread_outcomes',
        profile=parity.SHARED_THREAD,
        sends=(
            _send('parity.add_thread', ('a', 2), ('b', 3)),
            _send('parity.refuse_thread', ('value', 7)),
            _send('parity.transient_thread', ('value', 1)),
        ),
        workflows=(),
    ),
    Scenario(
        name='isolated_timeout',
        profile=parity.ISOLATED,
        sends=(_send('parity.sleep_past_deadline'),),
        workflows=(),
    ),
    Scenario(
        name='shared_async_timeout_cancel_grace',
        profile=parity.SHARED_ASYNC,
        sends=(_send('parity.async_past_deadline'),),
        workflows=(),
    ),
    Scenario(
        name='shared_thread_timeout',
        profile=parity.SHARED_THREAD,
        sends=(_send('parity.thread_past_deadline'),),
        workflows=(),
    ),
    Scenario(
        name='idempotency_key',
        profile=parity.ISOLATED,
        sends=(
            _send('parity.add', ('a', 1), ('b', 1), key='parity-key'),
            _send('parity.add', ('a', 1), ('b', 1), key='parity-key'),
        ),
        workflows=(),
    ),
    Scenario(
        name='workflow_chain',
        profile=parity.ISOLATED,
        sends=(),
        workflows=(parity.CHAIN_NAME,),
    ),
)


def scenario_named(name: str) -> Scenario:
    """The scenario of `name`. Raises `KeyError` for an unknown name."""
    for scenario in SCENARIOS:
        if scenario.name == name:
            return scenario
    raise KeyError(f'no parity scenario {name!r}')
