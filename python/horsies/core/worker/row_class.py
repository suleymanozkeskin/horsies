"""The class of a claimed row against the task that this worker registered.

A row stores the ``Execution`` of its task at send. A deploy can change the
class of the function after that, for example from ``async def`` with
``SHARED_ASYNC`` to ``def``. The parent checks each claimed row before it
picks a child for it: a shared child runs only functions of its class, and
cannot hand a row back.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from typing import TypeAlias, assert_never

from horsies.core.codec.json_io import loads_json
from horsies.core.codec.task_options import stored_execution
from horsies.core.models.tasks import Execution
from horsies.core.types.result import Err, Ok


@dataclass(frozen=True, slots=True)
class RowClassFits:
    """A child of the row's route can run the registered function."""


@dataclass(frozen=True, slots=True)
class RowClassMismatch:
    """The row asks for a shared class that the registered function does not
    have, for example ``SHARED_ASYNC`` on the row after a deploy changed the
    function to ``def``."""

    row_class: Execution
    registered_class: Execution


@dataclass(frozen=True, slots=True)
class TaskNotRegistered:
    """This worker registered no task of the row's name. The child fails the
    row with its usual error, as it does without a class."""


RowClassCheck: TypeAlias = RowClassFits | RowClassMismatch | TaskNotRegistered


def check_row_class(
    row_class: Execution,
    task_name: str,
    registered: Mapping[str, Execution],
) -> RowClassCheck:
    """Compare the class stored on a claimed row with the registered task.

    An ``ISOLATED`` row fits any function: an isolated child runs both kinds.
    A row of a shared class fits only a function of that class. Does no I/O.
    """
    match registered.get(task_name):
        case None:
            return TaskNotRegistered()
        case registered_class:
            pass
    match row_class:
        case Execution.ISOLATED:
            return RowClassFits()
        case Execution.SHARED_ASYNC | Execution.SHARED_THREAD:
            if registered_class == row_class:
                return RowClassFits()
            return RowClassMismatch(
                row_class=row_class,
                registered_class=registered_class,
            )
        case _ as unreachable:
            assert_never(unreachable)


def parse_row_class(task_id: str, stored: object) -> Execution:
    """The ``Execution`` of the ``execution`` column of a claimed row.

    The column has a CHECK on the ``Execution`` values, and the claim reads
    only classes of the plan, so another value is a defect:
    ``AssertionError`` with the task id and the value.
    """
    for member in Execution:
        if stored == member.value:
            return member
    raise AssertionError(
        f'claimed task {task_id} has execution {stored!r}, '
        'which is not an Execution value',
    )


def registered_task_classes(
    tasks: Iterable[tuple[str, str | None]],
) -> dict[str, Execution]:
    """The ``Execution`` of each registered task, by task name.

    ``tasks`` holds the name and the serialized task options of each task,
    as the decorator stores them. The decorator serialized and checked those
    options, so options that do not decode to a class are a defect:
    ``AssertionError`` with the task name and the cause.
    """
    classes: dict[str, Execution] = {}
    for task_name, options_json in tasks:
        classes[task_name] = _registered_class(task_name, options_json)
    return classes


def _registered_class(task_name: str, options_json: str | None) -> Execution:
    match options_json:
        case None:
            return Execution.ISOLATED
        case str() as text:
            pass
    match loads_json(text):
        case Ok(options):
            pass
        case Err(error):
            raise AssertionError(
                f'task {task_name!r} has task options that are not JSON: {error}',
            )
    match stored_execution(options):
        case Ok(execution):
            return execution
        case Err(invalid):
            raise AssertionError(
                f'task {task_name!r} has task options without a valid '
                f'execution class: {invalid.detail}',
            )
