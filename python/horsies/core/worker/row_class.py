"""The ``Execution`` of each task that the app registered, from its stored
task options.

A row stores the ``Execution`` of its task at send. A deploy can change the
class of the function after that, for example from ``async def`` with
``SHARED_ASYNC`` to ``def``. The worker manifest carries the registered
classes to the core, which checks each claimed row against them before it
picks a child: a shared child runs only functions of its class.
"""

from __future__ import annotations

from collections.abc import Iterable

from horsies.core.codec.json_io import loads_json
from horsies.core.codec.task_options import stored_execution
from horsies.core.models.tasks import Execution
from horsies.core.types.result import Err, Ok


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
