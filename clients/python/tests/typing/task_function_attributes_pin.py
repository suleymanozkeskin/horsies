"""Type pin: the attributes that ``TaskFunction`` declares.

pyright checks this file through ``pyrightconfig.clean.json``. pytest does
not collect it. A declared queue and a serialized options string can be absent,
so a read must not type as ``str``. If the Protocol drops an attribute, the
read fails. If it narrows an attribute to ``str``, the ignore comment is
unneeded and the check fails.
"""

from __future__ import annotations

from horsies import Horsies
from horsies.core.task_decorator import TaskFunction


def declared_queue_may_be_absent(fn: TaskFunction[[], int]) -> None:
    declared: str | None = fn.queue
    _ = declared
    must_exist: str = fn.queue  # pyright: ignore[reportAssignmentType]
    _ = must_exist


def serialized_options_may_be_absent(fn: TaskFunction[[], int]) -> None:
    serialized: str | None = fn.task_options_json
    _ = serialized
    must_exist: str = fn.task_options_json  # pyright: ignore[reportAssignmentType]
    _ = must_exist


def registry_holds_task_functions(app: Horsies) -> None:
    task = app.tasks.get('report')
    declared: str | None = task.queue if task is not None else None
    _ = declared
