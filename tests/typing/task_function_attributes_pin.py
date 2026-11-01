"""Type pin: the attributes that ``TaskFunction`` declares.

pyright checks this file through ``pyrightconfig.clean.json``. pytest does
not collect it. A queue name and a serialized options string can be absent,
so a read must not type as ``str``. If the Protocol drops an attribute, the
read fails. If it narrows an attribute to ``str``, the ignore comment is
unneeded and the check fails.
"""

from __future__ import annotations

from horsies.core.task_decorator import TaskFunction


def declared_queue_name_may_be_absent(fn: TaskFunction[[], int]) -> None:
    declared: str | None = fn.task_queue_name
    _ = declared
    must_exist: str = fn.task_queue_name  # pyright: ignore[reportAssignmentType]
    _ = must_exist


def serialized_options_may_be_absent(fn: TaskFunction[[], int]) -> None:
    serialized: str | None = fn.task_options_json
    _ = serialized
    must_exist: str = fn.task_options_json  # pyright: ignore[reportAssignmentType]
    _ = must_exist
