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


import pytest

from horsies.core.app import Horsies
from horsies.core.models.app import AppConfig
from horsies.core.models.broker import PostgresConfig
from horsies.core.models.tasks import Execution, TaskError
from horsies.core.types.result import Ok, Result
from horsies.core.worker.row_class import (
    registered_task_classes,
)
from pydantic import SecretStr

pytestmark = [pytest.mark.unit]


ISOLATED = Execution.ISOLATED
SHARED_ASYNC = Execution.SHARED_ASYNC
SHARED_THREAD = Execution.SHARED_THREAD


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
                    database_url=SecretStr('postgresql://u:p@localhost/db'),
                ),
            ),
        )

        @app.task('plain')
        def plain() -> Result[int, TaskError]:  # pyright: ignore[reportUnusedFunction]
            return Ok(1)

        @app.task('awaited', execution=Execution.SHARED_ASYNC, timeout_ms=1000)
        async def awaited() -> Result[int, TaskError]:  # pyright: ignore[reportUnusedFunction]
            return Ok(1)

        @app.task('threaded', execution=Execution.SHARED_THREAD, timeout_ms=1000)
        def threaded() -> Result[int, TaskError]:  # pyright: ignore[reportUnusedFunction]
            return Ok(1)

        classes = registered_task_classes(
            (name, task.task_options_json) for name, task in app.tasks.items()
        )
        assert classes == {
            'plain': ISOLATED,
            'awaited': SHARED_ASYNC,
            'threaded': SHARED_THREAD,
        }
