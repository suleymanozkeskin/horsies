"""Public operations of the library, called as an application calls them.

Each call here is public API of horsies 0.7.0 and of the current release, with
the same signature in both: ``Horsies``, ``AppConfig``, ``PostgresConfig``,
``app.task``, ``send``, ``TaskNode``, ``app.workflow``, ``WorkflowSpec.start``,
``PostgresBroker.close``. The harness of each release runs this file
unchanged (``tests/perf/releases/README.md``), so a comparison of two releases
measures the same call through the product of each release.

A call writes new rows with ids that the library mints. The rows of a scenario
carry a task name or a workflow name that only that scenario uses; its cleanup
deletes them by that name.
"""

from __future__ import annotations

from typing import Final

from pydantic import SecretStr
from sqlalchemy import text
from sqlalchemy.engine import Connection

from horsies import (
    AppConfig,
    Err,
    Horsies,
    Ok,
    PostgresConfig,
    TaskError,
    TaskNode,
    TaskResult,
)

INPUT_TEXT_BYTES: Final = 200
"""The length of the one text argument that each call sends."""

SEND_TASK_NAME: Final = 'perf.public.send'
"""The task of the send scenario. No other code sends it."""

WORKFLOW_NAME: Final = 'perf.public.workflow'
"""The workflow of the start scenario. No other code starts it."""

WORKFLOW_DEFINITION_KEY: Final = 'perf.public.workflow.v1'

WORKFLOW_TASK_NAME: Final = 'perf.public.workflow-node'
"""The task of the one node of the started workflow."""

_DELETE_TASKS_BY_NAME_SQL = text("""
    DELETE FROM horsies_tasks WHERE task_name = :task_name
""")

# The delete of a workflow row cascades to its node rows (ON DELETE CASCADE).
_DELETE_WORKFLOWS_BY_NAME_SQL = text("""
    DELETE FROM horsies_workflows WHERE name = :workflow_name
""")


def _app(dsn: str) -> Horsies:
    """An app whose broker connects to ``dsn``. No I/O."""
    return Horsies(AppConfig(broker=PostgresConfig(database_url=SecretStr(dsn))))


class SendCall:
    """One call is ``send`` of one task with one text argument, as a producer
    sends it. Each call writes one PENDING task row; no worker runs it."""

    def __init__(self, dsn: str) -> None:
        """Define the task on a new app of ``dsn``. No I/O: the broker
        connects at the first call."""
        app = _app(dsn)

        @app.task(SEND_TASK_NAME)
        def perf_public_send(*, text: str) -> TaskResult[str, TaskError]:
            return TaskResult(ok=text)

        self._broker = app.get_broker()
        self._task = perf_public_send
        self._text = 'x' * INPUT_TEXT_BYTES

    def call(self) -> None:
        """Send the task once. Writes one task row.

        Raises:
            RuntimeError: the send returned an error; it names the error. The
                run stops: a measured operation must apply.
        """
        match self._task.send(text=self._text):
            case Ok():
                return
            case Err(err_value=error):
                raise RuntimeError(f'send of {SEND_TASK_NAME} returned {error!r}')

    def close(self) -> None:
        """Close the broker. A failed close is discarded: the process of the
        run ends after it, and the result of the run does not depend on it."""
        _ = self._broker.close()


class WorkflowStartCall:
    """One call is ``start`` of a one-node workflow, as an application starts
    it. Each call writes the workflow row, its node row and the task row of
    the node; no worker runs it."""

    def __init__(self, dsn: str) -> None:
        """Define the task and the workflow on a new app of ``dsn``. No I/O:
        the broker connects at the first call."""
        app = _app(dsn)

        @app.task(WORKFLOW_TASK_NAME)
        def perf_public_workflow_node(*, text: str) -> TaskResult[str, TaskError]:
            return TaskResult(ok=text)

        node = TaskNode(
            fn=perf_public_workflow_node,
            kwargs={'text': 'x' * INPUT_TEXT_BYTES},
        )
        self._broker = app.get_broker()
        self._spec = app.workflow(
            name=WORKFLOW_NAME,
            tasks=[node],
            output=node,
            definition_key=WORKFLOW_DEFINITION_KEY,
        )

    def call(self) -> None:
        """Start the workflow once, with a workflow id that the library mints.

        Raises:
            RuntimeError: the start returned an error; it names the error. The
                run stops: a measured operation must apply.
        """
        match self._spec.start():
            case Ok():
                return
            case Err(err_value=error):
                raise RuntimeError(f'start of {WORKFLOW_NAME} returned {error!r}')

    def close(self) -> None:
        """Close the broker. A failed close is discarded: the process of the
        run ends after it, and the result of the run does not depend on it."""
        _ = self._broker.close()


def delete_sent_tasks(connection: Connection) -> None:
    """Delete every task row of ``SEND_TASK_NAME`` and commit."""
    connection.execute(_DELETE_TASKS_BY_NAME_SQL, {'task_name': SEND_TASK_NAME})
    connection.commit()


def delete_started_workflows(connection: Connection) -> None:
    """Delete every workflow of ``WORKFLOW_NAME`` with its node rows, then the
    task rows of its node, and commit."""
    connection.execute(_DELETE_WORKFLOWS_BY_NAME_SQL, {'workflow_name': WORKFLOW_NAME})
    connection.execute(_DELETE_TASKS_BY_NAME_SQL, {'task_name': WORKFLOW_TASK_NAME})
    connection.commit()
