"""The ``HORSIES_SUPPRESS_SENDS`` environment variable.

``are_sends_suppressed()`` is true when the variable is exactly ``1`` after
trimming spaces. The former name ``TASKLIB_SUPPRESS_SENDS`` has no effect.
A suppressed send returns ``Err(TaskSendSuppressed)`` and names the variable.
"""

from __future__ import annotations

import pytest
from pydantic import SecretStr

from horsies.core.app import Horsies
from horsies.core.models.app import AppConfig
from horsies.core.models.queues import QueueMode
from horsies.core.models.task_send_types import TaskSendSuppressed
from horsies.core.models.tasks import TaskError, TaskResult
from horsies.core.types.result import Err, is_err
from horsies.core.brokers.postgres import PostgresConfig


def _build_app() -> Horsies:
    return Horsies(
        AppConfig(
            queue_mode=QueueMode.DEFAULT,
            broker=PostgresConfig(
                database_url=SecretStr('postgresql+psycopg://user:pass@localhost:5432/db'),
            ),
        )
    )


@pytest.fixture(autouse=True)
def clean_environment(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv('HORSIES_SUPPRESS_SENDS', raising=False)
    monkeypatch.delenv('TASKLIB_SUPPRESS_SENDS', raising=False)


def test_variable_one_suppresses(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv('HORSIES_SUPPRESS_SENDS', ' 1 ')
    assert _build_app().are_sends_suppressed() is True


@pytest.mark.parametrize('value', ['', '0', 'true'])
def test_other_values_do_not_suppress(
    monkeypatch: pytest.MonkeyPatch, value: str,
) -> None:
    monkeypatch.setenv('HORSIES_SUPPRESS_SENDS', value)
    assert _build_app().are_sends_suppressed() is False


def test_former_name_has_no_effect(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv('TASKLIB_SUPPRESS_SENDS', '1')
    assert _build_app().are_sends_suppressed() is False


def test_suppressed_send_names_the_variable(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    app = _build_app()

    @app.task('suppressed_probe')
    def suppressed_probe(*, value: int) -> TaskResult[int, TaskError]:
        return TaskResult(ok=value)

    monkeypatch.setenv('HORSIES_SUPPRESS_SENDS', '1')
    sent = suppressed_probe.send(value=1)
    assert isinstance(sent, Err)
    assert is_err(sent)
    assert isinstance(sent.err_value, TaskSendSuppressed)
    assert 'HORSIES_SUPPRESS_SENDS=1' in sent.err_value.message
