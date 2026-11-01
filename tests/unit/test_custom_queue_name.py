"""Unit tests for the CustomQueueConfig queue name rule."""

from __future__ import annotations

import pytest
from sqlalchemy import String

from horsies.core.errors import ConfigurationError, ErrorCode
from horsies.core.models.queues import (
    QUEUE_NAME_MAX_LENGTH,
    CustomQueueConfig,
    QueueNameEdgeWhitespace,
    QueueNameEmpty,
    QueueNameTooLong,
    check_queue_name,
)
from horsies.core.types.result import Err, Ok
from horsies.core.models.task_pg import TaskModel
from horsies.core.models.workflow_pg import WorkflowTaskModel


@pytest.mark.parametrize('name', ['', ' ', '   '])
def test_empty_or_blank_name_is_refused(name: str) -> None:
    with pytest.raises(ConfigurationError) as exc_info:
        CustomQueueConfig(name=name)
    assert exc_info.value.code == ErrorCode.CONFIG_INVALID_QUEUE_MODE


@pytest.mark.parametrize('name', [' fast', 'fast ', '\tfast', 'fast\n'])
def test_name_with_edge_whitespace_is_refused(name: str) -> None:
    with pytest.raises(ConfigurationError) as exc_info:
        CustomQueueConfig(name=name)
    assert 'whitespace' in str(exc_info.value)


def test_name_longer_than_column_is_refused() -> None:
    with pytest.raises(ConfigurationError) as exc_info:
        CustomQueueConfig(name='q' * (QUEUE_NAME_MAX_LENGTH + 1))
    assert 'maximum' in str(exc_info.value)


def test_name_at_column_length_is_accepted() -> None:
    config = CustomQueueConfig(name='q' * QUEUE_NAME_MAX_LENGTH)
    assert len(config.name) == QUEUE_NAME_MAX_LENGTH


def test_name_with_inner_space_is_accepted() -> None:
    assert CustomQueueConfig(name='fast lane').name == 'fast lane'


def test_max_length_matches_database_columns() -> None:
    for column in (
        TaskModel.__table__.c.queue_name,
        WorkflowTaskModel.__table__.c.queue_name,
    ):
        assert isinstance(column.type, String)
        assert column.type.length == QUEUE_NAME_MAX_LENGTH


class TestCheckQueueName:
    def test_empty_name_names_the_empty_rule(self) -> None:
        assert check_queue_name('') == Err(QueueNameEmpty())

    def test_blank_name_names_the_whitespace_rule(self) -> None:
        assert check_queue_name('   ') == Err(QueueNameEdgeWhitespace())

    def test_long_name_names_the_length(self) -> None:
        name = 'q' * (QUEUE_NAME_MAX_LENGTH + 1)
        assert check_queue_name(name) == Err(QueueNameTooLong(length=len(name)))

    def test_usable_name_is_returned(self) -> None:
        assert check_queue_name('fast') == Ok('fast')
