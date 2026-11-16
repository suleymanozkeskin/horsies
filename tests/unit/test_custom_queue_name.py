"""Unit tests for the CustomQueueConfig queue name rule."""

from __future__ import annotations

import pytest

from horsies.core.errors import ConfigurationError, ErrorCode
from horsies.core.models.queues import (
    QUEUE_NAME_MAX_BYTES,
    CustomQueueConfig,
    QueueNameEdgeWhitespace,
    QueueNameEmpty,
    QueueNameNotUtf8,
    QueueNameTooLong,
    check_queue_name,
)
from horsies.core.types.result import Err, Ok

# Two bytes of UTF-8 text per character.
_TWO_BYTE_CHARACTER = '\u00e9'


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


def test_bound_leaves_the_notify_channel_within_63_bytes() -> None:
    channel = f'task_queue_{"q" * QUEUE_NAME_MAX_BYTES}'
    assert len(channel.encode('utf-8')) == 63


def test_name_over_the_byte_bound_is_refused() -> None:
    with pytest.raises(ConfigurationError) as exc_info:
        CustomQueueConfig(name='q' * (QUEUE_NAME_MAX_BYTES + 1))
    assert exc_info.value.code == ErrorCode.CONFIG_INVALID_QUEUE_MODE
    assert '53 bytes' in str(exc_info.value)


def test_name_at_the_byte_bound_is_accepted() -> None:
    config = CustomQueueConfig(name='q' * QUEUE_NAME_MAX_BYTES)
    assert len(config.name.encode('utf-8')) == QUEUE_NAME_MAX_BYTES


def test_multibyte_name_is_measured_in_bytes() -> None:
    # 27 characters, 54 bytes: within a character bound, over the byte bound.
    with pytest.raises(ConfigurationError):
        CustomQueueConfig(name=_TWO_BYTE_CHARACTER * 27)


def test_multibyte_name_at_the_byte_bound_is_accepted() -> None:
    name = _TWO_BYTE_CHARACTER * (QUEUE_NAME_MAX_BYTES // 2)
    assert CustomQueueConfig(name=name).name == name


def test_name_with_a_lone_surrogate_is_refused() -> None:
    with pytest.raises(ConfigurationError) as exc_info:
        CustomQueueConfig(name='fast\ud800')
    assert 'UTF-8' in str(exc_info.value)


def test_name_with_inner_space_is_accepted() -> None:
    assert CustomQueueConfig(name='fast lane').name == 'fast lane'


class TestCheckQueueName:
    def test_empty_name_names_the_empty_rule(self) -> None:
        assert check_queue_name('') == Err(QueueNameEmpty())

    def test_blank_name_names_the_whitespace_rule(self) -> None:
        assert check_queue_name('   ') == Err(QueueNameEdgeWhitespace())

    def test_long_name_names_the_byte_length(self) -> None:
        name = _TWO_BYTE_CHARACTER * 27
        assert check_queue_name(name) == Err(QueueNameTooLong(byte_length=54))

    def test_lone_surrogate_names_the_utf8_rule(self) -> None:
        assert check_queue_name('fast\ud800') == Err(QueueNameNotUtf8())

    def test_usable_name_is_returned(self) -> None:
        assert check_queue_name('fast') == Ok('fast')
