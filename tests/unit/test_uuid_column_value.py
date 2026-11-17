"""``uuid_column_value``: a uuid column value under either driver presentation."""

from __future__ import annotations

from uuid import UUID

import pytest

from horsies.core.utils.db import uuid_column_value

pytestmark = [pytest.mark.unit]

TASK_ID = UUID('0198c0de-0000-7000-8000-000000000001')


def test_a_uuid_value_is_returned() -> None:
    assert uuid_column_value(TASK_ID, column='horsies_tasks.id') == TASK_ID


def test_text_is_read_as_its_uuid() -> None:
    assert uuid_column_value(str(TASK_ID), column='horsies_tasks.id') == TASK_ID


def test_text_that_is_not_a_uuid_raises_value_error_naming_the_column() -> None:
    with pytest.raises(ValueError, match='horsies_tasks.id'):
        uuid_column_value('not-a-uuid', column='horsies_tasks.id')


def test_another_type_raises_type_error_naming_the_column() -> None:
    with pytest.raises(TypeError, match='horsies_tasks.id'):
        uuid_column_value(TASK_ID.int, column='horsies_tasks.id')
