"""`HorsiesErrorCode` is the catalog that `horsies-codegen` renders from the core.

The core `HorsiesErrorCode` (`core/src/core/error/mod.rs`) is the one list of the
`HRS-nnn` codes. `horsies-codegen check` fails when
`horsies._catalog.error_codes` differs from it. `test_docs_enum_lists.py`
compares each docs list of the codes with `horsies.HorsiesErrorCode`, which is this
class.
"""

from __future__ import annotations

import pytest

import horsies
from horsies._catalog import error_codes
from horsies.core import errors
from horsies.core.errors import ConfigurationError
from horsies.core.models.queues import CustomQueueConfig
from horsies.core.registry.tasks import NotRegistered, TaskRegistry

pytestmark = [pytest.mark.unit]


def test_the_public_error_code_is_the_generated_catalog() -> None:
    assert errors.HorsiesErrorCode is error_codes.HorsiesErrorCode
    assert horsies.HorsiesErrorCode is error_codes.HorsiesErrorCode
    assert error_codes.HorsiesErrorCode.__module__ == 'horsies._catalog.error_codes'


def test_a_config_refusal_raises_with_the_generated_member() -> None:
    with pytest.raises(ConfigurationError) as raised:
        CustomQueueConfig(name='')

    assert raised.value.code is error_codes.HorsiesErrorCode.CONFIG_INVALID_QUEUE_MODE
    assert 'error[HRS-200]:' in str(raised.value)


def test_a_registry_refusal_raises_with_the_generated_member() -> None:
    registry: TaskRegistry[object] = TaskRegistry()

    with pytest.raises(NotRegistered) as raised:
        _ = registry['missing']

    assert raised.value.code is error_codes.HorsiesErrorCode.TASK_NOT_REGISTERED
    assert 'error[HRS-300]:' in str(raised.value)
