"""No code enum in `horsies` has an alias member.

An alias is a second member name with the value of an earlier member.
`E.__members__` holds every name, aliases included; iteration and `len(E)`
skip the aliases. So `len(E.__members__) == len(E)` holds only when `E` has
no alias.

Selection rule: every `Enum` subclass defined at module level in a module
of the `horsies` package (its `__module__` is that module) whose class name
ends in `Code`. The test finds the enums by a walk of the package, not by a
hand list, so a new code enum is checked when it is added.

The walk imports every module. `horsies.web` needs `fastapi`; the `dev`
dependency group installs it, so the walk imports the web package in the
unit environment, and the test asserts that it did. A module or package
that fails to import fails this test; it is not skipped.
"""

from __future__ import annotations

import enum
import importlib
import pkgutil
from typing import Final

import pytest

import horsies
from horsies.core.brokers.result_types import BrokerErrorCode
from horsies.core.errors import ErrorCode
from horsies.core.models.task_send_types import TaskSendErrorCode
from horsies.core.models.tasks import (
    ContractCode,
    OperationalErrorCode,
    OutcomeCode,
    RetrievalCode,
)
from horsies.core.models.workflow.handle_types import HandleErrorCode
from horsies.core.workflows.start_types import WorkflowStartErrorCode
from horsies.monitoring.errors import MonitoringQueryErrorCode
from horsies.monitoring.task_actions import TaskActionErrorCode

pytestmark = [pytest.mark.unit]

CODE_ENUM_SUFFIX: Final = 'Code'
WEB_PACKAGE: Final = 'horsies.web'

# The walk must find at least these. A walk that finds nothing, or that
# misses a subpackage, fails here instead of passing with nothing checked.
REQUIRED_CODE_ENUMS: Final[frozenset[type[enum.Enum]]] = frozenset(
    {
        ErrorCode,
        ContractCode,
        OperationalErrorCode,
        RetrievalCode,
        OutcomeCode,
        BrokerErrorCode,
        TaskSendErrorCode,
        WorkflowStartErrorCode,
        HandleErrorCode,
        MonitoringQueryErrorCode,
        TaskActionErrorCode,
    },
)


def _fail_on_package_import_error(package_name: str) -> None:
    """Stop the walk when a package cannot be imported."""
    raise AssertionError(
        f'package {package_name!r} failed to import during the code enum walk',
    )


def _walk_horsies_modules() -> list[str]:
    """Import every module of the `horsies` package and return their names."""
    module_names: list[str] = []
    for module_info in pkgutil.walk_packages(
        horsies.__path__,
        prefix=f'{horsies.__name__}.',
        onerror=_fail_on_package_import_error,
    ):
        importlib.import_module(module_info.name)
        module_names.append(module_info.name)
    return module_names


def _code_enums_defined_in(module_name: str) -> list[type[enum.Enum]]:
    """The code enums that the module `module_name` defines at module level."""
    module = importlib.import_module(module_name)
    code_enums: list[type[enum.Enum]] = []
    for attribute_name, value in vars(module).items():
        if (
            isinstance(value, type)
            and issubclass(value, enum.Enum)
            and value.__module__ == module_name
            and attribute_name.endswith(CODE_ENUM_SUFFIX)
        ):
            code_enums.append(value)
    return code_enums


def _alias_names(code_enum: type[enum.Enum]) -> list[str]:
    """Member names of `code_enum` that are aliases of an earlier member."""
    return [
        name
        for name, member in code_enum.__members__.items()
        if name != member.name
    ]


def test_no_code_enum_has_an_alias() -> None:
    module_names = _walk_horsies_modules()
    code_enums = {
        code_enum
        for module_name in module_names
        for code_enum in _code_enums_defined_in(module_name)
    }

    assert WEB_PACKAGE in module_names
    assert code_enums
    missing = sorted(
        code_enum.__qualname__ for code_enum in REQUIRED_CODE_ENUMS - code_enums
    )
    assert missing == []

    enums_with_aliases = [
        f'{code_enum.__module__}.{code_enum.__qualname__}: {_alias_names(code_enum)}'
        for code_enum in sorted(
            code_enums,
            key=lambda code_enum: (code_enum.__module__, code_enum.__qualname__),
        )
        if len(code_enum.__members__) != len(code_enum)
    ]
    assert enums_with_aliases == []
