"""The identity rebinding, the tripwires and the call counters of a dual-run.

Python run: every module global and class attribute that *is* a production
entry point of the table is replaced with a counted copy of its reference,
each broker built during the test gets a core handle whose switched
methods raise ``TripwireHit``, and every module global that *is* a switched
function of ``horsies._core`` raises it too. Core run: every module global and class
attribute that is a production entry point is replaced with a counted copy of
itself, and every one that is a reference is replaced with a tripwire.

The walk covers the loaded modules under ``horsies`` and ``tests`` and the
classes that their globals name, so ``from x import f`` in a test file and in
a production caller is covered. A broker built before the ``implementation``
fixture (for example by a session fixture) keeps the core handle it has.
Every change goes through ``monkeypatch``, so the teardown of the test undoes
it.
"""

from __future__ import annotations

import functools
import importlib
import inspect
import sys
from collections.abc import Callable, Iterator
from dataclasses import dataclass, field
from types import FunctionType, ModuleType
from typing import Final, NoReturn

import pytest

from tests.dualrun.operations.table import (
    DualRunOperation,
    Implementation,
    TripwireHit,
)

REBOUND_PACKAGES: Final = ('horsies', 'tests')
"""The top-level packages whose loaded modules the rebinding walks."""

CORE_HANDLE_MODULE: Final = 'horsies._core'
"""The module of ``CoreHandle``."""

HANDLE_TAKING_CLASSES: Final = ('CoreScheduler',)
"""The classes of ``horsies._core`` whose constructor takes a ``CoreHandle``.
A native constructor refuses the Python wrapper of the Python run, so in
that run it gets the native handle that the wrapper holds. Native code calls
the core directly, never a Python method of the handle, so the wrapper would
trip nothing there."""

type AnyFunction = Callable[..., object]


@dataclass(frozen=True, slots=True)
class ResolvedOperation:
    """An operation of a table with its two functions loaded."""

    operation: DualRunOperation
    production: AnyFunction
    reference: AnyFunction


@dataclass(slots=True)
class CallCounts:
    """Calls per operation name during one run of one test."""

    by_operation: dict[str, int] = field(default_factory=dict[str, int])

    def count(self, operation: str) -> None:
        self.by_operation[operation] = self.by_operation.get(operation, 0) + 1

    def total(self) -> int:
        return sum(self.by_operation.values())


def resolve(path: str) -> AnyFunction:
    """The function at ``'<module>:<attribute path>'``, read from the
    namespaces (a class attribute is the plain function, not a bound one).

    A path that does not name a Python function is a defect of the table:
    ``AssertionError``.
    """
    module_name, separator, attribute_path = path.partition(':')
    assert separator == ':' and attribute_path, f'not a <module>:<attribute> path: {path!r}'
    target: object = importlib.import_module(module_name)
    for part in attribute_path.split('.'):
        namespace = vars(target)
        assert part in namespace, f'{path!r}: {part!r} is not defined in {target!r}'
        target = namespace[part]
    assert isinstance(target, FunctionType), f'{path!r} names {target!r}, not a function'
    return target


def resolve_operation(operation: DualRunOperation) -> ResolvedOperation:
    """Load both functions of ``operation``."""
    return ResolvedOperation(
        operation=operation,
        production=resolve(operation.production_path),
        reference=resolve(operation.reference_path),
    )


def _rebound_modules() -> Iterator[ModuleType]:
    for name, module in tuple(sys.modules.items()):
        package = name.partition('.')[0]
        if package in REBOUND_PACKAGES and name != CORE_HANDLE_MODULE:
            yield module


def rebind(original: object, replacement: object, monkeypatch: pytest.MonkeyPatch) -> int:
    """Replace every module global and class attribute that is ``original``.

    Returns the number of places replaced.
    """
    replaced = 0
    seen_classes: set[int] = set()
    for module in _rebound_modules():
        for name, value in tuple(vars(module).items()):
            if value is original:
                monkeypatch.setattr(module, name, replacement)
                replaced += 1
            if isinstance(value, type) and id(value) not in seen_classes:
                seen_classes.add(id(value))
                for attribute, member in tuple(vars(value).items()):
                    if member is original:
                        monkeypatch.setattr(value, attribute, replacement)
                        replaced += 1
    return replaced


def counted(function: AnyFunction, operation: str, counts: CallCounts) -> AnyFunction:
    """``function`` with each call counted under ``operation``."""
    if inspect.iscoroutinefunction(function):

        @functools.wraps(function)
        async def counted_coroutine(*args: object, **kwargs: object) -> object:
            counts.count(operation)
            return await function(*args, **kwargs)

        return counted_coroutine

    @functools.wraps(function)
    def counted_call(*args: object, **kwargs: object) -> object:
        counts.count(operation)
        return function(*args, **kwargs)

    return counted_call


def _caller_of_tripwire() -> str:
    frame = inspect.currentframe()
    # This function, the tripwire, then its caller.
    for _ in range(2):
        if frame is None:
            return 'unknown caller'
        frame = frame.f_back
    if frame is None:
        return 'unknown caller'
    return f'{frame.f_code.co_filename}:{frame.f_lineno}'


def tripwire(operation: str, implementation: Implementation) -> Callable[..., NoReturn]:
    """A function that raises ``TripwireHit`` for ``operation`` when it is called."""

    def tripped(*_args: object, **_kwargs: object) -> NoReturn:
        raise TripwireHit(operation, implementation.value, _caller_of_tripwire())

    return tripped


class TripwireCoreHandle:
    """A ``CoreHandle`` whose methods in ``tripped`` raise ``TripwireHit``.
    Every other attribute is the handle's."""

    def __init__(self, handle: object, tripped: dict[str, str]) -> None:
        self._handle = handle
        self._tripped = tripped
        """Method name to operation name."""

    @property
    def native_handle(self) -> object:
        """The ``CoreHandle`` that this wrapper holds."""
        return self._handle

    def __getattr__(self, name: str) -> object:
        operation = self._tripped.get(name)
        if operation is not None:
            return tripwire(operation, Implementation.PYTHON_REFERENCE)
        return getattr(self._handle, name)


def _native_argument(value: object) -> object:
    """``value``, or the native handle of a ``TripwireCoreHandle``."""
    match value:
        case TripwireCoreHandle():
            return value.native_handle
        case _:
            return value


def given_the_native_handle(native_class: AnyFunction) -> AnyFunction:
    """A constructor of ``native_class`` that passes the native handle of
    each ``TripwireCoreHandle`` argument."""

    def build(*args: object, **kwargs: object) -> object:
        return native_class(
            *(_native_argument(value) for value in args),
            **{name: _native_argument(value) for name, value in kwargs.items()},
        )

    return build


def install_python_run(
    operations: tuple[ResolvedOperation, ...],
    counts: CallCounts,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Put each counted reference in the place of its production entry point,
    and trip the switched methods of the core handles that brokers build."""
    for resolved in operations:
        name = resolved.operation.name
        replaced = rebind(
            resolved.production,
            counted(resolved.reference, name, counts),
            monkeypatch,
        )
        assert replaced > 0, f'{name}: no loaded module names {resolved.operation.production_path}'
    tripped = {
        method: resolved.operation.name
        for resolved in operations
        for method in resolved.operation.core_methods
    }
    core_module = importlib.import_module(CORE_HANDLE_MODULE)
    core_handle_class: object = getattr(core_module, 'CoreHandle')
    assert callable(core_handle_class), 'horsies._core.CoreHandle is a class'

    def build_tripwire_handle(*args: object, **kwargs: object) -> TripwireCoreHandle:
        return TripwireCoreHandle(core_handle_class(*args, **kwargs), tripped)

    _ = rebind(core_handle_class, build_tripwire_handle, monkeypatch)
    for class_name in HANDLE_TAKING_CLASSES:
        native_class: object = getattr(core_module, class_name)
        assert callable(native_class), f'horsies._core.{class_name} is a class'
        _ = rebind(native_class, given_the_native_handle(native_class), monkeypatch)
    for resolved in operations:
        for function_name in resolved.operation.core_functions:
            core_function: object = getattr(core_module, function_name)
            _ = rebind(
                core_function,
                tripwire(resolved.operation.name, Implementation.PYTHON_REFERENCE),
                monkeypatch,
            )


def install_core_run(
    operations: tuple[ResolvedOperation, ...],
    counts: CallCounts,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Count each production entry point, and trip each reference."""
    for resolved in operations:
        name = resolved.operation.name
        replaced = rebind(
            resolved.production,
            counted(resolved.production, name, counts),
            monkeypatch,
        )
        assert replaced > 0, f'{name}: no loaded module names {resolved.operation.production_path}'
        _ = rebind(
            resolved.reference,
            tripwire(name, Implementation.CORE),
            monkeypatch,
        )
