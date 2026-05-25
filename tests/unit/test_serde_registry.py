"""Tests for the serde class registry and signature walker.

The registry replaces the dynamic ``import_module`` path inside
``rehydrate_value``.  This commit adds the registry infrastructure and
the walker that auto-populates it from task signatures; the rehydrate
side switches to registry-only lookup in the follow-up commit.
"""

from __future__ import annotations

import dataclasses
from typing import Annotated, Any, Optional, Union

import pytest
from pydantic import BaseModel

from horsies.core.codec.serde_registry import (
    SerdeTypeRegistry,
    horsies_serdetype,
    qualified_key,
    register_serde_type,
    walk_callable_for_serde_types,
)
from horsies.core.models.tasks import TaskError, TaskResult


# ---------------------------------------------------------------------------
# Registry semantics
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestSerdeTypeRegistry:
    def test_register_returns_class_unchanged(self) -> None:
        registry = SerdeTypeRegistry()

        class MyModel(BaseModel):
            x: int

        result = registry.register(MyModel)
        assert result is MyModel

    def test_register_stores_under_module_qualname(self) -> None:
        registry = SerdeTypeRegistry()

        class StoredModel(BaseModel):
            x: int

        registry.register(StoredModel)
        key = f'{StoredModel.__module__}:{StoredModel.__qualname__}'
        assert registry.get(key) is StoredModel
        assert key in registry

    def test_re_registering_same_class_is_noop(self) -> None:
        registry = SerdeTypeRegistry()

        class SameModel(BaseModel):
            x: int

        registry.register(SameModel)
        registry.register(SameModel)  # second call shouldn't raise
        assert registry.get(qualified_key(SameModel)) is SameModel

    def test_re_registering_different_class_under_same_key_raises(self) -> None:
        """A name collision must surface as ValueError, not silently shadow."""
        registry = SerdeTypeRegistry()

        class FirstModel(BaseModel):
            x: int

        registry.register(FirstModel)

        # Build a second class with the same __module__ and __qualname__ but
        # a different identity.  Manually patch __qualname__ to force a
        # collision (a normal redefinition in test scope produces a new class
        # object with the same qualname).
        class FirstModel(BaseModel):  # noqa: F811 — intentional redefinition
            x: int
            y: int

        with pytest.raises(ValueError, match='different class object'):
            registry.register(FirstModel)

    def test_register_non_class_raises_typeerror(self) -> None:
        registry = SerdeTypeRegistry()
        with pytest.raises(TypeError, match='expected a class'):
            registry.register('not a class')  # type: ignore[arg-type]

    def test_get_missing_key_returns_none(self) -> None:
        registry = SerdeTypeRegistry()
        assert registry.get('nope:Nope') is None


# ---------------------------------------------------------------------------
# Module-level singleton
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestModuleLevelSingleton:
    def test_task_error_pre_registered(self) -> None:
        """``TaskError`` is part of the baseline so user code never has to register it."""
        from horsies.core.codec.serde_registry import get_registered_type

        assert get_registered_type(qualified_key(TaskError)) is TaskError

    def test_task_result_pre_registered(self) -> None:
        from horsies.core.codec.serde_registry import get_registered_type

        assert get_registered_type(qualified_key(TaskResult)) is TaskResult

    def test_decorator_form_registers(self) -> None:
        from horsies.core.codec.serde_registry import get_registered_type

        @horsies_serdetype
        class DecoratedModel(BaseModel):
            x: int

        assert get_registered_type(qualified_key(DecoratedModel)) is DecoratedModel

    def test_call_form_registers(self) -> None:
        from horsies.core.codec.serde_registry import get_registered_type

        class DirectModel(BaseModel):
            x: int

        register_serde_type(DirectModel)
        assert get_registered_type(qualified_key(DirectModel)) is DirectModel


# ---------------------------------------------------------------------------
# Signature walker — what it registers
# ---------------------------------------------------------------------------


class _Inner(BaseModel):
    value: int


class _Outer(BaseModel):
    inner: _Inner
    name: str


@dataclasses.dataclass
class _NestedDataclass:
    label: str
    nested: _Inner


@pytest.mark.unit
class TestSignatureWalker:
    def test_walks_basemodel_parameter(self) -> None:
        registry = SerdeTypeRegistry()

        def fn(arg: _Inner) -> TaskResult[int, TaskError]:
            return TaskResult(ok=arg.value)

        walk_callable_for_serde_types(fn, registry=registry)
        assert registry.get(qualified_key(_Inner)) is _Inner

    def test_walks_basemodel_fields_recursively(self) -> None:
        registry = SerdeTypeRegistry()

        def fn(arg: _Outer) -> TaskResult[int, TaskError]:
            return TaskResult(ok=arg.inner.value)

        walk_callable_for_serde_types(fn, registry=registry)
        assert registry.get(qualified_key(_Outer)) is _Outer
        assert registry.get(qualified_key(_Inner)) is _Inner

    def test_walks_dataclass_fields(self) -> None:
        registry = SerdeTypeRegistry()

        def fn(arg: _NestedDataclass) -> TaskResult[str, TaskError]:
            return TaskResult(ok=arg.label)

        walk_callable_for_serde_types(fn, registry=registry)
        assert registry.get(qualified_key(_NestedDataclass)) is _NestedDataclass
        assert registry.get(qualified_key(_Inner)) is _Inner

    def test_walks_optional(self) -> None:
        registry = SerdeTypeRegistry()

        def fn(arg: Optional[_Inner]) -> TaskResult[None, TaskError]:
            return TaskResult(ok=None)

        walk_callable_for_serde_types(fn, registry=registry)
        assert registry.get(qualified_key(_Inner)) is _Inner

    def test_walks_union(self) -> None:
        registry = SerdeTypeRegistry()

        def fn(arg: Union[_Inner, _Outer]) -> TaskResult[None, TaskError]:
            return TaskResult(ok=None)

        walk_callable_for_serde_types(fn, registry=registry)
        assert registry.get(qualified_key(_Inner)) is _Inner
        assert registry.get(qualified_key(_Outer)) is _Outer

    def test_walks_list_dict_tuple(self) -> None:
        registry = SerdeTypeRegistry()

        def fn(
            xs: list[_Inner],
            ys: dict[str, _Outer],
            zs: tuple[_Inner, ...],
        ) -> TaskResult[None, TaskError]:
            return TaskResult(ok=None)

        walk_callable_for_serde_types(fn, registry=registry)
        assert registry.get(qualified_key(_Inner)) is _Inner
        assert registry.get(qualified_key(_Outer)) is _Outer

    def test_walks_annotated(self) -> None:
        registry = SerdeTypeRegistry()

        def fn(arg: Annotated[_Inner, 'metadata']) -> TaskResult[None, TaskError]:
            return TaskResult(ok=None)

        walk_callable_for_serde_types(fn, registry=registry)
        assert registry.get(qualified_key(_Inner)) is _Inner

    def test_walks_task_result_ok_type(self) -> None:
        registry = SerdeTypeRegistry()

        def fn() -> TaskResult[_Outer, TaskError]:
            return TaskResult(ok=_Outer(inner=_Inner(value=1), name='x'))

        walk_callable_for_serde_types(fn, registry=registry)
        assert registry.get(qualified_key(_Outer)) is _Outer
        assert registry.get(qualified_key(_Inner)) is _Inner

    def test_does_not_register_basemodel_itself(self) -> None:
        """The abstract base ``BaseModel`` must never appear in the registry."""
        registry = SerdeTypeRegistry()

        def fn(arg: BaseModel) -> TaskResult[None, TaskError]:
            return TaskResult(ok=None)

        walk_callable_for_serde_types(fn, registry=registry)
        assert registry.get(qualified_key(BaseModel)) is None

    def test_does_not_register_primitives(self) -> None:
        registry = SerdeTypeRegistry()

        def fn(a: int, b: str, c: float, d: bool) -> TaskResult[int, TaskError]:
            return TaskResult(ok=a)

        walk_callable_for_serde_types(fn, registry=registry)
        for prim in (int, str, float, bool):
            assert registry.get(qualified_key(prim)) is None

    def test_does_not_register_any_or_object(self) -> None:
        registry = SerdeTypeRegistry()

        def fn(arg: Any, obj: object) -> TaskResult[Any, TaskError]:
            return TaskResult(ok=arg)

        walk_callable_for_serde_types(fn, registry=registry)
        assert registry.get(qualified_key(object)) is None


# ---------------------------------------------------------------------------
# Cycle protection
# ---------------------------------------------------------------------------


class _Tree(BaseModel):
    """Recursive model — children list contains more Trees."""

    value: int
    children: list['_Tree'] = []


_Tree.model_rebuild()


@pytest.mark.unit
class TestWalkerCycleProtection:
    def test_recursive_model_terminates(self) -> None:
        registry = SerdeTypeRegistry()

        def fn(arg: _Tree) -> TaskResult[int, TaskError]:
            return TaskResult(ok=arg.value)

        # Must terminate (not infinitely recurse).
        walk_callable_for_serde_types(fn, registry=registry)
        assert registry.get(qualified_key(_Tree)) is _Tree


# ---------------------------------------------------------------------------
# Tolerant failure modes
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestWalkerTolerance:
    def test_callable_without_signature_is_skipped(self) -> None:
        """Builtins and C-extension callables sometimes lack signatures."""
        registry = SerdeTypeRegistry()
        # ``print`` has no inspectable signature in some interpreters.
        walk_callable_for_serde_types(print, registry=registry)
        # Should not raise; registry contents not asserted.

    def test_unresolvable_forward_ref_is_skipped(self) -> None:
        """If get_type_hints can't resolve, walker silently bails."""
        registry = SerdeTypeRegistry()

        def fn(arg: 'NonexistentForwardRef') -> None:  # type: ignore[name-defined]  # noqa: F821
            ...

        # Should not raise.
        walk_callable_for_serde_types(fn, registry=registry)
