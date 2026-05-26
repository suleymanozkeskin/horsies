"""Class registry for serde rehydration.

When ``rehydrate_value`` encounters a ``__h_pydantic__`` or ``__h_dataclass__``
envelope, it looks the carried ``(module, qualname)`` up in this registry
instead of calling ``importlib.import_module`` on attacker-controlled
strings.  Types reach the registry through two channels:

1. **Signature walking** at ``@app.task`` registration.  A conservative
   recursive walker scans parameter and return annotations and registers
   every reachable BaseModel / dataclass subclass.  Walker rules in
   ``_walk_annotation`` below.

2. **Explicit registration** via :func:`horsies_serdetype` (decorator)
   or :meth:`SerdeTypeRegistry.register` (direct call).  Use this for
   types that aren't reachable by the walker, e.g. types only carried
   inside ``dict[str, Any]`` fields, or types used by code that doesn't
   live in a task signature.

The registry is module-level (one per Python process) and append-only.
Re-registering the exact same class object is a no-op; registering a
*different* class under the same key raises ``ValueError`` so an
accidental name collision surfaces immediately rather than silently
shadowing.
"""

from __future__ import annotations

import dataclasses
import inspect
import typing
from collections.abc import Callable
from typing import Any, get_args, get_origin

from pydantic import BaseModel

from horsies.core.logging import get_logger
from horsies.core.models.tasks import TaskError, TaskResult


logger = get_logger('serde_registry')


# ---------------------------------------------------------------------------
# Registry
# ---------------------------------------------------------------------------


class SerdeTypeRegistry:
    """Append-only ``(module:qualname) → class`` mapping.

    Created as a module-level singleton; user code reaches it through
    :func:`register_serde_type` / :func:`get_registered_type`.  Direct
    instantiation is supported only for tests that want isolation.
    """

    def __init__(self) -> None:
        self._classes: dict[str, type] = {}

    def register(self, cls: type) -> type:
        """Register a class for serde rehydration.

        Returns the class unchanged so callers can use the function as
        a decorator without storing the return value.
        """
        if not isinstance(cls, type):
            raise TypeError(
                f'register expected a class, got {type(cls).__name__}: {cls!r}',
            )
        key = qualified_key(cls)
        existing = self._classes.get(key)
        if existing is None:
            self._classes[key] = cls
            return cls
        if existing is cls:
            return cls
        raise ValueError(
            f'Cannot register {key!r}: a different class object is already '
            f'registered under that name (existing={existing!r}, new={cls!r}). '
            f'This usually means two modules with the same qualified path were '
            f'imported separately, or a class was re-defined at runtime.',
        )

    def get(self, key: str) -> type | None:
        return self._classes.get(key)

    def __contains__(self, key: str) -> bool:
        return key in self._classes

    def __len__(self) -> int:
        return len(self._classes)

    def keys(self) -> list[str]:
        """Snapshot of registered keys, for diagnostics."""
        return list(self._classes.keys())


def qualified_key(cls: type) -> str:
    """Build the registry key for a class.

    Mirrors the ``f"{module}:{qualname}"`` format that ``to_jsonable``
    embeds in serialized ``__h_pydantic__`` / ``__h_dataclass__``
    envelopes.
    """
    return f'{cls.__module__}:{cls.__qualname__}'


# Module-level singleton.  The serde rehydration path reaches this through
# ``get_registered_type``; task registration populates it via
# ``walk_callable_for_serde_types``.
_REGISTRY: SerdeTypeRegistry = SerdeTypeRegistry()


def register_serde_type(cls: type) -> type:
    """Register a class for serde rehydration (decorator or call form).

    Example::

        @register_serde_type
        class MyModel(BaseModel):
            x: int

        # Or:
        register_serde_type(MyModel)

    The class must be defined at module level (``__main__`` and local
    classes can't be re-imported in worker processes).  Re-registering
    the same class is a no-op; a different class under the same name
    raises ``ValueError``.
    """
    return _REGISTRY.register(cls)


# Decorator alias — clearer intent at the user's call site.
horsies_serdetype = register_serde_type


def get_registered_type(key: str) -> type | None:
    """Look up a class by ``module:qualname`` key. Returns ``None`` if absent."""
    return _REGISTRY.get(key)


# ---------------------------------------------------------------------------
# Baseline registrations — horsies-internal types
# ---------------------------------------------------------------------------
#
# These types appear in task signatures (TaskResult[T, TaskError]) and in
# engine-produced envelopes (TaskError data).  We register them once at
# module import so user code never has to.

_REGISTRY.register(TaskError)
_REGISTRY.register(TaskResult)


# ---------------------------------------------------------------------------
# Signature walker
# ---------------------------------------------------------------------------
#
# Conservative recursive walk: stops at primitives / Any / object / abstract
# bases, registers BaseModel / dataclass subclasses (and recurses into their
# fields).  Cycle protection via ``visited``.  Forward refs resolved through
# ``typing.get_type_hints`` at the callable level.

# Types the walker never registers — primitives + library bases that aren't
# concrete serde types.
_NEVER_REGISTER: frozenset[type] = frozenset({
    bool,
    int,
    float,
    str,
    bytes,
    bytearray,
    type(None),
    object,
    type,
    BaseModel,
})

# Origins (from get_origin) that are pure containers; walker recurses into
# their type args but doesn't register the container itself.
_CONTAINER_ORIGINS: frozenset[Any] = frozenset({
    list,
    set,
    frozenset,
    tuple,
    dict,
    type(None),  # for None in Union → NoneType
})


def _walk_annotation(
    annotation: Any,
    *,
    visited: set[int],
    registry: SerdeTypeRegistry,
) -> None:
    """Walk a single annotation and register reachable BaseModel/dataclass types.

    Uses ``id(annotation)`` for visited tracking because annotations include
    non-hashable parametrized generics (e.g. ``list[MyModel]``).
    """
    if annotation is None or annotation is type(None):  # noqa: E721
        return

    visit_id = id(annotation)
    if visit_id in visited:
        return
    visited.add(visit_id)

    origin = get_origin(annotation)
    args = get_args(annotation)

    # Annotated[T, ...] — drop the metadata, walk T.
    if origin is typing.Annotated:
        if args:
            _walk_annotation(args[0], visited=visited, registry=registry)
        return

    # Optional[T] / Union[T1, T2, ...] — walk each branch.
    if origin is typing.Union:
        for arg in args:
            _walk_annotation(arg, visited=visited, registry=registry)
        return

    # list[T] / set[T] / frozenset[T] / tuple[T, ...] / dict[K, V]
    if origin in _CONTAINER_ORIGINS:
        for arg in args:
            _walk_annotation(arg, visited=visited, registry=registry)
        return

    # TaskResult[OkT, ErrT] — register TaskResult itself (handled by baseline),
    # walk OkT.  TaskError is the baseline ErrT and is also pre-registered.
    if origin is TaskResult or annotation is TaskResult:
        if args:
            _walk_annotation(args[0], visited=visited, registry=registry)
        return

    # Plain class without generic parameters.
    target_cls: type | None = None
    if isinstance(annotation, type):
        target_cls = annotation
    elif origin is not None and isinstance(origin, type):
        # Generic alias of a user class, e.g. MyGenericModel[int].
        target_cls = origin
        for arg in args:
            _walk_annotation(arg, visited=visited, registry=registry)

    if target_cls is None or target_cls in _NEVER_REGISTER:
        return

    # Pydantic BaseModel subclass — register + walk field annotations.
    if isinstance(target_cls, type) and issubclass(target_cls, BaseModel):
        if target_cls is BaseModel:
            return
        registry.register(target_cls)
        for field_info in target_cls.model_fields.values():
            field_annotation = field_info.annotation
            if field_annotation is not None:
                _walk_annotation(field_annotation, visited=visited, registry=registry)
        return

    # Dataclass — register + walk field annotations.
    if dataclasses.is_dataclass(target_cls):
        registry.register(target_cls)
        try:
            type_hints = typing.get_type_hints(target_cls, include_extras=True)
        except Exception:
            # Unresolvable forward refs: registration of the class itself still
            # happened above; field walking is best-effort.
            return
        for field in dataclasses.fields(target_cls):
            field_annotation = type_hints.get(field.name, field.type)
            if field_annotation is not None:
                _walk_annotation(field_annotation, visited=visited, registry=registry)
        return

    # Anything else (bare ``Any``, ``object``, library types we don't
    # recognise) is silently skipped — explicit registration via
    # ``horsies_serdetype`` is the documented escape hatch.


def walk_callable_for_serde_types(
    fn: Callable[..., Any],
    *,
    registry: SerdeTypeRegistry | None = None,
) -> int:
    """Walk ``fn``'s parameter and return annotations, registering reachable types.

    Called at task registration so most user types end up in the registry
    automatically.  Forward references in annotations are resolved via
    ``typing.get_type_hints``; if resolution fails (missing module imports,
    string refs to private classes), the walker silently skips that
    annotation — explicit ``@horsies_serdetype`` covers the gap.
    """
    target = registry if registry is not None else _REGISTRY
    try:
        type_hints = typing.get_type_hints(fn, include_extras=True)
    except Exception as exc:
        logger.debug(
            f'serde walker: could not resolve type hints for {fn!r}: '
            f'{type(exc).__name__}: {exc}',
        )
        return 0

    visited: set[int] = set()
    try:
        sig = inspect.signature(fn)
    except (TypeError, ValueError):
        return 0

    before_count = len(target)

    for param in sig.parameters.values():
        annotation = type_hints.get(param.name, param.annotation)
        if annotation is inspect.Parameter.empty:
            continue
        _walk_annotation(annotation, visited=visited, registry=target)

    return_annotation = type_hints.get('return', sig.return_annotation)
    if return_annotation is not inspect.Signature.empty:
        _walk_annotation(return_annotation, visited=visited, registry=target)

    return len(target) - before_count
