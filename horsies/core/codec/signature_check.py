"""Strict signature validator for @app.task and app.check().

Enforces the closed allow list from `ignored-content/design/strict-serde.md` §2
on every task signature parameter and return annotation. Fails at task
registration time — loud, recoverable in seconds — instead of at runtime.

Phase 1a deliverable: validator only; no wiring into the @app.task decorator
yet. Phase 1c attaches it to `create_task_wrapper`.
"""

from __future__ import annotations

import collections.abc
import dataclasses
import datetime
import decimal
import enum
import inspect
import pathlib
import types
import uuid
from typing import (
    Any,
    Callable,
    Literal,
    TypeVar,
    Union,
    cast,
    get_args,
    get_origin,
    get_type_hints,
    is_typeddict,
)

from pydantic import BaseModel
from pydantic.fields import FieldInfo

from horsies.core.codec.json_value import JsonValue
from horsies.core.models.tasks import TaskError, TaskResult
from horsies.core.models.workflow.context import (
    SubWorkflowSummary,
    WorkflowContext,
    WorkflowMeta,
)


__all__ = [
    'INTERNAL_CODEC_TYPES',
    'SignatureValidationError',
    'check_task_signature',
]


_DOCS_URL = 'https://suleymanozkeskin.github.io/horsies/internals/strict-serde'

INTERNAL_CODEC_TYPES: frozenset[type] = frozenset({
    TaskError,
    WorkflowContext,
    WorkflowMeta,
    SubWorkflowSummary,
})

class SignatureValidationError(Exception):
    """Raised when a task signature contains a banned type.

    Message shape (one error per offending position) names: task, position,
    banned type, why, the documented fix, and a docs URL.
    """


@dataclasses.dataclass(frozen=True)
class _Rejection:
    """Structured rejection raised by classifier walks."""

    banned: str
    reason: str
    fix: str


class _RejectedError(Exception):
    """Internal signal; converted to SignatureValidationError at entry."""

    def __init__(self, rejection: _Rejection) -> None:
        self.rejection = rejection
        super().__init__(rejection.reason)


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------


def check_task_signature(
    fn: Callable[..., Any],
    *,
    task_name: str,
) -> None:
    """Validate that `fn`'s signature uses only allow-list types.

    Args:
        fn: Task function being registered. Pre-decoration is not handled
            here — `create_task_wrapper` already rejects pre-decorated fns.
        task_name: Registered name; used in error messages.

    Raises:
        SignatureValidationError: with task, position, banned type, fix,
            docs URL. Fails fast — one error per call; further positions
            are not inspected after the first rejection.
    """
    hints = get_type_hints(fn, include_extras=True)
    sig = inspect.signature(fn)

    for param_name, param in sig.parameters.items():
        # Variadic and positional-only params rejected for v1.
        # Strict-serde rejects positional args at every producer; only
        # kwargs reach the wire. A positional-only param (`def f(x, /)`)
        # would register cleanly but could never be invoked through the
        # queue, since `encode_kwargs` binds against named parameters
        # only. `**kwargs` defeats producer-side signature binding for
        # the same reason — every wire kwarg name must correspond to a
        # declared parameter, with no catch-all.
        if param.kind is inspect.Parameter.VAR_POSITIONAL:
            raise SignatureValidationError(_format_error(
                task_name=task_name,
                position=f"parameter '*{param_name}'",
                banned='*args (VAR_POSITIONAL)',
                reason='variadic positional params have no wire support in v1',
                fix='accept named kwargs only: `def f(items: list[T]) -> ...`',
            ))
        if param.kind is inspect.Parameter.VAR_KEYWORD:
            raise SignatureValidationError(_format_error(
                task_name=task_name,
                position=f"parameter '**{param_name}'",
                banned='**kwargs (VAR_KEYWORD)',
                reason='catch-all kwargs defeat producer-side signature binding',
                fix='declare each accepted kwarg as a named parameter',
            ))
        if param.kind is inspect.Parameter.POSITIONAL_ONLY:
            raise SignatureValidationError(_format_error(
                task_name=task_name,
                position=f"parameter '{param_name}' (positional-only)",
                banned='positional-only parameter (`def f(x, /)`)',
                reason='strict-serde rejects positional args; positional-only params cannot be bound by keyword',
                fix='remove the `/` from the signature so the parameter is keyword-bindable',
            ))

        annot = hints.get(param_name)
        if annot is None:
            # Reject missing annotations here. The earlier "deferred to an
            # existing TASK_PARAM_NO_TYPE check" comment was wrong — that
            # error code does not exist (errors.py defines TASK_NO_RETURN_TYPE
            # only). Without this gate, untyped params (incl. `*args` /
            # `**kwargs`) silently passed the strict validator.
            raise SignatureValidationError(_format_error(
                task_name=task_name,
                position=f"parameter '{param_name}'",
                banned='<no annotation>',
                reason='parameter has no type annotation',
                fix=(
                    f"add an explicit type annotation: "
                    f"`{param_name}: YourType` "
                    f"(use `JsonValue` for raw JSON)"
                ),
            ))

        try:
            _classify(annot, json_value_allowed_position=True)
        except _RejectedError as err:
            raise SignatureValidationError(_format_error(
                task_name=task_name,
                position=f"parameter '{param_name}'",
                banned=err.rejection.banned,
                reason=err.rejection.reason,
                fix=err.rejection.fix,
            )) from None

    return_hint = hints.get('return')
    if return_hint is None:
        # Existing TASK_NO_RETURN_TYPE check (HRS-100, raised by
        # `create_task_wrapper`) covers this.
        return
    _validate_return_annotation(return_hint, task_name=task_name)


def _validate_return_annotation(
    annot: object,
    *,
    task_name: str,
) -> None:
    """Return must be `TaskResult[OkT, TaskError]`. ErrT enforced; OkT walked."""
    origin = get_origin(annot)
    if origin is not TaskResult:
        raise SignatureValidationError(_format_error(
            task_name=task_name,
            position='return type',
            banned=repr(annot),
            reason='task return type must be TaskResult[OkT, TaskError]',
            fix='change return type to `-> TaskResult[YourType, TaskError]`',
        ))
    args = get_args(annot)
    if len(args) != 2:
        raise SignatureValidationError(_format_error(
            task_name=task_name,
            position='return type',
            banned=repr(annot),
            reason='TaskResult must have exactly 2 type parameters',
            fix='use `-> TaskResult[YourType, TaskError]`',
        ))
    ok_type, err_type = args
    if err_type is not TaskError:
        raise SignatureValidationError(_format_error(
            task_name=task_name,
            position='return type',
            banned=f'TaskResult[..., {err_type!r}]',
            reason=f'TaskResult error parameter must be TaskError, got {err_type!r}',
            fix='change return type to `-> TaskResult[YourType, TaskError]`',
        ))
    try:
        _classify(ok_type, json_value_allowed_position=True)
    except _RejectedError as err:
        raise SignatureValidationError(_format_error(
            task_name=task_name,
            position='return TaskResult ok slot',
            banned=err.rejection.banned,
            reason=err.rejection.reason,
            fix=err.rejection.fix,
        )) from None


# ---------------------------------------------------------------------------
# Classifier
# ---------------------------------------------------------------------------


def _classify(
    annot: object,
    *,
    json_value_allowed_position: bool,
    visited: frozenset[type] = frozenset(),
) -> None:
    """Classify `annot` per §2; raise `_RejectedError` if banned.

    Args:
        annot: The annotation to classify.
        json_value_allowed_position: True at task boundary positions (param,
            return ok slot) and inside BaseModel/dataclass fields. False
            when classifying generic parameters of a user type — JsonValue
            is rejected there.
        visited: BaseModel / dataclass classes already in the active walk
            path. Used by `_walk_model_fields` / `_walk_dataclass_fields`
            to break recursive-model cycles (e.g. `class Node: child: Node`)
            instead of blowing the Python stack at @app.task time.
    """
    if annot is JsonValue:
        if not json_value_allowed_position:
            raise _RejectedError(_Rejection(
                banned='JsonValue',
                reason='JsonValue may not appear as a generic parameter of a user type',
                fix='use JsonValue only at task boundary positions or inside BaseModel/dataclass fields',
            ))
        return

    if annot is type(None):
        return

    if annot is bool or annot is int or annot is float or annot is str:
        return

    if (
        annot is datetime.datetime
        or annot is datetime.date
        or annot is datetime.time
    ):
        return

    if annot is uuid.UUID:
        return

    if annot is decimal.Decimal:
        return

    if annot is bytes:
        raise _RejectedError(_Rejection(
            banned='bytes',
            reason='bytes round-trip is unsafe (encode/decode are not inverses through JSON)',
            fix='wrap a model with explicit base64-encoded fields, or pass str',
        ))

    if annot is Any:
        raise _RejectedError(_Rejection(
            banned='Any',
            reason='Any is not a concrete type',
            fix='use a concrete type, or `JsonValue` for raw JSON data',
        ))

    if annot is object:
        raise _RejectedError(_Rejection(
            banned='object',
            reason='object is not a concrete type',
            fix='use a concrete type, or `JsonValue` for raw JSON data',
        ))

    if isinstance(annot, TypeVar):
        raise _RejectedError(_Rejection(
            banned=f'TypeVar {annot!r}',
            reason='TypeVar has no concrete type for the decoder',
            fix='define a wrapper task per concrete instantiation',
        ))

    # Class-based checks (use isinstance for issubclass safety).
    if isinstance(annot, type):
        annot_cls: type[Any] = annot
        if annot_cls in INTERNAL_CODEC_TYPES:
            return

        if issubclass(annot_cls, enum.Enum):
            # Member values must be JSON-native scalars: anything else
            # encodes through mode='json' coercion (tuple → list, ...) and
            # then fails enum-by-value decode on the consumer — exactly the
            # deferred runtime failure this validator exists to prevent.
            for member in annot_cls:
                value = member.value
                if value is not None and not isinstance(value, (str, int, float)):
                    raise _RejectedError(_Rejection(
                        banned=(
                            f'Enum {annot_cls.__name__} '
                            f'(member {member.name!r} has value {value!r})'
                        ),
                        reason=(
                            'enum member values must be JSON-native scalars '
                            '(str/int/float/bool/None) to round-trip by value'
                        ),
                        fix='use str or int member values',
                    ))
            return

        if is_typeddict(annot_cls):
            raise _RejectedError(_Rejection(
                banned=f'TypedDict {annot_cls.__name__}',
                reason='TypedDict is runtime-weak (just a dict at runtime)',
                fix='use a Pydantic BaseModel or @dataclass for structured fields',
            ))

        if annot_cls is BaseModel:
            raise _RejectedError(_Rejection(
                banned='BaseModel (bare)',
                reason='bare BaseModel has no schema to validate against',
                fix='use a concrete BaseModel subclass',
            ))

        if issubclass(annot_cls, BaseModel):
            # Pydantic v2 generics: `Box[int]` is itself a ModelMetaclass
            # instance, so it hits this branch (not the tail generic-alias
            # branch). Pull the substituted args via Pydantic's metadata
            # and classify each at non-boundary position, so §3's
            # "JsonValue may not appear as a generic parameter of a user
            # type" still fires — otherwise the substituted field annotation
            # would reach `_walk_model_fields` already concretized to
            # JsonValue (boundary position) and get silently accepted.
            #
            # Defense-in-depth: even if a future Pydantic resolves the
            # alias to its underlying `None | bool | ... | list[JsonValue]
            # | dict[str, JsonValue]` union, that union still descends
            # into `_classify_union` at non-boundary position and lands
            # on "mixed union types are rejected" (the union has a non-
            # primitive member). Probe verified both rejection paths.
            generic_meta = getattr(annot_cls, '__pydantic_generic_metadata__', None)
            if generic_meta is not None:
                for arg in generic_meta.get('args', ()):
                    _classify(
                        arg,
                        json_value_allowed_position=False,
                        visited=visited,
                    )
            _walk_model_fields(annot_cls, visited=visited)
            return

        if dataclasses.is_dataclass(annot_cls):
            _walk_dataclass_fields(annot_cls, visited=visited)
            return

        if issubclass(annot_cls, pathlib.PurePath):
            raise _RejectedError(_Rejection(
                banned=annot_cls.__name__,
                reason="pathlib paths are filesystem-local; semantics don't survive process boundaries",
                fix='pass str and convert at the boundary if path semantics matter',
            ))

        if annot_cls is list or annot_cls is dict or annot_cls is tuple:
            raise _RejectedError(_Rejection(
                banned=f'bare {annot_cls.__name__}',
                reason="unparameterized containers don't tell the decoder what's inside",
                fix=f'use {annot_cls.__name__}[T] with a concrete element type',
            ))

        if annot_cls is set or annot_cls is frozenset:
            raise _RejectedError(_Rejection(
                banned=annot_cls.__name__,
                reason="JSON arrays don't preserve set semantics (order, uniqueness)",
                fix='use list[T] and convert at the boundary if set membership matters',
            ))

    origin = get_origin(annot)
    args = get_args(annot)

    # TaskResult[OkT, TaskError] anywhere (kwarg or return).
    if origin is TaskResult:
        if len(args) != 2:
            raise _RejectedError(_Rejection(
                banned=repr(cast(object, annot)),
                reason='TaskResult must have exactly 2 type parameters',
                fix='use TaskResult[YourType, TaskError]',
            ))
        ok_t, err_t = args
        if err_t is not TaskError:
            raise _RejectedError(_Rejection(
                banned=f'TaskResult[..., {err_t!r}]',
                reason=f'TaskResult error parameter must be TaskError, got {err_t!r}',
                fix='use TaskResult[YourType, TaskError]',
            ))
        _classify(ok_t, json_value_allowed_position=True, visited=visited)
        return

    # Annotated[T, meta...]
    if origin is not None and hasattr(cast(object, annot), '__metadata__'):
        underlying = args[0]
        metadata = args[1:]
        underlying_origin = get_origin(underlying)
        if underlying_origin is Union or underlying_origin is types.UnionType:
            discriminator = _find_discriminator(metadata)
            if discriminator is not None:
                for branch in get_args(underlying):
                    _classify(
                        branch,
                        json_value_allowed_position=False,
                        visited=visited,
                    )
                return
            _classify_union(
                get_args(underlying),
                json_value_allowed_position=json_value_allowed_position,
                visited=visited,
            )
            return
        _classify(
            underlying,
            json_value_allowed_position=json_value_allowed_position,
            visited=visited,
        )
        return

    if origin is Literal:
        for member in args:
            if member is None:
                continue
            if isinstance(member, (bool, int, str)):
                continue
            if isinstance(member, enum.Enum):
                continue
            raise _RejectedError(_Rejection(
                banned=f'Literal member {member!r}',
                reason=f'Literal member {member!r} is not a primitive or Enum',
                fix='Literal members must be None, bool, int, str, or Enum members',
            ))
        return

    if origin is list:
        if not args:
            raise _RejectedError(_Rejection(
                banned='bare list',
                reason='unparameterized list is rejected',
                fix='use list[T] with a concrete element type',
            ))
        for arg in args:
            _classify(
                arg,
                json_value_allowed_position=json_value_allowed_position,
                visited=visited,
            )
        return

    if origin is dict:
        if len(args) != 2:
            raise _RejectedError(_Rejection(
                banned='bare dict',
                reason='unparameterized dict is rejected',
                fix='use dict[str, V] with a concrete value type',
            ))
        key_t, val_t = args
        if key_t is not str:
            raise _RejectedError(_Rejection(
                banned=f'dict[{key_t!r}, _]',
                reason=f'dict key type must be str, got {key_t!r}',
                fix='JSON requires string keys; use dict[str, V]',
            ))
        _classify(
            val_t,
            json_value_allowed_position=json_value_allowed_position,
            visited=visited,
        )
        return

    if origin is tuple:
        if not args:
            raise _RejectedError(_Rejection(
                banned='bare tuple',
                reason='unparameterized tuple is rejected',
                fix='use tuple[T, ...] or tuple[T1, T2, ...] with concrete element types',
            ))
        for arg in args:
            if arg is Ellipsis:
                continue
            _classify(
                arg,
                json_value_allowed_position=json_value_allowed_position,
                visited=visited,
            )
        return

    if origin is Union or origin is types.UnionType:
        _classify_union(
            args,
            json_value_allowed_position=json_value_allowed_position,
            visited=visited,
        )
        return

    if origin is set or origin is frozenset:
        raise _RejectedError(_Rejection(
            banned=f'{cast(type, origin).__name__}[...]',
            reason="JSON arrays don't preserve set semantics",
            fix='use list[T] and convert at the boundary',
        ))

    if origin is cast(object, collections.abc.Callable):
        raise _RejectedError(_Rejection(
            banned='Callable',
            reason='callables are not serializable as data',
            fix='refactor the task to take serializable inputs',
        ))

    # Generic BaseModel: MyGeneric[int] etc.
    #
    # Pydantic v2 creates a concrete subclass for each parameterization
    # (so `Box[int]` is itself a ModelMetaclass instance and is usually
    # caught by the `isinstance(annot, type)` branch above). This tail
    # branch handles edge cases where `annot` isn't recognized as a type
    # but `get_origin(annot)` still returns a BaseModel subclass.
    #
    # Walk via the *parameterized* form (`annot` would have resolved
    # field annotations if reachable here) — but `_walk_model_fields`
    # uses `model_cls.model_fields[...].annotation`, which Pydantic
    # resolves under generic substitution, so passing `origin` works
    # for the unparameterized walk path and never sees raw TypeVars.
    if origin is not None and isinstance(origin, type) and issubclass(origin, BaseModel):
        for arg in args:
            _classify(arg, json_value_allowed_position=False, visited=visited)
        target = annot if isinstance(annot, type) and issubclass(annot, BaseModel) else origin
        _walk_model_fields(target, visited=visited)
        return

    raise _RejectedError(_Rejection(
        banned=repr(cast(object, annot)),
        reason='unrecognized type',
        fix='use a type from the allow list (primitives, datetime, BaseModel/dataclass, parameterized containers, JsonValue, ...) — see strict-serde docs',
    ))


def _classify_union(
    args: tuple[object, ...],
    *,
    json_value_allowed_position: bool,
    visited: frozenset[type] = frozenset(),
) -> None:
    """Apply the syntactic union rules from §2."""
    non_none = [a for a in args if a is not type(None)]

    if not non_none:
        return

    if len(non_none) == 1:
        _classify(
            non_none[0],
            json_value_allowed_position=json_value_allowed_position,
            visited=visited,
        )
        return

    all_primitives = all(
        a is bool or a is int or a is float or a is str
        for a in non_none
    )
    if all_primitives:
        return

    has_model_or_dataclass = any(
        (isinstance(a, type) and issubclass(a, BaseModel))
        or (isinstance(a, type) and dataclasses.is_dataclass(a))
        for a in non_none
    )
    if has_model_or_dataclass:
        raise _RejectedError(_Rejection(
            banned=f'Union {tuple(non_none)!r}',
            reason='BaseModel/dataclass unions require a discriminator',
            fix="use Annotated[Union[...], Field(discriminator='your_tag_field')]",
        ))

    raise _RejectedError(_Rejection(
        banned=f'Union {tuple(non_none)!r}',
        reason='mixed union types are rejected',
        fix='union members must be drawn from {None, bool, int, float, str}, or use a discriminated union',
    ))


# ---------------------------------------------------------------------------
# Recursive walks
# ---------------------------------------------------------------------------


def _walk_model_fields(
    model_cls: type[BaseModel],
    *,
    visited: frozenset[type] = frozenset(),
) -> None:
    """Recurse into a BaseModel subclass's field annotations.

    Skips walks for `INTERNAL_CODEC_TYPES` members (they have hand-maintained
    schemas that legitimately use Any).

    Uses `model_cls.model_fields[...].annotation` rather than
    `typing.get_type_hints(model_cls)` so that Pydantic's generic
    substitution is honored — for `Box[int]`, `model_fields['value'].annotation`
    is the concrete `int`, while `get_type_hints` would return the
    unresolved `TypeVar`.

    `visited` breaks cycles on self-referential models (`class Node:
    child: Node`). Without it the walk recurses forever and the
    @app.task decorator surfaces a RecursionError instead of a
    SignatureValidationError.
    """
    if model_cls in INTERNAL_CODEC_TYPES:
        return
    if model_cls in visited:
        return
    next_visited = visited | {model_cls}
    for field_name, field_info in model_cls.model_fields.items():
        if field_name.startswith('_'):
            continue
        field_type = field_info.annotation
        if field_type is None:
            continue
        try:
            _classify(
                field_type,
                json_value_allowed_position=True,
                visited=next_visited,
            )
        except _RejectedError as err:
            raise _RejectedError(_Rejection(
                banned=err.rejection.banned,
                reason=f"in BaseModel field '{model_cls.__name__}.{field_name}': {err.rejection.reason}",
                fix=err.rejection.fix,
            )) from None


def _walk_dataclass_fields(
    dc: type,
    *,
    visited: frozenset[type] = frozenset(),
) -> None:
    """Recurse into a dataclass's field annotations.

    `visited` breaks cycles on self-referential dataclasses (same risk as
    BaseModel — without it the walk would blow the Python stack and
    surface a RecursionError at task registration).
    """
    if dc in INTERNAL_CODEC_TYPES:
        return
    if dc in visited:
        return
    next_visited = visited | {dc}
    hints = get_type_hints(dc, include_extras=True)
    for field_name, field_type in hints.items():
        if field_name.startswith('_'):
            continue
        try:
            _classify(
                field_type,
                json_value_allowed_position=True,
                visited=next_visited,
            )
        except _RejectedError as err:
            raise _RejectedError(_Rejection(
                banned=err.rejection.banned,
                reason=f"in dataclass field '{dc.__name__}.{field_name}': {err.rejection.reason}",
                fix=err.rejection.fix,
            )) from None


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _find_discriminator(metadata: tuple[object, ...]) -> str | None:
    """Look up a Pydantic Field(discriminator=...) inside Annotated metadata."""
    for m in metadata:
        if isinstance(m, FieldInfo):
            discriminator = getattr(m, 'discriminator', None)
            if isinstance(discriminator, str):
                return discriminator
    return None


def _format_error(
    *,
    task_name: str,
    position: str,
    banned: str,
    reason: str,
    fix: str,
) -> str:
    """Render the structured error per design-doc §2 'Error message shape'."""
    return (
        f"task '{task_name}' {position} uses banned type `{banned}`.\n"
        f'        {reason}\n'
        f'        Fix: {fix}\n'
        f'        See: {_DOCS_URL}'
    )
