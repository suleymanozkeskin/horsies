"""Typed kwargs codec — phase 3+4 of strict-serde.

Bridges between the producer/worker call sites and the typed primitives
in `horsies/core/codec/typed.py`. Encoders run at producer sites
(`.send()`, scheduler enqueue, workflow lifecycle); the decoder runs at
the worker's `_run_task_entry`.

Strict binding: every kwarg key must correspond to a named parameter
the receiver actually declares. Unknown keys, user-supplied
engine-injected names (`workflow_ctx` / `workflow_meta`), and
user-supplied TaskResult-typed kwargs (the engine populates those via
`args_from`) all fail closed producer-side.

Decode-side mirrors the producer: unknown kwarg names on the wire fail
closed. Engine transport keys (`__h_*` prefix) and TaskResult-typed
envelopes (args_from) pass through unchanged so the worker entry
point's downstream logic can unpack them.

The signature validator now rejects `*args` / `**kwargs` (see
`signature_check.py`), so this module doesn't need a catch-all decode
path — every legitimate wire kwarg name corresponds to a declared
parameter.
"""

from __future__ import annotations

import inspect
from collections.abc import Collection, Mapping
from dataclasses import dataclass
from types import UnionType
from typing import (
    Annotated,
    Any,
    Callable,
    Literal,
    Union,
    assert_never,
    cast,
    get_args,
    get_origin,
    get_type_hints,
)

from horsies.core.codec.json_value import StrictJsonError
from horsies.core.codec.typed import (
    Json,
    decode_value,
    encode_or_raise_on_refusal,
    validate_omitted_default,
)
from horsies.core.models.tasks import TaskResult
from horsies.core.types.result import Err, Ok, Result


__all__ = [
    'KwargBinding',
    'MissingKwargs',
    'decode_kwargs',
    'declared_taskresult_ok_type',
    'decode_subworkflow_kwargs',
    'encode_kwargs_read_back',
    'fill_build_with_defaults',
    'fill_omitted_defaults',
    'is_engine_filled',
    'missing_required_kwargs',
    'resolve_build_with_hints',
    'resolve_hints_and_sig',
    'resolve_producer_kwarg_binding',
    'subworkflow_target_ok_type',
    'underlying_task_fn',
]


def underlying_task_fn(task: Any) -> Callable[..., Any]:
    """Return the original user function wrapped by ``@app.task``.

    The decorator stores the user's callable under ``_original_fn``; older
    builds used ``_fn``. Tests sometimes pass a bare callable. One helper
    so producer (``.send`` / scheduler / workflow lifecycle / workflow
    engine child enqueue) and worker (`_run_task_entry`) all bind against
    the same callable — drift here means the strict signature validator
    runs against a different function than `encode_kwargs` / `decode_kwargs`.

    Args:
        task: The decorated task wrapper, or a bare callable.

    Returns:
        The underlying callable to introspect for ``get_type_hints`` and
        ``inspect.signature``.
    """
    return getattr(task, '_original_fn', getattr(task, '_fn', task))


_INJECTED_PARAM_NAMES: frozenset[str] = frozenset({
    'workflow_ctx',
    'workflow_meta',
})
"""User-facing parameter names the engine fills in at worker time.

Producers must never supply values for these — the engine pre-populates
them on the wire under the `__h_*` transport prefix, and the worker
unpacks them into the named parameter before calling the user
function. A producer-side value would either be silently overwritten
or collide with the engine injection; rejecting at encode is the
honest contract.
"""


_TRANSPORT_KEY_PREFIXES: tuple[str, ...] = ('__h_',)
"""Engine transport-key prefix preserved by decode_kwargs.

Strict-serde §8 narrows the engine-emitted namespace to ``__h_*`` only;
the legacy ``__horsies_*`` prefix has been renamed across all engine
emitters and is rejected at decode by `_scan_reserved_keys` if it
appears in user data. Pass-through here lets the worker entry point pop
the engine-injected keys before invoking the user task.

Engine-emitted ``__h_*`` keys after phase 6:
- ``__h_workflow_ctx__`` (engine → worker, kwarg position)
- ``__h_workflow_meta__`` (engine → worker, kwarg position)
- ``__h_taskresult_envelope__`` (args_from wrapper, kwarg position)
- ``__h_task_result__`` (workflow / task result envelope, not in kwargs)
- ``__h_outputless_terminals__`` (outputless workflow result envelope
  marker, not in kwargs)

See ``codec/typed.py::_RESERVED_INTERNAL_KEYS`` for the authoritative
list.
"""


def resolve_hints_and_sig(
    task_fn: Callable[..., Any],
) -> tuple[dict[str, Any], inspect.Signature] | None:
    """Resolve annotation hints and signature, or None if the callable
    can't be introspected. A caller that has a kwarg to bind refuses None.
    """
    try:
        hints = get_type_hints(task_fn, include_extras=True)
    except (TypeError, NameError):
        return None
    try:
        sig = inspect.signature(task_fn)
    except (TypeError, ValueError):
        return None
    return hints, sig


# Backwards-compatible alias for the previously private name. Kept until
# the next sweep; new code should use ``resolve_hints_and_sig`` directly.
_resolve_hints_and_sig = resolve_hints_and_sig


def _hints_or_raise(
    task_fn: Callable[..., Any],
) -> tuple[dict[str, Any], inspect.Signature]:
    """`resolve_hints_and_sig` for a caller that binds kwargs.

    Raises:
        StrictJsonError: the hints do not resolve, so no kwarg can be checked.
    """
    resolved = resolve_hints_and_sig(task_fn)
    if resolved is None:
        raise StrictJsonError(f'the hints of {_fn_name(task_fn)!r} do not resolve')
    return resolved


def _fn_name(task_fn: Callable[..., Any]) -> str:
    return getattr(task_fn, '__name__', '<callable>')


@dataclass(frozen=True)
class KwargBinding:
    """Result of resolving a single producer-side kwarg against a callable
    signature.

    Shared by ``encode_kwargs`` (raises ``StrictJsonError`` on
    ``kind='error'``) and ``WorkflowSpec._snapshot_kwargs_values``
    (collects errors per-key as workflow validation errors). Single
    source of truth so the two paths cannot drift.

    Producer-side semantics: engine-injected names
    (``workflow_ctx`` / ``workflow_meta``) and TaskResult-typed kwargs
    (engine populates via ``args_from``) are errors here. Decode-side
    treats them differently and uses its own per-key logic.
    """

    kind: Literal['encode', 'error']
    annotation: object | None = None
    error_message: str | None = None


def resolve_producer_kwarg_binding(
    task_fn: Callable[..., Any],
    key: str,
    hints: Mapping[str, Any],
    sig: inspect.Signature,
) -> KwargBinding:
    """Resolve how a producer-supplied kwarg key binds against `task_fn`.

    Returns ``KwargBinding(kind='encode', annotation=annot)`` for valid
    kwargs that need wire encoding. Returns ``KwargBinding(kind='error',
    error_message=...)`` for invalid producer use:

    - kwarg name shadows an engine-injected param
      (``workflow_ctx`` / ``workflow_meta``).
    - kwarg name not declared on the callable's signature, or the name of its
      ``*args`` or ``**params``: a catch-all has no declared type, so it takes
      nothing from a producer.
    - kwarg name declared but missing a type annotation.
    - kwarg name annotated as ``TaskResult`` — engine populates via
      ``args_from``, producer must not supply.

    Caller decides whether ``kind='error'`` raises or accumulates.
    """
    if key in _INJECTED_PARAM_NAMES:
        return KwargBinding(
            kind='error',
            error_message=(
                f'kwarg {key!r} is engine-injected; producer code must '
                f'not supply it (the engine populates {key!r} at worker '
                f'time)'
            ),
        )
    parameter = sig.parameters.get(key)
    if parameter is None or not _is_addressable_named_param(parameter):
        return KwargBinding(
            kind='error',
            error_message=(
                f'unknown kwarg {key!r} for task {_fn_name(task_fn)!r}; '
                f'declared params are {sorted(_named_parameters(sig))}'
            ),
        )
    annot = hints.get(key)
    if annot is None:
        return KwargBinding(
            kind='error',
            error_message=(
                f'kwarg {key!r} has no type annotation on '
                f'{_fn_name(task_fn)!r}'
            ),
        )
    if _annotates_taskresult(annot):
        return KwargBinding(
            kind='error',
            error_message=(
                f'kwarg {key!r} is TaskResult-typed; the workflow engine '
                f'populates it via args_from. Producer must not supply a '
                f'value directly'
            ),
        )
    return KwargBinding(kind='encode', annotation=annot)


def encode_kwargs_read_back(
    task_fn: Callable[..., Any],
    kwargs: Mapping[str, Any],
) -> dict[str, Json]:
    """Encode kwargs that enter the system, and prove each one reads back.

    For `.send()` and schedules. A workflow stores the form that its spec proved.

    Binds strictly: every key must match a declared parameter name on
    `task_fn`. Engine-injected names (`workflow_ctx` / `workflow_meta`)
    and TaskResult-typed parameters fail closed when supplied by the
    producer — the engine populates those on the wire itself.

    Args:
        task_fn: The task callable; inspected via `get_type_hints` and
            `inspect.signature`.
        kwargs: User-supplied kwarg dict.

    Returns:
        Dict mapping kwarg names to JSON-shaped values.

    Raises:
        StrictJsonError: on unknown kwarg name, user-supplied
            engine-injected name, user-supplied TaskResult-typed kwarg,
            or kwarg without a type annotation.
        ValueValidationError: when a value doesn't satisfy its
            declared type, or when the worker could not read it back.
    """
    if not kwargs:
        return {}
    hints, sig = _hints_or_raise(task_fn)
    encoded: dict[str, Json] = {}
    for key, value in kwargs.items():
        binding = resolve_producer_kwarg_binding(task_fn, key, hints, sig)
        if binding.kind == 'error':
            # Single source of truth via the shared resolver — keeps
            # this function and snapshot validation from drifting.
            raise StrictJsonError(binding.error_message or '')
        encoded[key] = encode_or_raise_on_refusal(
            value, binding.annotation, location=(key,),
        )
    return encoded


@dataclass(frozen=True, slots=True)
class MissingKwargs:
    """The required parameters that a direct send does not supply.

    Attributes:
        producer: Parameters that the caller of `.send()` must supply.
        workflow_only: Parameters that only a workflow can fill: an
            engine-injected name or an `args_from` slot.
    """

    producer: tuple[str, ...]
    workflow_only: tuple[str, ...]


def missing_required_kwargs(
    task_fn: Callable[..., Any],
    supplied: Collection[str],
) -> MissingKwargs:
    """List the required parameters of `task_fn` that `supplied` does not hold.

    For a direct send only. A workflow path supplies part of the kwargs and
    the engine fills the rest, so it does not call this.

    A callable with no type hints passes, the same as in `encode_kwargs`:
    `@app.task` guarantees hints on a real task.

    Args:
        task_fn: The task callable.
        supplied: The names of the kwargs that the caller gave.

    Returns:
        The missing names, split by who can supply them. Both tuples are
        empty when nothing is missing.
    """
    resolved = resolve_hints_and_sig(task_fn)
    if resolved is None or not resolved[0]:
        return MissingKwargs(producer=(), workflow_only=())
    hints, sig = resolved
    missing = [
        name
        for name, parameter in sig.parameters.items()
        if parameter.default is inspect.Parameter.empty
        and parameter.kind
        in (inspect.Parameter.KEYWORD_ONLY, inspect.Parameter.POSITIONAL_OR_KEYWORD)
        and name not in supplied
    ]
    workflow_only = tuple(
        name for name in missing if is_engine_filled(name, hints.get(name))
    )
    return MissingKwargs(
        producer=tuple(name for name in missing if name not in workflow_only),
        workflow_only=workflow_only,
    )


def is_engine_filled(name: str, annot: object) -> bool:
    """Whether the engine fills this parameter: an injected name or an `args_from` slot."""
    return name in _INJECTED_PARAM_NAMES or _annotates_taskresult(annot)


def fill_omitted_defaults(
    target_fn: Callable[..., Any],
    kwargs: Mapping[str, Any],
) -> dict[str, Any]:
    """Return `kwargs` with each omitted parameter set to its validated default.

    An omitted parameter crosses no wire, so no decode runs its validators.
    Without this the function gets the raw default, not its declared type.

    Call this after the engine keys are gone, right before the call: a parameter
    that the engine fills is not omitted. Engine-injected names and `args_from`
    slots are skipped, and so is a parameter with no type hint, such as the `cls`
    and `app` of a `build_with`.

    A callable whose hints do not resolve returns unchanged, as in
    `encode_kwargs`.

    Args:
        target_fn: The task function or the `build_with`.
        kwargs: The decoded kwargs.

    Returns:
        A new dict with the validated defaults added.

    Raises:
        StrictJsonError: on a lone surrogate in the text of a validator.
        ValueValidationError: when a default does not hold its declared type,
            or a validator refuses it. The text holds no input value.
    """
    resolved = resolve_hints_and_sig(target_fn)
    if resolved is None or not resolved[0]:
        return dict(kwargs)
    hints, sig = resolved
    return _with_validated_defaults(hints, sig, kwargs)


def fill_build_with_defaults(
    workflow_def: Any,
    kwargs: Mapping[str, Any],
) -> dict[str, Any]:
    """`fill_omitted_defaults` for a child `build_with`.

    Raises:
        StrictJsonError: the hints do not resolve, or a validator wrote a lone
            surrogate in its text.
        ValueValidationError: when a default does not hold its declared type.
    """
    hints, sig = _build_with_hints_or_raise(workflow_def)
    return _with_validated_defaults(hints, sig, kwargs)


def _with_validated_defaults(
    hints: Mapping[str, Any],
    sig: inspect.Signature,
    kwargs: Mapping[str, Any],
) -> dict[str, Any]:
    filled = dict(kwargs)
    for name, parameter in sig.parameters.items():
        if name in filled or parameter.default is inspect.Parameter.empty:
            continue
        annot = hints.get(name)
        if annot is None or is_engine_filled(name, annot):
            continue
        filled[name] = validate_omitted_default(
            parameter.default,
            annot,
            location=(name,),
        )
    return filled


def _annotates_taskresult(annot: Any) -> bool:
    """Whether a parameter annotation marks an ``args_from`` slot.

    ``TaskResult[T, TaskError]`` is the plain form. Under ``join='any'`` /
    ``'quorum'`` a source may be absent, so the documented target annotation is
    ``TaskResult[T, TaskError] | None`` — a union, whose origin is the union
    itself and never ``TaskResult``. Unwrapping it mirrors
    ``models.workflow.typing_utils._extract_taskresult_ok_type``.

    One member is enough, where that function requires exactly one: it must
    name a single Ok type and an ambiguous union has none, while this only
    decides that the slot carries an engine envelope rather than user data.
    """
    if get_origin(annot) is Annotated:
        return _annotates_taskresult(get_args(annot)[0])
    if get_origin(annot) is TaskResult:
        return True
    if get_origin(annot) in (Union, UnionType):
        return any(get_origin(member) is TaskResult for member in get_args(annot))
    return False


def decode_kwargs(
    task_fn: Callable[..., Any],
    raw_kwargs: dict[str, Json],
) -> dict[str, Any]:
    """Decode raw wire kwargs against the receiver's declared types.

    Strict binding mirrors encode: unknown kwarg names on the wire fail
    closed. Engine transport keys (`__h_*` prefix) pass through
    verbatim; TaskResult-typed kwargs pass through raw because the wire
    form is an `__h_taskresult_envelope__` envelope that the worker
    entry point unpacks downstream. A parameter typed
    `TaskResult[T, TaskError] | None` is such a slot too — that is the
    documented shape for an `args_from` target whose source may be
    absent under `join='any'` / `'quorum'`.

    Args:
        task_fn: The task callable; inspected via `get_type_hints` and
            `inspect.signature`.
        raw_kwargs: Raw JSON-shaped kwargs as they came off the wire.

    Returns:
        Dict mapping kwarg names to typed Python values.

    Raises:
        StrictJsonError: on unknown kwarg name on the wire, or kwarg
            without a type annotation.
        ValueValidationError: when a raw value doesn't satisfy its
            declared type.
    """
    if not raw_kwargs:
        return {}
    hints, sig = _hints_or_raise(task_fn)
    decoded: dict[str, Any] = {}
    for key, raw_value in raw_kwargs.items():
        if any(key.startswith(p) for p in _TRANSPORT_KEY_PREFIXES):
            decoded[key] = raw_value
            continue
        if key not in sig.parameters:
            raise StrictJsonError(
                f"unknown kwarg {key!r} for task {_fn_name(task_fn)!r} on "
                f"the wire; declared params are {sorted(sig.parameters)}",
            )
        annot = hints.get(key)
        if annot is None:
            raise StrictJsonError(
                f"kwarg {key!r} has no type annotation on "
                f"{_fn_name(task_fn)!r}",
            )
        if _annotates_taskresult(annot):
            # args_from envelope — handled by the worker entry point
            # after decode_kwargs returns.
            decoded[key] = raw_value
            continue
        decoded[key] = decode_value(raw_value, annot, location=(key,))
    return decoded


def declared_taskresult_ok_type(
    target_fn: Callable[..., Any],
    key: str,
) -> Any | None:
    """Return the `T` of a parameter declared `TaskResult[T, TaskError]`.

    Reads the hints with `Annotated` kept, so a constraint on `T` stays.

    Args:
        target_fn: The receiver: a task function or a `build_with`.
        key: The parameter name.

    Returns:
        The declared ok type, or None when the hints do not resolve, the
        parameter is not a `TaskResult` slot, or `T` is a TypeVar: then
        nothing further can be checked on the value.
    """
    resolved = resolve_hints_and_sig(target_fn)
    if resolved is None:
        return None
    hints, _ = resolved
    return _slot_ok_type(hints.get(key))


def _slot_ok_type(annot: object) -> Any | None:
    """The `T` of a `TaskResult[T, TaskError]` slot, through `Annotated`."""
    from horsies.core.models.workflow.typing_utils import (
        _extract_taskresult_ok_type,
        _normalize_resolved_ok_type,
    )

    if get_origin(annot) is Annotated:
        annot = get_args(annot)[0]
    return _normalize_resolved_ok_type(_extract_taskresult_ok_type(annot))


def subworkflow_target_ok_type(workflow_def: Any, key: str) -> Any | None:
    """`declared_taskresult_ok_type` for a parameter of a child `build_with`."""
    match resolve_build_with_hints(workflow_def):
        case Ok(ok_value=(hints, _)):
            return _slot_ok_type(hints.get(key))
        case Err():
            return None
        case _ as unreachable:
            assert_never(unreachable)


def resolve_build_with_hints(
    workflow_def: Any,
) -> Result[tuple[dict[str, Any], inspect.Signature], str]:
    """Resolve the hints and the signature of a child `build_with`.

    A `build_with` often imports `Horsies` under `TYPE_CHECKING`, so both names
    that the library passes to it are given here. Any other name must resolve in
    the module of the `build_with`.

    A `SubWorkflowNode` crosses the wire, so a caller must refuse an Err: without
    hints no kwarg could be checked.

    Args:
        workflow_def: The child `WorkflowDefinition` class.

    Returns:
        Ok with the hints and the signature, or Err with the cause.
    """
    from horsies.core.app import Horsies
    from horsies.core.models.workflow.spec import WorkflowSpec

    build_with = _resolve_build_with(workflow_def)
    source = inspect.unwrap(build_with)

    def parameters() -> None: ...

    # The return annotation crosses no wire, so a name that only it uses, such
    # as a `WorkflowSpec[Report]` under `TYPE_CHECKING`, must not refuse.
    parameters.__annotations__ = {
        name: annotation
        for name, annotation in getattr(source, '__annotations__', {}).items()
        if name != 'return'
    }
    try:
        hints = get_type_hints(
            parameters,
            globalns=getattr(source, '__globals__', {}),
            localns={'Horsies': Horsies, 'WorkflowSpec': WorkflowSpec},
            include_extras=True,
        )
        return Ok((hints, inspect.signature(build_with)))
    except (TypeError, ValueError, NameError) as error:
        return Err(str(error))


def _resolve_build_with(workflow_def: Any) -> Callable[..., Any]:
    """Return the unwrapped ``build_with`` for type introspection.

    Workflow definitions are wrapped by ``WorkflowDefinitionMeta`` which
    stores the original under ``_original_build_with``. Fall back to
    ``build_with`` directly when the wrapper isn't present (test stubs).
    """
    original = getattr(workflow_def, '_original_build_with', None)
    if callable(original):
        return cast('Callable[..., Any]', original)
    return cast('Callable[..., Any]', workflow_def.build_with)


def _workflow_def_name(workflow_def: Any) -> str:
    return getattr(workflow_def, 'name', None) or workflow_def.__name__


_ADDRESSABLE_PARAM_KINDS: frozenset[int] = frozenset({
    inspect.Parameter.POSITIONAL_OR_KEYWORD.value,
    inspect.Parameter.KEYWORD_ONLY.value,
})
"""Parameter kinds that can be addressed by name in ``SubWorkflowNode.kwargs``.

``VAR_POSITIONAL`` (``*args``) and ``VAR_KEYWORD`` (``**params``) param
*names* show up in ``sig.parameters`` too, but they are NOT addressable
by name — they only describe the catch-all slot. A key that names one is
refused like any unknown key.
"""


def _is_addressable_named_param(param: inspect.Parameter) -> bool:
    """Return True iff `param` is a normal named parameter (not *args/**kwargs)."""
    return param.kind.value in _ADDRESSABLE_PARAM_KINDS


def _named_parameters(sig: inspect.Signature) -> list[str]:
    return [
        name
        for name, param in sig.parameters.items()
        if _is_addressable_named_param(param)
    ]


def _build_with_hints_or_raise(
    workflow_def: Any,
) -> tuple[dict[str, Any], inspect.Signature]:
    """`resolve_build_with_hints` for the wire paths, which must not guess.

    Raises:
        StrictJsonError: the hints do not resolve, so no kwarg can be checked.
    """
    match resolve_build_with_hints(workflow_def):
        case Ok(ok_value=resolved):
            return resolved
        case Err(err_value=cause):
            raise StrictJsonError(
                f'build_with hints of {_workflow_def_name(workflow_def)!r} do not '
                f'resolve: {cause}',
            )
        case _ as unreachable:
            assert_never(unreachable)


def decode_subworkflow_kwargs(
    workflow_def: Any,
    raw_kwargs: Mapping[str, Json],
) -> dict[str, Any]:
    """Decode raw wire kwargs against the child ``build_with`` signature.

    A workflow spec encodes these kwargs, with the same binding rule. A key that
    no named parameter takes is refused, also in a row from an earlier version.

    ``TaskResult``-typed parameters are merged in-process by the engine
    after this function returns (the engine attaches live ``TaskResult``
    instances at the call site); decode must not see them on the wire.

    Raises:
        StrictJsonError: on unknown / TaskResult-typed / unannotated
            wire kwargs.
        ValueValidationError: when a wire value doesn't satisfy its
            declared annotation.
    """
    hints, sig = _build_with_hints_or_raise(workflow_def)
    decoded: dict[str, Any] = {}
    for key, raw_value in raw_kwargs.items():
        param = sig.parameters.get(key)
        if param is not None and _is_addressable_named_param(param):
            annot = hints.get(key)
            if annot is None:
                raise StrictJsonError(
                    f'kwarg {key!r} has no type annotation on build_with '
                    f'for workflow {_workflow_def_name(workflow_def)!r}',
                )
            if get_origin(annot) is TaskResult:
                raise StrictJsonError(
                    f'kwarg {key!r} is TaskResult-typed; static '
                    f'SubWorkflowNode kwargs must not carry args_from '
                    f'values',
                )
            decoded[key] = decode_value(raw_value, annot, location=(key,))
            continue
        raise StrictJsonError(
            f'unknown kwarg {key!r} on the wire for build_with of '
            f'{_workflow_def_name(workflow_def)!r}; declared params are '
            f'{sorted(_named_parameters(sig))}',
        )
    return decoded
