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
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any, Callable, Literal, cast, get_origin, get_type_hints

from horsies.core.codec.json_value import StrictJsonError
from horsies.core.codec.typed import Json, decode_value, encode_value
from horsies.core.models.tasks import TaskResult


__all__ = [
    'KwargBinding',
    'decode_kwargs',
    'decode_subworkflow_kwargs',
    'encode_kwargs',
    'encode_subworkflow_kwargs',
    'resolve_hints_and_sig',
    'resolve_producer_kwarg_binding',
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
    can't be introspected.

    Returning None lets the caller decide between failing open or
    closed. Production tasks pass `@app.task` strict-serde validation
    before reaching here, so unresolvable hints are concentrated in
    test scaffolding (Mock, lambda, forward refs without globalns)
    where the contract is intentionally loose.
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
    - kwarg name not declared on the callable's signature.
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
    if key not in sig.parameters:
        return KwargBinding(
            kind='error',
            error_message=(
                f'unknown kwarg {key!r} for task {_fn_name(task_fn)!r}; '
                f'declared params are {sorted(sig.parameters)}'
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
    if get_origin(annot) is TaskResult:
        return KwargBinding(
            kind='error',
            error_message=(
                f'kwarg {key!r} is TaskResult-typed; the workflow engine '
                f'populates it via args_from. Producer must not supply a '
                f'value directly'
            ),
        )
    return KwargBinding(kind='encode', annotation=annot)


def encode_kwargs(
    task_fn: Callable[..., Any],
    kwargs: Mapping[str, Any],
) -> dict[str, Json]:
    """Encode user kwargs against the receiver's declared types.

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
        pydantic.ValidationError: when a value doesn't satisfy its
            declared type.
    """
    resolved = resolve_hints_and_sig(task_fn)
    if resolved is None:
        # Mock / unresolvable forward refs — production tasks pass
        # `@app.task` strict-serde validation before reaching here, so
        # this only fires for test scaffolding. Pass through and let
        # downstream `dumps_json` fail loudly on non-JSON values.
        return cast('dict[str, Json]', dict(kwargs))
    hints, sig = resolved
    if not hints:
        # Callable with no annotations at all (typically a lambda used
        # as a test mock). Production tasks pass strict-serde validation
        # at registration which guarantees every param is annotated, so
        # an entirely-unannotated callable can't be a real registered
        # task. Pass through.
        return cast('dict[str, Json]', dict(kwargs))
    encoded: dict[str, Json] = {}
    for key, value in kwargs.items():
        binding = resolve_producer_kwarg_binding(task_fn, key, hints, sig)
        if binding.kind == 'error':
            # Single source of truth via the shared resolver — keeps
            # encode_kwargs and snapshot validation from drifting.
            raise StrictJsonError(binding.error_message or '')
        encoded[key] = encode_value(value, binding.annotation)
    return encoded


def decode_kwargs(
    task_fn: Callable[..., Any],
    raw_kwargs: dict[str, Json],
) -> dict[str, Any]:
    """Decode raw wire kwargs against the receiver's declared types.

    Strict binding mirrors encode: unknown kwarg names on the wire fail
    closed. Engine transport keys (`__h_*` prefix) pass through
    verbatim; TaskResult-typed kwargs pass through raw because the wire
    form is an `__h_taskresult_envelope__` envelope that the worker
    entry point unpacks downstream.

    Args:
        task_fn: The task callable; inspected via `get_type_hints` and
            `inspect.signature`.
        raw_kwargs: Raw JSON-shaped kwargs as they came off the wire.

    Returns:
        Dict mapping kwarg names to typed Python values.

    Raises:
        StrictJsonError: on unknown kwarg name on the wire, or kwarg
            without a type annotation.
        pydantic.ValidationError: when a raw value doesn't satisfy its
            declared type.
    """
    resolved = resolve_hints_and_sig(task_fn)
    if resolved is None:
        return dict(raw_kwargs)
    hints, sig = resolved
    if not hints:
        # Test-scaffolding mirror of `encode_kwargs`'s no-hints
        # fallback: lambdas / Mocks have no annotations to bind against;
        # pass through.
        return dict(raw_kwargs)
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
        if get_origin(annot) is TaskResult:
            # args_from envelope — handled by the worker entry point
            # after decode_kwargs returns.
            decoded[key] = raw_value
            continue
        decoded[key] = decode_value(raw_value, annot)
    return decoded


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


def _signature_accepts_var_keyword(sig: inspect.Signature) -> bool:
    """Return True iff the signature declares ``**kwargs``-style catch-all."""
    return any(
        param.kind == inspect.Parameter.VAR_KEYWORD
        for param in sig.parameters.values()
    )


_ADDRESSABLE_PARAM_KINDS: frozenset[int] = frozenset({
    inspect.Parameter.POSITIONAL_OR_KEYWORD.value,
    inspect.Parameter.KEYWORD_ONLY.value,
})
"""Parameter kinds that can be addressed by name in ``SubWorkflowNode.kwargs``.

``VAR_POSITIONAL`` (``*args``) and ``VAR_KEYWORD`` (``**params``) param
*names* show up in ``sig.parameters`` too, but they are NOT addressable
by name — they only describe the catch-all slot. A static kwarg whose
key happens to be ``"args"`` or ``"params"`` against
``build_with(cls, app, *args: Any, **params: Any)`` must fall through
to the ``JsonValue`` fallback, not pick up the catch-all's ``Any``
annotation (which would bypass the strict-encode contract).
"""


def _is_addressable_named_param(param: inspect.Parameter) -> bool:
    """Return True iff `param` is a normal named parameter (not *args/**kwargs).

    Static ``SubWorkflowNode.kwargs`` keys can only bind to
    positional-or-keyword and keyword-only params. VAR_POSITIONAL /
    VAR_KEYWORD names match the catch-all slot, not an addressable
    parameter — they must route through the opaque-kwargs fallback.
    """
    return param.kind.value in _ADDRESSABLE_PARAM_KINDS


def encode_subworkflow_kwargs(
    workflow_def: Any,
    kwargs: Mapping[str, Any],
) -> dict[str, Json]:
    """Encode ``SubWorkflowNode.kwargs`` against the child ``build_with`` signature.

    For each kwarg, the binding resolves against the child workflow's
    unwrapped ``build_with`` callable:

    - Annotated, non-``TaskResult`` parameter → ``encode_value(value, annotation)``.
    - ``TaskResult[...]`` parameter → rejected. Those slots are
      engine-populated via ``args_from`` and must not appear in static
      ``SubWorkflowNode.kwargs``.
    - Unknown key, no ``**kwargs`` catch-all → rejected.
    - Annotated parameter with missing annotation → rejected.
    - Unknown key with opaque ``**params: Any`` catch-all → strict
      ``JsonValue`` fallback. Raw JSON-native values pass through;
      Pydantic models / dataclasses / bytes raise, surfacing the missing
      type declaration as an error instead of silently smuggling a
      ``to_jsonable`` dump onto the wire.

    Raises:
        StrictJsonError: on unknown / TaskResult-typed / unannotated kwargs,
            or non-JSON-native values supplied to an opaque ``**params``.
        pydantic.ValidationError: when a typed value doesn't satisfy its
            declared annotation.
    """
    from horsies.core.codec.json_value import JsonValue

    build_with = _resolve_build_with(workflow_def)
    resolved = resolve_hints_and_sig(build_with)
    if resolved is None:
        # ``inspect.signature`` failed — test scaffolding only; treat
        # every value as ``JsonValue`` (strict).
        return {k: encode_value(v, JsonValue) for k, v in kwargs.items()}
    hints, sig = resolved
    accepts_var_kw = _signature_accepts_var_keyword(sig)
    encoded: dict[str, Json] = {}
    for key, value in kwargs.items():
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
                    f'kwarg {key!r} is TaskResult-typed; the workflow '
                    f'engine populates it via args_from. SubWorkflowNode '
                    f'must not supply a static value',
                )
            encoded[key] = encode_value(value, annot)
            continue
        if accepts_var_kw:
            # Opaque ``**params: Any`` catch-all — JSON-native only.
            # This also catches static kwargs whose key happens to be
            # the VAR_POSITIONAL / VAR_KEYWORD param name itself.
            encoded[key] = encode_value(value, JsonValue)
            continue
        raise StrictJsonError(
            f'unknown kwarg {key!r} for build_with of '
            f'{_workflow_def_name(workflow_def)!r}; declared params are '
            f'{sorted(sig.parameters)}',
        )
    return encoded


def decode_subworkflow_kwargs(
    workflow_def: Any,
    raw_kwargs: Mapping[str, Json],
) -> dict[str, Any]:
    """Decode raw wire kwargs against the child ``build_with`` signature.

    Symmetric to ``encode_subworkflow_kwargs``. Annotated parameters get
    typed decode; opaque ``**params: Any`` slots fall through ``JsonValue``
    (raw JSON-native shapes pass through; non-native shapes never make
    it past encode in the first place, so decode is closed by construction).

    ``TaskResult``-typed parameters are merged in-process by the engine
    after this function returns (the engine attaches live ``TaskResult``
    instances at the call site); decode must not see them on the wire.

    Raises:
        StrictJsonError: on unknown / TaskResult-typed / unannotated
            wire kwargs, or non-JSON-native values under opaque ``**params``.
        pydantic.ValidationError: when a wire value doesn't satisfy its
            declared annotation.
    """
    from horsies.core.codec.json_value import JsonValue

    build_with = _resolve_build_with(workflow_def)
    resolved = resolve_hints_and_sig(build_with)
    if resolved is None:
        return {k: decode_value(v, JsonValue) for k, v in raw_kwargs.items()}
    hints, sig = resolved
    accepts_var_kw = _signature_accepts_var_keyword(sig)
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
            decoded[key] = decode_value(raw_value, annot)
            continue
        if accepts_var_kw:
            decoded[key] = decode_value(raw_value, JsonValue)
            continue
        raise StrictJsonError(
            f'unknown kwarg {key!r} on the wire for build_with of '
            f'{_workflow_def_name(workflow_def)!r}; declared params are '
            f'{sorted(sig.parameters)}',
        )
    return decoded
