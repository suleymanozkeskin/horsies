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
closed. Engine transport keys (`__horsies_*` / `__h_*` prefix) and
TaskResult-typed envelopes (args_from) pass through unchanged so the
worker entry point's downstream logic can handle them.

The signature validator now rejects `*args` / `**kwargs` (see
`signature_check.py`), so this module doesn't need a catch-all decode
path — every legitimate wire kwarg name corresponds to a declared
parameter.
"""

from __future__ import annotations

import inspect
from collections.abc import Mapping
from typing import Any, Callable, cast, get_origin, get_type_hints

from horsies.core.codec.json_value import StrictJsonError
from horsies.core.codec.typed import Json, decode_value, encode_value
from horsies.core.models.tasks import TaskResult


__all__ = ['decode_kwargs', 'encode_kwargs', 'underlying_task_fn']


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
them on the wire under the `__horsies_*` transport prefix, and the
worker unpacks them into the named parameter before calling the user
function. A producer-side value would either be silently overwritten
or collide with the engine injection; rejecting at encode is the
honest contract.
"""


_TRANSPORT_KEY_PREFIXES: tuple[str, ...] = ('__horsies_', '__h_')
"""Engine transport-key prefixes preserved by decode_kwargs.

`__horsies_*` is the legacy form still in active engine use
(`workflows/engine.py` emits `__horsies_workflow_ctx__` etc.). `__h_*`
is the design's target prefix once the rename lands. Both are accepted
as pass-through on decode; the worker entry pops them before calling
the user function.
"""


def _resolve_hints_and_sig(
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


def _fn_name(task_fn: Callable[..., Any]) -> str:
    return getattr(task_fn, '__name__', '<callable>')


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
    resolved = _resolve_hints_and_sig(task_fn)
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
        if key in _INJECTED_PARAM_NAMES:
            raise StrictJsonError(
                f"kwarg {key!r} is engine-injected; producer code must "
                f"not supply it (the engine populates {key!r} at worker "
                f"time)",
            )
        if key not in sig.parameters:
            raise StrictJsonError(
                f"unknown kwarg {key!r} for task {_fn_name(task_fn)!r}; "
                f"declared params are {sorted(sig.parameters)}",
            )
        annot = hints.get(key)
        if annot is None:
            # Strict validator at registration rejects untyped params,
            # so this is unreachable for real registered tasks. Fail
            # closed here for defense in depth.
            raise StrictJsonError(
                f"kwarg {key!r} has no type annotation on "
                f"{_fn_name(task_fn)!r}",
            )
        if get_origin(annot) is TaskResult:
            raise StrictJsonError(
                f"kwarg {key!r} is TaskResult-typed; the workflow engine "
                f"populates it via args_from. Producer must not supply a "
                f"value directly",
            )
        encoded[key] = encode_value(value, annot)
    return encoded


def decode_kwargs(
    task_fn: Callable[..., Any],
    raw_kwargs: dict[str, Json],
) -> dict[str, Any]:
    """Decode raw wire kwargs against the receiver's declared types.

    Strict binding mirrors encode: unknown kwarg names on the wire fail
    closed. Engine transport keys (`__horsies_*` / `__h_*` prefix) pass
    through verbatim; TaskResult-typed kwargs pass through raw because
    the wire form is an `__horsies_taskresult__` envelope that the
    worker entry point unpacks downstream.

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
    resolved = _resolve_hints_and_sig(task_fn)
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
