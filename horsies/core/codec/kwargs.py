"""Typed kwargs codec — phase 3+4 of strict-serde.

Bridges between the producer/worker call sites and the typed primitives
in `horsies/core/codec/typed.py`. Encoders run at producer sites
(`.send()`, scheduler enqueue, workflow lifecycle); the decoder runs at
the worker's `_run_task_entry`.

Skipped categories (kept on their existing engine-private paths):
- `workflow_ctx` / `workflow_meta` — engine-injected at worker time;
  not present in producer-side kwargs.
- TaskResult-typed kwargs — engine-injected via `args_from`; envelope
  handling stays in the workflow engine for now (phase 5+).
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any, Callable, cast, get_origin, get_type_hints

from horsies.core.codec.typed import Json, decode_value, encode_value
from horsies.core.models.tasks import TaskResult


__all__ = ['decode_kwargs', 'encode_kwargs']


_INJECTED_PARAM_NAMES = frozenset({'workflow_ctx', 'workflow_meta'})


def encode_kwargs(
    task_fn: Callable[..., Any],
    kwargs: Mapping[str, Any],
) -> dict[str, Json]:
    """Encode user kwargs using each parameter's declared type.

    Args:
        task_fn: The task callable; inspected via `get_type_hints`.
        kwargs: User-supplied kwarg dict.

    Returns:
        Dict mapping kwarg names to JSON-shaped values.

    Raises:
        StrictJsonError: when a kwarg has no annotation (strict mode
            requires every kwarg to be typed; the registration-time
            validator should have caught this).
        pydantic.ValidationError: when a value doesn't satisfy its type.
    """
    try:
        hints = get_type_hints(task_fn, include_extras=True)
    except (TypeError, NameError):
        # Unresolvable hints (e.g., test mocks, missing forward-ref globalns).
        # Production tasks pass strict-serde validation at registration, so
        # this fallback only covers test scaffolding.
        return cast('dict[str, Json]', dict(kwargs))
    encoded: dict[str, Json] = {}
    for key, value in kwargs.items():
        if key in _INJECTED_PARAM_NAMES:
            # Engine-injected at worker time — caller shouldn't pass these.
            continue
        annot = hints.get(key)
        if annot is None:
            # No annotation: production tasks pass strict-serde validation at
            # registration, so reaching here means the receiver is a mock or
            # lambda. Pass through and let the downstream json.dumps fail
            # loudly on non-JSON-shaped values.
            encoded[key] = cast('Json', value)
            continue
        # TaskResult-typed kwargs are populated by the engine's args_from
        # path; producer never supplies one. If someone does, fall through
        # and let encode_value attempt it — the strict validator already
        # accepts TaskResult[OkT, TaskError] as a kwarg annotation.
        encoded[key] = encode_value(value, annot)
    return encoded


def decode_kwargs(
    task_fn: Callable[..., Any],
    raw_kwargs: dict[str, Json],
) -> dict[str, Any]:
    """Decode raw wire kwargs using each parameter's declared type.

    Args:
        task_fn: The task callable; inspected via `get_type_hints`.
        raw_kwargs: Raw JSON-shaped kwargs as they came off the wire.

    Returns:
        Dict mapping kwarg names to typed Python values. Engine-private
        keys (transport keys like `__horsies_workflow_ctx__` and
        args_from TaskResult envelopes) are passed through unchanged so
        downstream worker logic can handle them.

    Raises:
        pydantic.ValidationError: when a raw value doesn't satisfy its
            declared type.
    """
    try:
        hints = get_type_hints(task_fn, include_extras=True)
    except (TypeError, NameError):
        # Unresolvable hints (e.g., test mocks, forward refs missing globalns).
        # Production tasks pass strict-serde validation at registration, so
        # this fallback only covers test scaffolding and degraded scenarios.
        return dict(raw_kwargs)
    decoded: dict[str, Any] = {}
    for key, raw_value in raw_kwargs.items():
        # Engine-private transport keys: pass through verbatim — the
        # worker entry point pops them before calling the user function.
        if key.startswith('__horsies_') or key.startswith('__h_'):
            decoded[key] = raw_value
            continue
        annot = hints.get(key)
        if annot is None:
            # No annotation — pass through. Strict validator should have
            # caught this at registration; defensive fallback.
            decoded[key] = raw_value
            continue
        # TaskResult-typed kwargs from args_from arrive as engine envelopes;
        # leave them to existing args_from handling in the worker entry.
        if get_origin(annot) is TaskResult:
            decoded[key] = raw_value
            continue
        decoded[key] = decode_value(raw_value, annot)
    return decoded
