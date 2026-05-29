"""Helpers for building library-emitted error payloads.

Strict-serde (design §8) routes library-constructed ``TaskError`` payloads
through ``encode_task_result`` for the wire envelope. The strict codec
cannot emit a raw ``BaseException``, so this module exposes the
``flatten_exception`` primitive that converts a live exception into the
``FlattenedException`` dict shape ``TaskError.exception`` is annotated to
accept on the wire side.

Kept out of ``codec/typed.py`` because typed.py is the generic strict
value codec; exception flattening is a TaskError / error-payload
concern. Kept out of ``models/tasks.py`` because ``models/`` should not
pull in ``traceback`` formatting.
"""

from __future__ import annotations

import traceback as tb
from typing import Any

from horsies.core.codec.json_io import dumps_json
from horsies.core.logging import get_logger
from horsies.core.models.tasks import FlattenedException, TaskError, TaskResult
from horsies.core.types.result import is_err


__all__ = ['flatten_exception', 'serialize_error_payload', 'FALLBACK_ERROR_JSON']

logger = get_logger('error_payload')


# Last-resort JSON when serializing an error payload itself fails.
# Hardcoded to the strict-serde envelope shape (``__h_task_result__``) so the
# wire stays consistent even when the primary encode path fails, and to avoid
# infinite recursion in error handlers.
FALLBACK_ERROR_JSON = (
    '{"__h_task_result__":true,"ok":null,"err":'
    '{"error_code":{"__builtin_task_code__":"WORKER_SERIALIZATION_ERROR"},'
    '"message":"secondary serialization failure","data":null,'
    '"exception":null}}'
)


def serialize_error_payload(tr: TaskResult[Any, TaskError]) -> str:
    """Serialize a library-constructed TaskResult for error responses.

    Routes through ``encode_task_result`` (strict-serde) so the emitted
    envelope matches the worker's success path. The ok slot is always ``None``
    here — these are err-only payloads built by the library itself — and the
    err slot is encoded against the fixed ``TaskError`` schema.

    Live ``BaseException`` on ``TaskError.exception`` is flattened to
    ``FlattenedException`` by ``encode_task_result`` itself, so callers can
    hand us ``TaskResult(err=TaskError(exception=<live exc>))`` without
    pre-flattening.

    Returns the JSON string on success, or a hardcoded fallback if
    serialization fails (should never happen for library-constructed
    TaskError payloads, but we refuse to raise).
    """
    from horsies.core.codec.typed import encode_task_result

    try:
        envelope = encode_task_result(tr, type(None))
    except Exception as exc:
        logger.error(f'encode_task_result failed for library error payload: {exc}')
        return FALLBACK_ERROR_JSON
    result = dumps_json(envelope)
    if is_err(result):
        logger.error(f'Secondary serialization failure: {result.err_value}')
        return FALLBACK_ERROR_JSON
    return result.ok_value


def flatten_exception(exc: BaseException) -> FlattenedException:
    """Convert a live ``BaseException`` to a wire-safe ``FlattenedException``.

    The shape extends the legacy ``_exception_to_json`` triple
    (``type`` / ``message`` / ``traceback``) with ``module`` and
    ``repr`` for class disambiguation. ``traceback`` is the high-value
    diagnostic field — preserved exactly as
    ``''.join(traceback.format_exception(...))`` so downstream tooling
    that grew up on the legacy shape keeps working.
    """
    return FlattenedException(
        type=type(exc).__name__,
        module=type(exc).__module__,
        message=str(exc),
        repr=repr(exc),
        traceback=''.join(tb.format_exception(type(exc), exc, exc.__traceback__)),
    )
