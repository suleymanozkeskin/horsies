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

from horsies.core.models.tasks import FlattenedException


__all__ = ['flatten_exception']


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
