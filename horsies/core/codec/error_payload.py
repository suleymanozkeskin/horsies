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
from typing import Any, Final

from pydantic import ValidationError

from horsies.core.codec.json_io import dumps_json
from horsies.core.codec.validation_errors import exception_text, validation_summary
from horsies.core.logging import get_logger
from horsies.core.models.tasks import FlattenedException, TaskError, TaskResult
from horsies.core.types.result import is_err


__all__ = [
    'FALLBACK_ERROR_JSON',
    'MAX_EXCEPTION_CHAIN_LINKS',
    'flatten_exception',
    'serialize_error_payload',
]

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


MAX_EXCEPTION_CHAIN_LINKS: Final = 64
"""Exceptions that ``flatten_exception`` visits in a chain and in group members."""

_CAUSE_SEPARATOR: Final = (
    '\nThe above exception was the direct cause of the following exception:\n\n'
)
_CONTEXT_SEPARATOR: Final = (
    '\nDuring handling of the above exception, another exception occurred:\n\n'
)
_GROUP_MEMBERS_NOT_SHOWN: Final = (
    '(sub-exceptions not shown: they hold a validation error)\n'
)


def flatten_exception(exc: BaseException) -> FlattenedException:
    """Convert a live ``BaseException`` to a wire-safe ``FlattenedException``.

    The shape extends the legacy ``_exception_to_json`` triple
    (``type`` / ``message`` / ``traceback``) with ``module`` and
    ``repr`` for class disambiguation. ``traceback`` is
    ``''.join(traceback.format_exception(...))`` when the printed chain
    holds no pydantic ``ValidationError``.

    A pydantic ``ValidationError`` text holds the refused input. Where one
    is ``exc``, a link of its printed chain, or a member of an exception
    group, ``message``, ``repr`` and that part of ``traceback`` use the
    validation summary instead. The walk visits at most
    ``MAX_EXCEPTION_CHAIN_LINKS`` exceptions; past the bound it treats the
    rest as holding a validation error.
    """
    return FlattenedException(
        type=type(exc).__name__,
        module=type(exc).__module__,
        message=exception_text(exc),
        repr=_repr_without_input(exc),
        traceback=_traceback_without_input(exc),
    )


def _repr_without_input(exc: BaseException) -> str:
    match exc:
        case ValidationError():
            return f'{type(exc).__name__}({validation_summary(exc)!r})'
        case BaseExceptionGroup() if _reaches_validation_error(exc):
            return f'{type(exc).__name__}({str(exc)!r})'
        case _:
            return repr(exc)


def _traceback_without_input(exc: BaseException) -> str:
    if not _reaches_validation_error(exc):
        return ''.join(tb.format_exception(type(exc), exc, exc.__traceback__))
    links = _printed_chain(exc)
    blocks: list[str] = []
    if len(links) == MAX_EXCEPTION_CHAIN_LINKS and _next_link(links[-1][0]) is not None:
        blocks.append(
            f'(older chained exceptions not shown: the chain has more than '
            f'{MAX_EXCEPTION_CHAIN_LINKS} links)\n',
        )
    for link, separator in reversed(links):
        blocks.append(_link_text(link))
        if separator is not None:
            blocks.append(separator)
    return ''.join(blocks)


def _printed_chain(exc: BaseException) -> list[tuple[BaseException, str | None]]:
    """The links that ``traceback.format_exception`` prints, newest first.

    Each link carries the separator that is printed after it. The walk stops
    at a repeated link (as the standard formatter does) or at the bound.
    """
    links: list[tuple[BaseException, str | None]] = []
    seen: set[int] = set()
    current: BaseException | None = exc
    separator: str | None = None
    for _ in range(MAX_EXCEPTION_CHAIN_LINKS):
        if current is None or id(current) in seen:
            break
        seen.add(id(current))
        links.append((current, separator))
        match _next_link(current):
            case None:
                current = None
            case (older, older_separator):
                current, separator = older, older_separator
    return links


def _next_link(exc: BaseException) -> tuple[BaseException, str] | None:
    match (exc.__cause__, exc.__context__, exc.__suppress_context__):
        case (BaseException() as cause, _, _):
            return (cause, _CAUSE_SEPARATOR)
        case (None, BaseException() as context, False):
            return (context, _CONTEXT_SEPARATOR)
        case _:
            return None


def _reaches_validation_error(exc: BaseException) -> bool:
    """True when a printed chain link or a group member, at any depth, is a
    pydantic ``ValidationError``. True also when the bound stops the walk
    with exceptions left to visit."""
    pending: list[BaseException] = [exc]
    seen: set[int] = set()
    for _ in range(MAX_EXCEPTION_CHAIN_LINKS):
        if not pending:
            return False
        current = pending.pop()
        if id(current) in seen:
            continue
        seen.add(id(current))
        if isinstance(current, ValidationError):
            return True
        match _next_link(current):
            case None:
                pass
            case (older, _):
                pending.append(older)
        match current:
            case BaseExceptionGroup():
                pending.extend(current.exceptions)
            case _:
                pass
    return bool(pending)


def _link_text(link: BaseException) -> str:
    match link:
        case ValidationError():
            return _header_only_text(link, validation_summary(link))
        case BaseExceptionGroup() if _reaches_validation_error(link):
            return _header_only_text(link, str(link)) + _GROUP_MEMBERS_NOT_SHOWN
        case _:
            return ''.join(
                tb.format_exception(type(link), link, link.__traceback__, chain=False),
            )


def _header_only_text(link: BaseException, text: str) -> str:
    """The frames of ``link`` and one ``<type>: <text>`` line."""
    frames = (
        ['Traceback (most recent call last):\n', *tb.format_tb(link.__traceback__)]
        if link.__traceback__ is not None
        else []
    )
    return ''.join([*frames, f'{_type_label(link)}: {text}\n'])


def _type_label(exc: BaseException) -> str:
    """The type name as ``traceback`` prints it."""
    module = type(exc).__module__
    qualname = type(exc).__qualname__
    match module:
        case '__main__' | 'builtins':
            return qualname
        case _:
            return f'{module}.{qualname}'
