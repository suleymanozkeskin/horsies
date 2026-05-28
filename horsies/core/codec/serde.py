# app/core/codec/serde.py
from __future__ import annotations
from typing import (
    Any,
    Dict,
    List,
    Optional,
    Union,
    Mapping,
    TypeGuard,
    cast,
)
import datetime as dt
import json
import traceback as tb
from pydantic import BaseModel
import dataclasses
from horsies.core.codec.json_value import (
    StrictJsonError,
    _reject_nonstandard_json_constant,
)
from horsies.core.models.tasks import (
    TaskOptions,
    TaskResult,
    TaskError,
)
from horsies.core.types.result import Ok, Err, Result, is_err
from horsies.core.logging import get_logger

logger = get_logger('serde')


Json = Union[None, bool, int, float, str, List['Json'], Dict[str, 'Json']]
"""
Union type for JSON-serializable values.
"""

type SerdeResult[T] = Result[T, SerializationError]


class SerializationError(Exception):
    """
    Raised when a value cannot be serialized to or deserialized from JSON.
    """

    pass


# ---------------------------------------------------------------------------
# Infallible helpers (no Result needed)
# ---------------------------------------------------------------------------


def _exception_to_json(ex: BaseException) -> Dict[str, Json]:
    """Convert a BaseException to a JSON-serializable dictionary."""
    return {
        'type': type(ex).__name__,
        'message': str(ex),
        'traceback': ''.join(tb.format_exception(type(ex), ex, ex.__traceback__)),
    }


def _task_error_to_json(err: TaskError) -> SerdeResult[Dict[str, Json]]:
    """Convert a TaskError to a JSON-serializable dictionary.

    Handles the exception field manually to avoid pydantic trying to
    serialize BaseException subclasses.
    """
    ex = err.exception
    try:
        data = err.model_dump(mode='json', exclude={'exception'})
    except Exception as exc:
        return Err(SerializationError(
            f'Failed to serialize TaskError: {exc}',
        ))

    if isinstance(ex, BaseException):
        ex_json: Optional[Dict[str, Json]] = _exception_to_json(ex)
    elif isinstance(ex, dict) or ex is None:
        # ``FlattenedException`` is structurally ``dict[str, str]`` which
        # satisfies ``Dict[str, Json]``; pyright doesn't infer this
        # narrowing through ``isinstance(ex, dict)`` on a TypedDict union.
        ex_json = cast(Optional[Dict[str, Json]], ex)
    else:
        # Unknown type: coerce to a simple shape of string
        ex_json = {'type': type(ex).__name__, 'message': str(ex)}

    if ex_json is not None:
        data['exception'] = ex_json

    return Ok({'__task_error__': True, **data})


def _is_task_result(value: Any) -> TypeGuard[TaskResult[Any, TaskError]]:
    """Type guard to properly narrow TaskResult types."""
    return isinstance(value, TaskResult)


# ---------------------------------------------------------------------------
# Serialization path (Python → JSON)
# ---------------------------------------------------------------------------


def _qualified_class_path(cls: type) -> SerdeResult[tuple[str, str]]:
    """Get module and qualname for a class, validating importability.

    Returns Err if the class is defined in __main__ or inside a function.
    """
    module_name = cls.__module__
    qualname = cls.__qualname__

    if module_name in ('__main__', '__mp_main__'):
        return Err(SerializationError(
            f"Cannot serialize '{qualname}' because it is defined in '__main__'. "
            'Please move this class to a separate module (file) so it can be imported by the worker.',
        ))

    if '<locals>' in qualname:
        return Err(SerializationError(
            f"Cannot serialize '{qualname}' because it is a local class defined inside a function. "
            'Please move this class to module level so it can be imported by the worker.',
        ))

    return Ok((module_name, qualname))


def _qualified_model_path(model: BaseModel) -> SerdeResult[tuple[str, str]]:
    """Get qualified path for a Pydantic BaseModel instance."""
    return _qualified_class_path(type(model))


def _qualified_dataclass_path(instance: Any) -> SerdeResult[tuple[str, str]]:
    """Get qualified path for a dataclass instance."""
    return _qualified_class_path(type(instance))


def to_jsonable(value: Any) -> SerdeResult[Json]:
    """Convert a Python value to a JSON-serializable form.

    Every recursive step propagates Result — a failure at any nesting
    depth surfaces as Err to the caller.
    """
    # Primitives — always safe
    if value is None or isinstance(value, (bool, int, float, str)):
        return Ok(value)

    # datetime.datetime is a subclass of datetime.date — check datetime first.
    if isinstance(value, dt.datetime):
        return Ok({'__datetime__': True, 'value': value.isoformat()})

    if isinstance(value, dt.date):
        return Ok({'__date__': True, 'value': value.isoformat()})

    if isinstance(value, dt.time):
        return Ok({'__time__': True, 'value': value.isoformat()})

    # Is value a `TaskResult`?
    if _is_task_result(value):
        ok_json: Json = None
        if value.is_ok():
            ok_result = to_jsonable(value.ok)
            if is_err(ok_result):
                return ok_result
            ok_json = ok_result.ok_value
        err_json: Optional[Dict[str, Json]] = None
        if value.err is not None:
            if isinstance(value.err, TaskError):
                task_err_result = _task_error_to_json(value.err)
                if is_err(task_err_result):
                    return task_err_result
                err_json = task_err_result.ok_value
            elif isinstance(value.err, BaseModel):
                err_json = value.err.model_dump()  # if someone used a model for error
            else:
                err_json = {'message': str(value.err)}
        return Ok({'__task_result__': True, 'ok': ok_json, 'err': err_json})

    # TaskError (standalone)
    if isinstance(value, TaskError):
        return _task_error_to_json(value)

    # Pydantic BaseModel
    if isinstance(value, BaseModel):
        path_result = _qualified_model_path(value)
        if is_err(path_result):
            return path_result
        module, qualname = path_result.ok_value
        return Ok({
            '__pydantic_model__': True,
            'module': module,
            'qualname': qualname,
            'data': value.model_dump(mode='json'),
        })

    # Dataclass
    if dataclasses.is_dataclass(value) and not isinstance(value, type):
        path_result = _qualified_dataclass_path(value)
        if is_err(path_result):
            return path_result
        module, qualname = path_result.ok_value
        field_data: Dict[str, Json] = {}
        for field in dataclasses.fields(value):
            field_value = getattr(value, field.name)
            field_result = to_jsonable(field_value)
            if is_err(field_result):
                return field_result
            field_data[field.name] = field_result.ok_value
        return Ok({
            '__dataclass__': True,
            'module': module,
            'qualname': qualname,
            'data': field_data,
        })

    # Mapping (dict-like). JSON object keys must be strings — reject non-str
    # keys rather than str()-coercing them, which is lossy on round-trip
    # (e.g. {1: ...} would come back as {'1': ...}). Matches the strict-serde
    # fence in `_validate_json_native`.
    if isinstance(value, Mapping):
        mapping = cast(Mapping[object, object], value)
        result_dict: Dict[str, Json] = {}
        for key, item in mapping.items():
            if not isinstance(key, str):
                return Err(SerializationError(
                    f'Mapping key must be str, got {type(key).__name__}: {key!r}',
                ))
            item_result = to_jsonable(item)
            if is_err(item_result):
                return item_result
            result_dict[key] = item_result.ok_value
        return Ok(result_dict)

    # List → JSON array. tuple/set/range and other sequences are NOT
    # JSON-native; reject them (they fall through to the Err below) rather
    # than silently coercing tuple -> list, which is lossy on round-trip.
    # Matches the strict-serde fence in `_validate_json_native`.
    if isinstance(value, list):
        seq = cast(List[object], value)
        result_list: List[Json] = []
        for item in seq:
            item_result = to_jsonable(item)
            if is_err(item_result):
                return item_result
            result_list.append(item_result.ok_value)
        return Ok(result_list)

    return Err(SerializationError(
        f'Cannot serialize value of type {type(value).__name__}',
    ))


def dumps_json(value: Any) -> SerdeResult[str]:
    """Serialize a Python value to a JSON string."""
    jsonable_result = to_jsonable(value)
    if is_err(jsonable_result):
        return jsonable_result
    try:
        encoded = json.dumps(
            jsonable_result.ok_value,
            ensure_ascii=False,
            separators=(',', ':'),
            allow_nan=False,
        )
    except (ValueError, TypeError) as exc:
        return Err(SerializationError(f'json.dumps failed: {exc}'))
    # ensure_ascii=False emits lone UTF-16 surrogates verbatim; they are not
    # UTF-8 encodable and would otherwise fail far downstream in the Postgres
    # TEXT insert. Fail closed here at the producer-side encode boundary.
    try:
        encoded.encode('utf-8')
    except UnicodeEncodeError as exc:
        return Err(SerializationError(
            f'serialized value is not valid UTF-8 (lone surrogate?): {exc}'
        ))
    return Ok(encoded)


def args_to_json(args: tuple[Any, ...]) -> SerdeResult[str]:
    """Serialize a tuple of positional arguments to a JSON string."""
    return dumps_json(list(args))


def kwargs_to_json(kwargs: dict[str, Any]) -> SerdeResult[str]:
    """Serialize a dictionary of keyword arguments to a JSON string."""
    return dumps_json(kwargs)


def serialize_task_options(task_options: TaskOptions) -> SerdeResult[str]:
    """Serialize TaskOptions to a JSON string."""
    return dumps_json(
        {
            'retry_policy': task_options.retry_policy.model_dump(
                mode='json',
                exclude_none=True,
            )
            if task_options.retry_policy
            else None,
            'good_until': task_options.good_until.isoformat()
            if task_options.good_until
            else None,
        },
    )


# ---------------------------------------------------------------------------
# Deserialization path (JSON → Python)
# ---------------------------------------------------------------------------


def loads_json(s: Optional[str]) -> SerdeResult[Json]:
    """Deserialize a JSON string.

    Returns Ok(None) for empty/None input. Wraps json.JSONDecodeError
    as SerializationError so callers handle a single error type.

    Routes through `parse_constant=_reject_nonstandard_json_constant`
    so Python's lenient acceptance of `NaN` / `Infinity` / `-Infinity`
    (not RFC 8259) fails closed at every raw-load site instead of
    smuggling non-finite floats past the producer-side strict fence.
    """
    if not s:
        return Ok(None)
    try:
        return Ok(json.loads(s, parse_constant=_reject_nonstandard_json_constant))
    except StrictJsonError as exc:
        return Err(SerializationError(f'JSON parse failed: {exc}'))
    except (json.JSONDecodeError, ValueError) as exc:
        return Err(SerializationError(f'JSON parse failed: {exc}'))


# ---------------------------------------------------------------------------
# Safe error serialization
# ---------------------------------------------------------------------------

# Last-resort JSON when serializing an error payload itself fails.
# Hardcoded to the strict-serde envelope shape (``__h_task_result__``)
# so the wire stays consistent even when the primary encode path fails.
# Hardcoded to avoid infinite recursion in error handlers.
FALLBACK_ERROR_JSON = (
    '{"__h_task_result__":true,"ok":null,"err":'
    '{"error_code":{"__builtin_task_code__":"WORKER_SERIALIZATION_ERROR"},'
    '"message":"secondary serialization failure","data":null,'
    '"exception":null}}'
)


def serialize_error_payload(tr: TaskResult[Any, TaskError]) -> str:
    """Serialize a library-constructed TaskResult for error responses.

    Strict-serde phase 5 routes through ``encode_task_result`` (not the
    legacy ``dumps_json(tr)`` path) so the emitted envelope matches the
    worker's success path. The ok slot is always ``None`` here — these
    are err-only payloads built by the library itself — and the err
    slot is encoded against the fixed ``TaskError`` schema (path-aware
    scan for the built-in code discriminator).

    Live ``BaseException`` on ``TaskError.exception`` is flattened to
    ``FlattenedException`` by ``encode_task_result`` itself (single
    source of truth for that invariant), so callers can hand us
    ``TaskResult(err=TaskError(exception=<live exc>))`` without
    pre-flattening.

    Returns the JSON string on success, or a hardcoded fallback if
    serialization fails (should never happen for library-constructed
    TaskError payloads, but we refuse to raise).
    """
    from horsies.core.codec.typed import encode_task_result

    try:
        envelope = encode_task_result(tr, type(None))
    except Exception as exc:
        logger.error(
            f'encode_task_result failed for library error payload: {exc}',
        )
        return FALLBACK_ERROR_JSON
    result = dumps_json(envelope)
    if is_err(result):
        logger.error(f'Secondary serialization failure: {result.err_value}')
        return FALLBACK_ERROR_JSON
    return result.ok_value
