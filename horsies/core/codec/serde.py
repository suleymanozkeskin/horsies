# app/core/codec/serde.py
from __future__ import annotations
from typing import (
    Any,
    Dict,
    List,
    Optional,
    Type,
    Union,
    Mapping,
    Sequence,
    TypeGuard,
    cast,
)
import datetime as dt
import json
import traceback as tb
from pydantic import BaseModel, ValidationError
import dataclasses
from horsies.core.models.tasks import (
    TaskOptions,
    TaskResult,
    TaskError,
    LibraryErrorCode,
)
from horsies.core.types.result import Ok, Err, Result, is_err
from importlib import import_module
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
        ex_json = ex  # already JSON-like or absent (e.g. None)
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
# Caches
# ---------------------------------------------------------------------------

_CLASS_CACHE: Dict[
    str, Type[BaseModel]
] = {}  # cache of resolved Pydantic classes by module name and qualname

_DATACLASS_CACHE: Dict[
    str, type
] = {}  # cache of resolved dataclass types by module name and qualname


def clear_serde_caches() -> None:
    """Clear module-level rehydration caches."""
    _CLASS_CACHE.clear()
    _DATACLASS_CACHE.clear()


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

    # Mapping (dict-like)
    if isinstance(value, Mapping):
        mapping = cast(Mapping[object, object], value)
        result_dict: Dict[str, Json] = {}
        for key, item in mapping.items():
            item_result = to_jsonable(item)
            if is_err(item_result):
                return item_result
            result_dict[str(key)] = item_result.ok_value
        return Ok(result_dict)

    # Sequence (list-like, excluding str/bytes)
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        seq = cast(Sequence[object], value)
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
        return Ok(json.dumps(
            jsonable_result.ok_value,
            ensure_ascii=False,
            separators=(',', ':'),
            allow_nan=False,
        ))
    except (ValueError, TypeError) as exc:
        return Err(SerializationError(f'json.dumps failed: {exc}'))


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
            'retry_policy': task_options.retry_policy.model_dump(mode='json')
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
    """
    if not s:
        return Ok(None)
    try:
        return Ok(json.loads(s))
    except (json.JSONDecodeError, ValueError) as exc:
        return Err(SerializationError(f'JSON parse failed: {exc}'))


def rehydrate_value(value: Json) -> SerdeResult[Any]:
    """Recursively rehydrate a JSON value, restoring typed objects.

    Every recursive step propagates Result. Fixes the ValueError leak
    from datetime.fromisoformat that existed in the old raising version.
    """
    # Pydantic model rehydration
    if isinstance(value, dict) and value.get('__pydantic_model__'):
        module_name = cast(str, value.get('module'))
        qualname = cast(str, value.get('qualname'))
        data = value.get('data')
        cache_key = f'{module_name}:{qualname}'

        try:
            if cache_key in _CLASS_CACHE:
                cls = _CLASS_CACHE[cache_key]
            else:
                try:
                    mod = import_module(module_name)
                except ImportError as e:
                    return Err(SerializationError(
                        f"Could not import module '{module_name}'. "
                        f'Did you move the file without leaving a re-export shim? Error: {e}',
                    ))

                cls = mod
                for part in qualname.split('.'):
                    cls = getattr(cls, part)

                if not (isinstance(cls, type) and issubclass(cls, BaseModel)):
                    return Err(SerializationError(f'{cache_key} is not a BaseModel'))

                _CLASS_CACHE[cache_key] = cls

            return Ok(cls.model_validate(data))

        except Exception as e:
            logger.error(
                f'Failed to rehydrate Pydantic model {cache_key}: {type(e).__name__}: {e}',
            )
            return Err(SerializationError(f'Failed to rehydrate {cache_key}: {e}'))

    # Dataclass rehydration
    if isinstance(value, dict) and value.get('__dataclass__'):
        module_name = cast(str, value.get('module'))
        qualname = cast(str, value.get('qualname'))
        data = value.get('data')
        cache_key = f'{module_name}:{qualname}'

        try:
            if cache_key in _DATACLASS_CACHE:
                dc_cls = _DATACLASS_CACHE[cache_key]
            else:
                try:
                    mod = import_module(module_name)
                except ImportError as e:
                    return Err(SerializationError(
                        f"Could not import module '{module_name}'. "
                        f'Did you move the file without leaving a re-export shim? Error: {e}',
                    ))

                resolved: Any = mod
                for part in qualname.split('.'):
                    resolved = getattr(resolved, part)

                if not isinstance(resolved, type) or not dataclasses.is_dataclass(resolved):
                    return Err(SerializationError(f'{cache_key} is not a dataclass'))

                dc_cls = resolved
                _DATACLASS_CACHE[cache_key] = dc_cls

            if not isinstance(data, dict):
                return Err(SerializationError(
                    f'Dataclass data must be a dict, got {type(data)}',
                ))

            # Rehydrate each field
            rehydrated_data: Dict[str, Any] = {}
            for k, v in data.items():
                field_result = rehydrate_value(v)
                if is_err(field_result):
                    return field_result
                rehydrated_data[k] = field_result.ok_value

            dc_fields = {f.name: f for f in dataclasses.fields(dc_cls)}
            init_kwargs: Dict[str, Any] = {}
            non_init_fields: Dict[str, Any] = {}
            for field_name, field_value in rehydrated_data.items():
                field_def = dc_fields.get(field_name)
                if field_def is None:
                    continue
                if field_def.init:
                    init_kwargs[field_name] = field_value
                else:
                    non_init_fields[field_name] = field_value

            instance = dc_cls(**init_kwargs)
            for fname, fvalue in non_init_fields.items():
                object.__setattr__(instance, fname, fvalue)

            return Ok(instance)

        except Exception as e:
            logger.error(
                f'Failed to rehydrate dataclass {cache_key}: {type(e).__name__}: {e}',
            )
            return Err(SerializationError(f'Failed to rehydrate dataclass {cache_key}: {e}'))

    # Datetime rehydration (datetime before date — subclass ordering)
    # Bug fix: fromisoformat can raise ValueError, which previously leaked
    # as a raw ValueError instead of SerializationError.
    if isinstance(value, dict) and value.get('__datetime__'):
        try:
            return Ok(dt.datetime.fromisoformat(cast(str, value['value'])))
        except (ValueError, KeyError) as exc:
            return Err(SerializationError(f'datetime rehydration failed: {exc}'))

    if isinstance(value, dict) and value.get('__date__'):
        try:
            return Ok(dt.date.fromisoformat(cast(str, value['value'])))
        except (ValueError, KeyError) as exc:
            return Err(SerializationError(f'date rehydration failed: {exc}'))

    if isinstance(value, dict) and value.get('__time__'):
        try:
            return Ok(dt.time.fromisoformat(cast(str, value['value'])))
        except (ValueError, KeyError) as exc:
            return Err(SerializationError(f'time rehydration failed: {exc}'))

    # Nested TaskResult (mutual recursion with task_result_from_json)
    if isinstance(value, dict) and value.get('__task_result__'):
        return task_result_from_json(value)

    # Recursively rehydrate nested dicts
    if isinstance(value, dict):
        result_dict: Dict[str, Any] = {}
        for k, v in value.items():
            v_result = rehydrate_value(v)
            if is_err(v_result):
                return v_result
            result_dict[k] = v_result.ok_value
        return Ok(result_dict)

    # Recursively rehydrate nested lists
    if isinstance(value, list):
        result_list: List[Any] = []
        for item in value:
            item_result = rehydrate_value(item)
            if is_err(item_result):
                return item_result
            result_list.append(item_result.ok_value)
        return Ok(result_list)

    # Primitive — return as-is
    return Ok(value)


def json_to_args(j: Json) -> SerdeResult[List[Any]]:
    """Deserialize a JSON value to a list of arguments."""
    if j is None:
        return Ok([])
    if not isinstance(j, list):
        return Err(SerializationError('Args payload is not a list JSON.'))
    result_list: List[Any] = []
    for item in j:
        item_result = rehydrate_value(item)
        if is_err(item_result):
            return item_result
        result_list.append(item_result.ok_value)
    return Ok(result_list)


def json_to_kwargs(j: Json) -> SerdeResult[Dict[str, Any]]:
    """Deserialize a JSON value to a dictionary of keyword arguments."""
    if j is None:
        return Ok({})
    if not isinstance(j, dict):
        return Err(SerializationError('Kwargs payload is not a dict JSON.'))
    result_dict: Dict[str, Any] = {}
    for k, v in j.items():
        v_result = rehydrate_value(v)
        if is_err(v_result):
            return v_result
        result_dict[k] = v_result.ok_value
    return Ok(result_dict)


def task_result_from_json(j: Json) -> SerdeResult[TaskResult[Any, TaskError]]:
    """Rehydrate a TaskResult from JSON.

    Triple outcome:
    - Ok(TaskResult(ok=value)) — deserialization succeeded, task succeeded
    - Ok(TaskResult(err=TaskError(...))) — deserialization succeeded, task failed
    - Err(SerializationError) — deserialization itself failed (corrupt data)

    The PYDANTIC_HYDRATION_ERROR conversion on the ok-path is deliberate:
    if the user's return type changed between serialization and deserialization,
    the task result should be an error, not a crash.
    """
    if not isinstance(j, dict) or '__task_result__' not in j:
        # Accept legacy "ok"/"err" shape
        if isinstance(j, dict) and ('ok' in j or 'err' in j):
            payload = j
        else:
            return Err(SerializationError('Not a TaskResult JSON'))
    else:
        payload = j

    ok = payload.get('ok', None)
    err = payload.get('err', None)

    # Task returned an error
    if err is not None:
        if isinstance(err, dict) and err.get('__task_error__'):
            err = {k: v for k, v in err.items() if k != '__task_error__'}
        # Bug fix: ValidationError from model_validate was previously unhandled
        try:
            task_err = TaskError.model_validate(err)
        except (ValidationError, Exception) as exc:
            return Err(SerializationError(
                f'Failed to validate TaskError from JSON: {exc}',
            ))
        return Ok(TaskResult(err=task_err))

    # Task returned a success — rehydrate the ok value
    ok_result = rehydrate_value(ok)
    if is_err(ok_result):
        # Rehydration failure becomes a domain-level error, not infrastructure error.
        # The JSON was structurally valid but the ok value couldn't be restored
        # (e.g., user's Pydantic model changed between versions).
        logger.warning(f'PYDANTIC_HYDRATION_ERROR: {ok_result.err_value}')
        return Ok(TaskResult(
            err=TaskError(
                error_code=LibraryErrorCode.PYDANTIC_HYDRATION_ERROR,
                message=str(ok_result.err_value),
                data={},
            ),
        ))
    return Ok(TaskResult(ok=ok_result.ok_value))


# ---------------------------------------------------------------------------
# Safe error serialization
# ---------------------------------------------------------------------------

# Last-resort JSON when serializing an error payload itself fails.
# Hardcoded to avoid infinite recursion in error handlers.
FALLBACK_ERROR_JSON = (
    '{"__task_result__":true,"ok":null,"err":'
    '{"error_code":"WORKER_SERIALIZATION_ERROR",'
    '"message":"secondary serialization failure","data":null}}'
)


def serialize_error_payload(tr: TaskResult[Any, TaskError]) -> str:
    """Serialize a library-constructed TaskResult for error responses.

    Returns the JSON string on success, or a hardcoded fallback if
    serialization somehow fails (should never happen for string-only
    TaskError payloads, but we refuse to raise).
    """
    result = dumps_json(tr)
    if is_err(result):
        logger.error(f'Secondary serialization failure: {result.err_value}')
        return FALLBACK_ERROR_JSON
    return result.ok_value
