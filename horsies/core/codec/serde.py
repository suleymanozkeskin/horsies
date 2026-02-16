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
from pydantic import BaseModel
import dataclasses
from horsies.core.models.tasks import (
    TaskOptions,
    TaskResult,
    TaskError,
    LibraryErrorCode,
)
from importlib import import_module
from horsies.core.logging import get_logger

logger = get_logger('serde')


Json = Union[None, bool, int, float, str, List['Json'], Dict[str, 'Json']]
"""
Union type for JSON-serializable values.
"""


class SerializationError(Exception):
    """
    Raised when a value cannot be serialized to JSON.
    """

    pass


def _exception_to_json(ex: BaseException) -> Dict[str, Json]:
    """
    Convert a BaseException to a JSON-serializable dictionary.

    Returns:
        A dict with following key-value pairs:
        - "type": str
        - "message": str
        - "traceback": str
    """
    return {
        'type': type(ex).__name__,
        'message': str(ex),
        'traceback': ''.join(tb.format_exception(type(ex), ex, ex.__traceback__)),
    }


def _task_error_to_json(err: TaskError) -> Dict[str, Json]:
    """
    Convert a `TaskError` BaseModel to a JSON-serializable dictionary.
    After converting to JSON, if the `exception` field is a `BaseException`,
    it will be converted to a JSON-serializable dictionary.
    If the `exception` field is already a JSON-serializable dictionary, it will be returned as is.
    If the `exception` field is not a `BaseException` or a JSON-serializable dictionary,
    it will be coerced to a simple shape of string.
    For more information, see `TaskError` model definition.

    Args:
        err: The `TaskError` BaseModel to convert to JSON.

    Returns:
        A dict with following key-value pairs:
        - "__task_error__": bool
        - "error_code": str | LibraryErrorCode
        - "message": str
        - "data": dict[str, Json]
    """
    # data = err.model_dump(mode="json")
    # ex = data.pop("exception", None)

    # Avoid pydantic trying to serialize Exception; handle it manually
    ex = err.exception
    data = err.model_dump(mode='json', exclude={'exception'})

    if isinstance(ex, BaseException):
        ex_json: Optional[Dict[str, Json]] = _exception_to_json(ex)
    elif isinstance(ex, dict) or ex is None:
        ex_json = ex  # already JSON-like or absent (e.g. None)
    else:
        # Unknown type: coerce to a simple shape of string
        ex_json = {'type': type(ex).__name__, 'message': str(ex)}

    if ex_json is not None:
        data['exception'] = ex_json

    return {'__task_error__': True, **data}


def _is_task_result(value: Any) -> TypeGuard[TaskResult[Any, TaskError]]:
    """Type guard to properly narrow TaskResult types."""
    return isinstance(value, TaskResult)


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


def _qualified_class_path(cls: type) -> tuple[str, str]:
    """
    Get the module and qualname for a class, with validation for importability.

    Raises SerializationError if the class is not importable by workers:
    - Defined in __main__ (entrypoint script)
    - Defined inside a function (local class with <locals> in qualname)
    """
    module_name = cls.__module__
    qualname = cls.__qualname__

    # STRICT CHECK: Refuse to serialize classes defined in the entrypoint script
    if module_name in ('__main__', '__mp_main__'):
        raise SerializationError(
            f"Cannot serialize '{qualname}' because it is defined in '__main__'. "
            'Please move this class to a separate module (file) so it can be imported by the worker.'
        )

    # STRICT CHECK: Refuse to serialize local classes (defined inside functions)
    if '<locals>' in qualname:
        raise SerializationError(
            f"Cannot serialize '{qualname}' because it is a local class defined inside a function. "
            'Please move this class to module level so it can be imported by the worker.'
        )

    return (module_name, qualname)


def _qualified_model_path(model: BaseModel) -> tuple[str, str]:
    """Get qualified path for a Pydantic BaseModel instance."""
    return _qualified_class_path(type(model))


def _qualified_dataclass_path(instance: Any) -> tuple[str, str]:
    """Get qualified path for a dataclass instance."""
    return _qualified_class_path(type(instance))


def to_jsonable(value: Any) -> Json:
    """
    Convert value to JSON with special handling for Pydantic models, TaskError, TaskResult.

    Args:
        value: The value to convert to JSON.

    Returns:
        A JSON-serializable value. For more information, see `Json` Union type.
    """
    # Fast O(1) primitive check — the recursive _is_json_native() guard was
    # removed here because the Mapping/Sequence branches below already handle
    # dicts and lists recursively, making the deep pre-scan redundant (O(2N)).
    if value is None or isinstance(value, (bool, int, float, str)):
        return value

    # datetime.datetime is a subclass of datetime.date — check datetime first.
    if isinstance(value, dt.datetime):
        return {'__datetime__': True, 'value': value.isoformat()}

    if isinstance(value, dt.date):
        return {'__date__': True, 'value': value.isoformat()}

    if isinstance(value, dt.time):
        return {'__time__': True, 'value': value.isoformat()}

    # Is value a `TaskResult`?
    if _is_task_result(value):
        # Represent discriminated union explicitly
        ok_json = to_jsonable(value.ok) if value.is_ok() else None
        err_json: Optional[Dict[str, Json]] = None
        if value.err is not None:
            if isinstance(value.err, TaskError):
                err_json = _task_error_to_json(value.err)
            elif isinstance(value.err, BaseModel):
                err_json = value.err.model_dump()  # if someone used a model for error
            else:
                # last resort: stringify
                err_json = {'message': str(value.err)}
        return {'__task_result__': True, 'ok': ok_json, 'err': err_json}

    # Is value a `TaskError`?
    if isinstance(value, TaskError):
        return _task_error_to_json(value)

    # Is value a `BaseModel`?
    if isinstance(value, BaseModel):
        # Include type metadata so we can rehydrate on the other side
        module, qualname = _qualified_model_path(value)
        return {
            '__pydantic_model__': True,
            'module': module,
            'qualname': qualname,
            # Use mode="json" to ensure JSON-compatible field values
            'data': value.model_dump(mode='json'),
        }

    # Dataclass support - serialize with metadata for round-trip reconstruction
    # Use field-by-field conversion instead of asdict() to preserve nested type metadata
    if dataclasses.is_dataclass(value) and not isinstance(value, type):
        module, qualname = _qualified_dataclass_path(value)
        # Convert each field via to_jsonable to preserve nested Pydantic/dataclass metadata
        field_data: Dict[str, Json] = {}
        for field in dataclasses.fields(value):
            field_value = getattr(value, field.name)
            field_data[field.name] = to_jsonable(field_value)
        return {
            '__dataclass__': True,
            'module': module,
            'qualname': qualname,
            'data': field_data,
        }

    # Handle dictionary-like objects (Mappings). This is a generic way to handle
    # not only `dict` but also other dictionary-like types such as `OrderedDict`
    # or `defaultdict`. It ensures all keys are strings and that values are
    # recursively made JSON-serializable.
    if isinstance(value, Mapping):
        mapping = cast(Mapping[object, object], value)
        return {str(key): to_jsonable(item) for key, item in mapping.items()}

    # Handle list-like objects (Sequences). This handles not only `list` but also
    # other sequence types like `tuple` or `set`. The check excludes `str`,
    # `bytes`, and `bytearray`, as they are treated as primitive types rather
    # than sequences of characters. It recursively ensures all items in the
    # sequence are JSON-serializable.
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        seq = cast(Sequence[object], value)
        return [to_jsonable(item) for item in seq]

    raise SerializationError(f'Cannot serialize value of type {type(value).__name__}')


def dumps_json(value: Any) -> str:
    """
    Serialize a value to JSON string.

    Args:
        value: The value to serialize.

    Returns:
        A JSON string.
    """
    return json.dumps(
        to_jsonable(value),
        ensure_ascii=False,
        separators=(',', ':'),
        allow_nan=False,  # Prevent NaN values in JSON
    )


def loads_json(s: Optional[str]) -> Json:
    """
    Deserialize a JSON string to a JSON value.

    Args:
        s: The JSON string to deserialize.

    Returns:
        A JSON value. For more information, see `Json` Union type.
    """
    return json.loads(s) if s else None


def args_to_json(args: tuple[Any, ...]) -> str:
    """
    Serialize a tuple of arguments to a JSON string.

    Args:
        args: The tuple of arguments to serialize.

    Returns:
        A JSON string.
    """
    return dumps_json(list(args))


def kwargs_to_json(kwargs: dict[str, Any]) -> str:
    """
    Serialize a dictionary of keyword arguments to a JSON string.

    Args:
        kwargs: The dictionary of keyword arguments to serialize.

    Returns:
        A JSON string.
    """
    return dumps_json(kwargs)


def rehydrate_value(value: Json) -> Any:
    """
    Recursively rehydrate a JSON value, restoring Pydantic models from their serialized form.

    Args:
        value: The JSON value to rehydrate.

    Returns:
        The rehydrated value with Pydantic models restored.

    Raises:
        SerializationError: If a Pydantic model cannot be rehydrated.
    """
    # Handle Pydantic model rehydration
    if isinstance(value, dict) and value.get('__pydantic_model__'):
        module_name = cast(str, value.get('module'))
        qualname = cast(str, value.get('qualname'))
        data = value.get('data')

        cache_key = f'{module_name}:{qualname}'

        try:
            # 1. Check Cache
            if cache_key in _CLASS_CACHE:
                cls = _CLASS_CACHE[cache_key]
            else:
                # 2. Dynamic Import
                try:
                    module = import_module(module_name)
                except ImportError as e:
                    raise SerializationError(
                        f"Could not import module '{module_name}'. "
                        f'Did you move the file without leaving a re-export shim? Error: {e}'
                    )

                # 3. Resolve Class
                cls = module
                # Handle nested classes (e.g. ClassA.ClassB)
                for part in qualname.split('.'):
                    cls = getattr(cls, part)

                if not (isinstance(cls, type) and issubclass(cls, BaseModel)):
                    raise SerializationError(f'{cache_key} is not a BaseModel')

                # 4. Save to Cache
                _CLASS_CACHE[cache_key] = cls

            # 5. Validate/Hydrate
            return cls.model_validate(data)

        except Exception as e:
            # Catch Pydantic ValidationErrors or AttributeErrors here
            logger.error(
                f'Failed to rehydrate Pydantic model {cache_key}: {type(e).__name__}: {e}'
            )
            raise SerializationError(f'Failed to rehydrate {cache_key}: {str(e)}')

    # Handle dataclass rehydration
    if isinstance(value, dict) and value.get('__dataclass__'):
        module_name = cast(str, value.get('module'))
        qualname = cast(str, value.get('qualname'))
        data = value.get('data')

        cache_key = f'{module_name}:{qualname}'

        try:
            # 1. Check Cache
            if cache_key in _DATACLASS_CACHE:
                dc_cls = _DATACLASS_CACHE[cache_key]
            else:
                # 2. Dynamic Import
                try:
                    module = import_module(module_name)
                except ImportError as e:
                    raise SerializationError(
                        f"Could not import module '{module_name}'. "
                        f'Did you move the file without leaving a re-export shim? Error: {e}'
                    )

                # 3. Resolve Class
                resolved: Any = module
                # Handle nested classes (e.g. ClassA.ClassB)
                for part in qualname.split('.'):
                    resolved = getattr(resolved, part)

                if not isinstance(resolved, type) or not dataclasses.is_dataclass(
                    resolved
                ):
                    raise SerializationError(f'{cache_key} is not a dataclass')

                dc_cls = resolved
                # 4. Save to Cache
                _DATACLASS_CACHE[cache_key] = dc_cls

            # 5. Instantiate dataclass with rehydrated field values
            if not isinstance(data, dict):
                raise SerializationError(
                    f'Dataclass data must be a dict, got {type(data)}'
                )

            # Rehydrate each field to restore nested Pydantic/dataclass types
            rehydrated_data = {k: rehydrate_value(v) for k, v in data.items()}

            # Separate init=True fields from init=False fields
            dc_fields = {f.name: f for f in dataclasses.fields(dc_cls)}
            init_kwargs: Dict[str, Any] = {}
            non_init_fields: Dict[str, Any] = {}
            for field_name, field_value in rehydrated_data.items():
                field_def = dc_fields.get(field_name)
                if field_def is None:
                    # Field not in dataclass definition - skip (could be removed field)
                    continue
                if field_def.init:
                    init_kwargs[field_name] = field_value
                else:
                    non_init_fields[field_name] = field_value

            # Construct with init fields only
            instance = dc_cls(**init_kwargs)

            # Set non-init fields directly on the instance
            for fname, fvalue in non_init_fields.items():
                object.__setattr__(instance, fname, fvalue)

            return instance

        except SerializationError:
            raise
        except Exception as e:
            logger.error(
                f'Failed to rehydrate dataclass {cache_key}: {type(e).__name__}: {e}'
            )
            raise SerializationError(
                f'Failed to rehydrate dataclass {cache_key}: {str(e)}'
            )

    # Handle datetime rehydration (datetime before date — subclass ordering)
    if isinstance(value, dict) and value.get('__datetime__'):
        return dt.datetime.fromisoformat(cast(str, value['value']))

    if isinstance(value, dict) and value.get('__date__'):
        return dt.date.fromisoformat(cast(str, value['value']))

    if isinstance(value, dict) and value.get('__time__'):
        return dt.time.fromisoformat(cast(str, value['value']))

    # Handle nested TaskResult rehydration
    if isinstance(value, dict) and value.get('__task_result__'):
        return task_result_from_json(value)

    # Recursively rehydrate nested dicts
    if isinstance(value, dict):
        return {k: rehydrate_value(v) for k, v in value.items()}

    # Recursively rehydrate nested lists
    if isinstance(value, list):
        return [rehydrate_value(item) for item in value]

    # Return primitive values as-is
    return value


def json_to_args(j: Json) -> List[Any]:
    """
    Deserialize a JSON value to a list of arguments, rehydrating Pydantic models.

    Args:
        j: The JSON value to deserialize.

    Returns:
        A list of arguments with Pydantic models rehydrated.

    Raises:
        SerializationError: If the JSON value is not a list.
    """
    if j is None:
        return []
    if isinstance(j, list):
        return [rehydrate_value(item) for item in j]
    raise SerializationError('Args payload is not a list JSON.')


def json_to_kwargs(j: Json) -> Dict[str, Any]:
    """
    Deserialize a JSON value to a dictionary of keyword arguments, rehydrating Pydantic models.

    Args:
        j: The JSON value to deserialize.

    Returns:
        A dictionary of keyword arguments with Pydantic models rehydrated.

    Raises:
        SerializationError: If the JSON value is not a dict.
    """
    if j is None:
        return {}
    if isinstance(j, dict):
        return {k: rehydrate_value(v) for k, v in j.items()}
    raise SerializationError('Kwargs payload is not a dict JSON.')


def task_result_from_json(j: Json) -> TaskResult[Any, TaskError]:
    """
    Rehydrate `TaskResult` from JSON.
    NOTES:
        - We don't recreate `Exception` objects;
        - We keep the flattened structure inside `TaskError.exception` (as dict) or `None`.

    Args:
        j: The JSON string to deserialize.

    Returns:
        A `TaskResult`.
    """
    if not isinstance(j, dict) or '__task_result__' not in j:
        # Accept legacy "ok"/"err" shape if present
        if isinstance(j, dict) and ('ok' in j or 'err' in j):
            payload = j
        else:
            raise SerializationError('Not a TaskResult JSON')
    else:
        payload = j

    ok = payload.get('ok', None)
    err = payload.get('err', None)

    # meaning task itself returned an error
    if err is not None:
        # Build TaskError from dict, letting pydantic validate
        if isinstance(err, dict) and err.get('__task_error__'):
            err = {k: v for k, v in err.items() if k != '__task_error__'}
        task_err = TaskError.model_validate(err)
        return TaskResult(err=task_err)
    else:
        # Try to rehydrate pydantic BaseModel if we have metadata (using reusable function)
        try:
            ok_value = rehydrate_value(ok)
            return TaskResult(ok=ok_value)
        except SerializationError as e:
            # Any failure during rehydration becomes a library error
            logger.warning(f'PYDANTIC_HYDRATION_ERROR: {e}')
            return TaskResult(
                err=TaskError(
                    error_code=LibraryErrorCode.PYDANTIC_HYDRATION_ERROR,
                    message=str(e),
                    data={},
                )
            )


def serialize_task_options(task_options: TaskOptions) -> str:
    return dumps_json(
        {
            'retry_policy': task_options.retry_policy.model_dump(mode='json')
            if task_options.retry_policy
            else None,
            'good_until': task_options.good_until.isoformat()
            if task_options.good_until
            else None,
        }
    )
