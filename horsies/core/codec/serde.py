# app/core/codec/serde.py
from __future__ import annotations
from typing import (
    Any,
    Dict,
    List,
    Optional,
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
    BuiltInTaskCode,
    ContractCode,
    OperationalErrorCode,
    TaskOptions,
    TaskResult,
    TaskError,
)
from horsies.core.types.result import Ok, Err, Result, is_err
from horsies.core.codec.serde_registry import get_registered_type
from horsies.core.logging import get_logger

logger = get_logger('serde')


Json = Union[None, bool, int, float, str, List['Json'], Dict[str, 'Json']]
"""
Union type for JSON-serializable values.
"""

type SerdeResult[T] = Result[T, SerializationError]


class SerializationError(Exception):
    """Raised when a value cannot be serialized to or deserialized from JSON.

    Carries an optional ``code`` so callers can dispatch on the origin of
    the failure (e.g. legacy tag, reserved key, unknown tag) rather than
    matching free-text messages.
    """

    def __init__(
        self,
        message: str,
        code: BuiltInTaskCode | None = None,
    ) -> None:
        super().__init__(message)
        self.code: BuiltInTaskCode | None = code


# ---------------------------------------------------------------------------
# Reserved serde tags / namespace
# ---------------------------------------------------------------------------

# All internal serde envelopes live under a single ``__h_*`` namespace so the
# rejection rule for user data is a one-line prefix check.  ``__builtin_task_code__``
# is intentionally outside this namespace — it is a Pydantic field-level enum
# discriminator on ``TaskError`` and is read by ``TaskError.field_validator``,
# not by ``rehydrate_value``.
_INTERNAL_NAMESPACE_PREFIX: str = '__h_'

_TAG_PYDANTIC: str = '__h_pydantic__'
_TAG_DATACLASS: str = '__h_dataclass__'
_TAG_TASK_RESULT: str = '__h_task_result__'
_TAG_TASK_ERROR: str = '__h_task_error__'
_TAG_DATETIME: str = '__h_datetime__'
_TAG_DATE: str = '__h_date__'
_TAG_TIME: str = '__h_time__'

_KNOWN_INTERNAL_TAGS: frozenset[str] = frozenset({
    _TAG_PYDANTIC,
    _TAG_DATACLASS,
    _TAG_TASK_RESULT,
    _TAG_TASK_ERROR,
    _TAG_DATETIME,
    _TAG_DATE,
    _TAG_TIME,
})

# Transport keys are also under the ``__h_*`` namespace but are consumed
# by ``child_runner`` AFTER serde rehydration completes.  They must pass
# through ``rehydrate_value`` as opaque dict keys without triggering the
# unknown-tag fail-closed branch.
_TRANSPORT_TAGS: frozenset[str] = frozenset({
    '__h_taskresult_envelope__',
    '__h_workflow_ctx__',
    '__h_workflow_meta__',
})

_RECOGNIZED_INTERNAL_TAGS: frozenset[str] = _KNOWN_INTERNAL_TAGS | _TRANSPORT_TAGS

# Pre-namespace tag names that previously triggered typed rehydration.  Any
# of these appearing in a deserialized payload after the migration means the
# payload was produced by an older horsies version; rehydration fails closed.
_LEGACY_SERDE_TAGS: frozenset[str] = frozenset({
    '__pydantic_model__',
    '__dataclass__',
    '__task_result__',
    '__task_error__',
    '__datetime__',
    '__date__',
    '__time__',
})

# ``__builtin_task_code__`` is a Pydantic discriminator on TaskError.error_code;
# it is not a serde envelope.  We still reject it as a user dict key in plain
# user mappings so callers don't accidentally smuggle a confusing shape.
_RESERVED_PYDANTIC_INTERNAL_KEY: str = '__builtin_task_code__'


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


def _scan_for_reserved_keys(value: Json) -> SerdeResult[None]:
    """Recursively reject any dict key matching the ``__h_*`` namespace.

    Invoked on serialization output that bypasses ``to_jsonable``'s Mapping
    branch — i.e. on ``model_dump`` results and on ``TaskError`` dumped data.
    Without this scan, a user model with a ``dict[str, Any]`` field could
    smuggle a forged ``__h_pydantic__`` envelope through Pydantic's serializer
    and cause the consumer to dispatch on it.

    The scan does NOT reject ``__builtin_task_code__`` because TaskError's
    own ``model_dump`` legitimately emits it for ``error_code`` discrimination.
    """
    if isinstance(value, dict):
        for key, item in value.items():
            if key.startswith(_INTERNAL_NAMESPACE_PREFIX):
                return Err(SerializationError(
                    f'User data contains reserved key {key!r}: the '
                    f'{_INTERNAL_NAMESPACE_PREFIX!r} prefix is reserved for '
                    f'horsies internal serde tags.',
                    code=ContractCode.RESERVED_KEY_IN_USER_DATA,
                ))
            sub = _scan_for_reserved_keys(item)
            if is_err(sub):
                return sub
        return Ok(None)
    if isinstance(value, list):
        for item in value:
            sub = _scan_for_reserved_keys(item)
            if is_err(sub):
                return sub
    return Ok(None)


def _validate_user_mapping_key(key: str) -> SerdeResult[None]:
    """Reject user dict keys that collide with horsies-internal names."""
    if key.startswith(_INTERNAL_NAMESPACE_PREFIX):
        return Err(SerializationError(
            f'User dict key {key!r} uses the reserved '
            f'{_INTERNAL_NAMESPACE_PREFIX!r} prefix.',
            code=ContractCode.RESERVED_KEY_IN_USER_DATA,
        ))
    if key == _RESERVED_PYDANTIC_INTERNAL_KEY:
        return Err(SerializationError(
            f'User dict key {key!r} is a reserved Pydantic discriminator '
            f'used by TaskError.error_code.',
            code=ContractCode.RESERVED_KEY_IN_USER_DATA,
        ))
    return Ok(None)


def _task_error_to_json(err: TaskError) -> SerdeResult[Dict[str, Json]]:
    """Convert a TaskError to a JSON-serializable dictionary.

    Handles the exception field manually to avoid pydantic trying to
    serialize BaseException subclasses.  Scans the dumped output for the
    ``__h_*`` namespace so a TaskError carrying a user-controlled ``data``
    field cannot smuggle a forged serde envelope.
    """
    ex = err.exception
    try:
        data = err.model_dump(mode='json', exclude={'exception'})
    except Exception as exc:
        return Err(SerializationError(
            f'Failed to serialize TaskError: {exc}',
        ))

    scan = _scan_for_reserved_keys(cast(Json, data))
    if is_err(scan):
        return Err(scan.err_value)

    if isinstance(ex, BaseException):
        ex_json: Optional[Dict[str, Json]] = _exception_to_json(ex)
    elif isinstance(ex, dict) or ex is None:
        ex_json = ex  # already JSON-like or absent (e.g. None)
    else:
        # Unknown type: coerce to a simple shape of string
        ex_json = {'type': type(ex).__name__, 'message': str(ex)}

    if ex_json is not None:
        ex_scan = _scan_for_reserved_keys(cast(Json, ex_json))
        if is_err(ex_scan):
            return Err(ex_scan.err_value)
        data['exception'] = ex_json

    return Ok({_TAG_TASK_ERROR: True, **data})


def _is_task_result(value: Any) -> TypeGuard[TaskResult[Any, TaskError]]:
    """Type guard to properly narrow TaskResult types."""
    return isinstance(value, TaskResult)


# ---------------------------------------------------------------------------
# Rehydration uses the class registry, not dynamic import_module
# ---------------------------------------------------------------------------
#
# Class lookup happens through ``serde_registry.get_registered_type``.  The
# registry is populated by ``@horsies_task`` (signature walker) and by
# explicit ``@horsies_serdetype`` decoration.  There is no fallback to
# ``import_module`` — unregistered types fail closed with
# ``UNREGISTERED_REHYDRATION_TYPE``.


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

    Strict path: the Mapping branch rejects user dict keys matching the
    ``__h_*`` namespace or ``__builtin_task_code__``.  Every recursive step
    propagates Result — a failure at any nesting depth surfaces as Err to
    the caller.
    """
    return _to_jsonable_impl(value, allow_internal_keys=False)


def _to_jsonable_impl(
    value: Any,
    *,
    allow_internal_keys: bool,
) -> SerdeResult[Json]:
    """Shared implementation for the strict and engine-internal serializers.

    ``allow_internal_keys`` only affects the Mapping branch's user-key
    validation.  All other branches (BaseModel, dataclass, TaskError,
    TaskResult) still scan their ``model_dump`` output for ``__h_*``
    keys because that data originates from user-defined types.
    """
    # Primitives — always safe
    if value is None or isinstance(value, (bool, int, float, str)):
        return Ok(value)

    # datetime.datetime is a subclass of datetime.date — check datetime first.
    if isinstance(value, dt.datetime):
        return Ok({_TAG_DATETIME: True, 'value': value.isoformat()})

    if isinstance(value, dt.date):
        return Ok({_TAG_DATE: True, 'value': value.isoformat()})

    if isinstance(value, dt.time):
        return Ok({_TAG_TIME: True, 'value': value.isoformat()})

    # Is value a `TaskResult`?
    if _is_task_result(value):
        ok_json: Json = None
        if value.is_ok():
            ok_result = _to_jsonable_impl(
                value.ok, allow_internal_keys=allow_internal_keys,
            )
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
                err_scan = _scan_for_reserved_keys(cast(Json, err_json))
                if is_err(err_scan):
                    return Err(err_scan.err_value)
            else:
                err_json = {'message': str(value.err)}
        return Ok({_TAG_TASK_RESULT: True, 'ok': ok_json, 'err': err_json})

    # TaskError (standalone)
    if isinstance(value, TaskError):
        return _task_error_to_json(value)

    # Pydantic BaseModel
    if isinstance(value, BaseModel):
        path_result = _qualified_model_path(value)
        if is_err(path_result):
            return path_result
        module, qualname = path_result.ok_value
        dumped = value.model_dump(mode='json')
        scan = _scan_for_reserved_keys(cast(Json, dumped))
        if is_err(scan):
            return Err(scan.err_value)
        return Ok({
            _TAG_PYDANTIC: True,
            'module': module,
            'qualname': qualname,
            'data': dumped,
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
            field_result = _to_jsonable_impl(
                field_value, allow_internal_keys=allow_internal_keys,
            )
            if is_err(field_result):
                return field_result
            field_data[field.name] = field_result.ok_value
        return Ok({
            _TAG_DATACLASS: True,
            'module': module,
            'qualname': qualname,
            'data': field_data,
        })

    # Mapping (dict-like)
    if isinstance(value, Mapping):
        mapping = cast(Mapping[object, object], value)
        result_dict: Dict[str, Json] = {}
        original_keys: Dict[str, object] = {}
        for key, item in mapping.items():
            str_key = str(key)
            if not allow_internal_keys:
                key_check = _validate_user_mapping_key(str_key)
                if is_err(key_check):
                    return Err(key_check.err_value)
            if str_key in result_dict:
                return Err(SerializationError(
                    f"Mapping key collision: {key!r} and {original_keys[str_key]!r} "
                    f"both resolve to '{str_key}' after stringification",
                ))
            item_result = _to_jsonable_impl(
                item, allow_internal_keys=allow_internal_keys,
            )
            if is_err(item_result):
                return item_result
            original_keys[str_key] = key
            result_dict[str_key] = item_result.ok_value
        return Ok(result_dict)

    # Sequence (list-like, excluding str/bytes)
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        seq = cast(Sequence[object], value)
        result_list: List[Json] = []
        for item in seq:
            item_result = _to_jsonable_impl(
                item, allow_internal_keys=allow_internal_keys,
            )
            if is_err(item_result):
                return item_result
            result_list.append(item_result.ok_value)
        return Ok(result_list)

    return Err(SerializationError(
        f'Cannot serialize value of type {type(value).__name__}',
    ))


def dumps_json(value: Any) -> SerdeResult[str]:
    """Serialize a Python value to a JSON string.

    Strict path: rejects user dict keys matching ``^__h_`` and
    ``__builtin_task_code__``.  This is what user-facing serialization
    (``args_to_json``, ``kwargs_to_json``, ``TaskNode.kwargs`` round-trip
    validation) flows through.
    """
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


def dumps_json_horsies_internal(value: Any) -> SerdeResult[str]:
    """Serialize a value containing horsies-internal ``__h_*`` keys.

    Engine-only.  Bypasses the user-key validation that ``dumps_json``
    applies, so engine-injected transport keys
    (``__h_workflow_ctx__``, ``__h_workflow_meta__``,
    ``__h_taskresult_envelope__``) round-trip without being rejected as
    user smuggling.

    **The caller is responsible for ensuring no untrusted user-supplied
    dict carries a ``__h_*`` key.**  In the engine's case the user-supplied
    portion (``TaskNode.kwargs``) has already been validated through the
    strict path at workflow construction (``WorkflowSpec`` validation
    round-trips kwargs through ``dumps_json``), so the only fresh
    ``__h_*`` keys at this point are the ones the engine itself added.
    """
    jsonable_result = _to_jsonable_impl(value, allow_internal_keys=True)
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
    """
    if not s:
        return Ok(None)
    try:
        return Ok(json.loads(s))
    except (json.JSONDecodeError, ValueError) as exc:
        return Err(SerializationError(f'JSON parse failed: {exc}'))


def _legacy_tag_in_dict(value: dict[str, Json]) -> str | None:
    """Return the first legacy serde tag present in this dict, if any."""
    for tag in _LEGACY_SERDE_TAGS:
        if value.get(tag):
            return tag
    return None


def _unknown_internal_tag_in_dict(value: dict[str, Json]) -> str | None:
    """Return the first unknown ``__h_*`` key present in this dict, if any.

    Recognises both serde tags (dispatched here) and transport tags
    (consumed downstream by ``child_runner``).
    """
    for key in value:
        if (
            key.startswith(_INTERNAL_NAMESPACE_PREFIX)
            and key not in _RECOGNIZED_INTERNAL_TAGS
        ):
            return key
    return None


def rehydrate_value(value: Json) -> SerdeResult[Any]:
    """Recursively rehydrate a JSON value, restoring typed objects.

    Branch order (each subsequent branch only fires when the previous
    branches don't match):

    1. Known internal tags (Pydantic, dataclass, datetime/date/time, TaskResult)
    2. Legacy tag detection → ``LEGACY_SERDE_TAG_UNSUPPORTED``
    3. Unknown ``__h_*`` tag detection → ``UNKNOWN_SERDE_TAG``
    4. Generic dict / list recursion
    5. Primitive passthrough
    """
    # Pydantic model rehydration
    if isinstance(value, dict) and value.get(_TAG_PYDANTIC):
        module_name = value.get('module')
        qualname = value.get('qualname')
        if not isinstance(module_name, str) or not isinstance(qualname, str):
            return Err(SerializationError(
                'Malformed Pydantic payload: missing or non-string "module"/"qualname"',
            ))
        data = value.get('data')
        registry_key = f'{module_name}:{qualname}'

        cls = get_registered_type(registry_key)
        if cls is None:
            return Err(SerializationError(
                f"Cannot rehydrate {registry_key!r}: type is not registered "
                f'with the serde class registry. Register it via '
                f'@horsies_serdetype, app.register_serde_type(), or by using '
                f'it in a task signature (the @app.task signature walker '
                f'auto-registers reachable types).',
                code=ContractCode.UNREGISTERED_REHYDRATION_TYPE,
            ))
        if not (isinstance(cls, type) and issubclass(cls, BaseModel)):
            return Err(SerializationError(
                f"Registered type {registry_key!r} is not a BaseModel "
                f'subclass (got {cls!r}).',
            ))

        try:
            return Ok(cls.model_validate(data))
        except Exception as e:
            logger.error(
                f'Failed to rehydrate Pydantic model {registry_key}: '
                f'{type(e).__name__}: {e}',
            )
            return Err(SerializationError(
                f'Failed to rehydrate {registry_key}: {e}',
            ))

    # Dataclass rehydration
    if isinstance(value, dict) and value.get(_TAG_DATACLASS):
        module_name = value.get('module')
        qualname = value.get('qualname')
        if not isinstance(module_name, str) or not isinstance(qualname, str):
            return Err(SerializationError(
                'Malformed dataclass payload: missing or non-string "module"/"qualname"',
            ))
        data = value.get('data')
        registry_key = f'{module_name}:{qualname}'

        dc_cls = get_registered_type(registry_key)
        if dc_cls is None:
            return Err(SerializationError(
                f"Cannot rehydrate {registry_key!r}: type is not registered "
                f'with the serde class registry. Register it via '
                f'@horsies_serdetype, app.register_serde_type(), or by using '
                f'it in a task signature (the @app.task signature walker '
                f'auto-registers reachable types).',
                code=ContractCode.UNREGISTERED_REHYDRATION_TYPE,
            ))
        if not (isinstance(dc_cls, type) and dataclasses.is_dataclass(dc_cls)):
            return Err(SerializationError(
                f"Registered type {registry_key!r} is not a dataclass "
                f'(got {dc_cls!r}).',
            ))

        try:
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
                f'Failed to rehydrate dataclass {registry_key}: '
                f'{type(e).__name__}: {e}',
            )
            return Err(SerializationError(
                f'Failed to rehydrate dataclass {registry_key}: {e}',
            ))

    # Datetime rehydration (datetime before date — subclass ordering)
    if isinstance(value, dict) and value.get(_TAG_DATETIME):
        try:
            return Ok(dt.datetime.fromisoformat(cast(str, value['value'])))
        except (ValueError, KeyError) as exc:
            return Err(SerializationError(f'datetime rehydration failed: {exc}'))

    if isinstance(value, dict) and value.get(_TAG_DATE):
        try:
            return Ok(dt.date.fromisoformat(cast(str, value['value'])))
        except (ValueError, KeyError) as exc:
            return Err(SerializationError(f'date rehydration failed: {exc}'))

    if isinstance(value, dict) and value.get(_TAG_TIME):
        try:
            return Ok(dt.time.fromisoformat(cast(str, value['value'])))
        except (ValueError, KeyError) as exc:
            return Err(SerializationError(f'time rehydration failed: {exc}'))

    # Nested TaskResult (mutual recursion with task_result_from_json)
    if isinstance(value, dict) and value.get(_TAG_TASK_RESULT):
        return task_result_from_json(value)

    # Legacy tag detection — fail closed so old typed payloads don't silently
    # downgrade to plain dicts (which would let user code receive an untyped
    # dict where it expects a model instance).
    if isinstance(value, dict):
        legacy = _legacy_tag_in_dict(value)
        if legacy is not None:
            return Err(SerializationError(
                f'Legacy serde tag {legacy!r} encountered: payload was '
                f'serialized by a pre-namespace horsies version. Drain '
                f'queues and finish in-flight workflows before upgrading.',
                code=OperationalErrorCode.LEGACY_SERDE_TAG_UNSUPPORTED,
            ))

        # Unknown __h_* tag — forward-compat fail-closed.  A newer producer
        # may emit a tag this consumer doesn't recognise; treat as fatal rather
        # than silently passing the dict through.
        unknown = _unknown_internal_tag_in_dict(value)
        if unknown is not None:
            return Err(SerializationError(
                f'Unknown internal serde tag {unknown!r}: this consumer is '
                f'older than the producer that wrote this payload.',
                code=OperationalErrorCode.UNKNOWN_SERDE_TAG,
            ))

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
    if not isinstance(j, dict) or _TAG_TASK_RESULT not in j:
        # Legacy ``__task_result__`` envelope — fail closed.
        if isinstance(j, dict) and '__task_result__' in j:
            return Err(SerializationError(
                f'Legacy serde tag {"__task_result__"!r} encountered: '
                f'payload was serialized by a pre-namespace horsies version. '
                f'Drain queues and finish in-flight workflows before upgrading.',
                code=OperationalErrorCode.LEGACY_SERDE_TAG_UNSUPPORTED,
            ))
        # Accept bare ``ok``/``err`` shape (engine-side construction paths).
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
        if isinstance(err, dict) and err.get(_TAG_TASK_ERROR):
            err = {k: v for k, v in err.items() if k != _TAG_TASK_ERROR}
        elif isinstance(err, dict) and err.get('__task_error__'):
            return Err(SerializationError(
                f'Legacy serde tag {"__task_error__"!r} encountered: '
                f'payload was serialized by a pre-namespace horsies version. '
                f'Drain queues and finish in-flight workflows before upgrading.',
                code=OperationalErrorCode.LEGACY_SERDE_TAG_UNSUPPORTED,
            ))
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
                error_code=ContractCode.PYDANTIC_HYDRATION_ERROR,
                message=str(ok_result.err_value),
                data={},
            ),
        ))
    return Ok(TaskResult(ok=ok_result.ok_value))


# ---------------------------------------------------------------------------
# Safe error serialization
# ---------------------------------------------------------------------------

# Last-resort JSON when serializing an error payload itself fails.
# Hardcoded to avoid infinite recursion in error handlers.  Uses the new
# ``__h_*`` namespace; ``__builtin_task_code__`` stays outside the namespace
# because it is a TaskError-internal discriminator.
FALLBACK_ERROR_JSON = (
    '{"' + _TAG_TASK_RESULT + '":true,"ok":null,"err":'
    '{"error_code":{"__builtin_task_code__":"WORKER_SERIALIZATION_ERROR"},'
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
