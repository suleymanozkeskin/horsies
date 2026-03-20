# tests/unit/test_error_code_families.py
"""Tests for the error code family design.

Covers:
- Tagged serialization format for built-in codes
- Round-trip identity (built-in enum identity survives JSON serialization)
- User string passthrough (non-reserved strings stay as plain str)
- Strict construction (reserved strings and foreign enums rejected)
- Nested model round-trip (the regression case)
- Family-level and member-level match behavior
- Collision guard on duplicate built-in values
- horsies check reserved-code collision detection
"""
from __future__ import annotations

import json
from enum import Enum as _Enum

import pytest
from pydantic import BaseModel, ValidationError

from horsies.core.models.tasks import (
    BUILTIN_CODE_REGISTRY,
    BuiltInTaskCode,
    ContractCode,
    OperationalErrorCode,
    OutcomeCode,
    RetrievalCode,
    TaskError,
)


pytestmark = [pytest.mark.unit]


# ---------------------------------------------------------------------------
# Tagged serialization format
# ---------------------------------------------------------------------------


class TestTaggedSerialization:
    """Built-in codes serialize as tagged dicts, user strings stay as plain strings."""

    def test_builtin_code_serializes_as_tagged_dict(self) -> None:
        te = TaskError(error_code=OperationalErrorCode.BROKER_ERROR, message='err')
        data = te.model_dump(mode='json')
        assert data['error_code'] == {'__builtin_task_code__': 'BROKER_ERROR'}

    def test_user_string_serializes_as_plain_string(self) -> None:
        te = TaskError(error_code='MY_CUSTOM_CODE', message='custom')
        data = te.model_dump(mode='json')
        assert data['error_code'] == 'MY_CUSTOM_CODE'

    def test_none_serializes_as_null(self) -> None:
        te = TaskError(error_code=None, message='none')
        data = te.model_dump(mode='json')
        assert data['error_code'] is None

    @pytest.mark.parametrize(
        'code',
        [
            OperationalErrorCode.WORKER_CRASHED,
            ContractCode.PYDANTIC_HYDRATION_ERROR,
            RetrievalCode.WAIT_TIMEOUT,
            OutcomeCode.WORKFLOW_PAUSED,
        ],
        ids=lambda c: f'{type(c).__name__}.{c.name}',
    )
    def test_all_families_use_tagged_format(self, code: BuiltInTaskCode) -> None:
        te = TaskError(error_code=code, message='test')
        data = te.model_dump(mode='json')
        assert data['error_code'] == {'__builtin_task_code__': code.value}

    def test_model_dump_json_produces_tagged_format(self) -> None:
        te = TaskError(error_code=OutcomeCode.TASK_CANCELLED, message='cancelled')
        raw = te.model_dump_json()
        parsed = json.loads(raw)
        assert parsed['error_code'] == {'__builtin_task_code__': 'TASK_CANCELLED'}

    def test_model_dump_python_mode_also_uses_tagged_format(self) -> None:
        """model_dump() produces tagged dicts too (when_used='always').

        All serialization paths are safe: model_dump(), model_dump(mode='json'),
        model_dump_json(), and json.dumps(model_dump()).
        """
        te = TaskError(error_code=OutcomeCode.WORKFLOW_PAUSED, message='test')
        python_dict = te.model_dump()
        assert python_dict['error_code'] == {'__builtin_task_code__': 'WORKFLOW_PAUSED'}

        # json.dumps(model_dump()) → model_validate_json() is safe
        via_json_dumps = json.dumps(python_dict)
        restored = TaskError.model_validate_json(via_json_dumps)
        assert restored.error_code is OutcomeCode.WORKFLOW_PAUSED

    def test_model_dump_python_mode_user_string_stays_plain(self) -> None:
        """User strings stay as plain strings in python-mode dump."""
        te = TaskError(error_code='MY_CUSTOM', message='test')
        python_dict = te.model_dump()
        assert python_dict['error_code'] == 'MY_CUSTOM'
        assert type(python_dict['error_code']) is str


# ---------------------------------------------------------------------------
# Round-trip coercion: built-in enum identity survives JSON round-trip
# ---------------------------------------------------------------------------


class TestRoundTripCoercion:
    """Built-in enum members must survive JSON serialization via model_validate."""

    @pytest.mark.parametrize(
        'code',
        [
            OperationalErrorCode.BROKER_ERROR,
            OperationalErrorCode.TASK_EXCEPTION,
            ContractCode.PYDANTIC_HYDRATION_ERROR,
            RetrievalCode.RESULT_NOT_READY,
            OutcomeCode.WORKFLOW_PAUSED,
            OutcomeCode.SUBWORKFLOW_FAILED,
        ],
        ids=lambda c: f'{type(c).__name__}.{c.name}',
    )
    def test_enum_identity_survives_json_roundtrip(
        self, code: BuiltInTaskCode,
    ) -> None:
        original = TaskError(error_code=code, message='test')
        serialized = original.model_dump_json()
        restored = TaskError.model_validate_json(serialized)

        assert restored.error_code is code
        assert isinstance(restored.error_code, type(code))

    @pytest.mark.parametrize(
        'code',
        [
            OperationalErrorCode.WORKER_CRASHED,
            ContractCode.RETURN_TYPE_MISMATCH,
            RetrievalCode.WAIT_TIMEOUT,
            OutcomeCode.TASK_CANCELLED,
        ],
        ids=lambda c: f'{type(c).__name__}.{c.name}',
    )
    def test_enum_identity_survives_dict_json_roundtrip(
        self, code: BuiltInTaskCode,
    ) -> None:
        original = TaskError(error_code=code, message='test')
        as_dict = original.model_dump(mode='json')
        restored = TaskError.model_validate(as_dict)

        assert restored.error_code is code
        assert isinstance(restored.error_code, type(code))


# ---------------------------------------------------------------------------
# Unknown user string passthrough
# ---------------------------------------------------------------------------


class TestUserStringPassthrough:
    """User-defined plain strings must remain str after round-trip."""

    @pytest.mark.parametrize(
        'user_code',
        ['MY_CUSTOM_CODE', 'RATE_LIMITED', 'PAYMENT_DECLINED'],
    )
    def test_user_string_stays_str_after_json_roundtrip(
        self, user_code: str,
    ) -> None:
        original = TaskError(error_code=user_code, message='test')
        serialized = original.model_dump_json()
        restored = TaskError.model_validate_json(serialized)

        assert restored.error_code == user_code
        assert type(restored.error_code) is str

    def test_none_stays_none(self) -> None:
        original = TaskError(error_code=None, message='test')
        restored = TaskError.model_validate_json(original.model_dump_json())
        assert restored.error_code is None


# ---------------------------------------------------------------------------
# Strict construction
# ---------------------------------------------------------------------------


class TestStrictConstruction:
    """TaskError(...) rejects reserved built-in strings and foreign enums."""

    def test_reserved_string_rejected(self) -> None:
        with pytest.raises(ValueError, match='reserved built-in error code'):
            TaskError(error_code='WORKFLOW_PAUSED')

    def test_reserved_string_rejected_operational(self) -> None:
        with pytest.raises(ValueError, match='reserved built-in error code'):
            TaskError(error_code='BROKER_ERROR')

    def test_reserved_string_rejected_contract(self) -> None:
        with pytest.raises(ValueError, match='reserved built-in error code'):
            TaskError(error_code='RETURN_TYPE_MISMATCH')

    def test_reserved_string_rejected_retrieval(self) -> None:
        with pytest.raises(ValueError, match='reserved built-in error code'):
            TaskError(error_code='RESULT_NOT_READY')

    def test_dynamically_constructed_reserved_string_rejected(self) -> None:
        code = 'WORKFLOW' + '_' + 'PAUSED'
        with pytest.raises(ValueError, match='reserved built-in error code'):
            TaskError(error_code=code)

    def test_enum_member_accepted(self) -> None:
        te = TaskError(error_code=OutcomeCode.WORKFLOW_PAUSED, message='ok')
        assert te.error_code is OutcomeCode.WORKFLOW_PAUSED

    def test_custom_string_accepted(self) -> None:
        te = TaskError(error_code='MY_CUSTOM_CODE', message='ok')
        assert te.error_code == 'MY_CUSTOM_CODE'
        assert type(te.error_code) is str

    def test_str_subclass_with_reserved_value_rejected(self) -> None:
        class Sneaky(str):
            pass

        with pytest.raises(ValueError, match='reserved built-in error code'):
            TaskError(error_code=Sneaky('BROKER_ERROR'))

    def test_user_str_enum_with_reserved_value_rejected(self) -> None:
        class UserCode(str, _Enum):
            COLLIDES = 'BROKER_ERROR'

        with pytest.raises(ValueError, match='User error codes must be plain str'):
            TaskError(error_code=UserCode.COLLIDES)

    def test_user_str_enum_with_nonreserved_value_rejected(self) -> None:
        """Foreign str,Enum types are always rejected, even for non-reserved values."""

        class UserCode(str, _Enum):
            CUSTOM = 'MY_CUSTOM_CODE'

        with pytest.raises(ValueError, match='User error codes must be plain str'):
            TaskError(error_code=UserCode.CUSTOM)

    def test_unknown_tagged_dict_rejected(self) -> None:
        with pytest.raises(ValidationError):
            TaskError.model_validate(
                {'error_code': {'__builtin_task_code__': 'NOT_A_REAL_CODE'}},
            )

    def test_non_tagged_dict_rejected(self) -> None:
        with pytest.raises(ValidationError):
            TaskError.model_validate(
                {'error_code': {'some_other_key': 'value'}},
            )


# ---------------------------------------------------------------------------
# Nested model round-trip (regression case)
# ---------------------------------------------------------------------------


class TestNestedModelRoundTrip:
    """TaskError nested inside user Pydantic models must survive round-trip.

    This was the regression introduced by the original two-path design:
    generic Pydantic model_validate() used to hit the strict validator,
    failing on valid persisted data.
    """

    def test_nested_taskerror_roundtrips_via_model_validate(self) -> None:
        class Payload(BaseModel):
            error: TaskError

        original = Payload(
            error=TaskError(
                error_code=OutcomeCode.WORKFLOW_PAUSED, message='paused',
            ),
        )
        data = original.model_dump(mode='json')
        restored = Payload.model_validate(data)

        assert restored.error.error_code is OutcomeCode.WORKFLOW_PAUSED

    def test_nested_taskerror_roundtrips_via_model_validate_json(self) -> None:
        class Payload(BaseModel):
            error: TaskError

        original = Payload(
            error=TaskError(
                error_code=OperationalErrorCode.BROKER_ERROR, message='err',
            ),
        )
        raw = original.model_dump_json()
        restored = Payload.model_validate_json(raw)

        assert restored.error.error_code is OperationalErrorCode.BROKER_ERROR

    def test_nested_user_string_roundtrips(self) -> None:
        class Payload(BaseModel):
            error: TaskError

        original = Payload(
            error=TaskError(error_code='MY_CODE', message='custom'),
        )
        raw = original.model_dump_json()
        restored = Payload.model_validate_json(raw)

        assert restored.error.error_code == 'MY_CODE'
        assert type(restored.error.error_code) is str

    def test_deeply_nested_taskerror_roundtrips(self) -> None:
        class Inner(BaseModel):
            error: TaskError

        class Outer(BaseModel):
            inner: Inner

        original = Outer(
            inner=Inner(
                error=TaskError(
                    error_code=RetrievalCode.TASK_NOT_FOUND, message='gone',
                ),
            ),
        )
        raw = original.model_dump_json()
        restored = Outer.model_validate_json(raw)

        assert restored.inner.error.error_code is RetrievalCode.TASK_NOT_FOUND


# ---------------------------------------------------------------------------
# Match behavior
# ---------------------------------------------------------------------------


class TestMatchBehavior:
    """Verify match/case works for both member-level and family-level matching."""

    def test_member_level_match(self) -> None:
        te = TaskError(error_code=OutcomeCode.WORKFLOW_PAUSED, message='paused')
        matched = False
        match te.error_code:
            case OutcomeCode.WORKFLOW_PAUSED:
                matched = True
            case _:
                pass
        assert matched

    def test_family_level_match(self) -> None:
        te = TaskError(error_code=OperationalErrorCode.BROKER_ERROR, message='err')
        matched_family: str | None = None
        match te.error_code:
            case OperationalErrorCode():
                matched_family = 'operational'
            case ContractCode():
                matched_family = 'contract'
            case RetrievalCode():
                matched_family = 'retrieval'
            case OutcomeCode():
                matched_family = 'outcome'
            case _:
                matched_family = 'unknown'
        assert matched_family == 'operational'

    def test_family_match_after_roundtrip(self) -> None:
        """Family-level match must work after JSON round-trip."""
        te = TaskError(error_code=RetrievalCode.WAIT_TIMEOUT, message='timeout')
        restored = TaskError.model_validate_json(te.model_dump_json())

        matched_family: str | None = None
        match restored.error_code:
            case RetrievalCode():
                matched_family = 'retrieval'
            case _:
                matched_family = 'other'
        assert matched_family == 'retrieval'

    def test_user_string_does_not_match_family(self) -> None:
        te = TaskError(error_code='MY_CODE', message='custom')
        matched_family: str | None = None
        match te.error_code:
            case OperationalErrorCode():
                matched_family = 'operational'
            case ContractCode():
                matched_family = 'contract'
            case RetrievalCode():
                matched_family = 'retrieval'
            case OutcomeCode():
                matched_family = 'outcome'
            case str():
                matched_family = 'user_string'
            case _:
                matched_family = 'unknown'
        assert matched_family == 'user_string'


# ---------------------------------------------------------------------------
# Registry integrity
# ---------------------------------------------------------------------------


class TestBuiltinCodeRegistry:
    """Verify the BUILTIN_CODE_REGISTRY is complete and consistent."""

    def test_registry_contains_all_families(self) -> None:
        for family in (OperationalErrorCode, ContractCode, RetrievalCode, OutcomeCode):
            for member in family:
                assert member.value in BUILTIN_CODE_REGISTRY
                assert BUILTIN_CODE_REGISTRY[member.value] is member

    def test_registry_count(self) -> None:
        expected = (
            len(OperationalErrorCode)
            + len(ContractCode)
            + len(RetrievalCode)
            + len(OutcomeCode)
        )
        assert len(BUILTIN_CODE_REGISTRY) == expected

    def test_no_cross_family_value_overlap(self) -> None:
        """Each string value appears in exactly one family."""
        all_values: dict[str, str] = {}
        for family in (OperationalErrorCode, ContractCode, RetrievalCode, OutcomeCode):
            for member in family:
                family_name = type(member).__name__
                assert member.value not in all_values, (
                    f'{member.value} appears in both '
                    f'{all_values[member.value]} and {family_name}'
                )
                all_values[member.value] = family_name


# ---------------------------------------------------------------------------
# horsies check: reserved-code collision detection
# ---------------------------------------------------------------------------


class TestCheckReservedCodeCollision:
    """horsies check must flag statically visible reserved-code collisions."""

    def test_exception_mapper_collision_detected(self) -> None:
        from horsies.core.app import Horsies
        from horsies.core.models.app import AppConfig
        from horsies.core.models.broker import PostgresConfig
        from horsies.core.errors import ErrorCode

        app = Horsies(
            config=AppConfig(
                broker=PostgresConfig(database_url='postgresql+psycopg://x/y'),
                exception_mapper={ValueError: 'BROKER_ERROR'},
            ),
        )
        errors = app._check_runtime_policy_safety()
        collision_errors = [
            e for e in errors
            if e.code == ErrorCode.CHECK_RESERVED_CODE_COLLISION
        ]
        assert len(collision_errors) == 1
        assert 'BROKER_ERROR' in collision_errors[0].message
        assert 'OperationalErrorCode' in collision_errors[0].message

    def test_global_default_collision_detected(self) -> None:
        from horsies.core.app import Horsies
        from horsies.core.models.app import AppConfig
        from horsies.core.models.broker import PostgresConfig
        from horsies.core.errors import ErrorCode

        app = Horsies(
            config=AppConfig(
                broker=PostgresConfig(database_url='postgresql+psycopg://x/y'),
                default_unhandled_error_code='BROKER_ERROR',
            ),
        )
        errors = app._check_runtime_policy_safety()
        collision_errors = [
            e for e in errors
            if e.code == ErrorCode.CHECK_RESERVED_CODE_COLLISION
        ]
        assert len(collision_errors) == 1
        assert 'BROKER_ERROR' in collision_errors[0].message

    def test_default_unhandled_exception_not_flagged(self) -> None:
        from horsies.core.app import Horsies
        from horsies.core.models.app import AppConfig
        from horsies.core.models.broker import PostgresConfig
        from horsies.core.errors import ErrorCode

        app = Horsies(
            config=AppConfig(
                broker=PostgresConfig(database_url='postgresql+psycopg://x/y'),
            ),
        )
        errors = app._check_runtime_policy_safety()
        collision_errors = [
            e for e in errors
            if e.code == ErrorCode.CHECK_RESERVED_CODE_COLLISION
        ]
        assert len(collision_errors) == 0

    def test_custom_string_not_flagged(self) -> None:
        from horsies.core.app import Horsies
        from horsies.core.models.app import AppConfig
        from horsies.core.models.broker import PostgresConfig
        from horsies.core.errors import ErrorCode

        app = Horsies(
            config=AppConfig(
                broker=PostgresConfig(database_url='postgresql+psycopg://x/y'),
                exception_mapper={ValueError: 'MY_CUSTOM_CODE'},
            ),
        )
        errors = app._check_runtime_policy_safety()
        collision_errors = [
            e for e in errors
            if e.code == ErrorCode.CHECK_RESERVED_CODE_COLLISION
        ]
        assert len(collision_errors) == 0
