# tests/unit/test_error_code_families.py
"""Tests for the error code family refactor.

Covers:
- Round-trip coercion (built-in enum identity survives JSON serialization)
- Unknown user string passthrough
- Reserved built-in string coercion
- Family-level and member-level match behavior
- Collision guard on duplicate built-in values
- horsies check reserved-code collision detection
"""
from __future__ import annotations

import pytest

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
# Round-trip coercion: built-in enum identity survives JSON round-trip
# ---------------------------------------------------------------------------


class TestRoundTripCoercion:
    """Built-in enum members must survive serialization via from_persisted path."""

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
        restored = TaskError.from_persisted_json(serialized)

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
    def test_enum_identity_survives_dict_roundtrip(
        self, code: BuiltInTaskCode,
    ) -> None:
        original = TaskError(error_code=code, message='test')
        as_dict = original.model_dump()
        restored = TaskError.from_persisted(as_dict)

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
# Reserved built-in string coercion
# ---------------------------------------------------------------------------


class TestStrictConstruction:
    """TaskError(...) rejects reserved built-in strings passed as plain str."""

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
        """Even dynamically constructed reserved strings are rejected."""
        code = 'WORKFLOW' + '_' + 'PAUSED'
        with pytest.raises(ValueError, match='reserved built-in error code'):
            TaskError(error_code=code)

    def test_model_validate_json_rejects_reserved_strings(self) -> None:
        """model_validate_json is NOT the rehydration path — it enforces strict rules.

        Use from_persisted_json() for stored payloads instead.
        """
        from pydantic import ValidationError

        with pytest.raises(ValidationError):
            TaskError.model_validate_json('{"error_code":"WORKFLOW_PAUSED"}')

    def test_enum_member_accepted(self) -> None:
        te = TaskError(error_code=OutcomeCode.WORKFLOW_PAUSED, message='ok')
        assert te.error_code is OutcomeCode.WORKFLOW_PAUSED

    def test_custom_string_accepted(self) -> None:
        te = TaskError(error_code='MY_CUSTOM_CODE', message='ok')
        assert te.error_code == 'MY_CUSTOM_CODE'
        assert type(te.error_code) is str

    def test_str_subclass_with_reserved_value_rejected(self) -> None:
        """str subclasses carrying reserved values are also rejected."""

        class Sneaky(str):
            pass

        with pytest.raises(ValueError, match='reserved built-in error code'):
            TaskError(error_code=Sneaky('BROKER_ERROR'))

    def test_user_str_enum_with_reserved_value_rejected(self) -> None:
        """User-defined str,Enum with a reserved value is also rejected."""
        from enum import Enum as _Enum

        class UserCode(str, _Enum):
            COLLIDES = 'BROKER_ERROR'

        with pytest.raises(ValueError, match='reserved built-in error code'):
            TaskError(error_code=UserCode.COLLIDES)


class TestPersistedRehydration:
    """TaskError.from_persisted() coerces reserved strings to enum members."""

    def test_persisted_outcome_rehydrates(self) -> None:
        restored = TaskError.from_persisted(
            {'error_code': 'WORKFLOW_PAUSED', 'message': 'paused'},
        )
        assert restored.error_code is OutcomeCode.WORKFLOW_PAUSED

    def test_persisted_operational_rehydrates(self) -> None:
        restored = TaskError.from_persisted(
            {'error_code': 'BROKER_ERROR', 'message': 'err'},
        )
        assert restored.error_code is OperationalErrorCode.BROKER_ERROR

    def test_persisted_contract_rehydrates(self) -> None:
        restored = TaskError.from_persisted(
            {'error_code': 'RETURN_TYPE_MISMATCH', 'message': 'mismatch'},
        )
        assert restored.error_code is ContractCode.RETURN_TYPE_MISMATCH

    def test_persisted_retrieval_rehydrates(self) -> None:
        restored = TaskError.from_persisted(
            {'error_code': 'RESULT_NOT_READY', 'message': 'not ready'},
        )
        assert restored.error_code is RetrievalCode.RESULT_NOT_READY

    def test_persisted_user_string_stays_str(self) -> None:
        restored = TaskError.from_persisted(
            {'error_code': 'MY_CUSTOM_CODE', 'message': 'custom'},
        )
        assert restored.error_code == 'MY_CUSTOM_CODE'
        assert type(restored.error_code) is str

    def test_from_persisted_json(self) -> None:
        import json

        data = {'error_code': 'WORKFLOW_PAUSED', 'message': 'paused'}
        restored = TaskError.from_persisted_json(json.dumps(data))
        assert restored.error_code is OutcomeCode.WORKFLOW_PAUSED

    def test_from_persisted_validates_fields(self) -> None:
        """from_persisted() still validates field types via Pydantic."""
        from pydantic import ValidationError

        with pytest.raises(ValidationError):
            TaskError.from_persisted({'error_code': 123, 'message': ['not-a-string']})

    def test_from_persisted_rejects_non_dict(self) -> None:
        with pytest.raises(ValueError, match='Expected dict'):
            TaskError.from_persisted_json('"not a dict"')


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
        """Family-level match must work even after persisted rehydration."""
        te = TaskError(error_code=RetrievalCode.WAIT_TIMEOUT, message='timeout')
        restored = TaskError.from_persisted_json(te.model_dump_json())

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
        """Global exception_mapper with a reserved built-in value → error."""
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
        """Global default_unhandled_error_code with non-default reserved value → error."""
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
        """The library default 'UNHANDLED_EXCEPTION' must not be flagged."""
        from horsies.core.app import Horsies
        from horsies.core.models.app import AppConfig
        from horsies.core.models.broker import PostgresConfig
        from horsies.core.errors import ErrorCode

        app = Horsies(
            config=AppConfig(
                broker=PostgresConfig(database_url='postgresql+psycopg://x/y'),
                # default_unhandled_error_code defaults to 'UNHANDLED_EXCEPTION'
            ),
        )
        errors = app._check_runtime_policy_safety()
        collision_errors = [
            e for e in errors
            if e.code == ErrorCode.CHECK_RESERVED_CODE_COLLISION
        ]
        assert len(collision_errors) == 0

    def test_custom_string_not_flagged(self) -> None:
        """Non-reserved user strings in exception_mapper must not be flagged."""
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
