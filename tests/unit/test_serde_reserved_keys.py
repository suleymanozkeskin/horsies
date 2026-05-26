"""Tests for the ``__h_*`` reserved-key rejection and legacy-tag fail-closed
behaviour introduced alongside the serde-class-registry security fix.

Threat model recap (see the security investigation in the branch history):
``rehydrate_value`` historically dispatched on free-text serde tags
(``__pydantic_model__`` etc.) and called ``import_module`` on the module
string carried in the payload.  Producers could smuggle forged envelopes
via ordinary task kwargs because ``to_jsonable`` did not validate user
dict keys.  Commit 1 migrates the tags to a single ``__h_*`` namespace and
rejects user dict keys under that prefix at serialize time, plus fails
closed on legacy and unknown tags at deserialize time.
"""

from __future__ import annotations

import dataclasses

import pytest
from pydantic import BaseModel

from horsies.core.codec.serde import (
    SerializationError,
    dumps_json,
    rehydrate_value,
    to_jsonable,
)
# Tests exercise the engine-internal variant directly to confirm the
# strict-vs-internal asymmetry; outside of tests this private name must
# only be imported by the workflow engine.
from horsies.core.codec.serde import _dumps_json_horsies_internal  # pyright: ignore[reportPrivateUsage]
from horsies.core.models.tasks import ContractCode, OperationalErrorCode
from horsies.core.types.result import is_err


# ---------------------------------------------------------------------------
# Serialize-side: user dict keys are rejected
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestUserKeyRejection:
    @pytest.mark.parametrize(
        'reserved_key',
        [
            '__h_pydantic__',
            '__h_dataclass__',
            '__h_task_result__',
            '__h_task_error__',
            '__h_datetime__',
            '__h_date__',
            '__h_time__',
            '__h_taskresult_envelope__',
            '__h_workflow_ctx__',
            '__h_workflow_meta__',
            '__h_anything_user_invented__',
        ],
    )
    def test_user_dict_with_h_namespaced_key_is_rejected(
        self, reserved_key: str,
    ) -> None:
        """Any ``__h_*`` key in a user-supplied dict is rejected at serialize."""
        result = to_jsonable({reserved_key: 'malicious'})
        assert is_err(result)
        err = result.err_value
        assert isinstance(err, SerializationError)
        assert err.code == ContractCode.RESERVED_KEY_IN_USER_DATA
        assert reserved_key in str(err)

    def test_user_dict_with_builtin_task_code_key_is_rejected(self) -> None:
        """``__builtin_task_code__`` is reserved even though it sits outside __h_*."""
        result = to_jsonable({'__builtin_task_code__': 'BROKER_ERROR'})
        assert is_err(result)
        err = result.err_value
        assert isinstance(err, SerializationError)
        assert err.code == ContractCode.RESERVED_KEY_IN_USER_DATA

    def test_nested_user_dict_rejection(self) -> None:
        """Reserved key buried in a nested user mapping is also rejected."""
        result = to_jsonable({
            'outer': {
                'inner': {'__h_pydantic__': True, 'module': 'os', 'qualname': 'X'},
            },
        })
        assert is_err(result)
        assert isinstance(result.err_value, SerializationError)
        assert result.err_value.code == ContractCode.RESERVED_KEY_IN_USER_DATA

    def test_reserved_key_inside_list_element_is_rejected(self) -> None:
        result = to_jsonable([1, 2, {'__h_dataclass__': True}, 4])
        assert is_err(result)
        assert isinstance(result.err_value, SerializationError)
        assert result.err_value.code == ContractCode.RESERVED_KEY_IN_USER_DATA

    def test_ordinary_user_dict_passes_through(self) -> None:
        """Sanity: a plain user dict with no reserved keys serializes cleanly."""
        result = dumps_json({'normal_key': 1, 'another': 'value'})
        assert not is_err(result)


# ---------------------------------------------------------------------------
# Serialize-side: BaseModel and TaskError dump scans
# ---------------------------------------------------------------------------


class _ModelWithDictField(BaseModel):
    """User-defined model with an ``Any``-typed field that could smuggle."""

    name: str
    metadata: dict[str, object]


@pytest.mark.unit
class TestModelDumpRecursiveScan:
    def test_basemodel_field_containing_reserved_key_is_rejected(self) -> None:
        """``model_dump`` output is scanned for ``__h_*`` keys at any depth."""
        model = _ModelWithDictField(
            name='harmless',
            metadata={'__h_pydantic__': 'smuggled', 'module': 'os'},
        )
        result = to_jsonable(model)
        assert is_err(result)
        assert isinstance(result.err_value, SerializationError)
        assert result.err_value.code == ContractCode.RESERVED_KEY_IN_USER_DATA

    def test_basemodel_with_deeply_nested_reserved_key_is_rejected(self) -> None:
        model = _ModelWithDictField(
            name='harmless',
            metadata={
                'level1': {
                    'level2': {
                        'level3': {'__h_dataclass__': True},
                    },
                },
            },
        )
        result = to_jsonable(model)
        assert is_err(result)
        assert isinstance(result.err_value, SerializationError)
        assert result.err_value.code == ContractCode.RESERVED_KEY_IN_USER_DATA

    def test_basemodel_without_reserved_keys_serializes(self) -> None:
        model = _ModelWithDictField(name='ok', metadata={'a': 1, 'b': [1, 2]})
        result = to_jsonable(model)
        assert not is_err(result)


# ---------------------------------------------------------------------------
# Deserialize-side: legacy tag fail-closed
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestLegacyTagFailClosed:
    @pytest.mark.parametrize(
        'legacy_tag',
        [
            '__pydantic_model__',
            '__dataclass__',
            '__task_error__',
            '__datetime__',
            '__date__',
            '__time__',
        ],
    )
    def test_legacy_tag_at_top_level(self, legacy_tag: str) -> None:
        """A pre-namespace serde envelope fails closed rather than dispatching."""
        payload = {legacy_tag: True, 'module': 'os', 'qualname': 'X', 'data': {}}
        result = rehydrate_value(payload)
        assert is_err(result)
        err = result.err_value
        assert isinstance(err, SerializationError)
        assert err.code == OperationalErrorCode.LEGACY_SERDE_TAG_UNSUPPORTED
        assert legacy_tag in str(err)

    def test_legacy_task_result_via_task_result_from_json(self) -> None:
        """``__task_result__`` envelope (old name) is rejected, not dispatched."""
        from horsies.core.codec.serde import task_result_from_json

        result = task_result_from_json({'__task_result__': True, 'ok': 1, 'err': None})
        assert is_err(result)
        assert isinstance(result.err_value, SerializationError)
        assert result.err_value.code == OperationalErrorCode.LEGACY_SERDE_TAG_UNSUPPORTED

    def test_legacy_tag_nested_inside_plain_dict(self) -> None:
        """Legacy tag at any depth fails closed (not silently downgraded)."""
        payload = {
            'outer': {
                'inner': {
                    '__pydantic_model__': True,
                    'module': 'os',
                    'qualname': 'X',
                },
            },
        }
        result = rehydrate_value(payload)
        assert is_err(result)
        assert isinstance(result.err_value, SerializationError)
        assert result.err_value.code == OperationalErrorCode.LEGACY_SERDE_TAG_UNSUPPORTED


# ---------------------------------------------------------------------------
# Deserialize-side: unknown ``__h_*`` tag fail-closed (forward-compat)
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestUnknownInternalTagFailClosed:
    def test_unknown_h_namespaced_tag_fails_closed(self) -> None:
        """A ``__h_*`` key the consumer doesn't recognise → UNKNOWN_SERDE_TAG."""
        result = rehydrate_value({'__h_future_feature__': True, 'data': 'x'})
        assert is_err(result)
        err = result.err_value
        assert isinstance(err, SerializationError)
        assert err.code == OperationalErrorCode.UNKNOWN_SERDE_TAG
        assert '__h_future_feature__' in str(err)

    def test_transport_tag_passes_through(self) -> None:
        """Transport tags (consumed by child_runner) are not unknown."""
        payload = {'__h_taskresult_envelope__': True, 'data': '...'}
        result = rehydrate_value(payload)
        assert not is_err(result)
        # Passes through as a plain dict — child_runner handles the envelope.
        assert result.ok_value == payload


# ---------------------------------------------------------------------------
# horsies-internal serializer permits __h_* keys
# ---------------------------------------------------------------------------


@dataclasses.dataclass
class _InternalSerializerBox:
    payload: dict[str, object]


@pytest.mark.unit
class TestHorsiesInternalSerializer:
    def test_internal_serializer_allows_transport_keys(self) -> None:
        """Engine-controlled transport keys round-trip through the internal API."""
        kwargs = {
            'user_kwarg': 'normal',
            '__h_workflow_ctx__': {'workflow_id': 'wf-1', 'task_index': 0},
            '__h_workflow_meta__': {'workflow_id': 'wf-1'},
        }
        result = _dumps_json_horsies_internal(kwargs)
        assert not is_err(result)
        assert '__h_workflow_ctx__' in result.ok_value

    def test_strict_serializer_rejects_what_internal_allows(self) -> None:
        """Confirms the asymmetry: strict path rejects same kwargs."""
        kwargs = {'__h_workflow_ctx__': {'workflow_id': 'wf-1'}}
        strict = dumps_json(kwargs)
        assert is_err(strict)
        assert isinstance(strict.err_value, SerializationError)
        assert strict.err_value.code == ContractCode.RESERVED_KEY_IN_USER_DATA

    def test_internal_serializer_allows_direct_taskresult_envelope(self) -> None:
        """args_from transport envelopes are allowed as direct kwarg values."""
        kwargs = {
            'upstream': {
                '__h_taskresult_envelope__': True,
                'data': '{"__h_task_result__":true,"ok":1,"err":null}',
            },
        }
        result = _dumps_json_horsies_internal(kwargs)
        assert not is_err(result)
        assert '__h_taskresult_envelope__' in result.ok_value

    def test_internal_serializer_rejects_nested_user_h_key(self) -> None:
        """Internal serializer still uses strict user-data rules below kwargs."""
        kwargs = {
            'user_kwarg': {
                'nested': {'__h_pydantic__': True, 'module': 'os', 'qualname': 'X'},
            },
        }
        result = _dumps_json_horsies_internal(kwargs)
        assert is_err(result)
        assert isinstance(result.err_value, SerializationError)
        assert result.err_value.code == ContractCode.RESERVED_KEY_IN_USER_DATA

    def test_internal_serializer_rejects_dataclass_field_smuggling(self) -> None:
        """A dataclass field containing dict[str, Any] cannot bypass the strict path."""
        kwargs = {
            'box': _InternalSerializerBox(
                payload={
                    '__h_pydantic__': True,
                    'module': 'os',
                    'qualname': 'PathLike',
                    'data': {},
                },
            ),
        }
        result = _dumps_json_horsies_internal(kwargs)
        assert is_err(result)
        assert isinstance(result.err_value, SerializationError)
        assert result.err_value.code == ContractCode.RESERVED_KEY_IN_USER_DATA


# ---------------------------------------------------------------------------
# Identity spoofing: __module__ / __qualname__ are mutable class attributes
# ---------------------------------------------------------------------------


class _RegisteredVictim(BaseModel):
    """Stand-in for an app-registered Pydantic model with side-effectful validators."""

    payload: str


from dataclasses import dataclass as _dataclass


@_dataclass
class _RegisteredDCVictim:
    """Module-level dataclass victim for the dataclass spoof test."""

    label: str


@_dataclass
class _FakeAttackerDC:
    """Attacker's fake dataclass — totally unrelated to ``_RegisteredDCVictim``."""

    label: str = 'attacker'


class _UnregisteredButValidModel(BaseModel):
    """Module-level Pydantic model that is intentionally NOT registered."""

    x: int


@pytest.mark.unit
class TestSpoofingIdentityGuard:
    """A producer must not be able to mutate __module__ / __qualname__ on a
    fake BaseModel / dataclass subclass to make it impersonate a real
    registered type at serialize time.  Without the identity guard the
    worker resolves the registered class and runs its validators against
    the fake instance's data.
    """

    def test_pydantic_spoofing_attempt_is_rejected(self) -> None:
        from horsies.core.codec.serde_registry import register_serde_type

        register_serde_type(_RegisteredVictim)

        # Attacker's fake model — completely unrelated class hierarchy.
        class _FakeAttackerModel(BaseModel):
            payload: str = 'attacker'

        # Mutate the class attributes to point at the registered victim.
        _FakeAttackerModel.__module__ = _RegisteredVictim.__module__
        _FakeAttackerModel.__qualname__ = _RegisteredVictim.__qualname__

        fake = _FakeAttackerModel(payload='attacker controls this')

        result = to_jsonable(fake)
        assert is_err(result)
        err = result.err_value
        assert isinstance(err, SerializationError)
        assert err.code == ContractCode.SPOOFED_SERIALIZATION_IDENTITY
        assert 'spoofing attempt' in str(err)

    def test_dataclass_spoofing_attempt_is_rejected(self) -> None:
        from horsies.core.codec.serde_registry import register_serde_type

        register_serde_type(_RegisteredDCVictim)

        # Spoof: point the attacker class's module/qualname at the victim.
        _FakeAttackerDC.__module__ = _RegisteredDCVictim.__module__
        _FakeAttackerDC.__qualname__ = _RegisteredDCVictim.__qualname__
        try:
            fake = _FakeAttackerDC(label='attacker controls this')
            result = to_jsonable(fake)
            assert is_err(result)
            err = result.err_value
            assert isinstance(err, SerializationError)
            assert err.code == ContractCode.SPOOFED_SERIALIZATION_IDENTITY
        finally:
            # Restore real attributes so subsequent tests aren't affected.
            _FakeAttackerDC.__module__ = __name__
            _FakeAttackerDC.__qualname__ = '_FakeAttackerDC'

    def test_legitimate_registered_model_serializes(self) -> None:
        """Sanity: the guard only fires on mismatch — registered class round-trips."""
        from horsies.core.codec.serde_registry import register_serde_type

        register_serde_type(_RegisteredVictim)

        legitimate = _RegisteredVictim(payload='ok')
        result = to_jsonable(legitimate)
        assert not is_err(result)

    def test_unregistered_type_is_not_blocked_at_serialize(self) -> None:
        """If the (module, qualname) isn't in the registry, the consumer's
        UNREGISTERED_REHYDRATION_TYPE check already handles it; the
        serializer doesn't need to reject.

        We can't actually exercise this without registering — every model
        in this test file ends up walked/registered eventually — so we
        only check that the model is constructible without the spoofing
        guard firing.  The behavioural check for the unregistered path
        is in test_serde_registry.py.
        """
        # Use a model that is NOT auto-registered by importing this test
        # file.  See _UnregisteredButValidModel at module level.
        instance = _UnregisteredButValidModel(x=1)
        result = to_jsonable(instance)
        assert not is_err(result)


# ---------------------------------------------------------------------------
# task_result_from_json fails closed on infra-level errors
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestTaskResultInfrastructureErrorPropagation:
    """The ``ok`` branch of ``task_result_from_json`` must propagate
    infrastructure-level deser failures (legacy tags, unknown tags,
    unregistered types) as Err so callers see them distinctly from
    task-level failures.  The previous behaviour wrapped everything as
    PYDANTIC_HYDRATION_ERROR, letting workflow allow_failed_deps logic
    treat a legacy payload as a normal task failure and continue past it.
    """

    def test_legacy_tag_in_ok_propagates_as_err(self) -> None:
        from horsies.core.codec.serde import task_result_from_json

        payload = {
            '__h_task_result__': True,
            'ok': {
                '__pydantic_model__': True,  # legacy tag
                'module': 'whatever',
                'qualname': 'Whatever',
                'data': {},
            },
            'err': None,
        }
        result = task_result_from_json(payload)
        assert is_err(result)
        err = result.err_value
        assert isinstance(err, SerializationError)
        assert err.code == OperationalErrorCode.LEGACY_SERDE_TAG_UNSUPPORTED

    def test_unknown_h_tag_in_ok_propagates_as_err(self) -> None:
        from horsies.core.codec.serde import task_result_from_json

        payload = {
            '__h_task_result__': True,
            'ok': {'__h_future_thing__': True},
            'err': None,
        }
        result = task_result_from_json(payload)
        assert is_err(result)
        err = result.err_value
        assert isinstance(err, SerializationError)
        assert err.code == OperationalErrorCode.UNKNOWN_SERDE_TAG

    def test_unregistered_type_in_ok_propagates_as_err(self) -> None:
        from horsies.core.codec.serde import task_result_from_json

        payload = {
            '__h_task_result__': True,
            'ok': {
                '__h_pydantic__': True,
                'module': 'never.registered',
                'qualname': 'Ghost',
                'data': {},
            },
            'err': None,
        }
        result = task_result_from_json(payload)
        assert is_err(result)
        err = result.err_value
        assert isinstance(err, SerializationError)
        assert err.code == ContractCode.UNREGISTERED_REHYDRATION_TYPE

    def test_genuine_return_type_drift_still_wraps_as_task_error(self) -> None:
        """Non-infrastructure rehydration errors keep the
        PYDANTIC_HYDRATION_ERROR wrapping (return-type drift between
        worker and consumer versions)."""
        from horsies.core.codec.serde import task_result_from_json
        from horsies.core.codec.serde_registry import register_serde_type

        class _DriftModel(BaseModel):
            x: int  # required

        register_serde_type(_DriftModel)

        payload = {
            '__h_task_result__': True,
            'ok': {
                '__h_pydantic__': True,
                'module': _DriftModel.__module__,
                'qualname': _DriftModel.__qualname__,
                'data': {'y': 'not_an_int'},  # validation will fail
            },
            'err': None,
        }
        result = task_result_from_json(payload)
        # NOT Err — task-level failure with PYDANTIC_HYDRATION_ERROR.
        assert not is_err(result)
        task_result = result.ok_value
        assert task_result.is_err()
        assert task_result.err.error_code == ContractCode.PYDANTIC_HYDRATION_ERROR
