"""Unit tests for horsies.core.codec.kwargs.

Covers the strict producer/worker binding:

- Unknown kwarg names rejected (encode + decode).
- User-supplied engine-injected names (workflow_ctx / workflow_meta)
  rejected at encode.
- User-supplied TaskResult-typed kwargs rejected at encode; engine
  envelopes for TaskResult-typed params pass through at decode.
- Engine transport-key prefixes (`__horsies_*` / `__h_*`) preserved
  verbatim by decode.
- No-hints / unresolvable-hints fallback: lambdas and Mock fall
  through to pass-through (test scaffolding only).
"""

from __future__ import annotations

from typing import cast

import pytest
from pydantic import BaseModel

from horsies.core.codec.json_value import JsonValue, StrictJsonError
from horsies.core.codec.kwargs import decode_kwargs, encode_kwargs
from horsies.core.codec.typed import Json
from horsies.core.models.tasks import TaskError, TaskResult


def _task_a(value: int) -> TaskResult[int, TaskError]:  # pyright: ignore[reportUnusedParameter]
    ...


def _task_with_ctx(value: int, workflow_ctx: object) -> TaskResult[int, TaskError]:  # pyright: ignore[reportUnusedParameter]
    ...


def _task_with_upstream(
    upstream_result: TaskResult[int, TaskError],
) -> TaskResult[int, TaskError]:  # pyright: ignore[reportUnusedParameter]
    ...


# Module-level fixtures for the wire-round-trip smoke. `dumps_json`
# refuses to serialize classes defined inside a test function ("local
# class can't be imported by the worker"), so the round-trip target
# must live at module scope.


class _RoundTripEnvelope(BaseModel):
    stream_id: str
    metadata: dict[str, JsonValue]


class _SmuggleEnvelope(BaseModel):
    metadata: dict[str, JsonValue]


def _ingest_round_trip(
    payload: _RoundTripEnvelope,
) -> TaskResult[int, TaskError]:  # pyright: ignore[reportUnusedParameter]
    ...


def _ingest_smuggle(
    payload: _SmuggleEnvelope,
) -> TaskResult[int, TaskError]:  # pyright: ignore[reportUnusedParameter]
    ...


class TestEncodeStrictBinding:
    def test_known_kwarg_encodes(self) -> None:
        encoded = encode_kwargs(_task_a, {'value': 42})
        assert encoded == {'value': 42}

    def test_unknown_kwarg_rejected(self) -> None:
        with pytest.raises(StrictJsonError, match="unknown kwarg 'valuee'"):
            encode_kwargs(_task_a, {'value': 1, 'valuee': 2})

    def test_workflow_ctx_user_supplied_rejected(self) -> None:
        with pytest.raises(
            StrictJsonError,
            match="kwarg 'workflow_ctx' is engine-injected",
        ):
            encode_kwargs(_task_with_ctx, {'value': 1, 'workflow_ctx': 'x'})

    def test_workflow_meta_user_supplied_rejected(self) -> None:
        with pytest.raises(
            StrictJsonError,
            match="kwarg 'workflow_meta' is engine-injected",
        ):
            encode_kwargs(_task_a, {'value': 1, 'workflow_meta': 'x'})

    def test_taskresult_typed_kwarg_user_supplied_rejected(self) -> None:
        # TaskResult-typed kwargs come from the engine's args_from path,
        # not from user code. Producer-side encode rejects to avoid
        # confusion with engine-injected envelopes.
        bogus: TaskResult[int, TaskError] = TaskResult(ok=42)
        with pytest.raises(
            StrictJsonError,
            match="kwarg 'upstream_result' is TaskResult-typed",
        ):
            encode_kwargs(_task_with_upstream, {'upstream_result': bogus})


class TestDecodeStrictBinding:
    def test_known_kwarg_decodes(self) -> None:
        decoded = decode_kwargs(_task_a, {'value': 42})
        assert decoded == {'value': 42}

    def test_unknown_kwarg_on_wire_rejected(self) -> None:
        with pytest.raises(StrictJsonError, match="unknown kwarg 'mystery'"):
            decode_kwargs(_task_a, {'value': 1, 'mystery': 2})

    def test_engine_transport_keys_pass_through(self) -> None:
        decoded = decode_kwargs(
            _task_with_ctx,
            {'value': 1, '__h_workflow_ctx__': {'wf': 'id'}},
        )
        assert decoded['value'] == 1
        assert decoded['__h_workflow_ctx__'] == {'wf': 'id'}

    def test_new_h_prefix_transport_keys_pass_through(self) -> None:
        # `__h_*` is the design's target transport prefix. Decode-side
        # treats it as a transport key alongside the legacy form.
        decoded = decode_kwargs(
            _task_a,
            {'value': 1, '__h_workflow_ctx__': {'wf': 'id'}},
        )
        assert decoded['__h_workflow_ctx__'] == {'wf': 'id'}

    def test_taskresult_typed_kwarg_envelope_passes_through(self) -> None:
        envelope: dict[str, Json] = {
            '__h_taskresult_envelope__': True,
            'data': '{"ok":42}',
        }
        decoded = decode_kwargs(
            _task_with_upstream,
            {'upstream_result': cast(Json, envelope)},
        )
        assert decoded['upstream_result'] == envelope


class TestNoHintsFallback:
    """Lambdas / Mocks have no annotations to bind against; pass-through
    keeps test scaffolding alive without weakening production strictness
    (registered tasks pass `@app.task` validation which guarantees
    every param is annotated)."""

    def test_lambda_encode_pass_through(self) -> None:
        def f(value):  # type: ignore[no-untyped-def]
            return value

        encoded = encode_kwargs(f, {'value': 42, 'anything': 'goes'})
        assert encoded == {'value': 42, 'anything': 'goes'}

    def test_lambda_decode_pass_through(self) -> None:
        def f(value):  # type: ignore[no-untyped-def]
            return value

        decoded = decode_kwargs(f, {'value': 42, 'anything': 'goes'})
        assert decoded == {'value': 42, 'anything': 'goes'}


class TestWireRoundTripNestedJsonValue:
    """Integration-level smoke for the strict-serde wire contract.

    The broker is opaque storage — it round-trips bytes. The actual
    serde surface is `encode_kwargs → dumps_json → loads_json →
    decode_kwargs`. Exercise that pipeline against a Pydantic model
    carrying nested `dict[str, JsonValue]` (the §3 raw-JSON-inside-
    Pydantic-field shape that motivated the recursive JsonValue
    fence). If this round-trip passes, the producer-to-worker flow
    is sound for the same payload at any broker.

    Why this is the right level: a real broker round-trip would add
    postgres setup but verify nothing more — the wire format is a
    string, and `dumps_json` / `loads_json` are the storage interface.
    """

    def test_pydantic_field_with_nested_jsonvalue_round_trips(self) -> None:
        from horsies.core.codec.serde import dumps_json, loads_json  # noqa: PLC0415
        from horsies.core.types.result import is_ok  # noqa: PLC0415

        payload = _RoundTripEnvelope(
            stream_id='s-1',
            metadata={
                'source': 'webhook',
                'received_at': '2026-05-26T12:00:00Z',
                'attrs': {
                    'retries': 3,
                    'tags': ['urgent', 'verified'],
                    'flags': {'idempotent': True, 'fanout': None},
                },
                'history': [
                    {'step': 1, 'ok': True},
                    {'step': 2, 'ok': False, 'error': 'transient'},
                ],
            },
        )

        # Producer pipeline.
        encoded = encode_kwargs(_ingest_round_trip, {'payload': payload})
        dumped_r = dumps_json(encoded)
        assert is_ok(dumped_r)
        wire = dumped_r.ok_value
        assert isinstance(wire, str)

        # Storage round-trip: in production this is a postgres column
        # write/read; here it's just `loads_json` straight back from the
        # wire string, which is the same JSON-decode the worker runs.
        loaded_r = loads_json(wire)
        assert is_ok(loaded_r)
        raw_kwargs = cast('dict[str, Json]', loaded_r.ok_value)

        # Worker pipeline.
        decoded = decode_kwargs(_ingest_round_trip, raw_kwargs)
        assert 'payload' in decoded
        decoded_payload = decoded['payload']
        assert isinstance(decoded_payload, _RoundTripEnvelope)
        assert decoded_payload == payload
        # Spot-check the nested JsonValue survives intact across the
        # wire (no Pydantic coercion of nested ints / bools / nulls).
        assert decoded_payload.metadata['attrs'] == {
            'retries': 3,
            'tags': ['urgent', 'verified'],
            'flags': {'idempotent': True, 'fanout': None},
        }

    def test_pydantic_field_with_nested_jsonvalue_rejects_smuggled_reserved_key(
        self,
    ) -> None:
        """If a producer sneaks `__h_*` into a `dict[str, JsonValue]`
        field at any depth, the encode-side reserved-key scan rejects
        before the value reaches the broker."""
        bad = _SmuggleEnvelope(metadata={'outer': {'__h_evil__': 1}})
        with pytest.raises(StrictJsonError, match='reserved key'):
            encode_kwargs(_ingest_smuggle, {'payload': bad})


class TestLegacyHorsiesPrefixSmuggleClosed:
    """The smuggling path the design closes: a user-supplied kwarg
    value carrying `__h_taskresult_envelope__` would reach the worker
    args_from handler and route to legacy class-identity rehydration.
    `encode_value` rejects the legacy prefix at user positions; this
    test confirms encode_kwargs propagates that rejection."""

    def test_legacy_envelope_in_dict_kwarg_rejected(self) -> None:
        def task(payload: dict[str, object]) -> TaskResult[int, TaskError]:  # pyright: ignore[reportUnusedParameter]
            ...

        with pytest.raises(StrictJsonError, match='reserved key'):
            encode_kwargs(
                task,
                {'payload': {'__h_taskresult_envelope__': True, 'data': 'evil'}},
            )


# ── SubWorkflowNode.kwargs typed serde ───────────────────────────────


from typing import Any  # noqa: E402

from horsies.core.app import Horsies  # noqa: E402
from horsies.core.codec.kwargs import (  # noqa: E402
    decode_subworkflow_kwargs,
    encode_subworkflow_kwargs,
)
from horsies.core.models.workflow import (  # noqa: E402
    TaskNode,
    WorkflowDefinition,
    WorkflowSpec,
)


def _mock_workflow_def_task() -> Any:
    """Return a callable usable as a TaskNode fn in test fixtures."""
    from unittest.mock import MagicMock

    fn = MagicMock(spec=lambda: None)
    fn.task_name = 'mock_task'
    fn._original_fn = lambda: TaskResult(ok=0)
    return fn


class _TypedNamedParamWorkflow(WorkflowDefinition[int]):
    """build_with(cls, app, *, value: int) — typed kwarg-only."""

    name = 'typed_named_param_workflow'
    definition_key = 'tests.unit.typed_named_param.v1'

    @classmethod
    def build_with(cls, app: Horsies, *, value: int) -> WorkflowSpec[int]:  # pyright: ignore[reportUnusedParameter]
        raise NotImplementedError


class _TypedPydanticParamWorkflow(WorkflowDefinition[int]):
    """build_with(cls, app, *, payload: PydanticModel) — Pydantic kwarg."""

    name = 'typed_pydantic_param_workflow'
    definition_key = 'tests.unit.typed_pydantic_param.v1'

    @classmethod
    def build_with(  # pyright: ignore[reportUnusedParameter]
        cls, app: Horsies, *, payload: '_RoundTripEnvelope',
    ) -> WorkflowSpec[int]:
        raise NotImplementedError


class _OpaqueParamsWorkflow(WorkflowDefinition[int]):
    """build_with(cls, app, *args: Any, **params: Any) — opaque catch-all."""

    name = 'opaque_params_workflow'
    definition_key = 'tests.unit.opaque_params.v1'

    @classmethod
    def build_with(  # pyright: ignore[reportUnusedParameter]
        cls, app: Horsies, *args: Any, **params: Any,
    ) -> WorkflowSpec[int]:
        raise NotImplementedError


class _PureKwargsWorkflow(WorkflowDefinition[int]):
    """build_with(cls, app, **kwargs: Any) — pure VAR_KEYWORD."""

    name = 'pure_kwargs_workflow'
    definition_key = 'tests.unit.pure_kwargs.v1'

    @classmethod
    def build_with(  # pyright: ignore[reportUnusedParameter]
        cls, app: Horsies, **kwargs: Any,
    ) -> WorkflowSpec[int]:
        raise NotImplementedError


class _TaskResultParamWorkflow(WorkflowDefinition[int]):
    """build_with(cls, app, *, dep_result: TaskResult[...]) — args_from-only."""

    name = 'taskresult_param_workflow'
    definition_key = 'tests.unit.taskresult_param.v1'

    @classmethod
    def build_with(  # pyright: ignore[reportUnusedParameter]
        cls, app: Horsies, *, dep_result: TaskResult[int, TaskError],
    ) -> WorkflowSpec[int]:
        raise NotImplementedError


class _UnannotatedParamWorkflow(WorkflowDefinition[int]):
    """build_with(cls, app, *, value) — missing annotation on declared param."""

    name = 'unannotated_param_workflow'
    definition_key = 'tests.unit.unannotated_param.v1'

    @classmethod
    def build_with(cls, app, *, value) -> WorkflowSpec[int]:  # type: ignore[no-untyped-def]  # pyright: ignore[reportUnknownParameterType,reportMissingParameterType,reportUnusedParameter]
        raise NotImplementedError


class TestEncodeSubworkflowKwargs:
    """Strict-serde phase 6+: typed encode of `SubWorkflowNode.kwargs`
    against the child `build_with` signature.

    Closes the `to_jsonable` fallback hole; opaque `**params: Any`
    catch-alls route through `JsonValue` so non-JSON-native shapes
    (Pydantic models, dataclasses, bytes) raise at the producer side
    instead of being silently smuggled onto the wire.
    """

    def test_typed_named_param_encodes(self) -> None:
        result = encode_subworkflow_kwargs(
            _TypedNamedParamWorkflow, {'value': 42},
        )
        assert result == {'value': 42}

    def test_typed_pydantic_param_round_trips_via_typeadapter(self) -> None:
        """Pydantic models survive encode via the typed path."""
        payload = _RoundTripEnvelope(
            stream_id='stream-1', metadata={'k': 'v'},
        )
        result = encode_subworkflow_kwargs(
            _TypedPydanticParamWorkflow, {'payload': payload},
        )
        # Encoded as a dict via the Pydantic annotation.
        assert isinstance(result['payload'], dict)
        payload_dict = cast('dict[str, Json]', result['payload'])
        assert payload_dict['stream_id'] == 'stream-1'

    def test_unknown_kwarg_rejected_when_signature_has_no_var_keyword(
        self,
    ) -> None:
        """No `**kwargs` catch-all → unknown kwarg fails closed."""
        with pytest.raises(StrictJsonError, match="unknown kwarg 'other'"):
            encode_subworkflow_kwargs(
                _TypedNamedParamWorkflow, {'other': 1},
            )

    def test_taskresult_typed_kwarg_rejected(self) -> None:
        """TaskResult-typed params are args_from-only."""
        with pytest.raises(StrictJsonError, match='TaskResult-typed'):
            encode_subworkflow_kwargs(
                _TaskResultParamWorkflow,
                {'dep_result': TaskResult(ok=1)},
            )

    def test_unannotated_param_rejected(self) -> None:
        with pytest.raises(StrictJsonError, match='no type annotation'):
            encode_subworkflow_kwargs(
                _UnannotatedParamWorkflow, {'value': 1},
            )

    def test_opaque_var_kw_accepts_json_native(self) -> None:
        """`**params: Any` falls through `JsonValue` strict encode."""
        result = encode_subworkflow_kwargs(
            _OpaqueParamsWorkflow,
            {'a': 1, 'b': 'two', 'c': [1, 2], 'd': {'nested': True}},
        )
        assert result == {
            'a': 1, 'b': 'two', 'c': [1, 2], 'd': {'nested': True},
        }

    def test_opaque_var_kw_rejects_pydantic_model(self) -> None:
        """No annotation → JsonValue fallback rejects Pydantic models.

        This is the strictness invariant: smuggling typed payloads
        through `**params: Any` is closed. Users must declare a real
        annotation to encode typed values.
        """
        payload = _RoundTripEnvelope(stream_id='s', metadata={})
        with pytest.raises(Exception):  # noqa: B017 - Pydantic ValidationError
            encode_subworkflow_kwargs(
                _OpaqueParamsWorkflow, {'data': payload},
            )

    def test_pure_var_kw_accepts_json_native(self) -> None:
        """Bare `**kwargs: Any` (no positional fixed params) also routes
        through `JsonValue` fallback."""
        result = encode_subworkflow_kwargs(
            _PureKwargsWorkflow, {'anything': 'goes'},
        )
        assert result == {'anything': 'goes'}

    def test_empty_kwargs_passes(self) -> None:
        assert encode_subworkflow_kwargs(_OpaqueParamsWorkflow, {}) == {}
        assert encode_subworkflow_kwargs(_TypedNamedParamWorkflow, {}) == {}


class TestDecodeSubworkflowKwargs:
    """Symmetric decode path."""

    def test_typed_named_param_decodes(self) -> None:
        result = decode_subworkflow_kwargs(
            _TypedNamedParamWorkflow, {'value': 42},
        )
        assert result == {'value': 42}

    def test_typed_pydantic_param_decodes_to_model(self) -> None:
        payload_dict: dict[str, Json] = {
            'stream_id': 'stream-1', 'metadata': {'k': 'v'},
        }
        result = decode_subworkflow_kwargs(
            _TypedPydanticParamWorkflow, {'payload': payload_dict},
        )
        assert isinstance(result['payload'], _RoundTripEnvelope)
        assert result['payload'].stream_id == 'stream-1'

    def test_unknown_kwarg_on_wire_rejected(self) -> None:
        with pytest.raises(StrictJsonError, match="unknown kwarg 'other'"):
            decode_subworkflow_kwargs(
                _TypedNamedParamWorkflow, {'other': 1},
            )

    def test_taskresult_typed_kwarg_rejected_on_wire(self) -> None:
        """Static kwargs must never carry args_from values on the wire."""
        with pytest.raises(StrictJsonError, match='TaskResult-typed'):
            decode_subworkflow_kwargs(
                _TaskResultParamWorkflow, {'dep_result': {'ok': 1}},
            )

    def test_opaque_var_kw_decodes_json_native_through(self) -> None:
        result = decode_subworkflow_kwargs(
            _OpaqueParamsWorkflow,
            {'a': 1, 'b': 'two', 'c': [1, 2], 'd': {'nested': True}},
        )
        assert result == {
            'a': 1, 'b': 'two', 'c': [1, 2], 'd': {'nested': True},
        }

    def test_empty_kwargs_passes(self) -> None:
        assert decode_subworkflow_kwargs(_OpaqueParamsWorkflow, {}) == {}
        assert decode_subworkflow_kwargs(_TypedNamedParamWorkflow, {}) == {}

    def test_round_trip_typed_pydantic(self) -> None:
        """encode(decode(x)) preserves equality for typed Pydantic payloads."""
        original = _RoundTripEnvelope(
            stream_id='abc', metadata={'count': 3, 'tags': ['a', 'b']},
        )
        encoded = encode_subworkflow_kwargs(
            _TypedPydanticParamWorkflow, {'payload': original},
        )
        decoded = decode_subworkflow_kwargs(
            _TypedPydanticParamWorkflow, encoded,
        )
        assert decoded['payload'] == original
