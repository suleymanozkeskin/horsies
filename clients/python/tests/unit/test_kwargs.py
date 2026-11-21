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
from horsies.core.codec.kwargs import decode_kwargs, encode_kwargs_read_back
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


def _task_with_optional_upstream(
    upstream_result: TaskResult[int, TaskError] | None,
) -> TaskResult[int, TaskError]:  # pyright: ignore[reportUnusedParameter]
    """An ``args_from`` target under ``join='any'`` / ``'quorum'``.

    The source may never complete, so the documented annotation admits None.
    """
    ...


def _task_with_plain_union(
    payload: int | None,
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
        encoded = encode_kwargs_read_back(_task_a, {'value': 42})
        assert encoded == {'value': 42}

    def test_unknown_kwarg_rejected(self) -> None:
        with pytest.raises(StrictJsonError, match="unknown kwarg 'valuee'"):
            encode_kwargs_read_back(_task_a, {'value': 1, 'valuee': 2})

    def test_workflow_ctx_user_supplied_rejected(self) -> None:
        with pytest.raises(
            StrictJsonError,
            match="kwarg 'workflow_ctx' is engine-injected",
        ):
            encode_kwargs_read_back(_task_with_ctx, {'value': 1, 'workflow_ctx': 'x'})

    def test_workflow_meta_user_supplied_rejected(self) -> None:
        with pytest.raises(
            StrictJsonError,
            match="kwarg 'workflow_meta' is engine-injected",
        ):
            encode_kwargs_read_back(_task_a, {'value': 1, 'workflow_meta': 'x'})

    def test_taskresult_typed_kwarg_user_supplied_rejected(self) -> None:
        # TaskResult-typed kwargs come from the engine's args_from path,
        # not from user code. Producer-side encode rejects to avoid
        # confusion with engine-injected envelopes.
        bogus: TaskResult[int, TaskError] = TaskResult(ok=42)
        with pytest.raises(
            StrictJsonError,
            match="kwarg 'upstream_result' is TaskResult-typed",
        ):
            encode_kwargs_read_back(_task_with_upstream, {'upstream_result': bogus})


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

    def test_optional_taskresult_kwarg_envelope_passes_through(self) -> None:
        # `TaskResult[T, TaskError] | None` is the documented annotation for an
        # args_from target under join='any' / 'quorum'. Its origin is the union,
        # so an origin-only test sends the envelope to decode_value, which
        # rejects the reserved envelope key and fails the node.
        envelope: dict[str, Json] = {
            '__h_taskresult_envelope__': True,
            'data': '{"ok":42}',
        }
        decoded = decode_kwargs(
            _task_with_optional_upstream,
            {'upstream_result': cast(Json, envelope)},
        )
        assert decoded['upstream_result'] == envelope

    def test_optional_taskresult_kwarg_accepts_an_absent_source(self) -> None:
        # The other half of the join='any' contract: no envelope at all.
        decoded = decode_kwargs(
            _task_with_optional_upstream,
            {'upstream_result': None},
        )
        assert decoded['upstream_result'] is None

    def test_union_without_taskresult_still_decodes_as_a_value(self) -> None:
        decoded = decode_kwargs(_task_with_plain_union, {'payload': 42})
        assert decoded['payload'] == 42

    def test_union_without_taskresult_still_rejects_the_reserved_key(self) -> None:
        # Only the union-of-TaskResult shape gains passthrough; every other
        # union keeps decode_value's smuggling guard.
        smuggled: dict[str, Json] = {'__h_taskresult_envelope__': True}
        with pytest.raises(
            StrictJsonError,
            match="reserved key '__h_taskresult_envelope__'",
        ):
            decode_kwargs(
                _task_with_plain_union,
                {'payload': cast(Json, smuggled)},
            )


class TestNoHints:
    """A callable with no annotations binds nothing, so each key is refused."""

    def test_encode_refuses_a_key(self) -> None:
        def f(value):  # type: ignore[no-untyped-def]
            return value

        with pytest.raises(StrictJsonError, match='no type annotation'):
            encode_kwargs_read_back(f, {'value': 42})

    def test_decode_refuses_a_key(self) -> None:
        def f(value):  # type: ignore[no-untyped-def]
            return value

        with pytest.raises(StrictJsonError, match='no type annotation'):
            decode_kwargs(f, {'value': 42})

    def test_an_empty_call_needs_no_hints(self) -> None:
        def f(*, value: 'NotDefinedHere' = 1) -> int:  # type: ignore[name-defined]  # noqa: F821
            return value

        assert encode_kwargs_read_back(f, {}) == {}
        assert decode_kwargs(f, {}) == {}

    def test_a_key_that_names_a_catch_all_does_not_bind(self) -> None:
        def f(**params: int) -> None:
            _ = params

        resolved = resolve_hints_and_sig(f)
        assert resolved is not None
        hints, sig = resolved

        binding = resolve_producer_kwarg_binding(f, 'params', hints, sig)

        assert binding.kind == 'error'
        assert "unknown kwarg 'params'" in (binding.error_message or '')

    def test_hints_that_do_not_resolve_are_refused(self) -> None:
        def f(*, value: 'NotDefinedHere') -> int:  # type: ignore[name-defined]  # noqa: F821
            return value

        with pytest.raises(StrictJsonError, match='do not resolve'):
            encode_kwargs_read_back(f, {'value': 42})
        with pytest.raises(StrictJsonError, match='do not resolve'):
            decode_kwargs(f, {'value': 42})


class TestWireRoundTripNestedJsonValue:
    """Integration-level smoke for the strict-serde wire contract.

    The broker is opaque storage — it round-trips bytes. The actual
    serde surface is `encode_kwargs_read_back → dumps_json → loads_json →
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
        from horsies.core.codec.json_io import dumps_json, loads_json  # noqa: PLC0415
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
        encoded = encode_kwargs_read_back(_ingest_round_trip, {'payload': payload})
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
            encode_kwargs_read_back(_ingest_smuggle, {'payload': bad})


class TestLegacyHorsiesPrefixSmuggleClosed:
    """The smuggling path the design closes: a user-supplied kwarg
    value carrying `__h_taskresult_envelope__` would reach the worker
    args_from handler and route to legacy class-identity rehydration.
    `encode_value` rejects the legacy prefix at user positions; this
    test confirms encode_kwargs_read_back propagates that rejection."""

    def test_legacy_envelope_in_dict_kwarg_rejected(self) -> None:
        def task(payload: dict[str, object]) -> TaskResult[int, TaskError]:  # pyright: ignore[reportUnusedParameter]
            ...

        with pytest.raises(StrictJsonError, match='reserved key'):
            encode_kwargs_read_back(
                task,
                {'payload': {'__h_taskresult_envelope__': True, 'data': 'evil'}},
            )


# ── SubWorkflowNode.kwargs typed serde ───────────────────────────────


from typing import Any  # noqa: E402

from horsies.core.app import Horsies  # noqa: E402
from horsies.core.codec.kwargs import (  # noqa: E402
    decode_subworkflow_kwargs,
    resolve_hints_and_sig,
    resolve_producer_kwarg_binding,
)
from horsies.core.errors import ErrorCode, WorkflowValidationError  # noqa: E402
from horsies.core.models.workflow import (  # noqa: E402
    SubWorkflowNode,
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


class _NamedAppWorkflow(WorkflowDefinition[int]):
    """build_with(cls, horsies_app, *, x) — the name of the app slot is free."""

    name = 'named_app_workflow'
    definition_key = 'tests.unit.named_app.v1'

    @classmethod
    def build_with(  # pyright: ignore[reportUnusedParameter]
        cls, horsies_app: Any, *, x: int,
    ) -> WorkflowSpec[int]:
        raise NotImplementedError


class _BareParamsWorkflow(WorkflowDefinition[int]):
    """build_with(cls, app, **params) — no annotation at all."""

    name = 'bare_params_workflow'
    definition_key = 'tests.unit.bare_params.v1'

    @classmethod
    def build_with(cls, app, **params):  # type: ignore[no-untyped-def]  # noqa: ANN001, ANN003, ANN206
        raise NotImplementedError


class _OpenBagWorkflow(WorkflowDefinition[int]):
    """build_with(cls, app, params: dict[str, JsonValue]) — the named open bag."""

    name = 'open_bag_workflow'
    definition_key = 'tests.unit.open_bag.v1'

    @classmethod
    def build_with(  # pyright: ignore[reportUnusedParameter]
        cls, app: Horsies, params: dict[str, JsonValue],
    ) -> WorkflowSpec[int]:
        raise NotImplementedError


def _int_source() -> TaskResult[int, TaskError]:
    return TaskResult(ok=1)


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


def _node_wire(
    workflow_def: type[WorkflowDefinition[int]],
    kwargs: dict[str, Any],
) -> dict[str, Any]:
    """The wire form that a spec keeps for one `SubWorkflowNode`."""
    node: SubWorkflowNode[int] = SubWorkflowNode(
        workflow_def=workflow_def, kwargs=kwargs,
    )
    wire = WorkflowSpec(name='wire', tasks=[node]).tasks[0].wire_kwargs
    assert wire is not None
    return dict(wire)


class TestSubworkflowNodeKwargs:
    """A spec binds each kwarg of a `SubWorkflowNode` to a named, typed parameter
    of the child `build_with`, and keeps the form that it proved."""

    def test_typed_named_param_encodes(self) -> None:
        assert _node_wire(_TypedNamedParamWorkflow, {'value': 42}) == {'value': 42}

    def test_typed_pydantic_param_round_trips_via_typeadapter(self) -> None:
        """Pydantic models survive encode via the typed path."""
        payload = _RoundTripEnvelope(
            stream_id='stream-1', metadata={'k': 'v'},
        )
        result = _node_wire(_TypedPydanticParamWorkflow, {'payload': payload})
        # Encoded as a dict via the Pydantic annotation.
        assert isinstance(result['payload'], dict)
        payload_dict = cast('dict[str, Json]', result['payload'])
        assert payload_dict['stream_id'] == 'stream-1'

    def test_unknown_kwarg_rejected(self) -> None:
        with pytest.raises(WorkflowValidationError, match='other'):
            _node_wire(_TypedNamedParamWorkflow, {'value': 1, 'other': 1})

    def test_taskresult_typed_kwarg_rejected(self) -> None:
        """TaskResult-typed params are args_from-only."""
        with pytest.raises(WorkflowValidationError, match='TaskResult'):
            _node_wire(_TaskResultParamWorkflow, {'dep_result': TaskResult(ok=1)})

    def test_unannotated_param_rejected(self) -> None:
        with pytest.raises(WorkflowValidationError, match='no type annotation'):
            _node_wire(_UnannotatedParamWorkflow, {'value': 1})

    def test_a_list_goes_in_a_named_open_bag(self) -> None:
        assert _node_wire(_OpenBagWorkflow, {'params': {'a': [1, 2]}}) == {
            'params': {'a': [1, 2]},
        }

    @pytest.mark.parametrize(
        'workflow_def',
        [_OpaqueParamsWorkflow, _PureKwargsWorkflow, _BareParamsWorkflow],
        ids=['args_and_params', 'pure_kwargs', 'bare_params'],
    )
    def test_a_catch_all_takes_no_kwarg(
        self, workflow_def: type[WorkflowDefinition[int]],
    ) -> None:
        with pytest.raises(WorkflowValidationError) as caught:
            _node_wire(workflow_def, {'a': 1})

        assert caught.value.code == ErrorCode.WORKFLOW_INVALID_KWARG_KEY
        assert 'takes nothing from a node' in str(caught.value)

    @pytest.mark.parametrize(
        'workflow_def',
        [_OpaqueParamsWorkflow, _PureKwargsWorkflow, _BareParamsWorkflow],
        ids=['args_and_params', 'pure_kwargs', 'bare_params'],
    )
    def test_a_catch_all_takes_no_args_from(
        self, workflow_def: type[WorkflowDefinition[int]],
    ) -> None:
        source: TaskNode[int] = TaskNode(fn=_int_source)
        node: SubWorkflowNode[int] = SubWorkflowNode(
            workflow_def=workflow_def,
            waits_for=[source],
            args_from={'a': source},
        )

        with pytest.raises(WorkflowValidationError) as caught:
            WorkflowSpec(name='wire', tasks=[source, node])

        assert caught.value.code == ErrorCode.WORKFLOW_INVALID_KWARG_KEY

    def test_a_declared_catch_all_with_nothing_bound_is_valid(self) -> None:
        assert _node_wire(_OpaqueParamsWorkflow, {}) == {}
        assert _node_wire(_BareParamsWorkflow, {}) == {}

    def test_the_app_slot_takes_no_kwarg_whatever_its_name(self) -> None:
        assert _node_wire(_NamedAppWorkflow, {'x': 1}) == {'x': 1}

        with pytest.raises(WorkflowValidationError) as caught:
            _node_wire(_NamedAppWorkflow, {'horsies_app': 1, 'x': 1})

        assert caught.value.code == ErrorCode.WORKFLOW_INVALID_KWARG_KEY

    def test_the_app_slot_takes_no_args_from_whatever_its_name(self) -> None:
        source: TaskNode[int] = TaskNode(fn=_int_source)
        node: SubWorkflowNode[int] = SubWorkflowNode(
            workflow_def=_NamedAppWorkflow,
            waits_for=[source],
            args_from={'horsies_app': source},
            kwargs={'x': 1},
        )

        with pytest.raises(WorkflowValidationError) as caught:
            WorkflowSpec(name='wire', tasks=[source, node])

        assert caught.value.code == ErrorCode.WORKFLOW_INVALID_KWARG_KEY

    def test_a_key_that_names_the_catch_all_is_refused(self) -> None:
        with pytest.raises(WorkflowValidationError):
            _node_wire(_OpaqueParamsWorkflow, {'args': [1]})
        with pytest.raises(WorkflowValidationError):
            _node_wire(_OpaqueParamsWorkflow, {'params': {'a': 1}})


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

    def test_a_stored_key_that_names_the_app_slot_is_refused(self) -> None:
        with pytest.raises(StrictJsonError, match="unknown kwarg 'horsies_app'"):
            decode_subworkflow_kwargs(_NamedAppWorkflow, {'horsies_app': 1, 'x': 1})

    def test_a_stored_key_that_reaches_a_catch_all_is_refused(self) -> None:
        with pytest.raises(StrictJsonError, match="unknown kwarg 'a'"):
            decode_subworkflow_kwargs(_OpaqueParamsWorkflow, {'a': 1})

    def test_empty_kwargs_passes(self) -> None:
        assert decode_subworkflow_kwargs(_OpaqueParamsWorkflow, {}) == {}
        assert decode_subworkflow_kwargs(_TypedNamedParamWorkflow, {}) == {}

    def test_round_trip_typed_pydantic(self) -> None:
        """encode(decode(x)) preserves equality for typed Pydantic payloads."""
        original = _RoundTripEnvelope(
            stream_id='abc', metadata={'count': 3, 'tags': ['a', 'b']},
        )
        encoded = _node_wire(_TypedPydanticParamWorkflow, {'payload': original})
        decoded = decode_subworkflow_kwargs(
            _TypedPydanticParamWorkflow, encoded,
        )
        assert decoded['payload'] == original


class TestSubworkflowKwargsVarParamNameCollision:
    """A key that happens to name `*args` or `**params` must not pick up the `Any`
    of the catch-all."""

    def test_decode_refuses_the_names_of_the_catch_alls(self) -> None:
        with pytest.raises(StrictJsonError, match="unknown kwarg 'args'"):
            decode_subworkflow_kwargs(_OpaqueParamsWorkflow, {'args': [1, 2, 3]})
        with pytest.raises(StrictJsonError, match="unknown kwarg 'params'"):
            decode_subworkflow_kwargs(_OpaqueParamsWorkflow, {'params': {'a': 1}})


# ── The resolution of a plain function is kept ─────────────────────────

import gc  # noqa: E402
import weakref  # noqa: E402
from typing import get_type_hints  # noqa: E402

from horsies.core.codec import kwargs as kwargs_module  # noqa: E402


class _CountedHints:
    """``get_type_hints`` that counts its calls."""

    def __init__(self) -> None:
        self.calls = 0

    def __call__(self, obj: Any, *, include_extras: bool) -> dict[str, Any]:
        self.calls += 1
        return get_type_hints(obj, include_extras=include_extras)


class TestResolutionIsKeptPerFunction:
    def test_a_function_resolves_once(self, monkeypatch: pytest.MonkeyPatch) -> None:
        counted = _CountedHints()
        monkeypatch.setattr(kwargs_module, 'get_type_hints', counted)

        def f(*, value: int) -> int:
            return value

        first = resolve_hints_and_sig(f)
        second = resolve_hints_and_sig(f)

        assert counted.calls == 1
        assert first is not None and first == second

    def test_each_call_gets_its_own_hints(self) -> None:
        def f(*, value: int) -> int:
            return value

        _ = resolve_hints_and_sig(f)
        kept = resolve_hints_and_sig(f)
        assert kept is not None
        kept[0]['value'] = str

        again = resolve_hints_and_sig(f)

        assert again is not None and again[0]['value'] is int

    def test_a_failed_resolution_is_not_kept(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        def f(*, value: 'LaterName') -> int:  # type: ignore[name-defined]  # noqa: F821
            return 0

        assert resolve_hints_and_sig(f) is None  # pyright: ignore[reportUnknownArgumentType]
        monkeypatch.setitem(f.__globals__, 'LaterName', int)

        resolved = resolve_hints_and_sig(f)  # pyright: ignore[reportUnknownArgumentType]

        assert resolved is not None and resolved[0]['value'] is int

    def test_a_kept_resolution_does_not_keep_its_function_alive(self) -> None:
        def f(*, value: int) -> int:
            return value

        assert resolve_hints_and_sig(f) is not None
        watcher = weakref.ref(f)
        del f
        _ = gc.collect()

        assert watcher() is None
