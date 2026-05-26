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

from horsies.core.codec.json_value import StrictJsonError
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
            {'value': 1, '__horsies_workflow_ctx__': {'wf': 'id'}},
        )
        assert decoded['value'] == 1
        assert decoded['__horsies_workflow_ctx__'] == {'wf': 'id'}

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
            '__horsies_taskresult__': True,
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


class TestLegacyHorsiesPrefixSmuggleClosed:
    """The smuggling path the design closes: a user-supplied kwarg
    value carrying `__horsies_taskresult__` would reach the worker
    args_from handler and route to legacy class-identity rehydration.
    `encode_value` rejects the legacy prefix at user positions; this
    test confirms encode_kwargs propagates that rejection."""

    def test_legacy_envelope_in_dict_kwarg_rejected(self) -> None:
        def task(payload: dict[str, object]) -> TaskResult[int, TaskError]:  # pyright: ignore[reportUnusedParameter]
            ...

        with pytest.raises(StrictJsonError, match='reserved key'):
            encode_kwargs(
                task,
                {'payload': {'__horsies_taskresult__': True, 'data': 'evil'}},
            )
