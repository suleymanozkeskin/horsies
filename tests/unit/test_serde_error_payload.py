"""Regression tests for `serialize_error_payload` exception flattening.

Strict-serde phase 5/6 regression: ``child_runner.py:1054`` writes a
live ``BaseException`` into ``TaskError.exception`` for tasks that raise.
The strict codec can't encode a raw ``BaseException`` — without an
explicit flatten step, every task-raised exception got silently
downgraded to ``WORKER_SERIALIZATION_ERROR`` via the fallback path,
hiding the real ``TASK_EXCEPTION`` from downstream consumers.

These tests lock in:

1. The happy path with ``exception=None`` still works (no behaviour
   change for library-built err payloads that don't carry an exception).
2. A live exception in ``TaskError.exception`` survives encoding by
   being flattened to a structured dict.
3. A pre-flattened dict in ``TaskError.exception`` round-trips as-is.
4. The original ``error_code`` / ``message`` are preserved end-to-end —
   no silent downgrade to ``WORKER_SERIALIZATION_ERROR``.
"""

from __future__ import annotations

import json
from typing import Any

import pytest

from horsies.core.codec.serde import serialize_error_payload
from horsies.core.models.tasks import (
    OperationalErrorCode,
    SubWorkflowError,
    TaskError,
    TaskResult,
)
from horsies.core.models.workflow.context import SubWorkflowSummary
from horsies.core.models.workflow.enums import WorkflowStatus


def _decode_envelope(wire: str) -> dict[str, Any]:
    parsed: dict[str, Any] = json.loads(wire)
    return parsed


@pytest.mark.unit
class TestSerializeErrorPayloadFlattensException:
    def test_exception_none_round_trips_unchanged(self) -> None:
        tr: TaskResult[Any, TaskError] = TaskResult(
            err=TaskError(
                error_code=OperationalErrorCode.BROKER_ERROR,
                message='nothing to flatten',
            ),
        )
        wire = serialize_error_payload(tr)
        envelope = _decode_envelope(wire)
        assert envelope['__h_task_result__'] is True
        assert envelope['ok'] is None
        err = envelope['err']
        assert err['error_code'] == {'__builtin_task_code__': 'BROKER_ERROR'}
        assert err['exception'] is None

    def test_live_exception_flattened_to_dict(self) -> None:
        """A raw BaseException survives encoding via the flatten step.

        Regression: without `_flatten_task_error_exception` the strict
        codec couldn't serialize the exception field, so the encoder
        raised and the payload fell back to WORKER_SERIALIZATION_ERROR
        (losing the original TASK_EXCEPTION + message).

        Locks the diagnostic-shape contract: the flattened dict carries
        ``type`` / ``message`` / ``traceback`` (matching the legacy
        ``_exception_to_json`` form so downstream tooling still works)
        plus ``module`` and ``repr`` for disambiguation. ``traceback``
        is the high-value field — it gets surfaced in logs and error
        UIs — and must not regress.
        """
        try:
            raise ValueError('boom')
        except ValueError as caught:
            original = caught

        tr: TaskResult[Any, TaskError] = TaskResult(
            err=TaskError(
                error_code=OperationalErrorCode.TASK_EXCEPTION,
                message=f'{type(original).__name__}: {original}',
                exception=original,
            ),
        )
        wire = serialize_error_payload(tr)
        envelope = _decode_envelope(wire)

        # Critical: error_code stays TASK_EXCEPTION — no silent
        # downgrade to WORKER_SERIALIZATION_ERROR.
        err = envelope['err']
        assert err['error_code'] == {'__builtin_task_code__': 'TASK_EXCEPTION'}
        assert 'ValueError' in err['message']
        assert 'boom' in err['message']

        # The exception field is flattened to a structured dict that
        # extends the legacy _exception_to_json shape.
        flattened = err['exception']
        assert isinstance(flattened, dict)
        assert flattened['type'] == 'ValueError'
        assert flattened['module'] == 'builtins'
        assert flattened['message'] == 'boom'
        assert flattened['repr'] == "ValueError('boom')"
        # Traceback is the high-value diagnostic field; must survive.
        assert isinstance(flattened['traceback'], str)
        assert 'ValueError: boom' in flattened['traceback']
        assert 'test_live_exception_flattened_to_dict' in flattened['traceback']

    def test_user_defined_exception_class_flattens(self) -> None:
        """Custom exception classes flatten to ``type`` + ``module``."""

        class MyDomainError(RuntimeError):
            pass

        original = MyDomainError('domain problem')
        tr: TaskResult[Any, TaskError] = TaskResult(
            err=TaskError(
                error_code=OperationalErrorCode.TASK_EXCEPTION,
                message='custom failure',
                exception=original,
            ),
        )
        wire = serialize_error_payload(tr)
        envelope = _decode_envelope(wire)
        flattened = envelope['err']['exception']
        assert flattened['type'] == 'MyDomainError'
        # Module is the test module — confirms the flatten uses
        # ``type(exc).__module__``, not a hardcoded value.
        assert flattened['module'] == __name__
        assert flattened['message'] == 'domain problem'

    def test_already_flattened_dict_passes_through(self) -> None:
        """A pre-flattened ``exception`` dict is encoded as-is."""
        pre_flattened: dict[str, Any] = {
            'type': 'ConnectionError',
            'module': 'builtins',
            'message': 'db down',
            'repr': "ConnectionError('db down')",
        }
        tr: TaskResult[Any, TaskError] = TaskResult(
            err=TaskError(
                error_code=OperationalErrorCode.BROKER_ERROR,
                message='db down',
                exception=pre_flattened,
            ),
        )
        wire = serialize_error_payload(tr)
        envelope = _decode_envelope(wire)
        # The pre-flattened dict round-trips unchanged.
        assert envelope['err']['exception'] == pre_flattened

    def test_serialize_error_payload_strips_sub_workflow_error_subclass_fields(
        self,
    ) -> None:
        """``serialize_error_payload`` is statically typed at
        ``TaskError`` and routes through ``encode_value(..., TaskError)``,
        which drops subclass fields like ``sub_workflow_id``.

        This is the *intentional* contract: every production caller of
        ``serialize_error_payload`` constructs a plain ``TaskError``.
        ``SubWorkflowError`` emission lives in
        ``engine.on_subworkflow_complete``, which builds the envelope by
        hand via ``error.model_dump(mode='json')`` so subclass fields
        survive. This test documents the constraint so a future
        refactor doesn't quietly start routing subworkflow err payloads
        through this helper and lose the discriminator fields.
        """
        summary: SubWorkflowSummary[Any] = SubWorkflowSummary(
            status=WorkflowStatus.FAILED,
            output=None,
            total_tasks=1,
            completed_tasks=0,
            failed_tasks=1,
            skipped_tasks=0,
            error_summary='downstream',
        )
        tr: TaskResult[Any, TaskError] = TaskResult(
            err=SubWorkflowError(
                error_code=OperationalErrorCode.UNHANDLED_EXCEPTION,
                message='subworkflow failed',
                sub_workflow_id='wf-child',
                sub_workflow_summary=summary,
            ),
        )
        wire = serialize_error_payload(tr)
        envelope = _decode_envelope(wire)
        err = envelope['err']
        # Base-class fields survive.
        assert err['error_code'] == {
            '__builtin_task_code__': 'UNHANDLED_EXCEPTION',
        }
        # Subclass fields are NOT present — engine emit path bypasses
        # this helper specifically to preserve them.
        assert 'sub_workflow_id' not in err
        assert 'sub_workflow_summary' not in err
