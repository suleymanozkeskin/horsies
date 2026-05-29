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

from horsies.core.codec.error_payload import serialize_error_payload
from horsies.core.models.tasks import (
    FlattenedException,
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
        """A pre-flattened ``exception`` dict is encoded as-is.

        Strict-serde phase 7 promoted ``TaskError.exception`` to
        ``BaseException | FlattenedException | None``. The dict variant
        must carry the full ``FlattenedException`` shape — all five
        keys (``type``, ``module``, ``message``, ``repr``, ``traceback``)
        — or model validation rejects it. This is the intentional
        tightening: callers building the dict by hand must produce the
        same shape ``flatten_exception`` would produce.
        """
        pre_flattened: FlattenedException = {
            'type': 'ConnectionError',
            'module': 'builtins',
            'message': 'db down',
            'repr': "ConnectionError('db down')",
            'traceback': 'Traceback (most recent call last):\n  ConnectionError: db down\n',
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

    def test_serialize_error_payload_preserves_sub_workflow_error_subclass_fields(
        self,
    ) -> None:
        """``serialize_error_payload`` routes through ``encode_task_result``
        which routes through ``encode_task_error``, so ``SubWorkflowError``
        subclass fields (``sub_workflow_id`` / ``sub_workflow_summary``)
        now survive the encode.

        Pre-fix this helper called ``encode_value(_, TaskError)`` which
        silently dropped subclass fields. Every production caller of
        ``serialize_error_payload`` happened to construct only plain
        ``TaskError`` instances, so the bug was latent — but the
        invariant is now positive: any subclass of ``TaskError`` that
        flows through this helper round-trips with its subclass fields
        intact.
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
        # Subclass fields are now preserved through the encode helper.
        assert err['sub_workflow_id'] == 'wf-child'
        assert isinstance(err['sub_workflow_summary'], dict)
        assert err['sub_workflow_summary']['failed_tasks'] == 1
        assert err['sub_workflow_summary']['error_summary'] == 'downstream'


@pytest.mark.unit
class TestTaskErrorModelDumpException:
    """Direct model_dump on a TaskError holding a live exception must not crash.

    The codec path excludes the field; these cover any other caller doing a
    plain model_dump(mode='json') / model_dump_json(), which used to raise
    PydanticSerializationError on a live BaseException.
    """

    def test_model_dump_json_mode_flattens_live_exception(self) -> None:
        try:
            raise ValueError('boom')
        except ValueError as caught:
            te = TaskError(
                error_code=OperationalErrorCode.TASK_EXCEPTION,
                message='m',
                exception=caught,
            )

        dumped = te.model_dump(mode='json')
        flattened = dumped['exception']
        assert isinstance(flattened, dict)
        assert flattened['type'] == 'ValueError'
        assert flattened['module'] == 'builtins'
        assert flattened['message'] == 'boom'
        assert flattened['repr'] == "ValueError('boom')"
        assert 'ValueError: boom' in flattened['traceback']

    def test_model_dump_json_string_does_not_raise(self) -> None:
        te = TaskError(message='m', exception=ValueError('boom'))
        wire = te.model_dump_json()
        parsed: dict[str, Any] = json.loads(wire)
        assert parsed['exception']['type'] == 'ValueError'

    def test_python_mode_keeps_live_exception_object(self) -> None:
        """when_used='json' must not touch python-mode dumps."""
        original = ValueError('boom')
        te = TaskError(message='m', exception=original)
        dumped = te.model_dump()
        assert dumped['exception'] is original

    def test_none_exception_json_mode_is_none(self) -> None:
        te = TaskError(message='m')
        assert te.model_dump(mode='json')['exception'] is None

    def test_already_flattened_dict_passes_through(self) -> None:
        flat: FlattenedException = {
            'type': 'ValueError', 'module': 'builtins', 'message': 'boom',
            'repr': "ValueError('boom')", 'traceback': 'tb',
        }
        te = TaskError(message='m', exception=flat)
        assert te.model_dump(mode='json')['exception'] == flat
