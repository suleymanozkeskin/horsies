"""Field provenance for the rerun enqueue: every column declares a side.

The classification is closed over the post-cutover enqueue-visible
column set. A column added to the live table must be classified here
before any rerun code can write it — the pin fails on an unclassified
column, so no field can silently inherit a side.
"""

from __future__ import annotations

from enum import Enum
from typing import Final


class FieldProvenance(Enum):
    """Which side of the replay principle a column derives from."""

    NEW_IDENTITY = 'NEW_IDENTITY'
    REPLAYED_FROM_SOURCE = 'REPLAYED_FROM_SOURCE'
    LINEAGE = 'LINEAGE'
    CALLER_EXPLICIT = 'CALLER_EXPLICIT'
    RESOLVED_AT_ENQUEUE = 'RESOLVED_AT_ENQUEUE'
    FRESH_RUNTIME_STATE = 'FRESH_RUNTIME_STATE'


RERUN_FIELD_PROVENANCE: Final[dict[str, FieldProvenance]] = {
    # A rerun is a new request with its own identity and clock.
    'id': FieldProvenance.NEW_IDENTITY,
    'created_at': FieldProvenance.NEW_IDENTITY,
    'enqueued_at': FieldProvenance.NEW_IDENTITY,
    'sent_at': FieldProvenance.NEW_IDENTITY,
    'updated_at': FieldProvenance.NEW_IDENTITY,
    # The source request specified these; the lineage claims
    # re-execution of THAT request.
    'task_name': FieldProvenance.REPLAYED_FROM_SOURCE,
    'queue_name': FieldProvenance.REPLAYED_FROM_SOURCE,
    'priority': FieldProvenance.REPLAYED_FROM_SOURCE,
    'args': FieldProvenance.REPLAYED_FROM_SOURCE,
    'kwargs': FieldProvenance.REPLAYED_FROM_SOURCE,
    'task_options': FieldProvenance.REPLAYED_FROM_SOURCE,
    'max_retries': FieldProvenance.REPLAYED_FROM_SOURCE,
    'is_workflow_task': FieldProvenance.REPLAYED_FROM_SOURCE,
    'input_digest': FieldProvenance.REPLAYED_FROM_SOURCE,
    # The atomic pair per the frozen CHECK.
    'rerun_of_task_id': FieldProvenance.LINEAGE,
    'rerun_root_task_id': FieldProvenance.LINEAGE,
    # The explicit new deadline or explicit no-deadline choice.
    'good_until': FieldProvenance.CALLER_EXPLICIT,
    # Each enqueue makes its own policy promise.
    'retention_class_key': FieldProvenance.RESOLVED_AT_ENQUEUE,
    'retain_rerun_input': FieldProvenance.RESOLVED_AT_ENQUEUE,
    'prepared_rerun_input_disposition': FieldProvenance.RESOLVED_AT_ENQUEUE,
    'prepared_rerun_input_version': FieldProvenance.RESOLVED_AT_ENQUEUE,
    'prepared_rerun_input_codec': FieldProvenance.RESOLVED_AT_ENQUEUE,
    'prepared_rerun_input_content_type': FieldProvenance.RESOLVED_AT_ENQUEUE,
    'prepared_rerun_input_digest': FieldProvenance.RESOLVED_AT_ENQUEUE,
    'prepared_rerun_input_inline': FieldProvenance.RESOLVED_AT_ENQUEUE,
    'prepared_rerun_input_reference': FieldProvenance.RESOLVED_AT_ENQUEUE,
    'idempotency_key_digest': FieldProvenance.RESOLVED_AT_ENQUEUE,
    'command_fingerprint_version': FieldProvenance.RESOLVED_AT_ENQUEUE,
    'command_fingerprint': FieldProvenance.RESOLVED_AT_ENQUEUE,
    'enqueue_sha': FieldProvenance.RESOLVED_AT_ENQUEUE,
    # Lifecycle zero-state; attempt numbering and budget anew.
    'status': FieldProvenance.FRESH_RUNTIME_STATE,
    'retry_count': FieldProvenance.FRESH_RUNTIME_STATE,
    'next_retry_at': FieldProvenance.FRESH_RUNTIME_STATE,
    'claimed': FieldProvenance.FRESH_RUNTIME_STATE,
    'claimed_at': FieldProvenance.FRESH_RUNTIME_STATE,
    'claimed_by_worker_id': FieldProvenance.FRESH_RUNTIME_STATE,
    'claim_expires_at': FieldProvenance.FRESH_RUNTIME_STATE,
    'started_at': FieldProvenance.FRESH_RUNTIME_STATE,
    'completed_at': FieldProvenance.FRESH_RUNTIME_STATE,
    'failed_at': FieldProvenance.FRESH_RUNTIME_STATE,
    'terminal_at': FieldProvenance.FRESH_RUNTIME_STATE,
    'result': FieldProvenance.FRESH_RUNTIME_STATE,
    'failed_reason': FieldProvenance.FRESH_RUNTIME_STATE,
    'error_code': FieldProvenance.FRESH_RUNTIME_STATE,
    'finalizing_at': FieldProvenance.FRESH_RUNTIME_STATE,
    'finalizing_by_worker_id': FieldProvenance.FRESH_RUNTIME_STATE,
    'worker_pid': FieldProvenance.FRESH_RUNTIME_STATE,
    'worker_hostname': FieldProvenance.FRESH_RUNTIME_STATE,
    'worker_process_name': FieldProvenance.FRESH_RUNTIME_STATE,
}
"""One entry per enqueue-visible column of the post-cutover live table."""
