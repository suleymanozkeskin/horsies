"""Frozen relation DDL for the task-history subsystem.

The history projection here is the ratified aggregate shape minus the two
gated column groups; the leaf catalog and lock function are owned by the
partitions module and the lookup type and manifest by the reads module —
this module assembles the order, it does not redefine their DDL.

Identifier columns are native `uuid` throughout, per the ratified type
ruling. Discriminants are constrained text rather than database enums so a
vocabulary addition never rewrites an enum type. Terminalization kinds are
rendered from the production `TerminalizationKind` enum — the CHECK and
the Python vocabulary cannot drift apart.
"""

from __future__ import annotations

from ...lifecycle.operations import TerminalizationKind
from ..names import (
    KEY_RESERVATIONS,
    RETENTION_CLASSES,
    TASK_HISTORY_FOREVER,
    TASK_HISTORY_PARENT,
    WORKFLOW_PHASE2_PENDING,
    WORKFLOW_PHASE2_QUARANTINE,
)

FOREVER_CLASS_KEY = 'forever'
"""The retention-class key of the explicit forever child."""


def _terminalization_kind_list() -> str:
    return ', '.join(f"'{kind.value}'" for kind in TerminalizationKind)


RETENTION_CLASSES_DDL = f"""
CREATE TABLE {RETENTION_CLASSES} (
    class_key varchar(64) PRIMARY KEY,
    duration interval,
    partition_interval interval,
    finite_parent_name text,
    created_at timestamptz NOT NULL,
    CHECK (octet_length(class_key) BETWEEN 1 AND 64),
    CHECK (
        (duration IS NULL
            AND partition_interval IS NULL
            AND finite_parent_name IS NULL)
        OR (duration > interval '0'
            AND partition_interval > interval '0'
            AND finite_parent_name IS NOT NULL)
    )
)
"""

FOREVER_CLASS_ROW_DML = f"""
INSERT INTO {RETENTION_CLASSES}
    (class_key, duration, partition_interval, finite_parent_name, created_at)
VALUES ('{FOREVER_CLASS_KEY}', NULL, NULL, NULL, statement_timestamp())
"""


TASK_HISTORY_PARENT_DDL = f"""
CREATE TABLE {TASK_HISTORY_PARENT} (
    task_id uuid NOT NULL,
    task_name varchar(255) NOT NULL,
    queue_name varchar(100) NOT NULL,
    priority integer NOT NULL CHECK (priority BETWEEN 1 AND 100),
    command_fingerprint_version smallint NOT NULL
        CHECK (command_fingerprint_version > 0),
    command_fingerprint bytea NOT NULL
        CHECK (octet_length(command_fingerprint) = 32),
    status text NOT NULL CHECK (
        status IN ('COMPLETED', 'FAILED', 'CANCELLED', 'EXPIRED')
    ),
    terminalization_kind varchar(32) NOT NULL CHECK (
        terminalization_kind IN ({_terminalization_kind_list()})
    ),
    terminal_at timestamptz NOT NULL,
    retention_anchor_at timestamptz NOT NULL,
    retention_class_key varchar(64) NOT NULL
        REFERENCES {RETENTION_CLASSES}(class_key),
    sent_at timestamptz,
    enqueued_at timestamptz NOT NULL,
    claimed_at timestamptz,
    started_at timestamptz,
    created_at timestamptz NOT NULL,
    good_until timestamptz,
    retry_count integer NOT NULL CHECK (retry_count >= 0),
    max_retries integer NOT NULL CHECK (max_retries >= 0),
    last_claimed_worker_id varchar(255),
    last_worker_hostname varchar(255),
    last_worker_pid integer,
    last_worker_process_name varchar(255),
    result_envelope_version smallint NOT NULL
        CHECK (result_envelope_version > 0),
    result_codec varchar(64) NOT NULL,
    result_content_type varchar(255) NOT NULL,
    result_payload bytea,
    prior_result_payload bytea,
    result_digest bytea,
    error_code text,
    final_failed_reason text,
    input_digest bytea,
    rerun_of_task_id uuid,
    rerun_root_task_id uuid,
    workflow_id uuid,
    is_workflow_task boolean NOT NULL,
    history_schema_version smallint NOT NULL
        CHECK (history_schema_version > 0),
    CHECK (retention_anchor_at = terminal_at),
    CHECK (octet_length(result_codec) BETWEEN 1 AND 64),
    CHECK (octet_length(result_content_type) BETWEEN 1 AND 255),
    CHECK (result_digest IS NULL OR octet_length(result_digest) = 32),
    CHECK (input_digest IS NULL OR octet_length(input_digest) = 32),
    CHECK (
        (rerun_of_task_id IS NULL AND rerun_root_task_id IS NULL)
        OR (rerun_of_task_id IS NOT NULL AND rerun_root_task_id IS NOT NULL)
    ),
    CHECK (
        terminalization_kind <> 'CANCEL_ADMIN'
        OR result_payload IS NULL
    ),
    CHECK (
        prior_result_payload IS NULL
        OR terminalization_kind = 'CANCEL_ADMIN'
    ),
    CHECK (result_payload IS NULL OR prior_result_payload IS NULL),
    CHECK (prior_result_payload IS NULL OR result_digest IS NOT NULL)
) PARTITION BY LIST (retention_class_key)
"""

TASK_HISTORY_FOREVER_DDL = f"""
CREATE TABLE {TASK_HISTORY_FOREVER}
    PARTITION OF {TASK_HISTORY_PARENT}
    FOR VALUES IN ('{FOREVER_CLASS_KEY}')
"""

TASK_HISTORY_FOREVER_ID_INDEX_DDL = f"""
CREATE INDEX {TASK_HISTORY_FOREVER}_task_idx
    ON {TASK_HISTORY_FOREVER} (task_id)
"""


WORKFLOW_PHASE2_QUARANTINE_DDL = f"""
CREATE TABLE {WORKFLOW_PHASE2_QUARANTINE} (
    task_id uuid PRIMARY KEY,
    workflow_id uuid NOT NULL,
    workflow_node_row_id uuid NOT NULL,
    node_id text NOT NULL,
    task_name varchar(255) NOT NULL,
    terminal_status text NOT NULL CHECK (
        terminal_status IN ('COMPLETED', 'FAILED', 'CANCELLED', 'EXPIRED')
    ),
    terminalization_kind varchar(32) NOT NULL CHECK (
        terminalization_kind IN ({_terminalization_kind_list()})
    ),
    terminal_at timestamptz NOT NULL,
    history_schema_version smallint NOT NULL
        CHECK (history_schema_version > 0),
    result_envelope_version smallint NOT NULL
        CHECK (result_envelope_version > 0),
    result_codec varchar(64) NOT NULL,
    result_content_type varchar(255) NOT NULL,
    result_payload bytea NOT NULL,
    result_digest bytea NOT NULL,
    source_history_class varchar(64) NOT NULL,
    source_history_anchor timestamptz NOT NULL,
    quarantine_reason text NOT NULL,
    quarantined_at timestamptz NOT NULL,
    CHECK (octet_length(result_codec) BETWEEN 1 AND 64),
    CHECK (octet_length(result_content_type) BETWEEN 1 AND 255),
    CHECK (octet_length(result_digest) = 32),
    CHECK (octet_length(source_history_class) BETWEEN 1 AND 64)
)
"""


WORKFLOW_PHASE2_PENDING_DDL = f"""
CREATE TABLE {WORKFLOW_PHASE2_PENDING} (
    task_id uuid PRIMARY KEY,
    workflow_id uuid NOT NULL,
    workflow_node_row_id uuid NOT NULL,
    terminal_status text NOT NULL CHECK (
        terminal_status IN ('COMPLETED', 'FAILED', 'CANCELLED', 'EXPIRED')
    ),
    terminal_at timestamptz NOT NULL,
    terminalization_kind varchar(32) NOT NULL CHECK (
        terminalization_kind IN ({_terminalization_kind_list()})
    ),
    recovery_source text NOT NULL CHECK (
        recovery_source IN ('HISTORY', 'QUARANTINE')
    ),
    history_class varchar(64)
        CHECK (
            history_class IS NULL
            OR octet_length(history_class) BETWEEN 1 AND 64
        ),
    history_anchor timestamptz,
    history_schema_version smallint NOT NULL
        CHECK (history_schema_version > 0),
    result_digest bytea NOT NULL CHECK (octet_length(result_digest) = 32),
    quarantine_task_id uuid
        REFERENCES {WORKFLOW_PHASE2_QUARANTINE}(task_id),
    phase2_generation uuid NOT NULL,
    created_at timestamptz NOT NULL,
    attempt_count integer NOT NULL CHECK (attempt_count >= 0),
    last_attempt_at timestamptz,
    last_failure_class varchar(64)
        CHECK (
            last_failure_class IS NULL
            OR octet_length(last_failure_class) BETWEEN 1 AND 64
        ),
    CHECK (
        (recovery_source = 'HISTORY'
            AND history_class IS NOT NULL
            AND history_anchor IS NOT NULL
            AND quarantine_task_id IS NULL)
        OR (recovery_source = 'QUARANTINE'
            AND quarantine_task_id IS NOT NULL)
    )
)
"""

WORKFLOW_PHASE2_PENDING_INDEX_DDL = (
    f"""
    CREATE INDEX {WORKFLOW_PHASE2_PENDING}_age_idx
        ON {WORKFLOW_PHASE2_PENDING} (created_at, task_id)
    """,
    f"""
    CREATE INDEX {WORKFLOW_PHASE2_PENDING}_node_idx
        ON {WORKFLOW_PHASE2_PENDING} (workflow_node_row_id)
    """,
    f"""
    CREATE INDEX {WORKFLOW_PHASE2_PENDING}_locator_idx
        ON {WORKFLOW_PHASE2_PENDING} (history_class, history_anchor, task_id)
        WHERE recovery_source = 'HISTORY'
    """,
    f"""
    CREATE INDEX {WORKFLOW_PHASE2_PENDING}_failure_idx
        ON {WORKFLOW_PHASE2_PENDING} (last_failure_class)
        WHERE last_failure_class IS NOT NULL
    """,
)


KEY_RESERVATIONS_DDL = f"""
CREATE TABLE {KEY_RESERVATIONS} (
    idempotency_key_digest bytea PRIMARY KEY,
    key_scope_version smallint NOT NULL CHECK (key_scope_version > 0),
    fingerprint_version smallint NOT NULL CHECK (fingerprint_version > 0),
    command_fingerprint bytea NOT NULL
        CHECK (octet_length(command_fingerprint) = 32),
    task_id uuid NOT NULL,
    disposition text NOT NULL CHECK (disposition IN ('LIVE', 'TERMINAL')),
    reservation_window interval NOT NULL CHECK (
        reservation_window > interval '0'
        AND reservation_window <= interval '30 days'
    ),
    expires_at timestamptz,
    CHECK (octet_length(idempotency_key_digest) = 32),
    CHECK (
        (disposition = 'LIVE' AND expires_at IS NULL)
        OR (disposition = 'TERMINAL' AND expires_at IS NOT NULL)
    )
)
"""
