"""``BASELINE_CATALOG``: the rule of every column of the baseline schema (migration 1000).

The table order is the labelling order: a table that owns an id comes before
the tables that refer to it, so a label is assigned by the owner's natural
key where possible.

Clock columns use one of three rules:

- ``RUN_CLOCK``: a value the run takes from the clock, or moves from it by
  minutes or hours. Within ``RUN_CLOCK_MARGIN`` of the run window it is a
  token; further out (a fixed test time) it compares exactly.
- ``ORDER``: a value whose order between rows is behaviour (send order,
  claim order), or a value derived from a time the row does not carry
  (expiry from a terminal time, a deadline a test computes from its own
  clock). Only the rank compares.
- ``anchored(column)``: a value computed from another clock column of the
  row plus a duration (lease end, retry time). The offset compares within
  ``ANCHOR_TOLERANCE``.

Stored JSON (inputs, options, results, errors, the attempt archive, the
rerun envelope) is parsed and compared as JSON; the ids and clock values
inside it have path rules. A digest column is checked against its source
column. Partition leaf names compare with their date or hour as an offset
from the run start.
"""

from __future__ import annotations

from datetime import timedelta
from typing import Final

from tests.dualrun.rules import (
    AnchorReference,
    ColumnRule,
    ColumnSpec,
    DigestOf,
    EachItem,
    Exact,
    FormatVersion,
    GeneratedName,
    IdFamily,
    IdsInText,
    Ignored,
    Index,
    JsonPath,
    JsonStorage,
    JsonTime,
    Key,
    NormalisedId,
    RunWindowReference,
    SnapshotCatalog,
    StoredJson,
    TableRules,
    TimeEncoding,
    TimeOrderOnly,
    TimeWithin,
    Unversioned,
)

RUN_CLOCK_MARGIN: Final = timedelta(days=1)
"""Widening of the run window on each side.

Test code moves clock values by minutes or hours from the run (a stale
claim, a task created an hour ago); such a value is a run value. A fixed
test time more than a day from the run compares exactly.
"""

ANCHOR_TOLERANCE: Final = timedelta(seconds=1)
"""Largest difference between two offsets from an anchor that still compares equal."""

EXACT: Final = Exact()
ORDER: Final = TimeOrderOnly()
RUN_CLOCK: Final = TimeWithin(reference=RunWindowReference(), bound=RUN_CLOCK_MARGIN)
TASK_ID: Final = NormalisedId(family=IdFamily.TASK)
WORKFLOW_ID: Final = NormalisedId(family=IdFamily.WORKFLOW)
WORKFLOW_NODE_ID: Final = NormalisedId(family=IdFamily.WORKFLOW_NODE)
WORKER_ID: Final = NormalisedId(family=IdFamily.WORKER)
COMMAND_FINGERPRINT: Final = NormalisedId(family=IdFamily.COMMAND_FINGERPRINT)
"""Version 1 covers good_until and the rerun lineage ids, so its bytes differ between runs."""
LEAF_NAME: Final = GeneratedName()
"""Leaf names carry the day or hour of their range."""
SCAN_POSITION: Final = NormalisedId(family=IdFamily.SCAN_POSITION)
"""A cursor position: a workflow id or a task id, by scan."""

_PROCESS_SAMPLE: Final = Ignored(
    reason=(
        'resource sample read from the operating system at snapshot time; it differs '
        'on every read and stores no decision of the code under test'
    ),
)
JSON: Final = StoredJson(storage=JsonStorage.TEXT, format=Unversioned(), paths=())
"""A JSON text with no ids or clock values inside."""

TASK_INPUT: Final = StoredJson(
    storage=JsonStorage.TEXT,
    format=Unversioned(),
    paths=(
        JsonPath(
            steps=(Key(name='__h_workflow_meta__'), Key(name='workflow_id')),
            rule=WORKFLOW_ID,
        ),
    ),
)
"""A task's input. The input of an engine-minted workflow task names its
workflow in ``__h_workflow_meta__``."""

WORKFLOW_IDS_IN_TEXT: Final = IdsInText(family=IdFamily.WORKFLOW)
"""A message that can name a workflow by its id."""

WORKFLOW_ERROR: Final = StoredJson(
    storage=JsonStorage.TEXT,
    format=Unversioned(),
    paths=(
        JsonPath(steps=(Key(name='data'), Key(name='workflow_id')), rule=WORKFLOW_ID),
        JsonPath(steps=(Key(name='sub_workflow_id'),), rule=WORKFLOW_ID),
        JsonPath(steps=(Key(name='message'),), rule=WORKFLOW_IDS_IN_TEXT),
    ),
)
"""A stored workflow error. The recovery limit error names its workflow in
``data.workflow_id``. The error of a workflow that a failed child paused
names the child in ``sub_workflow_id`` and in ``message`` (``Subworkflow
<id> failed``)."""

NODE_RESULT: Final = StoredJson(
    storage=JsonStorage.TEXT,
    format=Unversioned(),
    paths=(
        JsonPath(
            steps=(Key(name='err'), Key(name='data'), Key(name='workflow_id')),
            rule=WORKFLOW_ID,
        ),
        JsonPath(
            steps=(Key(name='err'), Key(name='sub_workflow_id')), rule=WORKFLOW_ID
        ),
        JsonPath(
            steps=(Key(name='err'), Key(name='message')), rule=WORKFLOW_IDS_IN_TEXT
        ),
    ),
)
"""A stored node result. The recovery limit result names its workflow in
``err.data.workflow_id``. The result of a sub-workflow node whose child
failed names the child in ``err.sub_workflow_id`` and in ``err.message``
(``Subworkflow <id> failed with status <status>``)."""

TASK_OPTIONS: Final = StoredJson(
    storage=JsonStorage.TEXT,
    format=Unversioned(),
    paths=(
        JsonPath(
            steps=(Key(name='good_until'),),
            rule=JsonTime(encoding=TimeEncoding.ISO_TEXT, rule=ORDER),
        ),
    ),
)
"""Serialized task options; ``good_until`` is ranked like the good_until column."""

_ERROR_DATA: Final = (Key(name='err'), Key(name='data'))

RESULT_ENVELOPE: Final = StoredJson(
    storage=JsonStorage.UTF8_BYTES,
    format=FormatVersion(column='result_envelope_version', version=1),
    paths=(
        JsonPath(steps=(*_ERROR_DATA, Key(name='task_id')), rule=TASK_ID),
        JsonPath(steps=(*_ERROR_DATA, Key(name='worker_id')), rule=WORKER_ID),
        JsonPath(
            steps=(*_ERROR_DATA, Key(name='started_at')),
            rule=JsonTime(encoding=TimeEncoding.ISO_TEXT, rule=RUN_CLOCK),
        ),
        JsonPath(
            steps=(*_ERROR_DATA, Key(name='detected_at')),
            rule=JsonTime(encoding=TimeEncoding.ISO_TEXT, rule=RUN_CLOCK),
        ),
    ),
)
"""Result envelope version 1: the result as UTF-8 JSON. The data of an error
that the worker or the stale recovery writes names the task and the worker,
and the stale recovery's also the start of the silent attempt and the time it
was detected."""

ATTEMPT_ARCHIVE: Final = StoredJson(
    storage=JsonStorage.UTF8_BYTES,
    format=FormatVersion(column='attempt_archive_version', version=1),
    paths=(
        JsonPath(
            steps=(EachItem(), Index(position=3)),
            rule=JsonTime(encoding=TimeEncoding.EPOCH_MICROSECONDS, rule=RUN_CLOCK),
        ),
        JsonPath(
            steps=(EachItem(), Index(position=4)),
            rule=JsonTime(encoding=TimeEncoding.EPOCH_MICROSECONDS, rule=RUN_CLOCK),
        ),
        JsonPath(steps=(EachItem(), Index(position=8)), rule=WORKER_ID),
    ),
)
"""Attempt archive version 1: one 12-field array per attempt (attempt, outcome,
will_retry, started_at, finished_at as epoch microseconds, error_code,
error_message, failed_reason, worker_id, worker_hostname, worker_pid,
worker_process_name)."""

RERUN_ENVELOPE: Final = StoredJson(
    storage=JsonStorage.UTF8_BYTES,
    format=FormatVersion(column='rerun_input_version', version=1),
    paths=(
        JsonPath(
            steps=(Key(name='options'), Key(name='good_until')),
            rule=JsonTime(encoding=TimeEncoding.ISO_TEXT, rule=ORDER),
        ),
    ),
)
"""Rerun envelope version 1: ``{"input": <input>, "options": <task options>}``."""

SLOTS_DOWN: Final = StoredJson(
    storage=JsonStorage.TEXT,
    format=Unversioned(),
    paths=(
        JsonPath(
            steps=(EachItem(), Key(name='down_since')),
            rule=JsonTime(encoding=TimeEncoding.ISO_TEXT, rule=RUN_CLOCK),
        ),
    ),
)
"""Down slots of a worker; ``down_since`` is a run clock value."""


def anchored(column: str) -> TimeWithin:
    """An offset from ``column`` of the same row, within ``ANCHOR_TOLERANCE``."""
    return TimeWithin(reference=AnchorReference(column=column), bound=ANCHOR_TOLERANCE)


def _columns(*pairs: tuple[str, ColumnRule]) -> tuple[ColumnSpec, ...]:
    return tuple(ColumnSpec(name=name, rule=rule) for name, rule in pairs)


_MIGRATIONS: Final = TableRules(
    name='horsies_migrations',
    columns=_columns(
        ('version', EXACT),
        ('description', EXACT),
        ('installed_on', RUN_CLOCK),
        ('success', EXACT),
        ('checksum', EXACT),
        (
            'execution_time',
            Ignored(
                reason=(
                    'install duration in nanoseconds as the installing client measured '
                    'it; it differs on every install and stores no schema state'
                ),
            ),
        ),
    ),
    order_by=('version',),
    identity=('version',),
)

_SCHEMA_VERSION: Final = TableRules(
    name='horsies_schema_version',
    columns=_columns(('version', EXACT), ('applied_at', RUN_CLOCK)),
    order_by=('version',),
    identity=('version',),
)

_CLIENT_FAMILY: Final = TableRules(
    name='horsies_client_family',
    columns=_columns(
        ('only_row', EXACT), ('family', EXACT), ('recorded_at', RUN_CLOCK)
    ),
    order_by=('only_row',),
    identity=('only_row',),
)

_RETENTION_CLASSES: Final = TableRules(
    name='horsies_retention_classes',
    columns=_columns(
        ('class_key', EXACT),
        ('class_kind', EXACT),
        ('retention_days', EXACT),
        ('heartbeat_horizon_hours', EXACT),
        ('created_at', RUN_CLOCK),
    ),
    order_by=('class_key',),
    identity=('class_key',),
)

_LEAF_CATALOG: Final = TableRules(
    name='horsies_task_history_leaf_catalog',
    columns=_columns(
        ('leaf_name', LEAF_NAME),
        ('class_key', EXACT),
        ('lower_bound_kind', EXACT),
        ('lower_anchor', ORDER),
        ('upper_anchor', ORDER),
        ('index_schema_version', EXACT),
        ('created_at', RUN_CLOCK),
        ('detached_at', RUN_CLOCK),
        ('dropped_at', RUN_CLOCK),
    ),
    order_by=('class_key', 'lower_bound_kind', 'lower_anchor', 'upper_anchor'),
    identity=('leaf_name',),
)

_LOOKUP_MANIFEST: Final = TableRules(
    name='horsies_task_lookup_manifest',
    columns=_columns(
        ('leaf_name', LEAF_NAME),
        ('probe_position', EXACT),
        ('lower_bound_kind', EXACT),
        ('lower_anchor', ORDER),
        ('upper_anchor', ORDER),
        ('published_at', RUN_CLOCK),
    ),
    order_by=('probe_position',),
    identity=('probe_position',),
)

_WORKFLOWS: Final = TableRules(
    name='horsies_workflows',
    columns=_columns(
        ('id', WORKFLOW_ID),
        ('name', EXACT),
        ('definition_key', EXACT),
        ('status', EXACT),
        ('on_error', EXACT),
        ('output_task_index', EXACT),
        ('success_policy', JSON),
        ('result', JSON),
        ('error', WORKFLOW_ERROR),
        ('parent_workflow_id', WORKFLOW_ID),
        ('parent_task_index', EXACT),
        ('root_workflow_id', WORKFLOW_ID),
        ('depth', EXACT),
        ('sent_at', ORDER),
        ('created_at', RUN_CLOCK),
        ('started_at', RUN_CLOCK),
        ('completed_at', RUN_CLOCK),
        ('updated_at', RUN_CLOCK),
    ),
    order_by=(
        'id',
        'depth',
        'parent_workflow_id',
        'parent_task_index',
        'name',
        'definition_key',
        'sent_at',
    ),
    identity=('id',),
)

_WORKFLOW_TASKS: Final = TableRules(
    name='horsies_workflow_tasks',
    columns=_columns(
        ('id', WORKFLOW_NODE_ID),
        ('workflow_id', WORKFLOW_ID),
        ('task_index', EXACT),
        ('node_id', EXACT),
        ('task_name', EXACT),
        ('input', JSON),
        ('queue_name', EXACT),
        ('priority', EXACT),
        ('dependencies', EXACT),
        ('args_from', JSON),
        ('workflow_ctx_from', EXACT),
        ('allow_failed_deps', EXACT),
        ('join_type', EXACT),
        ('min_success', EXACT),
        ('task_options', TASK_OPTIONS),
        ('max_retries', EXACT),
        ('execution', EXACT),
        ('good_until', ORDER),
        ('retention_class_key', EXACT),
        ('downstream_subworkflow', EXACT),
        ('status', EXACT),
        ('task_id', TASK_ID),
        ('is_subworkflow', EXACT),
        ('sub_workflow_id', WORKFLOW_ID),
        ('sub_workflow_name', EXACT),
        ('sub_definition_key', EXACT),
        ('sub_workflow_params', EXACT),
        ('sub_workflow_summary', JSON),
        ('result', NODE_RESULT),
        ('error', JSON),
        ('created_at', RUN_CLOCK),
        ('started_at', RUN_CLOCK),
        ('completed_at', RUN_CLOCK),
    ),
    order_by=('workflow_id', 'task_index'),
    identity=('id',),
)

_TASKS: Final = TableRules(
    name='horsies_tasks',
    columns=_columns(
        ('id', TASK_ID),
        ('task_name', EXACT),
        ('queue_name', EXACT),
        ('priority', EXACT),
        ('input', TASK_INPUT),
        ('task_options', TASK_OPTIONS),
        ('execution', EXACT),
        ('retention_class_key', EXACT),
        ('sent_at', ORDER),
        ('created_at', RUN_CLOCK),
        ('command_fingerprint_version', EXACT),
        ('command_fingerprint', COMMAND_FINGERPRINT),
        ('idempotency_key_digest', EXACT),
        ('status', EXACT),
        ('enqueued_at', ORDER),
        ('claimed_at', RUN_CLOCK),
        ('claimed_by_worker_id', WORKER_ID),
        ('claim_expires_at', anchored('claimed_at')),
        ('started_at', RUN_CLOCK),
        ('finalizing_at', RUN_CLOCK),
        ('finalizing_by_worker_id', WORKER_ID),
        ('worker_pid', EXACT),
        ('worker_hostname', EXACT),
        ('worker_process_name', EXACT),
        ('retry_count', EXACT),
        ('max_retries', EXACT),
        ('next_retry_at', anchored('updated_at')),
        ('good_until', ORDER),
        ('is_workflow_task', EXACT),
        ('updated_at', RUN_CLOCK),
        ('retain_rerun_input', EXACT),
        ('rerun_of_task_id', TASK_ID),
        ('rerun_root_task_id', TASK_ID),
    ),
    order_by=(
        'id',
        'task_name',
        'queue_name',
        'priority',
        'input',
        'sent_at',
        'enqueued_at',
    ),
    identity=('id',),
)

_TASK_HISTORY: Final = TableRules(
    name='horsies_task_history',
    columns=_columns(
        ('tableoid', LEAF_NAME),
        ('task_id', TASK_ID),
        ('task_name', EXACT),
        ('queue_name', EXACT),
        ('priority', EXACT),
        ('command_fingerprint_version', EXACT),
        ('command_fingerprint', COMMAND_FINGERPRINT),
        ('retention_class_key', EXACT),
        ('is_workflow_task', EXACT),
        ('workflow_id', WORKFLOW_ID),
        ('rerun_of_task_id', TASK_ID),
        ('rerun_root_task_id', TASK_ID),
        ('sent_at', ORDER),
        ('created_at', RUN_CLOCK),
        ('enqueued_at', ORDER),
        ('claimed_at', RUN_CLOCK),
        ('started_at', RUN_CLOCK),
        ('good_until', ORDER),
        ('terminal_at', RUN_CLOCK),
        ('status', EXACT),
        ('terminalization_kind', EXACT),
        ('retry_count', EXACT),
        ('max_retries', EXACT),
        ('last_claimed_worker_id', WORKER_ID),
        ('last_worker_hostname', EXACT),
        ('last_worker_pid', EXACT),
        ('last_worker_process_name', EXACT),
        ('error_code', EXACT),
        ('final_failed_reason', EXACT),
        ('result_envelope_version', EXACT),
        ('result_payload', RESULT_ENVELOPE),
        ('result_digest', DigestOf(column='result_payload')),
        ('history_schema_version', EXACT),
        ('attempt_archive_version', EXACT),
        ('attempt_snapshot', ATTEMPT_ARCHIVE),
        ('attempt_snapshot_digest', DigestOf(column='attempt_snapshot')),
        ('rerun_input_disposition', EXACT),
        ('rerun_input_version', EXACT),
        ('rerun_input_digest', DigestOf(column='rerun_input_inline')),
        ('rerun_input_inline', RERUN_ENVELOPE),
    ),
    order_by=(
        'task_id',
        'task_name',
        'queue_name',
        'priority',
        'sent_at',
        'enqueued_at',
    ),
    identity=('task_id',),
)

_TASK_ATTEMPTS: Final = TableRules(
    name='horsies_task_attempts',
    columns=_columns(
        ('task_id', TASK_ID),
        ('attempt', EXACT),
        ('outcome', EXACT),
        ('will_retry', EXACT),
        ('started_at', RUN_CLOCK),
        ('finished_at', RUN_CLOCK),
        ('error_code', EXACT),
        ('error_message', EXACT),
        ('failed_reason', EXACT),
        ('worker_id', WORKER_ID),
        ('worker_hostname', EXACT),
        ('worker_pid', EXACT),
        ('worker_process_name', EXACT),
    ),
    order_by=('task_id', 'attempt'),
    identity=('task_id', 'attempt'),
)

_HEARTBEATS: Final = TableRules(
    name='horsies_heartbeats',
    columns=_columns(
        ('tableoid', LEAF_NAME),
        ('task_id', TASK_ID),
        ('role', EXACT),
        ('sent_at', ORDER),
        ('sender_id', WORKER_ID),
        ('hostname', EXACT),
        ('pid', EXACT),
    ),
    order_by=('task_id', 'role', 'sent_at'),
    identity=('task_id', 'role', 'sent_at'),
)

_WORKER_STATES: Final = TableRules(
    name='horsies_worker_states',
    columns=_columns(
        ('worker_id', WORKER_ID),
        ('snapshot_at', ORDER),
        ('hostname', EXACT),
        ('pid', EXACT),
        ('processes', EXACT),
        ('max_claim_batch', EXACT),
        ('max_claim_per_worker', EXACT),
        ('cluster_wide_cap', EXACT),
        ('queues', EXACT),
        ('queue_priorities', JSON),
        ('queue_max_concurrency', JSON),
        ('recovery_config', JSON),
        ('tasks_running', EXACT),
        ('tasks_claimed', EXACT),
        ('memory_usage_mb', _PROCESS_SAMPLE),
        ('memory_percent', _PROCESS_SAMPLE),
        ('cpu_percent', _PROCESS_SAMPLE),
        ('children_memory_mb', _PROCESS_SAMPLE),
        ('worker_started_at', RUN_CLOCK),
        ('slots_down', SLOTS_DOWN),
        ('class_capacity', JSON),
    ),
    order_by=('worker_id', 'hostname', 'pid', 'snapshot_at'),
    identity=('worker_id', 'snapshot_at'),
)

_PHASE2_QUARANTINE: Final = TableRules(
    name='horsies_workflow_phase2_quarantine',
    columns=_columns(
        ('task_id', TASK_ID),
        ('workflow_id', WORKFLOW_ID),
        ('workflow_node_row_id', WORKFLOW_NODE_ID),
        ('terminalization_kind', EXACT),
        ('terminal_at', RUN_CLOCK),
        ('retention_class_key', EXACT),
        ('history_schema_version', EXACT),
        ('result_envelope_version', EXACT),
        ('result_payload', RESULT_ENVELOPE),
        ('result_digest', DigestOf(column='result_payload')),
        ('quarantined_at', RUN_CLOCK),
    ),
    order_by=('task_id',),
    identity=('task_id',),
)

_PHASE2_RESULT_DIGEST: Final = Ignored(
    reason=(
        'SHA-256 of the result payload of the history row of the same task, which is '
        'in another table; a result that names its task or worker (a crash or a worker '
        'failure) gives another digest in every run. The history row checks its own '
        'result_digest against that payload'
    ),
)

_PHASE2_PENDING: Final = TableRules(
    name='horsies_workflow_phase2_pending',
    columns=_columns(
        ('task_id', TASK_ID),
        ('workflow_id', WORKFLOW_ID),
        ('workflow_node_row_id', WORKFLOW_NODE_ID),
        ('terminalization_kind', EXACT),
        ('recovery_source', EXACT),
        ('history_class', EXACT),
        ('history_anchor', RUN_CLOCK),
        ('history_schema_version', EXACT),
        ('result_digest', _PHASE2_RESULT_DIGEST),
        ('quarantine_task_id', TASK_ID),
        ('created_at', RUN_CLOCK),
        ('attempt_count', EXACT),
        ('last_attempt_at', RUN_CLOCK),
        ('last_failure_class', EXACT),
    ),
    order_by=('task_id',),
    identity=('task_id',),
)

_RECOVERY_FAILURES: Final = TableRules(
    name='horsies_workflow_recovery_failures',
    columns=_columns(
        ('workflow_id', WORKFLOW_ID),
        ('failed_passes', EXACT),
        ('last_pass_id', NormalisedId(family=IdFamily.RECOVERY_PASS)),
        ('first_failed_at', RUN_CLOCK),
        ('last_failed_at', RUN_CLOCK),
        ('last_case', EXACT),
        ('last_task_index', EXACT),
        ('last_error_type', EXACT),
    ),
    order_by=('workflow_id',),
    identity=('workflow_id',),
)

_SCAN_CURSORS: Final = TableRules(
    name='horsies_recovery_scan_cursors',
    columns=_columns(
        ('scan_name', EXACT),
        ('last_created_at', RUN_CLOCK),
        ('last_id', SCAN_POSITION),
        ('cycle_upper_created_at', RUN_CLOCK),
        ('cycle_upper_id', SCAN_POSITION),
        ('claim_token', NormalisedId(family=IdFamily.RECOVERY_CLAIM)),
        ('claim_expires_at', anchored('last_scan_at')),
        ('completed_cycles', EXACT),
        ('last_scan_rows', EXACT),
        ('last_candidate_rows', EXACT),
        ('last_scan_at', RUN_CLOCK),
    ),
    order_by=('scan_name',),
    identity=('scan_name',),
)

_KEY_RESERVATIONS: Final = TableRules(
    name='horsies_key_reservations',
    columns=_columns(
        ('idempotency_key_digest', EXACT),
        ('key_scope_version', EXACT),
        ('fingerprint_version', EXACT),
        ('command_fingerprint', COMMAND_FINGERPRINT),
        ('task_id', TASK_ID),
        ('disposition', EXACT),
        ('reservation_window', EXACT),
        ('expires_at', ORDER),
    ),
    order_by=('idempotency_key_digest',),
    identity=('idempotency_key_digest',),
)

_CLOCK_EXCEPTIONS: Final = TableRules(
    name='horsies_task_history_clock_exceptions',
    columns=_columns(
        ('task_id', TASK_ID),
        ('retention_class_key', EXACT),
        ('terminal_at', RUN_CLOCK),
    ),
    order_by=('task_id',),
    identity=('task_id',),
)

_SCHEDULE_STATE: Final = TableRules(
    name='horsies_schedule_state',
    columns=_columns(
        ('schedule_name', EXACT),
        ('next_run_at', ORDER),
        ('last_run_at', ORDER),
        ('last_task_id', TASK_ID),
        ('run_count', EXACT),
        ('config_hash', EXACT),
        ('updated_at', RUN_CLOCK),
    ),
    order_by=('schedule_name',),
    identity=('schedule_name',),
)

BASELINE_CATALOG: Final = SnapshotCatalog(
    tables=(
        _MIGRATIONS,
        _SCHEMA_VERSION,
        _CLIENT_FAMILY,
        _RETENTION_CLASSES,
        _LEAF_CATALOG,
        _LOOKUP_MANIFEST,
        _WORKFLOWS,
        _WORKFLOW_TASKS,
        _TASKS,
        _TASK_HISTORY,
        _TASK_ATTEMPTS,
        _HEARTBEATS,
        _WORKER_STATES,
        _PHASE2_QUARANTINE,
        _PHASE2_PENDING,
        _RECOVERY_FAILURES,
        _SCAN_CURSORS,
        _KEY_RESERVATIONS,
        _CLOCK_EXCEPTIONS,
        _SCHEDULE_STATE,
    ),
)
"""The rules of the 20 baseline tables, in labelling order."""
