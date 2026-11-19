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
- ``anchored(column, when_anchor_null)``: a value computed from another
  clock column of the row plus a duration (lease end, retry time). The
  offset compares within ``ANCHOR_TOLERANCE``. A value whose anchor is NULL
  fails the capture, or, for a lease that a test seeds on a row it never
  claimed, is ranked among such values (``AnchorNull``).

Stored JSON (inputs, options, results, errors, the attempt archive, the
rerun envelope) is parsed and compared as JSON; the ids and clock values
inside it have path rules. A stored text that is not JSON compares as its
text (``UnparsedDocument``). A digest column is checked against its source
column. Partition leaf names compare with their date or hour as an offset
from the run start.
"""

from __future__ import annotations

from datetime import timedelta
from enum import Enum
from typing import Final

from tests.parity.snapshot.rules import (
    CANONICAL_UUID,
    AnchorNull,
    AnchorReference,
    ColumnRule,
    ColumnSpec,
    DigestOf,
    EachItem,
    EachMarkedMember,
    EachMember,
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
    PathStep,
    RunMeasure,
    RunMeasuresInText,
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

WORKFLOW_IDS_IN_TEXT: Final = IdsInText(family=IdFamily.WORKFLOW)
"""A message that can name a workflow by its id."""

_RUN_CLOCK_TEXT: Final = JsonTime(encoding=TimeEncoding.ISO_TEXT, rule=RUN_CLOCK)


def stored_error_paths(error: tuple[PathStep, ...]) -> tuple[JsonPath, ...]:
    """The rules of a stored error (a ``TaskError`` as JSON) at the path ``error``.

    The library writes these ids, clock values and texts into an error:

    - ``data.task_id`` and ``data.worker_id``: the task and the worker of a
      worker failure (``{"task_id": ..., "worker_id": ...}``); the stale
      recovery's crash data names the worker and holds ``data.started_at``
      and ``data.detected_at``, as in the result envelope;
    - ``data.workflow_id``: the workflow of the recovery limit error and of
      the output-skipped error (``WORKFLOW_OUTPUT_SKIPPED``);
    - ``sub_workflow_id``: the child of a ``SUBWORKFLOW_FAILED`` error;
    - ``message`` and ``sub_workflow_summary.error_summary``: texts that name
      a workflow by its id (``Subworkflow <id> failed``, ``Workflow <id> is
      COMPLETED and has no output: ...``).
    """
    data = (*error, Key(name='data'))
    return (
        JsonPath(steps=(*data, Key(name='task_id')), rule=TASK_ID),
        JsonPath(steps=(*data, Key(name='worker_id')), rule=WORKER_ID),
        JsonPath(steps=(*data, Key(name='workflow_id')), rule=WORKFLOW_ID),
        JsonPath(steps=(*data, Key(name='started_at')), rule=_RUN_CLOCK_TEXT),
        JsonPath(steps=(*data, Key(name='detected_at')), rule=_RUN_CLOCK_TEXT),
        JsonPath(steps=(*error, Key(name='sub_workflow_id')), rule=WORKFLOW_ID),
        JsonPath(steps=(*error, Key(name='message')), rule=WORKFLOW_IDS_IN_TEXT),
        JsonPath(
            steps=(*error, Key(name='sub_workflow_summary'), Key(name='error_summary')),
            rule=WORKFLOW_IDS_IN_TEXT,
        ),
    )


_RESULTS_BY_ID_ERROR: Final = (
    Key(name='results_by_id'),
    EachMember(),
    Key(name='err'),
)
"""The error of each stored task result of a ``results_by_id`` map (node id to
result envelope)."""

_CONTEXT: Final = Key(name='__h_workflow_ctx__')

ARGS_FROM_MARKER: Final = '__h_taskresult_envelope__'
"""The marker of an ``args_from`` entry: a member of a task input that the
engine writes in place of the keyword argument that a node takes from a
dependency (``core/src/workflow_engine/args_merge.rs``,
``ARGS_FROM_ENVELOPE_KEY``)."""

_ARGS_FROM_ERROR: Final = (
    EachMarkedMember(marker=ARGS_FROM_MARKER),
    Key(name='inner'),
    Key(name='err'),
)
"""The error of the dependency's result in each ``args_from`` entry
(``{"__h_taskresult_envelope__": true, "source_task_name": ...,
"source_definition_key": ..., "inner": <result envelope>}``)."""

TASK_INPUT: Final = StoredJson(
    storage=JsonStorage.TEXT,
    format=Unversioned(),
    paths=(
        JsonPath(
            steps=(Key(name='__h_workflow_meta__'), Key(name='workflow_id')),
            rule=WORKFLOW_ID,
        ),
        JsonPath(steps=(_CONTEXT, Key(name='workflow_id')), rule=WORKFLOW_ID),
        *stored_error_paths((_CONTEXT, *_RESULTS_BY_ID_ERROR)),
        *stored_error_paths(_ARGS_FROM_ERROR),
        JsonPath(
            steps=(
                _CONTEXT,
                Key(name='summaries_by_id'),
                EachMember(),
                Key(name='error_summary'),
            ),
            rule=WORKFLOW_IDS_IN_TEXT,
        ),
    ),
)
"""A task's input. The input of an engine-minted workflow task names its
workflow in ``__h_workflow_meta__``. The input of a task that receives the
workflow context names its workflow in ``__h_workflow_ctx__.workflow_id``,
holds the result of each dependency in ``__h_workflow_ctx__.results_by_id``
(each error by ``stored_error_paths``) and the summary of each sub-workflow
dependency in ``__h_workflow_ctx__.summaries_by_id`` (``error_summary`` can
name a workflow). The input of a node that takes keyword arguments from its
dependencies (``args_from``) holds one entry per argument, marked by
``ARGS_FROM_MARKER``; the error of the dependency's result in it
(``inner.err``) follows ``stored_error_paths``: the output-skipped error of
a child and the error of a child result that does not parse name the child
in ``data.workflow_id`` and in ``message``."""

WORKFLOW_ERROR: Final = StoredJson(
    storage=JsonStorage.TEXT,
    format=Unversioned(),
    paths=stored_error_paths(()),
)
"""A stored workflow error, by ``stored_error_paths``: the recovery limit
error names its workflow in ``data.workflow_id``; the error of a workflow
that a failed child paused names the child in ``sub_workflow_id`` and in
``message`` (``Subworkflow <id> failed``); the error that a failed task left
holds the data of that task's error."""

NODE_RESULT: Final = StoredJson(
    storage=JsonStorage.TEXT,
    format=Unversioned(),
    paths=stored_error_paths((Key(name='err'),)),
)
"""A stored node result, its error by ``stored_error_paths``: the recovery
limit result names its workflow in ``err.data.workflow_id``; the result of a
sub-workflow node whose child failed names the child in
``err.sub_workflow_id``, in ``err.message`` (``Subworkflow <id> failed with
status <status>``) and in ``err.sub_workflow_summary.error_summary``; the
result of a task node holds its task's error data."""

WORKFLOW_RESULT: Final = StoredJson(
    storage=JsonStorage.TEXT,
    format=Unversioned(),
    paths=(
        *stored_error_paths((Key(name='err'),)),
        *stored_error_paths((Key(name='ok'), *_RESULTS_BY_ID_ERROR)),
    ),
)
"""A stored workflow result, its error by ``stored_error_paths`` (the
output-skipped error names the workflow in ``err.data.workflow_id`` and in
``err.message``). The result of a workflow without an output node holds the
result of each terminal node in ``ok.results_by_id``
(``{"__h_outputless_terminals__": true, "ok": {"results_by_id": ...}}``),
each error by ``stored_error_paths``."""

SUB_WORKFLOW_SUMMARY: Final = StoredJson(
    storage=JsonStorage.TEXT,
    format=Unversioned(),
    paths=(JsonPath(steps=(Key(name='error_summary'),), rule=WORKFLOW_IDS_IN_TEXT),),
)
"""The stored summary of a sub-workflow node's child; ``error_summary`` can
name a workflow (``Workflow <id> is COMPLETED and has no output: ...``)."""

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

BLOCKED_TIME: Final = RunMeasure(
    name='blocked_ms',
    pattern=r' for (\d+) ms, past the block limit',
)
"""The time that a task held the event loop of its shared async child, as the
worker measured it, in the ``TASK_BLOCKED_LOOP`` and ``NEIGHBOUR_BLOCKED_LOOP``
reports: ``... held the event loop of its shared async child for <n> ms, past
the block limit, ...``
(``core/src/worker/executor/slots/seat_runs.rs``)."""

KILLED_CHILD_PID: Final = RunMeasure(
    name='killed_child_pid',
    pattern=r'the worker killed the child \(pid=(\d+), ',
)
"""The process id of the worker child that the worker killed, in the
``the worker killed the child (pid=<n>, <exit>)`` part of a kill report. Each
run starts its own processes; the whole-run catalog ignores the process id
columns for the same reason (``tests/parity/snapshot/worker_run.py``)."""

KILL_REPORT_MEASURES: Final = RunMeasuresInText(measures=(BLOCKED_TIME, KILLED_CHILD_PID))
"""The rule of a stored text that can hold a kill report: the measured block
time and the killed child's process id become tokens; the rest of the text,
the codes and the other fields compare exactly."""

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


def anchored(column: str, when_anchor_null: AnchorNull) -> TimeWithin:
    """An offset from ``column`` of the same row, within ``ANCHOR_TOLERANCE``."""
    return TimeWithin(
        reference=AnchorReference(column=column, when_anchor_null=when_anchor_null),
        bound=ANCHOR_TOLERANCE,
    )


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
        ('result', WORKFLOW_RESULT),
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
        'status',
    ),
    identity=('id',),
)
"""``status`` orders the workflows that a test seeds with one name and one
send time and that differ only in their status."""

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
        ('sub_workflow_summary', SUB_WORKFLOW_SUMMARY),
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
        ('claim_expires_at', anchored('claimed_at', AnchorNull.RANKED)),
        ('started_at', RUN_CLOCK),
        ('finalizing_at', RUN_CLOCK),
        ('finalizing_by_worker_id', WORKER_ID),
        ('worker_pid', EXACT),
        ('worker_hostname', EXACT),
        ('worker_process_name', EXACT),
        ('retry_count', EXACT),
        ('max_retries', EXACT),
        ('next_retry_at', anchored('updated_at', AnchorNull.REFUSED)),
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
        'execution',
        'status',
    ),
    identity=('id',),
)
"""``execution`` and ``status`` order the tasks that a test seeds with one
name, queue, priority, input and send time and that differ only in their
execution class or their status (a claim test's rows of three classes, the
rows that a claim took and the rows it left)."""

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

_WORKER_LIVENESS: Final = TableRules(
    name='horsies_worker_liveness',
    columns=_columns(
        ('worker_id', WORKER_ID),
        (
            'generation',
            Ignored(
                reason=(
                    'a value of the sequence horsies_worker_liveness_generation, which no '
                    'seed reset rewinds: it counts the lock acquisitions of every earlier run'
                ),
            ),
        ),
    ),
    order_by=('worker_id',),
    identity=('worker_id',),
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
        ('claim_expires_at', anchored('last_scan_at', AnchorNull.REFUSED)),
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
        _WORKER_LIVENESS,
        _PHASE2_QUARANTINE,
        _PHASE2_PENDING,
        _RECOVERY_FAILURES,
        _SCAN_CURSORS,
        _KEY_RESERVATIONS,
        _CLOCK_EXCEPTIONS,
        _SCHEDULE_STATE,
    ),
)
"""The rules of the 21 baseline tables, in labelling order."""


# ---- Notifications ----


class NotificationPayload(Enum):
    """What the payload of a notification channel holds."""

    TASK_ID = 'a task id'
    WORKFLOW_ID = 'a workflow id'
    QUEUE_WAKE = 'a fixed text, or the retry prefix and the id of the retried task'


TASK_CHANNELS: Final = ('task_new', 'task_done', 'horsies_task_status')
"""Channels whose payload is a task id (``0001_tables.sql`` triggers, ``0006_history_functions.sql`` moves)."""

WORKFLOW_CHANNELS: Final = ('workflow_done', 'horsies_workflow_status')
"""Channels whose payload is a workflow id."""

QUEUE_CHANNEL_PREFIX: Final = 'task_queue_'
"""The wake channel of a queue, ``task_queue_<queue>``; its payload is
``new``, ``capacity``, ``class`` or ``retry:<task id>``."""

RETRY_WAKE_PREFIX: Final = 'retry:'
"""The prefix of the wake that a retry sends on the queue of its task:
``retry:<task id>`` (the worker's retry, ``core/src/worker/execution.rs``;
the stale recovery's retry, ``core/src/broker/stale_recovery.rs``)."""


def notification_payload(channel: str) -> NotificationPayload:
    """The payload kind of ``channel``. A channel outside the three lists is a
    defect of the caller, which listens only on known channels (asserted)."""
    match channel:
        case _ if channel in TASK_CHANNELS:
            return NotificationPayload.TASK_ID
        case _ if channel in WORKFLOW_CHANNELS:
            return NotificationPayload.WORKFLOW_ID
        case _ if channel.startswith(QUEUE_CHANNEL_PREFIX):
            return NotificationPayload.QUEUE_WAKE
        case _:
            raise AssertionError(
                f'no payload rule for the notification channel {channel!r}'
            )


NOTIFICATIONS: Final = TableRules(
    name='notifications',
    columns=_columns(
        ('channel', EXACT),
        ('task_id', TASK_ID),
        ('workflow_id', WORKFLOW_ID),
        ('payload', EXACT),
    ),
    order_by=('channel', 'payload', 'task_id', 'workflow_id'),
    identity=('channel', 'task_id', 'workflow_id', 'payload'),
)
"""The notifications of a run, one row each: the channel, and the payload in
the column of its kind (``notification_row``). A retry wake puts the id of
its task into ``task_id`` and keeps ``RETRY_WAKE_PREFIX`` as its payload.
Rows with one identity compare as a multiset, so the section compares as a
multiset per channel.

The order key is the channel, the payload and the two ids. The rows of one
channel whose id has no label yet (no table holds it, for example the
status events of workflows that a test deleted) tie on it. ``normalize``
orders them by the rows that hold their ids, and frees a tie whose rows
split into equal blocks, one per id (``normalize._interchangeable``): the
two status events of each workflow of a chain. No column of a notification
can order such rows, because they differ only in their ids."""

NOTIFIED_CATALOG: Final = SnapshotCatalog(
    tables=(*BASELINE_CATALOG.tables, NOTIFICATIONS),
)
"""The baseline tables, then the notifications: a payload id takes the label
that its row in a table gave it."""


def _queue_wake_row(channel: str, payload: str) -> tuple[str | None, ...]:
    """A queue wake. ``retry:<task id>``, with the id in its canonical form
    (``CANONICAL_UUID``), puts the id into ``task_id`` and keeps the prefix
    as the payload. Any other payload stays as its text and compares exactly."""
    task_id = payload.removeprefix(RETRY_WAKE_PREFIX)
    match task_id != payload and CANONICAL_UUID.fullmatch(task_id) is not None:
        case True:
            return (channel, task_id, None, RETRY_WAKE_PREFIX)
        case False:
            return (channel, None, None, payload)


def notification_row(channel: str, payload: str) -> tuple[str | None, ...]:
    """One raw row of ``NOTIFICATIONS``."""
    match notification_payload(channel):
        case NotificationPayload.TASK_ID:
            return (channel, payload, None, None)
        case NotificationPayload.WORKFLOW_ID:
            return (channel, None, payload, None)
        case NotificationPayload.QUEUE_WAKE:
            return _queue_wake_row(channel, payload)
