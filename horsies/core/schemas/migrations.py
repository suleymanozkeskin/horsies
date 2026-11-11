"""PostgreSQL schema migrations — idempotent ALTER TABLE statements."""

from __future__ import annotations

from sqlalchemy import text

from horsies.core.models.tasks import Execution


# ---- Schema infrastructure ----
#
# v3: claim-path indexes (idx_horsies_tasks_claim_pending,
#     idx_horsies_tasks_claim_expired, idx_horsies_tasks_worker_status,
#     idx_horsies_worker_states_worker_snapshot).
# v4: widen horsies_heartbeats.id / horsies_worker_states.id to BIGINT
#     (int4 sequences exhaust within months at heartbeat insert rates).
# v5: drop write-amplifying single-column indexes on horsies_tasks whose
#     queries are served by the v3 partial composites (or that no query
#     uses); replace full good_until index with a partial one.
# v6: split notify triggers into INSERT/UPDATE pairs with WHEN clauses so
#     non-status updates (lease renewals etc.) never invoke plpgsql.
# v7: ordered partial index for the expired claim arm
#     (idx_horsies_tasks_claim_expired_ordered) — the v3 expiry-filter
#     index cannot serve the arm's ORDER BY, so deep expired backlogs
#     paid a full sort under SKIP LOCKED.
# v8: composite (workflow_id, status, task_index) on horsies_workflow_tasks
#     (idx_horsies_workflow_tasks_wf_status_idx) — first-failed lookups ran
#     an O(N) ordered scan per FAILED completion inside the workflow
#     completion lock.
# v9: add horsies_worker_states.children_memory_mb — summed RSS of executor
#     children. The existing memory_usage_mb is parent-only, so per-child
#     memory growth (the dyno-quota driver) was invisible in worker telemetry.

# v10: horsies_claim(...) plpgsql function — collapses the claim critical
#      section (advisory-lock acquisition + cap counts + windowed claim) into
#      ONE server-side statement so the xact-scoped advisory lock is never held
#      across a client round trip. Removes the client-stall-while-holding-lock
#      freeze. Cap semantics preserved (never over-claims; may under-claim under
#      SKIP-LOCKED contention, deferring work to the next pass).
#
# v11: retention eligibility indexes. idx_horsies_tasks_retention — partial
#      expression index on COALESCE(completed_at, failed_at, updated_at,
#      created_at) over terminal statuses; idx_horsies_worker_states_snapshot_at.
#      The hourly retention deletes seq-scanned both heaps on every pass,
#      including passes with zero eligible rows.
#
# v12: horsies_claim returns claimed_at — the claim-generation fence (C10).
#      Finalize CASes fenced on (status, worker_id) alone let a stale finalize
#      from a reaper-requeued attempt clobber a live attempt the SAME worker
#      re-claimed (worker_id matches, status matches). claimed_at identifies
#      the claim generation: set by the claim, cleared by every requeue.
#      The return-type change requires DROP + CREATE (CREATE OR REPLACE
#      cannot change OUT columns).
#      Also idx_horsies_heartbeats_sent_at: heartbeat retention deletes filter
#      sent_at < cutoff, but the composite (task_id, role, sent_at DESC) index
#      cannot serve a leading-column sent_at range, so every hourly pass
#      scanned the heartbeats heap — the v11 retention indexes covered tasks
#      and worker_states and omitted heartbeats.
#
# v13: idx_horsies_workflows_retention — partial expression index on
#      COALESCE(completed_at, updated_at, created_at) over terminal workflow
#      statuses. The workflow and workflow_tasks retention deletes both
#      filter horsies_workflows on this predicate; with no supporting index
#      and no statistics on the expression, the planner overestimated
#      eligibility and chose a stop-early pkey walk whose LIMIT never
#      filled — a full-table walk per statement, twice per hourly pass,
#      serial under FOR UPDATE, regardless of how few rows were eligible.
#      Completes the v11/v12 retention-index set (tasks, worker_states,
#      heartbeats, now workflows).
#
# v14: extended expression statistics for the retention predicates, plus
#      ANALYZE. The planner never uses statistics gathered on a PARTIAL
#      index for whole-table selectivity (they describe only rows
#      satisfying the index predicate), so the v11/v13 retention indexes
#      provide no estimate for their own COALESCE expressions — the
#      planner costs the cutoff comparison at the default 1/3 selectivity.
#      Whether the retention DELETE then uses its index is a coin flip on
#      table size: at 1M retained tasks the walk is expensive enough that
#      the index wins anyway; at 36k retained workflows the planner kept a
#      full-table walk (est 12k eligible vs 13 actual, 4-5s per statement
#      per hourly pass) and the misestimate also degraded the not-exists
#      guard into a seq scan of the 1M-row tasks table. CREATE STATISTICS
#      ON (expression) (PostgreSQL 14+, the supported floor) builds
#      whole-table expression statistics the planner does use; with them
#      the estimate collapses to the true trickle and both the index scan
#      and the per-row guard probe are chosen. ANALYZE populates the
#      statistics objects in the same transaction — extended statistics
#      are empty until the table is analyzed after their creation.

#
# v15: idx_horsies_tasks_queue_retention — composite partial index
#      (queue_name, COALESCE(completed_at, failed_at, updated_at,
#      created_at)) over terminal statuses, serving the per-queue
#      retention-override deletes (queue_terminal_record_retention_hours).
#      The v11 expression index keeps serving the remainder delete; an
#      override window's cutoff is far more recent than the global one,
#      so without the queue-leading composite every other queue's retained
#      terminal rows inside that gap are heap-filter misses. Maintained
#      once per task lifetime (finalize transition), like v11.

#
# v16: monitoring read-path indexes — idx_horsies_tasks_enqueued_at
#      (task list default sort; also unblocked by dropping the nulls_last
#      wrapper on NOT NULL sort columns, whose DESC NULLS LAST ordering a
#      plain btree cannot satisfy) and idx_horsies_tasks_task_name
#      (task-name facet as an index-only scan). Neither column is in any
#      worker-path predicate or leading sort key; the claim plan test
#      pins idx_horsies_tasks_claim_pending.

# v17: canonical terminal_at on horsies_tasks. Additive: the column and the
#      sixteen writer SET clauses only. Backfill and the CHECK constraint
#      tying terminal_at to terminal status both ship in the next release,
#      together in one migration.
#
#      The constraint cannot ship here: migrations apply at worker startup,
#      so a rolling restart would leave un-upgraded workers writing terminal
#      statuses with terminal_at NULL against a constraint their SQL cannot
#      satisfy, failing their finalize statements. NOT VALID does not help —
#      it skips existing rows but still enforces new writes.
#
#      The backfill is deferred with it rather than run here, because rows
#      terminalized by un-upgraded workers during that same rolling window
#      would be written after a v17 backfill had already passed over them.
#      Backfilling in the migration that installs the constraint covers the
#      window in one pass.

# v18: terminal_at completeness — backfill, then a CHECK tying a terminal
#      status to a terminal instant, in that order in one transaction so the
#      constraint's precondition holds when it is enforced.
#
#      It ships inside this artifact rather than ahead of it. The apply path
#      exits early once the stored version is at or above SCHEMA_VERSION, so a
#      database that reached v19 without v18 would never receive it. The rule
#      that follows: the artifact declaring a version must contain every
#      migration below it that is not already published.

# v19: terminalization_kind on horsies_tasks, plus the database-owned terminal
#      operations that write it. Additive and nullable, with a value-domain
#      CHECK only — see schemas/terminalization.py for why the status arm of
#      that constraint cannot ship in the same release.
#
#      The column is never supplied by a caller: each operation function
#      hardcodes its own kind, so a row cannot claim provenance it does not
#      have. Rows terminalized before this release keep a NULL kind, which is
#      read as unknown provenance and never inferred from.
#
#      Every merged change to the installed database program — a function
#      body, a new operation, an equivalence class, the outcome shape —
#      advances this version. A database already at the current version
#      receives nothing, so an unversioned change to a function is a change
#      that only fresh databases ever see.

# v20: the failure operations — horsies_fail_locked_task, which merges the
#      two locked-failure statements behind one COALESCE on failed_reason,
#      and horsies_fail_stale_task, whose guard is staleness rather than
#      ownership and whose refusals carry the staleness evidence. No schema
#      change; the version is what delivers the new function bodies to a
#      database that already reached v19.

# v21: the expiry operations — horsies_expire_owned_claim, fenced on worker
#      ownership but deliberately not on claim generation, refusing with the
#      deadline it judged; and horsies_expire_pending_tasks, the first
#      discovery batch: it selects its own targets under the deadline
#      predicate and reports only the rows it transitioned. Also replaces
#      the stale-failure body: the guard now captures every observable it
#      compares, with the instant it compares them at, in the one statement
#      that locks the row, and its evidence carries finalizing_at and
#      evaluated_at. No schema change.

# v22: horsies_expire_pending_tasks rejects a NULL or non-positive batch
#      size before mutating anything — LIMIT NULL means no limit at all,
#      which would disable the notification-burst bound the batch exists
#      to enforce.

# v23: the cancellation operations — horsies_cancel_locked_task preserves the
#      operator's explicit RUNNING opt-in under its existing row lock;
#      horsies_cancel_owned_orphan fences one claim and reports a runnable
#      workflow-task link as guard evidence; horsies_cancel_orphaned_tasks is
#      the bounded, SKIP LOCKED discovery form of the same orphan predicate.
#      The sweep's effective task source set is CLAIMED/PENDING; the legacy
#      READY/ENQUEUED literals are not TaskStatus values and are removed.

# v24: the workflow pause/cancellation operations — three ownership shapes
#      for each semantic family: one claim, one id-keyed pairwise claim batch,
#      and one workflow-scoped cross-worker batch. Id-keyed results carry and
#      are verified by ordinality; workflow-scoped batches report transitions
#      only. No schema change.

# v25: replace the owned-claim expiry body so its common apply path relies on
#      the guarded UPDATE's row lock rather than issuing SELECT FOR UPDATE
#      first. Refusals still lock and capture every deadline observable before
#      classification; eligible transitions no longer emit a separate
#      tuple-lock WAL record. The stale-failure wire also carries the public
#      millisecond thresholds without truncating fractional seconds; the
#      function converts them only when constructing its intervals. No table
#      schema or function argument-type change.

# v26: the terminal writer owns the complete final-attempt summary. No requeue
#      path clears failed_reason, so a row that failed, requeued, and then
#      reached a different terminal outcome carried the earlier attempt's
#      reason beside that outcome. Five body changes: horsies_fail_locked_task
#      assigns p_failed_reason unconditionally (the COALESCE preserved a value
#      this transition never owned), and the completion pair plus the expiry
#      pair clear failed_reason the way completion already cleared error_code.
#      Earlier attempts keep their reasons in horsies_task_attempts. No table
#      schema or function argument-type change.

# v27: transitional live cutover columns. The columns of the declared
#      live-cutover fragment (horsies.core.history.terminalization
#      .live_cutover.LIVE_CUTOVER_COLUMNS_DDL, the authoritative final
#      shape), added NULLABLE with no checks and no domain change — a
#      catalog-only ALTER safe on any install, following the v17
#      terminal_at precedent. The converted enqueue statement writes real
#      values into them for every new row; rows written by older code
#      carry NULLs that the cutover migration's backfill owns before it
#      tightens the columns to the declared shape. The column list is
#      DERIVED from the fragment programmatically so the transitional
#      migration and the authoritative shape can never drift.

# v28: the keyed-enqueue reservation registry. The table installs its
#      FINAL FROZEN SHAPE directly, imported verbatim from the frozen
#      DDL module — the permissive-then-tighten two-step exists for
#      populated relations whose rows would violate final constraints
#      before backfill, and a new empty table has no such rows; there is
#      no tightening debt. The reservation functions install per the
#      claim-function precedent: dropped and recreated on every apply,
#      because a function body is program rather than state.
#      DELIBERATELY ABSENT: the registry maintenance indexes — wave-gated
#      conditional DDL whose absence here was sequencing, not an omission
#      to repair; v30 emits them. The claim path rides the table's
#      primary key.

# v29: the task-history foundation at its final shape. One version
#      installs the frozen history program (minus the entries v28
#      already owns — the registry table and the reservation function
#      program, subtracted by imported identity) and then the two gated
#      column families (attempt-snapshot, rerun-input) on the parent.
#      One version for all three parts because the parent is a NEW
#      EMPTY surface: the permissive-then-tighten two-step exists for
#      populated relations, and the gated columns are NOT NULL —
#      addable only while the parent is empty. A version boundary
#      between creation and the gated ALTERs would mint a deployable
#      intermediate state whose rows could never be tightened without
#      backfill. The parent is born final. Nothing in shipped code
#      writes these tables until the cutover wave; the ordinary chain
#      installs state the old fleet ignores — programs arrive with
#      their owners, so the terminalization/move bodies are NOT here.

# v30: the reservation-registry maintenance indexes, closing v28's
#      recorded deliberate absence. Separate from v29 because they
#      target an already-deployed surface (the v28 registry, which may
#      hold rows) — the empty-surface argument that makes v29
#      reviewable does not cover them. Plain CREATE INDEX: the
#      registry is bounded pre-cutover; idempotence lives at the
#      runner's existence check against the imported index name.

# v31: WorkflowStatus.EXPIRED joins the workflow terminal set (paused
#      past the deployment's declared age policy). The generated
#      partial-index predicate of idx_horsies_workflows_retention
#      changes with the set, and IF NOT EXISTS never rebuilds — the
#      migration drops and recreates the index explicitly (the class
#      rule for any changed generated predicate). The retention
#      DELETEs' IN-lists render from the same literal set and begin
#      admitting expired workflows, which is the point.

# v32: the fresh-world arm reaches the cutover's end state. v27 adds
#      the live cutover columns TRANSITIONALLY — nullable, unchecked —
#      because an upgraded install's old writers would violate the
#      declared shape before the cutover backfills it, and the offline
#      tighten stage is what brings those installs to the final shape.
#      A uuid-born install has no old writers and never runs that
#      stage, so it wore the transitional shape permanently while the
#      fresh-world arm claimed it was born at the end state: three of
#      the end state's four parts (status domain, move-family program,
#      partitioned heartbeats) were applied and the column tightening
#      was not. The arm now applies the same rendered statements the
#      tighten stage applies, guarded on the class column already being
#      required. Upgraded installs are unchanged — the chain runs at
#      broker init, before the cutover, where legacy NULLs still exist.

# v33: the locator contract deletes on cascade. Pending is evidence FOR
#      a node's progression, so a node that is gone leaves nothing to
#      progress — the consumer's own conclusion for a workflow that
#      reached terminal is to dispose of the evidence, and the cascade
#      applies that conclusion transactionally when the delete is the
#      only remaining actor. Refusing instead would tie workflow
#      retention to consumption liveness. The fresh-world arm guards on
#      the DELETE ACTION rather than the constraint's existence: adding
#      a constraint that already exists never rebuilds it, so a database
#      born at v32 would keep the refusing form forever.

# v34: the per-leaf enqueue-order index on task history. Every leaf
#      carried only the task-ID index, so the monitoring list's default
#      sort (enqueued_at DESC) planned a sequential scan plus a full
#      sort over every in-window leaf per call. With a btree on
#      enqueued_at per leaf the planner merge-appends leaves in index
#      order and stops at the LIMIT. New leaves are born with the index
#      (partition manager, forever DDL, transcode replacements); this
#      migration walks the partition tree itself — pg_partition_tree,
#      not the leaf catalog, because attached leaves are what the parent
#      query scans — and creates the index where the PROPERTY (a
#      non-partial single-key btree on enqueued_at) is absent. Guarding
#      on the property rather than an index name follows the generated-
#      predicate class rule: the transcode swap attaches relations whose
#      index names differ from the canonical derivation, and a name
#      guard would either miss them or create duplicates.

# v35: durable offline-cutover completion marker, plus RANGE subpartitioning
#      for the forever history class. The migration version and independently
#      operated cutover are separate state dimensions: an upgraded pre-cutover
#      database records v35 but does not receive the completion row. Fresh and
#      already-cut-over databases receive it only after structural validation.

# v36: durable, upper-bounded recovery cursors. The two recovery indexes are
#      built concurrently before the bounded database program is activated.

# v37: close the workflow-node status set and reject unsupported phase-two inputs.

# v38: partial index on nonterminal workflow nodes for recovery probes.

# v39: claim candidates bounded by queue cap and batch limit, selected by a
#      generically planned helper, returned in dispatch order; the claimed
#      non-runnable workflow lookup becomes a bounded uuid-world function.

# v40: horsies_phase2_consume runs with plan_cache_mode = force_generic_plan.

# v41: the claim-fenced pause abandons re-check PAUSED under the workflow
#      lock and refuse when a resume committed first.

# v42: the single-row move hashes the result once, and the six batch moves
#      encode each attempt snapshot once.

# v43: idx_horsies_tasks_retention and idx_horsies_tasks_queue_retention are
#      dropped concurrently; terminal tasks no longer age in horsies_tasks.

# v44: horsies_expire_pending_tasks returns its outcomes in ascending
#      good_until order.

# v45: ix_horsies_workflow_tasks_node_id, ix_horsies_workflow_tasks_workflow_id
#      and idx_horsies_workflow_tasks_deps are dropped concurrently.

# v46: uuid databases drop horsies_task_notify_update_trigger; the move
#      program sends task_done itself.

# v47: idx_horsies_history_catalog_active, a partial index on the leaf
#      catalog's records whose leaf is not dropped.

# v48: the move program's duplicate-identity guard probes only the leaves
#      that can hold an id born at its time; ids moved with a terminal time
#      before their birth minus the clock bound are recorded as exceptions.

# v49: the history parent's 24 single-column CHECKs are dropped; the
#      writers hold those rules (see horsies.core.history.ddl.column_rules).
#      The 8 cross-column CHECKs stay.
# v50: the six batch moves hash the batch result once, not once per row.
# v51: stx_horsies_tasks_retention is dropped. Its expression has no reader
#      since the task retention indexes and DELETEs left (v43); fresh
#      databases no longer build it.
# v52: horsies_workflow_recovery_failures, the stored count of the workflow
#      recovery limit.
# v53: horsies_worker_states.slots_down, the worker slots that were down for a
#      boot failure cause that can clear. Nullable, no backfill: a row from an
#      earlier worker did not record it.
# v54: horsies_tasks.execution, the Execution of each task, with a CHECK on
#      its value domain. The constant default makes the ADD catalog-only on
#      PostgreSQL 11+; existing rows, and rows from a process that does not
#      know the column, read ISOLATED. idx_horsies_tasks_claim_pending_class
#      (queue_name, execution, priority, enqueued_at, id) over PENDING rows is
#      built concurrently: one ordered range per queue and class.
# v55: the claim has a budget per execution class. horsies_claim takes
#      p_budgets (each budget class to its capacity in service) and p_routes
#      (each served class to the budget class that claims it) in place of
#      p_processes, and returns each row's execution; the earlier signatures
#      of the claim pair are dropped. Each (queue, class) is its own claim
#      stream; horsies_claim_plan chooses the rows of a pass in one walk over
#      the heads of all streams. idx_horsies_tasks_claim_expired_class
#      (queue_name, execution, priority, enqueued_at, id) over CLAIMED rows
#      is built concurrently;
#      idx_horsies_tasks_claim_pending and
#      idx_horsies_tasks_claim_expired_ordered are dropped concurrently.
#      horsies_worker_states.class_capacity holds the capacity of each budget
#      and the classes it serves. Nullable, no backfill: a row from an
#      earlier worker did not record it.
# v56: horsies_complete_tasks_fused, the batched plain-success completion.
#      It calls horsies_complete_task_fused once per row, in sorted task-id
#      order, and returns each row at its input ordinality.

SCHEMA_VERSION = 56

from horsies.core.history.terminalization.live_cutover import (  # noqa: E402
    transitional_cutover_columns_ddl,
)

# Migration (v27): the transitional live cutover columns, derived from
# the authoritative fragment (see the v27 note above).
ADD_TRANSITIONAL_CUTOVER_COLUMNS_SQL = text(
    transitional_cutover_columns_ddl()
)

# Migration (v28): the reservation registry, verbatim from the frozen
# module (see the v28 note above). One-owner is enforced by the import
# graph itself; idempotence lives at the runner's existence check, not
# in a second DDL text.
from horsies.core.history.ddl.tables import (  # noqa: E402
    KEY_RESERVATIONS_DDL,
)
from horsies.core.history.identity.reservations import (  # noqa: E402
    KEY_RESERVATION_CLAIM_FUNCTION,
    KEY_RESERVATION_CLEANUP_FUNCTION,
    KEY_RESERVATION_OUTCOME_TYPE,
    KEY_RESERVATION_TERMINALIZE_BATCH_FUNCTION,
    KEY_RESERVATION_TERMINALIZE_FUNCTION,
    reservation_function_fragments,
)
from horsies.core.history.names import KEY_RESERVATIONS  # noqa: E402

KEY_RESERVATIONS_TABLE_EXISTS_SQL = text(
    f"SELECT to_regclass('{KEY_RESERVATIONS}') IS NOT NULL"
)
CREATE_KEY_RESERVATIONS_TABLE_SQL = text(KEY_RESERVATIONS_DDL)
DROP_RESERVATION_PROGRAM_SQL = tuple(
    text(statement)
    for statement in (
        f'DROP FUNCTION IF EXISTS {KEY_RESERVATION_CLAIM_FUNCTION}'
        '(bytea, smallint, interval, smallint, bytea, uuid)',
        f'DROP FUNCTION IF EXISTS {KEY_RESERVATION_TERMINALIZE_FUNCTION}'
        '(bytea, uuid, timestamptz)',
        f'DROP FUNCTION IF EXISTS '
        f'{KEY_RESERVATION_TERMINALIZE_BATCH_FUNCTION}'
        '(bytea[], uuid[], timestamptz)',
        f'DROP FUNCTION IF EXISTS {KEY_RESERVATION_CLEANUP_FUNCTION}'
        '(integer)',
        f'DROP TYPE IF EXISTS {KEY_RESERVATION_OUTCOME_TYPE}',
    )
)
CREATE_RESERVATION_PROGRAM_SQL = tuple(
    text(statement) for statement in reservation_function_fragments()
)

# Migration (v29): the history foundation and gated column families,
# consumed by import (see the v29 note above). `gated_fragment()` is
# the single text authority for conditional DDL; calling it here is
# the act of production emission.
from horsies.core.history.ddl.conditional import (  # noqa: E402
    GatedFragment,
    RESERVATION_REGISTRY_EXPIRY_INDEX,
    gated_fragment,
)
from horsies.core.history.ddl.fragments import (  # noqa: E402
    history_foundation_fragments,
)
from horsies.core.history.names import (  # noqa: E402
    TASK_HISTORY_PARENT,
)

# The two-worlds predicate: the identity column's actual birth shape,
# read from the catalog. A uuid-born database is the fresh world (born
# at the cutover's end state); a varchar-born one is the upgraded world
# that reaches that state only through the offline cutover program.
FRESH_IDENTITY_PREDICATE_SQL = text(
    """SELECT atttypid = 'uuid'::regtype FROM pg_attribute
    WHERE attrelid = 'horsies_tasks'::regclass AND attname = 'id'"""
)

TASK_HISTORY_PARENT_EXISTS_SQL = text(
    f"SELECT to_regclass('{TASK_HISTORY_PARENT}') IS NOT NULL"
)

# Migration (v34): attached task-history leaves missing the enqueue-order
# index, from the live partition tree (see the v34 note above). The NOT
# EXISTS probe pins the property — a non-partial single-key btree whose
# key column is enqueued_at — never an index name.
TASK_HISTORY_LEAVES_MISSING_ORDERING_INDEX_SQL = text(
    f"""
    SELECT c.relname AS leaf_name
    FROM pg_partition_tree(CAST('{TASK_HISTORY_PARENT}' AS regclass)) AS t
    JOIN pg_class AS c ON c.oid = t.relid
    WHERE t.isleaf
      AND NOT EXISTS (
          SELECT 1
          FROM pg_index AS i
          JOIN pg_class AS ic ON ic.oid = i.indexrelid
          JOIN pg_am AS am ON am.oid = ic.relam
          WHERE i.indrelid = t.relid
            AND am.amname = 'btree'
            AND i.indpred IS NULL
            AND i.indnkeyatts = 1
            AND i.indkey[0] = (
                SELECT a.attnum
                FROM pg_attribute AS a
                WHERE a.attrelid = t.relid
                  AND a.attname = 'enqueued_at'
            )
      )
    ORDER BY c.relname
    """
)
CREATE_HISTORY_FOUNDATION_SQL = tuple(
    text(statement) for statement in history_foundation_fragments()
)
ADD_ATTEMPT_SNAPSHOT_COLUMNS_SQL = tuple(
    text(statement)
    for statement in gated_fragment(GatedFragment.ATTEMPT_SNAPSHOT_COLUMNS)
)
ADD_RERUN_INPUT_COLUMNS_SQL = tuple(
    text(statement)
    for statement in gated_fragment(GatedFragment.RERUN_INPUT_COLUMNS)
)

# Migration (v30): the registry maintenance indexes (see the v30 note
# above). The existence check probes the same imported name the DDL
# creates.
RESERVATION_REGISTRY_INDEX_EXISTS_SQL = text(
    f"SELECT to_regclass('{RESERVATION_REGISTRY_EXPIRY_INDEX}') "
    'IS NOT NULL'
)
CREATE_RESERVATION_REGISTRY_INDEXES_SQL = tuple(
    text(statement)
    for statement in gated_fragment(
        GatedFragment.RESERVATION_REGISTRY_INDEXES
    )
)

from .terminalization import (  # noqa: E402
    ADD_TERMINAL_AT_CHECK_SQL,
    ADD_TERMINALIZATION_KIND_CHECK_SQL,
    ADD_TERMINALIZATION_KIND_COLUMN_SQL,
    BACKFILL_TERMINAL_AT_SQL,
    CREATE_OUTCOME_TYPE_SQL,
    CREATE_TERMINALIZATION_FUNCTIONS_SQL,
    DROP_TERMINALIZATION_FUNCTIONS_SQL,
    VALIDATE_TERMINAL_AT_CHECK_SQL,
    VALIDATE_TERMINALIZATION_KIND_CHECK_SQL,
)

# v51: drops the task retention statistics only where they are the object
# v14 built: on horsies_tasks in the current schema.
DROP_TASKS_RETENTION_STATISTICS_SQL = text("""
DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM pg_statistic_ext
        WHERE stxname = 'stx_horsies_tasks_retention'
          AND stxnamespace = current_schema()::regnamespace
          AND stxrelid = 'horsies_tasks'::regclass
    ) THEN
        DROP STATISTICS stx_horsies_tasks_retention;
    END IF;
END
$$
""")

SCHEMA_ADVISORY_LOCK_SQL = text("""
    SELECT pg_advisory_xact_lock(CAST(:key AS BIGINT))
""")

# The COALESCE expression must stay structurally identical (same columns,
# same order) to the workflow retention index in schemas/indexes.py and the
# workflow retention DELETE in worker/sql.py — the planner matches parsed
# expressions, and a drifted column list silently loses the statistics.
CREATE_WORKFLOWS_RETENTION_STATISTICS_SQL = text("""
    CREATE STATISTICS IF NOT EXISTS stx_horsies_workflows_retention
    ON (COALESCE(completed_at, updated_at, created_at))
    FROM horsies_workflows;
""")

# ANALYZE is sampled (bounded by default_statistics_target regardless of
# table size), takes SHARE UPDATE EXCLUSIVE (never blocks reads or writes;
# a conflicting autovacuum worker yields), and is legal inside the
# migration transaction — its statistics commit with the schema version.
ANALYZE_EXPRESSION_INDEXED_TABLES_SQL = text("""
    ANALYZE horsies_tasks, horsies_workflows;
""")

# Collapsed claim. The advisory lock is the FIRST imperative statement (a
# plpgsql function executes sequentially), guaranteeing acquisition before any
# read with predictable ordering — a raw CTE cannot guarantee this. Held until
# the caller COMMITs (xact-scoped); the only remaining client gap while held is
# the commit round trip.
#
# Budgets (v55). p_budgets maps each budget class to its capacity in service;
# p_routes maps each execution class that the worker serves to the budget class
# that claims its rows. A class that is not a key of p_routes is not claimed.
# This worker's RUNNING and live CLAIMED rows count against the budget that
# their class routes to. The capacity of the worker is the sum over its
# budgets. In soft cap mode each budget, and the worker, may hold
# p_prefetch_buffer rows more than its capacity. A budget whose capacity in
# service is 0 (every child or seat of it is down) gets no prefetch, and
# neither does a worker whose budgets all have capacity 0: such a worker
# would hold rows that no child of it can run, while other workers have
# seats for them. p_max_claim_per_worker,
# p_max_claim_batch, the queue caps and p_cluster_wide_cap keep their meaning
# for the worker as a whole.
#
# Claim ordering (rank: qprio, priority, enqueued_at, id):
#   - Each (queue, execution class) is its own stream in (priority,
#     enqueued_at, id) order. Priority holds within one class only.
#   - A budget that serves more than one class reads the merge of their
#     streams in that order.
#   - One walk in rank order takes each row whose queue, budget and pass
#     have room left (horsies_claim_plan).
#   - Distinct queue priorities: queue priority dominates.
#   - EQUAL queue priorities: the equal-priority band is pooled and ordered by
#     task priority, then FIFO. Pinned by tests/integration/test_claim_function.py.
CREATE_CLAIM_FUNCTION_SQL = text("""
CREATE OR REPLACE FUNCTION horsies_claim(
    p_worker_id text,
    p_queues jsonb,
    p_queue_priority jsonb,
    p_queue_max_concurrency jsonb,
    p_hard_cap_mode boolean,
    p_budgets jsonb,
    p_routes jsonb,
    p_prefetch_buffer int,
    p_max_claim_per_worker int,
    p_max_claim_batch int,
    p_cluster_wide_cap int,
    p_lease_ms bigint,
    p_lock_keys jsonb
)
RETURNS TABLE(
    row_kind text,
    id text,
    task_name varchar,
    args text,
    kwargs text,
    queue_name varchar,
    is_workflow_task boolean,
    task_options text,
    claimed_at timestamptz,
    execution text,
    short_by integer
)
LANGUAGE plpgsql
SET jit = off
AS $func$
#variable_conflict use_column
DECLARE
    v_key bigint;
    v_capped text[];
    v_my_claimed int;
    v_my_in_flight int;
    v_global_in_flight int;
    v_queue_counts jsonb;
    v_budget_counts jsonb;
    v_capacity int;
    v_extra int;
    v_max_claimed int;
    v_local_available int;
    v_global_remaining int;
    v_total int;
    v_available jsonb;
BEGIN
    -- 0. The worker builds both maps from one claim plan, so a route to a
    --    budget that p_budgets does not hold is a defect of the caller.
    IF EXISTS (
        SELECT 1
        FROM jsonb_each_text(p_routes) AS r(row_class, budget_class)
        WHERE NOT (p_budgets ? r.budget_class)
    ) THEN
        RAISE EXCEPTION
            'horsies_claim: p_routes names a budget class that p_budgets does not hold'
            USING ERRCODE = 'invalid_parameter_value';
    END IF;

    -- 1. Acquire advisory locks first, in deadlock-safe ascending order.
    FOR v_key IN
        SELECT elem::bigint AS k
        FROM jsonb_array_elements_text(p_lock_keys) AS elem
        ORDER BY 1
    LOOP
        PERFORM pg_advisory_xact_lock(v_key);
    END LOOP;

    SELECT COALESCE(array_agg(key), ARRAY[]::text[])
    INTO v_capped
    FROM jsonb_object_keys(p_queue_max_concurrency) AS key;

    -- 2. Counts under the lock snapshot. Per-queue counts: hard mode counts
    --    RUNNING+CLAIMED, soft counts RUNNING. Per-budget counts: this
    --    worker's in-flight rows, by the budget that their class routes to.
    WITH in_flight AS (
        SELECT t.queue_name, t.status, t.claimed_by_worker_id, t.execution
        FROM horsies_tasks t
        WHERE t.status = 'RUNNING'
           OR (t.status = 'CLAIMED'
               AND (t.claim_expires_at IS NULL OR t.claim_expires_at > now()))
    )
    SELECT
        COUNT(*) FILTER (
            WHERE f.claimed_by_worker_id = p_worker_id AND f.status = 'CLAIMED'),
        COUNT(*) FILTER (WHERE f.claimed_by_worker_id = p_worker_id),
        COUNT(*),
        COALESCE((
            SELECT jsonb_object_agg(g.queue_name, g.cnt) FROM (
                SELECT q.queue_name, COUNT(*) AS cnt
                FROM in_flight q
                WHERE q.queue_name = ANY(v_capped)
                  AND (p_hard_cap_mode OR q.status = 'RUNNING')
                GROUP BY q.queue_name
            ) g
        ), '{}'::jsonb),
        COALESCE((
            SELECT jsonb_object_agg(g.budget_class, g.cnt) FROM (
                SELECT p_routes ->> m.execution AS budget_class, COUNT(*) AS cnt
                FROM in_flight m
                WHERE m.claimed_by_worker_id = p_worker_id
                  AND p_routes ? m.execution
                GROUP BY 1
            ) g
        ), '{}'::jsonb)
    INTO v_my_claimed, v_my_in_flight, v_global_in_flight, v_queue_counts,
         v_budget_counts
    FROM in_flight f;

    -- 3. Budget math. The capacity of the worker is the sum over budgets.
    SELECT COALESCE(SUM(b.capacity::int), 0)
    INTO v_capacity
    FROM jsonb_each_text(p_budgets) AS b(budget_class, capacity);
    v_extra := CASE WHEN p_hard_cap_mode THEN 0 ELSE p_prefetch_buffer END;
    v_max_claimed := CASE
        WHEN p_max_claim_per_worker > 0 THEN p_max_claim_per_worker
        WHEN p_prefetch_buffer > 0 THEN v_capacity + p_prefetch_buffer
        ELSE v_capacity END;
    IF v_my_claimed >= v_max_claimed THEN
        RETURN;
    END IF;

    -- A worker with no capacity in service gets no prefetch.
    v_local_available := CASE
        WHEN v_capacity = 0 THEN 0
        ELSE GREATEST(0, v_capacity + v_extra - v_my_in_flight) END;

    IF p_cluster_wide_cap IS NOT NULL THEN
        v_global_remaining := GREATEST(0, p_cluster_wide_cap - v_global_in_flight);
    ELSE
        v_global_remaining := 2147483647;
    END IF;

    v_total := LEAST(
        v_local_available, v_max_claimed - v_my_claimed, v_global_remaining);
    IF v_total <= 0 THEN
        RETURN;
    END IF;

    -- A budget with no capacity in service gets no prefetch.
    SELECT COALESCE(jsonb_object_agg(
               b.budget_class,
               CASE WHEN b.capacity::int = 0 THEN 0
                   ELSE GREATEST(0, b.capacity::int + v_extra
                       - COALESCE((v_budget_counts ->> b.budget_class)::int, 0))
                   END
           ), '{}'::jsonb)
    INTO v_available
    FROM jsonb_each_text(p_budgets) AS b(budget_class, capacity);

    -- 4. Candidate selection runs in horsies_claim_candidates, which reuses
    --    one generic plan; the accounting above keeps parameter-specific
    --    plans. Everything stays in this transaction.
    RETURN QUERY
    SELECT * FROM horsies_claim_candidates(
        v_total, v_available, p_routes, p_queues, p_queue_priority,
        p_queue_max_concurrency, v_queue_counts, p_max_claim_batch,
        p_worker_id, p_lease_ms
    );
END;
$func$;
""")

# v39: candidate selection runs in a helper that is planned once per
# connection (plan_cache_mode = force_generic_plan). The parent keeps
# automatic planning: forcing a generic plan on the in-flight accounting
# makes it slower under a large in-flight population.
#
# The three functions run with jit = off. The generic plan does not know the
# LIMIT of each stream, so its cost estimate passes jit_above_cost, and a
# server with JIT compiles the statement on every pass: 28.8 ms for 0.3 ms
# of work with the v54 pair, 630 ms with the first v55 pair, both at a
# 90,000-row backlog on PostgreSQL 17.
#
# v55: two phases, so a claim locks only the rows it takes.
#   1. horsies_claim_plan reads, without locks, the head of each (queue,
#      class) stream: the pending and expired rows of the stream merged in
#      (priority, enqueued_at, id) order, at most as many as the queue's room
#      and the budget's available rows allow. One walk over all heads in
#      rank order takes a row when its queue, its budget and the pass each
#      have room left, and counts the row against all three. A row that one
#      limit drops counts against none. It returns the rows to take from each
#      stream and arm.
#   2. horsies_claim_candidates takes that many rows from each stream and
#      arm with FOR UPDATE SKIP LOCKED. A row that another claim holds is
#      skipped and the walk goes on, so priority holds within each stream
#      also under contention.
# A stream takes a prefix of its rows: once one of its limits has no room,
# no later row of the stream is taken. So the head of a stream, bounded by
# its queue's room and its budget's available rows, holds every row that the
# walk can take from it.
# Every stream is one ordered range of idx_horsies_tasks_claim_pending_class
# (pending) or idx_horsies_tasks_claim_expired_class (expired).
CREATE_CLAIM_PLAN_FUNCTION_SQL = text("""
CREATE OR REPLACE FUNCTION horsies_claim_plan(
    p_total integer,
    p_available jsonb,
    p_routes jsonb,
    p_queues jsonb,
    p_queue_priority jsonb,
    p_queue_max_concurrency jsonb,
    p_queue_counts jsonb,
    p_max_claim_batch integer
)
RETURNS TABLE(queue_name text, row_class text, arm text, take integer)
LANGUAGE plpgsql
SET plan_cache_mode = force_generic_plan
SET jit = off
AS $plan$
#variable_conflict use_column
DECLARE
    -- Room left in each queue (by position in p_queues), in each budget
    -- (by position in v_budgets), and in the pass.
    v_queue_room int[];
    v_budgets text[];
    v_budget_room int[];
    v_total_room int := p_total;
    v_picked_queue text[] := ARRAY[]::text[];
    v_picked_class text[] := ARRAY[]::text[];
    v_picked_arm text[] := ARRAY[]::text[];
    v_row record;
BEGIN
    SELECT array_agg(LEAST(
               p_total,
               CASE WHEN p_queue_max_concurrency ? q.name THEN
                   GREATEST(0, (p_queue_max_concurrency ->> q.name)::int
                       - COALESCE((p_queue_counts ->> q.name)::int, 0))
                   ELSE p_total END,
               CASE WHEN p_max_claim_batch > 0 THEN p_max_claim_batch
                   ELSE p_total END
           ) ORDER BY q.slot)
    INTO v_queue_room
    FROM jsonb_array_elements_text(p_queues) WITH ORDINALITY AS q(name, slot);

    SELECT array_agg(b.budget_class ORDER BY b.budget_class),
           array_agg(b.available::int ORDER BY b.budget_class)
    INTO v_budgets, v_budget_room
    FROM jsonb_each_text(p_available) AS b(budget_class, available);

    FOR v_row IN
        SELECT s.queue_slot, s.budget_slot, s.queue_name, s.row_class, c.arm
        FROM (
            SELECT q.slot::int AS queue_slot, q.name AS queue_name,
                   COALESCE((p_queue_priority ->> q.name)::int, 100) AS qprio,
                   r.row_class, b.budget_slot,
                   LEAST(v_queue_room[q.slot::int],
                         v_budget_room[b.budget_slot]) AS head
            FROM jsonb_array_elements_text(p_queues) WITH ORDINALITY AS q(name, slot)
            CROSS JOIN jsonb_each_text(p_routes) AS r(row_class, budget_class)
            CROSS JOIN LATERAL (
                SELECT array_position(v_budgets, r.budget_class) AS budget_slot
            ) b
            WHERE LEAST(v_queue_room[q.slot::int],
                        v_budget_room[b.budget_slot]) > 0
        ) s
        CROSS JOIN LATERAL (
            SELECT m.arm, m.priority, m.enqueued_at, m.id
            FROM (
                (SELECT 'PENDING'::text AS arm, t.priority, t.enqueued_at, t.id
                 FROM horsies_tasks t
                 WHERE t.queue_name = s.queue_name
                   AND t.execution = s.row_class
                   AND t.status = 'PENDING'
                   AND t.enqueued_at <= now()
                   AND (t.next_retry_at IS NULL OR t.next_retry_at <= now())
                   AND (t.good_until IS NULL OR t.good_until > now())
                 ORDER BY t.priority ASC, t.enqueued_at ASC, t.id ASC
                 LIMIT s.head)
                UNION ALL
                (SELECT 'EXPIRED'::text AS arm, t.priority, t.enqueued_at, t.id
                 FROM horsies_tasks t
                 WHERE t.queue_name = s.queue_name
                   AND t.execution = s.row_class
                   AND t.status = 'CLAIMED'
                   AND t.claim_expires_at IS NOT NULL
                   AND t.claim_expires_at < now()
                   AND t.enqueued_at <= now()
                   AND (t.next_retry_at IS NULL OR t.next_retry_at <= now())
                   AND (t.good_until IS NULL OR t.good_until > now())
                 ORDER BY t.priority ASC, t.enqueued_at ASC, t.id ASC
                 LIMIT s.head)
            ) m
            ORDER BY m.priority ASC, m.enqueued_at ASC, m.id ASC
            LIMIT s.head
        ) c
        ORDER BY s.qprio ASC, c.priority ASC, c.enqueued_at ASC, c.id ASC
    LOOP
        EXIT WHEN v_total_room = 0;
        CONTINUE WHEN v_queue_room[v_row.queue_slot] = 0
            OR v_budget_room[v_row.budget_slot] = 0;
        v_queue_room[v_row.queue_slot] := v_queue_room[v_row.queue_slot] - 1;
        v_budget_room[v_row.budget_slot] := v_budget_room[v_row.budget_slot] - 1;
        v_total_room := v_total_room - 1;
        v_picked_queue := v_picked_queue || v_row.queue_name;
        v_picked_class := v_picked_class || v_row.row_class;
        v_picked_arm := v_picked_arm || v_row.arm;
    END LOOP;

    RETURN QUERY
    SELECT p.queue_name, p.row_class, p.arm, count(*)::int
    FROM unnest(v_picked_queue, v_picked_class, v_picked_arm)
        AS p(queue_name, row_class, arm)
    GROUP BY p.queue_name, p.row_class, p.arm;
END;
$plan$;
""")

# Phase 2 of the claim (see horsies_claim_plan). One 'CLAIMED' row per task
# it took, in rank order: queue priority, task priority, enqueued_at, id.
# The plan comes from an unlocked read, so another transaction can hold or
# take a planned row before the take. When the take gets fewer rows than the
# plan, one 'SHORT' row follows, whose short_by counts the rows it missed;
# the worker then claims again with no wait, as no NOTIFY announces the
# rows that remain. The task columns of a 'SHORT' row are NULL, and the
# short_by of a 'CLAIMED' row is NULL.
CREATE_CLAIM_CANDIDATES_FUNCTION_SQL = text("""
CREATE OR REPLACE FUNCTION horsies_claim_candidates(
    p_total integer,
    p_available jsonb,
    p_routes jsonb,
    p_queues jsonb,
    p_queue_priority jsonb,
    p_queue_max_concurrency jsonb,
    p_queue_counts jsonb,
    p_max_claim_batch integer,
    p_worker_id text,
    p_lease_ms bigint
)
RETURNS TABLE(
    row_kind text,
    id text,
    task_name varchar,
    args text,
    kwargs text,
    queue_name varchar,
    is_workflow_task boolean,
    task_options text,
    claimed_at timestamptz,
    execution text,
    short_by integer
)
LANGUAGE plpgsql
SET plan_cache_mode = force_generic_plan
SET jit = off
AS $candidates$
#variable_conflict use_column
DECLARE
    v_queue text[];
    v_class text[];
    v_arm text[];
    v_take int[];
    v_planned int;
    v_taken int;
BEGIN
    SELECT COALESCE(array_agg(p.queue_name), ARRAY[]::text[]),
           COALESCE(array_agg(p.row_class), ARRAY[]::text[]),
           COALESCE(array_agg(p.arm), ARRAY[]::text[]),
           COALESCE(array_agg(p.take), ARRAY[]::int[]),
           COALESCE(sum(p.take), 0)::int
    INTO v_queue, v_class, v_arm, v_take, v_planned
    FROM horsies_claim_plan(
        p_total, p_available, p_routes, p_queues, p_queue_priority,
        p_queue_max_concurrency, p_queue_counts, p_max_claim_batch
    ) AS p;
    IF v_planned = 0 THEN
        RETURN;
    END IF;

    RETURN QUERY
    WITH planned AS (
        SELECT p.queue_name, p.row_class, p.arm, p.take
        FROM unnest(v_queue, v_class, v_arm, v_take)
            AS p(queue_name, row_class, arm, take)
    ),
    -- FOR UPDATE is not allowed on UNION operands, so each arm locks in its
    -- own CTE.
    locked_pending AS (
        SELECT l.id
        FROM planned p
        CROSS JOIN LATERAL (
            SELECT t.id
            FROM horsies_tasks t
            WHERE t.queue_name = p.queue_name
              AND t.execution = p.row_class
              AND t.status = 'PENDING'
              AND t.enqueued_at <= now()
              AND (t.next_retry_at IS NULL OR t.next_retry_at <= now())
              AND (t.good_until IS NULL OR t.good_until > now())
            ORDER BY t.priority ASC, t.enqueued_at ASC, t.id ASC
            FOR UPDATE SKIP LOCKED
            LIMIT p.take
        ) l
        WHERE p.arm = 'PENDING'
    ),
    locked_expired AS (
        SELECT l.id
        FROM planned p
        CROSS JOIN LATERAL (
            SELECT t.id
            FROM horsies_tasks t
            WHERE t.queue_name = p.queue_name
              AND t.execution = p.row_class
              AND t.status = 'CLAIMED'
              AND t.claim_expires_at IS NOT NULL
              AND t.claim_expires_at < now()
              AND t.enqueued_at <= now()
              AND (t.next_retry_at IS NULL OR t.next_retry_at <= now())
              AND (t.good_until IS NULL OR t.good_until > now())
            ORDER BY t.priority ASC, t.enqueued_at ASC, t.id ASC
            FOR UPDATE SKIP LOCKED
            LIMIT p.take
        ) l
        WHERE p.arm = 'EXPIRED'
    ),
    -- Targets are looked up by id; the selected batch never drives a scan
    -- of the live table.
    updated AS (
        UPDATE horsies_tasks t
        SET status = 'CLAIMED',
            claimed = TRUE,
            claimed_at = now(),
            claimed_by_worker_id = p_worker_id,
            claim_expires_at = now() + p_lease_ms * INTERVAL '1 millisecond',
            finalizing_at = NULL,
            finalizing_by_worker_id = NULL,
            updated_at = now()
        WHERE t.id = ANY(ARRAY(
            SELECT locked_pending.id FROM locked_pending
            UNION ALL
            SELECT locked_expired.id FROM locked_expired
        ))
        RETURNING t.id AS task_key, t.id::text AS id, t.task_name, t.args,
                  t.kwargs, t.queue_name, t.is_workflow_task, t.task_options,
                  t.claimed_at, t.execution, t.priority, t.enqueued_at
    )
    SELECT 'CLAIMED'::text, u.id, u.task_name, u.args, u.kwargs,
           u.queue_name, u.is_workflow_task, u.task_options, u.claimed_at,
           u.execution, NULL::integer
    FROM updated u
    ORDER BY COALESCE((p_queue_priority ->> u.queue_name)::int, 100) ASC,
             u.priority ASC, u.enqueued_at ASC, u.task_key ASC;

    GET DIAGNOSTICS v_taken = ROW_COUNT;
    IF v_taken < v_planned THEN
        RETURN QUERY
        SELECT 'SHORT'::text, NULL::text, NULL::varchar, NULL::text,
               NULL::text, NULL::varchar, NULL::boolean, NULL::text,
               NULL::timestamptz, NULL::text, v_planned - v_taken;
    END IF;
END;
$candidates$;
""")

# The claim pair before v55: horsies_claim took one process count, and
# neither function returned the execution class. A changed argument list
# makes a second function next to the old one, and CREATE OR REPLACE cannot
# change OUT columns, so each old signature is dropped by its exact argument
# types. The horsies_claim signature is also the v10 to v11 one (v12 changed
# only its OUT columns). The current signatures of the pair are dropped too,
# so an apply installs their OUT columns also over a pair of the same
# argument types and other OUT columns.
DROP_PREVIOUS_CLAIM_FUNCTIONS_SQL = (
    text("""
DROP FUNCTION IF EXISTS horsies_claim(
    text, jsonb, jsonb, jsonb, boolean, int, int, int, int, int, bigint, jsonb
)
"""),
    text("""
DROP FUNCTION IF EXISTS horsies_claim_candidates(
    integer, jsonb, jsonb, jsonb, jsonb, integer, text, bigint
)
"""),
    text("""
DROP FUNCTION IF EXISTS horsies_claim(
    text, jsonb, jsonb, jsonb, boolean, jsonb, jsonb, int, int, int, int,
    bigint, jsonb
)
"""),
    text("""
DROP FUNCTION IF EXISTS horsies_claim_candidates(
    integer, jsonb, jsonb, jsonb, jsonb, jsonb, jsonb, integer, text, bigint
)
"""),
)

CREATE_SCHEMA_VERSION_TABLE_SQL = text("""
    CREATE TABLE IF NOT EXISTS horsies_schema_version (
        version INTEGER PRIMARY KEY,
        applied_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    )
""")

SCHEMA_VERSION_TABLE_EXISTS_SQL = text("""
    SELECT to_regclass('horsies_schema_version') IS NOT NULL
""")

READ_SCHEMA_VERSION_SQL = text("""
    SELECT COALESCE(MAX(version), 0) FROM horsies_schema_version
""")

INSERT_SCHEMA_VERSION_SQL = text("""
    INSERT INTO horsies_schema_version (version)
    VALUES (:version)
    ON CONFLICT (version) DO NOTHING
""")


# ---- Task table migrations ----

SET_TASK_COLUMN_DEFAULTS_SQL = text("""
    ALTER TABLE horsies_tasks
    ALTER COLUMN claimed SET DEFAULT FALSE,
    ALTER COLUMN retry_count SET DEFAULT 0,
    ALTER COLUMN max_retries SET DEFAULT 0,
    ALTER COLUMN priority SET DEFAULT 100,
    ALTER COLUMN created_at SET DEFAULT NOW(),
    ALTER COLUMN updated_at SET DEFAULT NOW();
""")

# Migration: add enqueued_at column and backfill from sent_at for existing rows.
# Column is added WITHOUT a default so existing rows stay NULL, allowing the
# backfill to copy historical sent_at values.  The default and NOT NULL
# constraint are applied after backfill.
ADD_ENQUEUED_AT_COLUMN_SQL = text("""
    ALTER TABLE horsies_tasks
    ADD COLUMN IF NOT EXISTS enqueued_at TIMESTAMPTZ;
""")
BACKFILL_ENQUEUED_AT_SQL = text("""
    UPDATE horsies_tasks
    SET enqueued_at = COALESCE(sent_at, NOW())
    WHERE enqueued_at IS NULL;
""")
SET_ENQUEUED_AT_NOT_NULL_SQL = text("""
    ALTER TABLE horsies_tasks
    ALTER COLUMN enqueued_at SET NOT NULL;
""")
SET_ENQUEUED_AT_DEFAULT_SQL = text("""
    ALTER TABLE horsies_tasks
    ALTER COLUMN enqueued_at SET DEFAULT NOW();
""")

# Migration: add enqueue_sha column for idempotent enqueue verification.
# 3-step: add nullable column, backfill NULLs, enforce NOT NULL.
ADD_ENQUEUE_SHA_COLUMN_SQL = text("""
    ALTER TABLE horsies_tasks
    ADD COLUMN IF NOT EXISTS enqueue_sha VARCHAR(64);
""")
BACKFILL_ENQUEUE_SHA_SQL = text("""
    UPDATE horsies_tasks SET enqueue_sha = 'legacy-pre-sha'
    WHERE enqueue_sha IS NULL;
""")
SET_ENQUEUE_SHA_NOT_NULL_SQL = text("""
    ALTER TABLE horsies_tasks
    ALTER COLUMN enqueue_sha SET NOT NULL;
""")


# ---- Workflow table migrations ----

ADD_TASK_OPTIONS_COLUMN_SQL = text("""
    ALTER TABLE horsies_workflow_tasks
    ADD COLUMN IF NOT EXISTS task_options TEXT;
""")

ADD_SUCCESS_POLICY_COLUMN_SQL = text("""
    ALTER TABLE horsies_workflows
    ADD COLUMN IF NOT EXISTS success_policy JSONB;
""")

ADD_JOIN_TYPE_COLUMN_SQL = text("""
    ALTER TABLE horsies_workflow_tasks
    ADD COLUMN IF NOT EXISTS join_type VARCHAR(10) NOT NULL DEFAULT 'all';
""")

ADD_MIN_SUCCESS_COLUMN_SQL = text("""
    ALTER TABLE horsies_workflow_tasks
    ADD COLUMN IF NOT EXISTS min_success INTEGER;
""")

ADD_NODE_ID_COLUMN_SQL = text("""
    ALTER TABLE horsies_workflow_tasks
    ADD COLUMN IF NOT EXISTS node_id VARCHAR(128);
""")


ADD_PARENT_WORKFLOW_ID_COLUMN_SQL = text("""
    ALTER TABLE horsies_workflows
    ADD COLUMN IF NOT EXISTS parent_workflow_id VARCHAR(36);
""")

ADD_PARENT_TASK_INDEX_COLUMN_SQL = text("""
    ALTER TABLE horsies_workflows
    ADD COLUMN IF NOT EXISTS parent_task_index INTEGER;
""")

ADD_DEPTH_COLUMN_SQL = text("""
    ALTER TABLE horsies_workflows
    ADD COLUMN IF NOT EXISTS depth INTEGER NOT NULL DEFAULT 0;
""")

ADD_ROOT_WORKFLOW_ID_COLUMN_SQL = text("""
    ALTER TABLE horsies_workflows
    ADD COLUMN IF NOT EXISTS root_workflow_id VARCHAR(36);
""")

ADD_DEFINITION_KEY_COLUMN_SQL = text("""
    ALTER TABLE horsies_workflows
    ADD COLUMN IF NOT EXISTS definition_key VARCHAR(255);
""")

ADD_IS_SUBWORKFLOW_COLUMN_SQL = text("""
    ALTER TABLE horsies_workflow_tasks
    ADD COLUMN IF NOT EXISTS is_subworkflow BOOLEAN NOT NULL DEFAULT FALSE;
""")

ADD_SUB_WORKFLOW_ID_COLUMN_SQL = text("""
    ALTER TABLE horsies_workflow_tasks
    ADD COLUMN IF NOT EXISTS sub_workflow_id VARCHAR(36);
""")

ADD_SUB_WORKFLOW_NAME_COLUMN_SQL = text("""
    ALTER TABLE horsies_workflow_tasks
    ADD COLUMN IF NOT EXISTS sub_workflow_name VARCHAR(255);
""")

DROP_SUB_WORKFLOW_RETRY_MODE_COLUMN_SQL = text("""
    ALTER TABLE horsies_workflow_tasks
    DROP COLUMN IF EXISTS sub_workflow_retry_mode;
""")

ADD_SUB_WORKFLOW_SUMMARY_COLUMN_SQL = text("""
    ALTER TABLE horsies_workflow_tasks
    ADD COLUMN IF NOT EXISTS sub_workflow_summary TEXT;
""")

ADD_SUB_DEFINITION_KEY_COLUMN_SQL = text("""
    ALTER TABLE horsies_workflow_tasks
    ADD COLUMN IF NOT EXISTS sub_definition_key VARCHAR(255);
""")

DROP_WORKFLOW_DEF_MODULE_COLUMN_SQL = text("""
    ALTER TABLE horsies_workflows
    DROP COLUMN IF EXISTS workflow_def_module;
""")

DROP_WORKFLOW_DEF_QUALNAME_COLUMN_SQL = text("""
    ALTER TABLE horsies_workflows
    DROP COLUMN IF EXISTS workflow_def_qualname;
""")

DROP_SUB_WORKFLOW_MODULE_COLUMN_SQL = text("""
    ALTER TABLE horsies_workflow_tasks
    DROP COLUMN IF EXISTS sub_workflow_module;
""")

DROP_SUB_WORKFLOW_QUALNAME_COLUMN_SQL = text("""
    ALTER TABLE horsies_workflow_tasks
    DROP COLUMN IF EXISTS sub_workflow_qualname;
""")

CREATE_TASK_ATTEMPTS_TABLE_SQL = text("""
    CREATE TABLE IF NOT EXISTS horsies_task_attempts (
        id BIGSERIAL PRIMARY KEY,
        task_id VARCHAR(36) NOT NULL REFERENCES horsies_tasks(id) ON DELETE CASCADE,
        attempt INTEGER NOT NULL,
        outcome VARCHAR(32) NOT NULL,
        will_retry BOOLEAN NOT NULL DEFAULT FALSE,
        started_at TIMESTAMPTZ NOT NULL,
        finished_at TIMESTAMPTZ NOT NULL,
        error_code TEXT,
        error_message TEXT,
        failed_reason TEXT,
        worker_id VARCHAR(255),
        worker_hostname VARCHAR(255),
        worker_pid INTEGER,
        worker_process_name VARCHAR(255),
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        CONSTRAINT uq_horsies_task_attempts_task_attempt UNIQUE (task_id, attempt),
        CONSTRAINT ck_horsies_task_attempts_outcome
            CHECK (outcome IN ('COMPLETED', 'FAILED', 'WORKER_FAILURE'))
    )
""")

ADD_ERROR_CODE_COLUMN_SQL = text("""
    ALTER TABLE horsies_tasks
    ADD COLUMN IF NOT EXISTS error_code TEXT;
""")

ADD_TASK_IS_WORKFLOW_TASK_COLUMN_SQL = text("""
    ALTER TABLE horsies_tasks
    ADD COLUMN IF NOT EXISTS is_workflow_task BOOLEAN NOT NULL DEFAULT FALSE;
""")

BACKFILL_TASK_IS_WORKFLOW_TASK_SQL = text("""
    UPDATE horsies_tasks t
    SET is_workflow_task = TRUE
    WHERE is_workflow_task = FALSE
      AND EXISTS (
          SELECT 1 FROM horsies_workflow_tasks wt WHERE wt.task_id = t.id
      );
""")

ADD_TASK_FINALIZING_COLUMNS_SQL = text("""
    ALTER TABLE horsies_tasks
    ADD COLUMN IF NOT EXISTS finalizing_at TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS finalizing_by_worker_id TEXT;
""")

# Migration (v17): canonical terminal instant. Nullable with no default, so
# the ALTER is a catalog-only change on PostgreSQL 11+ regardless of table
# size. Non-NULL exactly for terminal rows; the CHECK enforcing that ships
# one release later (see the SCHEMA_VERSION note).
ADD_TASK_TERMINAL_AT_COLUMN_SQL = text("""
    ALTER TABLE horsies_tasks
    ADD COLUMN IF NOT EXISTS terminal_at TIMESTAMPTZ;
""")

# Migration (v54): the execution class of each task. The constant default
# is a catalog-only change on PostgreSQL 11+. The default stays after the
# migration: a process of an earlier version that inserts a task does not
# know the column, and its rows run isolated.
ADD_TASK_EXECUTION_COLUMN_SQL = text("""
    ALTER TABLE horsies_tasks
    ADD COLUMN IF NOT EXISTS execution TEXT NOT NULL DEFAULT 'ISOLATED';
""")


def _execution_domain() -> str:
    """The Execution values as sorted SQL literals."""
    return ', '.join(sorted(f"'{member.value}'" for member in Execution))


# The CHECK is guarded on its definition, not on its name: a constraint of
# this name with another value domain is dropped and added again, so a new
# Execution member reaches every deployed database with the next version.
# The expected definition comes from the same constraint on a temporary
# table, so both sides are rendered by the same server.
ADD_TASK_EXECUTION_CHECK_SQL = text(f"""
DO $$
DECLARE
    v_actual text;
    v_expected text;
BEGIN
    CREATE TEMP TABLE horsies_expected_task_execution (execution text)
        ON COMMIT DROP;
    ALTER TABLE horsies_expected_task_execution
        ADD CONSTRAINT ck_horsies_tasks_execution
        CHECK (execution IN ({_execution_domain()}));
    SELECT pg_get_constraintdef(oid) INTO v_expected
    FROM pg_constraint
    WHERE conrelid = 'horsies_expected_task_execution'::regclass
      AND conname = 'ck_horsies_tasks_execution';
    SELECT pg_get_constraintdef(oid) INTO v_actual
    FROM pg_constraint
    WHERE conrelid = 'horsies_tasks'::regclass
      AND conname = 'ck_horsies_tasks_execution'
      AND convalidated;
    DROP TABLE horsies_expected_task_execution;
    IF v_actual IS DISTINCT FROM v_expected THEN
        ALTER TABLE horsies_tasks
        DROP CONSTRAINT IF EXISTS ck_horsies_tasks_execution;
        ALTER TABLE horsies_tasks
        ADD CONSTRAINT ck_horsies_tasks_execution
        CHECK (execution IN ({_execution_domain()})) NOT VALID;
    END IF;
END
$$""")

# ADD and VALIDATE share the migration transaction. Validating a valid
# constraint is a no-op, so a later release does not scan again.
VALIDATE_TASK_EXECUTION_CHECK_SQL = text("""
    ALTER TABLE horsies_tasks
    VALIDATE CONSTRAINT ck_horsies_tasks_execution
""")

# Migration (v4): widen timeseries PKs to BIGINT. Both tables are
# retention-bounded (hourly/periodic cleanup), so the type rewrite is cheap.
WIDEN_HEARTBEATS_ID_TO_BIGINT_SQL = text("""
    ALTER TABLE horsies_heartbeats
    ALTER COLUMN id TYPE BIGINT;
""")

WIDEN_WORKER_STATES_ID_TO_BIGINT_SQL = text("""
    ALTER TABLE horsies_worker_states
    ALTER COLUMN id TYPE BIGINT;
""")

# Migration (v5): every horsies_tasks lifecycle UPDATE touches an indexed
# column, making updates non-HOT — each one writes new entries into ALL
# indexes. Indexes that serve no query (or whose queries are covered by the
# v3 partial composites) are pure churn on the hottest table.
DROP_REDUNDANT_TASK_INDEXES_SQL = text("""
    DROP INDEX IF EXISTS ix_horsies_tasks_claimed;
    DROP INDEX IF EXISTS ix_horsies_tasks_claim_expires_at;
    DROP INDEX IF EXISTS ix_horsies_tasks_is_workflow_task;
    DROP INDEX IF EXISTS ix_horsies_tasks_finalizing_at;
    DROP INDEX IF EXISTS ix_horsies_tasks_good_until;
    DROP INDEX IF EXISTS ix_horsies_tasks_next_retry_at;
    DROP INDEX IF EXISTS ix_horsies_worker_states_worker_id;
""")

# good_until is immutable per row and mostly NULL; the expiry scans
# (status='PENDING' AND good_until <= NOW() ORDER BY good_until) only ever
# touch rows where it is set.
CREATE_TASKS_GOOD_UNTIL_PARTIAL_INDEX_SQL = text("""
    CREATE INDEX IF NOT EXISTS idx_horsies_tasks_good_until_set
    ON horsies_tasks (good_until)
    WHERE good_until IS NOT NULL;
""")

ADD_WORKFLOW_SENT_AT_COLUMN_SQL = text("""
    ALTER TABLE horsies_workflows
    ADD COLUMN IF NOT EXISTS sent_at TIMESTAMPTZ;
""")
BACKFILL_WORKFLOW_SENT_AT_SQL = text("""
    UPDATE horsies_workflows
    SET sent_at = COALESCE(created_at, NOW())
    WHERE sent_at IS NULL;
""")
SET_WORKFLOW_SENT_AT_NOT_NULL_SQL = text("""
    ALTER TABLE horsies_workflows
    ALTER COLUMN sent_at SET NOT NULL;
""")
SET_WORKFLOW_SENT_AT_DEFAULT_SQL = text("""
    ALTER TABLE horsies_workflows
    ALTER COLUMN sent_at SET DEFAULT NOW();
""")

# Migration (v9): expose executor-child memory in worker telemetry. Nullable,
# no backfill — pre-existing snapshots predate the metric and stay NULL.
ADD_WORKER_STATES_CHILDREN_MEMORY_COLUMN_SQL = text("""
    ALTER TABLE horsies_worker_states
    ADD COLUMN IF NOT EXISTS children_memory_mb DOUBLE PRECISION;
""")

# Migration (v53): the slots that a worker reports as down. Nullable, no
# backfill: a snapshot written before this column did not record slot health.
ADD_WORKER_STATES_SLOTS_DOWN_COLUMN_SQL = text("""
    ALTER TABLE horsies_worker_states
    ADD COLUMN IF NOT EXISTS slots_down JSONB;
""")

# Migration (v55): the capacity of each claim budget of a worker, and the
# classes it serves. Nullable, no backfill: a snapshot written before this
# column did not record it.
ADD_WORKER_STATES_CLASS_CAPACITY_COLUMN_SQL = text("""
    ALTER TABLE horsies_worker_states
    ADD COLUMN IF NOT EXISTS class_capacity JSONB;
""")
