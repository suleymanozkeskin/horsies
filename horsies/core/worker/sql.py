"""SQL constants for the Worker."""

from __future__ import annotations

from sqlalchemy import text

from horsies.core.models.workflow import WORKFLOW_TERMINAL_STATES
from horsies.core.schemas.indexes import (
    TASK_TERMINAL_STATUS_SQL_LITERALS,
    WORKFLOW_TERMINAL_STATUS_SQL_LITERALS,
)
from horsies.core.types.status import TASK_TERMINAL_STATES, TaskStatus

WORKFLOW_TERMINAL_VALUES: list[str] = [s.value for s in WORKFLOW_TERMINAL_STATES]
TASK_TERMINAL_VALUES: list[str] = [s.value for s in TASK_TERMINAL_STATES]

# Complement of the terminal set, derived from the same enum so the two can
# never drift: together they cover every TaskStatus. Non-terminal statuses
# are in-flight work — a small set of rows by definition — which lets the
# retention guard probe ix_horsies_tasks_status instead of scanning for
# "NOT terminal" (an inequality no index serves).
TASK_NONTERMINAL_STATUS_SQL_LITERALS: str = ', '.join(
    f"'{s.value}'" for s in sorted(
        (s for s in TaskStatus if s not in TASK_TERMINAL_STATES),
        key=lambda s: s.value,
    )
)


# ---------- Claim SQL (priority + enqueued_at) ----------
# All claimed tasks receive a bounded lease via claim_expires_at.
# Expired leases are reclaimable by any worker (crash recovery + soft-cap prefetch).
#
# The two eligibility arms (fresh PENDING, expired CLAIMED lease) are scanned
# separately so each walks its partial index in ORDER BY order and stops at
# :lim — a single OR'd scan forces a sort of the whole eligible backlog.
# Merging the per-arm top-:lim and re-limiting selects exactly the same rows
# as the single ordered scan; the only difference is that up to :lim
# additional candidate rows from the losing arm stay row-locked until the
# (short) claim transaction commits.

CLAIM_SQL = text("""
WITH pending AS (
  SELECT id, priority, enqueued_at
  FROM horsies_tasks
  WHERE queue_name = :queue
    AND status = 'PENDING'
    AND enqueued_at <= now()
    AND (next_retry_at IS NULL OR next_retry_at <= now())
    AND (good_until IS NULL OR good_until > now())
  ORDER BY priority ASC, enqueued_at ASC, id ASC
  FOR UPDATE SKIP LOCKED
  LIMIT :lim
),
expired AS (
  SELECT id, priority, enqueued_at
  FROM horsies_tasks
  WHERE queue_name = :queue
    AND status = 'CLAIMED' AND claim_expires_at IS NOT NULL AND claim_expires_at < now()
    AND enqueued_at <= now()
    AND (next_retry_at IS NULL OR next_retry_at <= now())
    AND (good_until IS NULL OR good_until > now())
  ORDER BY priority ASC, enqueued_at ASC, id ASC
  FOR UPDATE SKIP LOCKED
  LIMIT :lim
),
next AS (
  SELECT id FROM (
    SELECT * FROM pending
    UNION ALL
    SELECT * FROM expired
  ) candidates
  ORDER BY priority ASC, enqueued_at ASC, id ASC
  LIMIT :lim
)
UPDATE horsies_tasks t
SET status = 'CLAIMED',
    claimed = TRUE,
    claimed_at = now(),
    claimed_by_worker_id = :worker_id,
    claim_expires_at = now() + :claim_lease_ms * INTERVAL '1 millisecond',
    finalizing_at = NULL,
    finalizing_by_worker_id = NULL,
    updated_at = now()
FROM next
WHERE t.id = next.id
RETURNING t.id, t.task_name, t.args, t.kwargs, t.queue_name, t.is_workflow_task, t.task_options, t.claimed_at;
""")


# ---------- Worker SQL constants ----------

# TIER 1: one server-side call performs lock acquisition + cap counts + the
# windowed claim (see horsies_claim, schemas/migrations.py). The advisory lock
# is held only across this one statement plus the caller's COMMIT — never across
# a client round trip. Replaces the per-pass lock loop + counts + per-queue
# CLAIM_SQL loop.
HORSIES_CLAIM_SQL = text("""
    SELECT id, task_name, args, kwargs, queue_name, is_workflow_task, task_options,
           claimed_at
    FROM horsies_claim(
        :p_worker_id,
        CAST(:p_queues AS jsonb),
        CAST(:p_queue_priority AS jsonb),
        CAST(:p_queue_max_concurrency AS jsonb),
        :p_hard_cap_mode,
        :p_processes,
        :p_prefetch_buffer,
        :p_max_claim_per_worker,
        :p_max_claim_batch,
        :p_cluster_wide_cap,
        :p_lease_ms,
        CAST(:p_lock_keys AS jsonb)
    )
""")

CLAIM_ADVISORY_LOCK_SQL = text("""
    SELECT pg_advisory_xact_lock(CAST(:key AS BIGINT))
""")

# Non-blocking gate for the reaper: every worker runs a reaper loop, but
# only one needs to execute the cluster-wide scans per interval.
REAPER_GATE_TRY_LOCK_SQL = text("""
    SELECT pg_try_advisory_xact_lock(CAST(:key AS BIGINT))
""")

# Effective in-flight counts: expired CLAIMED tasks are excluded because they
# are reclaimable and must not consume cap budget.  The predicate for an
# "active" claim is: claim_expires_at IS NULL OR claim_expires_at > now().

# One round trip for ALL claim-pass cap accounting (replaces up to
# 2 + Q + 1 sequential count statements under the claim advisory locks;
# see the RTT hot-path audit). The in_flight CTE is the shared predicate
# of every original count (RUNNING, or CLAIMED with an unexpired lease);
# each column mirrors one original statement exactly:
#   my_claimed       = COUNT_CLAIMED_FOR_WORKER_SQL
#   my_running       = COUNT_RUNNING_FOR_WORKER_SQL
#   my_in_flight     = COUNT_IN_FLIGHT_FOR_WORKER_SQL
#   global_in_flight = COUNT_GLOBAL_IN_FLIGHT_SQL
#   queue_hard_counts[q] = COUNT_QUEUE_IN_FLIGHT_HARD_SQL per capped queue
#   queue_soft_counts[q] = COUNT_QUEUE_IN_FLIGHT_SOFT_SQL per capped queue
# One deliberate improvement: all counts share a single now() instant
# instead of one per statement. Empty :capped_queues -> NULL aggs -> {}.
# The single-purpose constants below remain for health snapshots.
CLAIM_PASS_COUNTS_SQL = text("""
    WITH in_flight AS (
        SELECT queue_name, status, claimed_by_worker_id
        FROM horsies_tasks
        WHERE status = 'RUNNING'
           OR (status = 'CLAIMED'
               AND (claim_expires_at IS NULL OR claim_expires_at > now()))
    )
    SELECT
        COUNT(*) FILTER (
            WHERE claimed_by_worker_id = CAST(:wid AS VARCHAR)
              AND status = 'CLAIMED'
        ) AS my_claimed,
        COUNT(*) FILTER (
            WHERE claimed_by_worker_id = CAST(:wid AS VARCHAR)
              AND status = 'RUNNING'
        ) AS my_running,
        COUNT(*) FILTER (
            WHERE claimed_by_worker_id = CAST(:wid AS VARCHAR)
        ) AS my_in_flight,
        COUNT(*) AS global_in_flight,
        (SELECT jsonb_object_agg(g.queue_name, g.cnt)
         FROM (SELECT queue_name, COUNT(*) AS cnt
               FROM in_flight
               WHERE queue_name = ANY(CAST(:capped_queues AS VARCHAR[]))
               GROUP BY queue_name) g) AS queue_hard_counts,
        (SELECT jsonb_object_agg(g.queue_name, g.cnt)
         FROM (SELECT queue_name, COUNT(*) AS cnt
               FROM in_flight
               WHERE queue_name = ANY(CAST(:capped_queues AS VARCHAR[]))
                 AND status = 'RUNNING'
               GROUP BY queue_name) g) AS queue_soft_counts
    FROM in_flight
""")

COUNT_GLOBAL_IN_FLIGHT_SQL = text("""
    SELECT COUNT(*) AS cnt FROM horsies_tasks
    WHERE status = 'RUNNING'
       OR (status = 'CLAIMED'
           AND (claim_expires_at IS NULL OR claim_expires_at > now()))
""")

COUNT_QUEUE_IN_FLIGHT_HARD_SQL = text("""
    SELECT COUNT(*) AS cnt FROM horsies_tasks
    WHERE queue_name = :q
      AND (status = 'RUNNING'
           OR (status = 'CLAIMED'
               AND (claim_expires_at IS NULL OR claim_expires_at > now())))
""")

COUNT_QUEUE_IN_FLIGHT_SOFT_SQL = text("""
    SELECT COUNT(*) AS cnt FROM horsies_tasks WHERE status = 'RUNNING' AND queue_name = :q
""")

COUNT_CLAIMED_FOR_WORKER_SQL = text("""
    SELECT COUNT(*) AS cnt
    FROM horsies_tasks
    WHERE claimed_by_worker_id = CAST(:wid AS VARCHAR)
      AND status = 'CLAIMED'
      AND (claim_expires_at IS NULL OR claim_expires_at > now())
""")

COUNT_RUNNING_FOR_WORKER_SQL = text("""
    SELECT COUNT(*) AS cnt
    FROM horsies_tasks
    WHERE claimed_by_worker_id = CAST(:wid AS VARCHAR)
      AND status = 'RUNNING'
""")

COUNT_IN_FLIGHT_FOR_WORKER_SQL = text("""
    SELECT COUNT(*) AS cnt
    FROM horsies_tasks
    WHERE claimed_by_worker_id = CAST(:wid AS VARCHAR)
      AND (status = 'RUNNING'
           OR (status = 'CLAIMED'
               AND (claim_expires_at IS NULL OR claim_expires_at > now())))
""")

GET_NONRUNNABLE_WORKFLOW_TASK_IDS_SQL = text("""
    SELECT t.id, w.status
    FROM horsies_tasks t
    JOIN horsies_workflow_tasks wt ON wt.task_id = t.id
    JOIN horsies_workflows w ON w.id = wt.workflow_id
    WHERE t.id = ANY(:ids)
      AND w.status IN ('PAUSED', 'CANCELLED')
""")

# Claim-generation fence (C10), pairwise: this guard runs in a transaction of
# its own after the claim committed, so a lease can lapse, the reaper can
# requeue, and this same worker can re-claim before the abandon lands. The
# (status, worker) pair cannot reject that — the row is CLAIMED by this worker
# again. Tasks in one batch may come from different claim transactions, so the
# generation travels alongside each id rather than as one scalar. A NULL entry
# disables the fence for that task, matching UNCLAIM_CLAIMED_TASK_SQL, so a
# claim path that does not report claimed_at still abandons rather than skips.
UNCLAIM_PAUSED_TASKS_SQL = text("""
    UPDATE horsies_tasks t
    SET status = 'CANCELLED',
        claimed = FALSE,
        claimed_at = NULL,
        claimed_by_worker_id = NULL,
        claim_expires_at = NULL,
        finalizing_at = NULL,
        finalizing_by_worker_id = NULL,
        error_code = 'TASK_CANCELLED',
        failed_reason = 'Workflow paused before task start',
        terminal_at = NOW(),
        updated_at = NOW()
    FROM unnest(
        CAST(:ids AS VARCHAR[]),
        CAST(:claimed_ats AS TIMESTAMPTZ[])
    ) AS g(id, claimed_at)
    WHERE t.id = g.id
      AND t.status = 'CLAIMED'
      AND t.claimed_by_worker_id = CAST(:wid AS VARCHAR)
      AND (g.claimed_at IS NULL OR t.claimed_at = g.claimed_at)
    RETURNING t.id
""")

# The optional :claimed_at fence scopes the release to a specific claim
# generation (C10); NULL disables it. Callers inside a transaction that
# already fence-locked the row via SELECT_WORKER_OWNED_IN_FLIGHT_FOR_UPDATE_SQL
# pass the same value; the standalone requeue path passes its dispatch's
# generation so it cannot release a re-claimed row.
UNCLAIM_CLAIMED_TASK_SQL = text("""
    UPDATE horsies_tasks
    SET status = 'PENDING',
        claimed = FALSE,
        claimed_at = NULL,
        claimed_by_worker_id = NULL,
        claim_expires_at = NULL,
        started_at = NULL,
        worker_pid = NULL,
        worker_hostname = NULL,
        worker_process_name = NULL,
        finalizing_at = NULL,
        finalizing_by_worker_id = NULL,
        updated_at = NOW()
    WHERE id = :id
      AND status = 'CLAIMED'
      AND claimed_by_worker_id = CAST(:wid AS VARCHAR)
      AND (CAST(:claimed_at AS TIMESTAMPTZ) IS NULL
           OR claimed_at = CAST(:claimed_at AS TIMESTAMPTZ))
""")

RESET_PAUSED_WORKFLOW_TASKS_SQL = text("""
    UPDATE horsies_workflow_tasks
    SET status = 'READY', task_id = NULL, started_at = NULL
    WHERE task_id = ANY(:ids)
      AND status IN ('ENQUEUED', 'RUNNING')
""")

# Same pairwise claim-generation fence as UNCLAIM_PAUSED_TASKS_SQL; both run
# from the post-claim guard and share its staleness window.
CANCEL_CANCELLED_WORKFLOW_TASKS_SQL = text("""
    UPDATE horsies_tasks t
    SET status = 'CANCELLED',
        claimed = FALSE,
        claimed_at = NULL,
        claimed_by_worker_id = NULL,
        claim_expires_at = NULL,
        finalizing_at = NULL,
        finalizing_by_worker_id = NULL,
        terminal_at = NOW(),
        updated_at = NOW()
    FROM unnest(
        CAST(:ids AS VARCHAR[]),
        CAST(:claimed_ats AS TIMESTAMPTZ[])
    ) AS g(id, claimed_at)
    WHERE t.id = g.id
      AND t.status = 'CLAIMED'
      AND t.claimed_by_worker_id = CAST(:wid AS VARCHAR)
      AND (g.claimed_at IS NULL OR t.claimed_at = g.claimed_at)
    RETURNING t.id
""")

SKIP_CANCELLED_WORKFLOW_TASKS_SQL = text("""
    UPDATE horsies_workflow_tasks
    SET status = 'SKIPPED',
        completed_at = NOW()
    WHERE task_id = ANY(:ids)
      AND status IN ('PENDING', 'READY', 'ENQUEUED')
""")

MARK_TASK_FAILED_WORKER_SQL = text("""
    UPDATE horsies_tasks
    SET status='FAILED',
        failed_at = NOW(),
        failed_reason = :reason,
        result = :result_json,
        error_code = :error_code,
        finalizing_at = NULL,
        finalizing_by_worker_id = NULL,
        terminal_at = NOW(),
        updated_at = NOW()
    WHERE id = :id
      AND status = 'RUNNING'
      AND claimed_by_worker_id = CAST(:wid AS VARCHAR)
    RETURNING id
""")

MARK_TASK_FAILED_SQL = text("""
    UPDATE horsies_tasks
    SET status='FAILED',
        failed_at = NOW(),
        result = :result_json,
        error_code = :error_code,
        finalizing_at = NULL,
        finalizing_by_worker_id = NULL,
        terminal_at = NOW(),
        updated_at = NOW()
    WHERE id = :id
      AND status = 'RUNNING'
      AND claimed_by_worker_id = CAST(:wid AS VARCHAR)
    RETURNING id
""")

MARK_TASK_COMPLETED_SQL = text("""
    UPDATE horsies_tasks
    SET status='COMPLETED',
        completed_at = NOW(),
        result = :result_json,
        error_code = NULL,
        finalizing_at = NULL,
        finalizing_by_worker_id = NULL,
        terminal_at = NOW(),
        updated_at = NOW()
    WHERE id = :id
      AND status = 'RUNNING'
      AND claimed_by_worker_id = CAST(:wid AS VARCHAR)
    RETURNING id
""")

NOTIFY_TASK_QUEUE_SQL = text("""
    SELECT pg_notify(:c2, :p)
""")

# Fused ok-path finalization for plain (non-workflow) tasks: lock the
# RUNNING row, upsert the COMPLETED attempt from the locked row's own
# context, CAS the task to COMPLETED, and fire the capacity wake — one
# statement, one transaction. Empty result = status/ownership changed
# (reaper reclaim or re-claim); nothing is written in that case.
#
# Claim-generation fence (C10): a (status, worker_id) fence alone cannot
# reject a stale finalize when the SAME worker re-claimed its own
# reaper-requeued task — worker_id matches and the row is RUNNING again.
# :claimed_at scopes the CAS to the claim generation the dispatch was born
# from (set by the claim, cleared by every requeue); NULL disables the
# fence for callers without a dispatch context. The same fence guards
# SELECT_RUNNING_TASK_CONTEXT_FOR_UPDATE_SQL,
# SELECT_WORKER_OWNED_IN_FLIGHT_FOR_UPDATE_SQL, UNCLAIM_CLAIMED_TASK_SQL,
# TERMINATE_ORPHANED_WORKFLOW_TASK_SQL, and the child's CLAIMED->RUNNING
# ownership confirm (child_runner._confirm_ownership_and_set_running) —
# every statement that acts on a row this worker believes it owns.
FINALIZE_TASK_COMPLETED_SQL = text("""
    WITH ctx AS (
        SELECT id, retry_count, started_at, claimed_by_worker_id,
               worker_hostname, worker_pid, worker_process_name,
               clock_timestamp() AS db_now
        FROM horsies_tasks
        WHERE id = :id
          AND status = 'RUNNING'
          AND claimed_by_worker_id = CAST(:wid AS VARCHAR)
          AND (CAST(:claimed_at AS TIMESTAMPTZ) IS NULL
               OR claimed_at = CAST(:claimed_at AS TIMESTAMPTZ))
        FOR UPDATE
    ),
    attempt AS (
        INSERT INTO horsies_task_attempts (
            task_id, attempt, outcome, will_retry,
            started_at, finished_at,
            error_code, error_message, failed_reason,
            worker_id, worker_hostname, worker_pid, worker_process_name
        )
        SELECT ctx.id, COALESCE(ctx.retry_count, 0) + 1, 'COMPLETED', FALSE,
               COALESCE(ctx.started_at, ctx.db_now), ctx.db_now,
               NULL, NULL, NULL,
               ctx.claimed_by_worker_id, ctx.worker_hostname, ctx.worker_pid,
               ctx.worker_process_name
        FROM ctx
        ON CONFLICT (task_id, attempt) DO UPDATE SET
            outcome = EXCLUDED.outcome,
            will_retry = EXCLUDED.will_retry,
            started_at = EXCLUDED.started_at,
            finished_at = EXCLUDED.finished_at,
            error_code = EXCLUDED.error_code,
            error_message = EXCLUDED.error_message,
            failed_reason = EXCLUDED.failed_reason,
            worker_id = EXCLUDED.worker_id,
            worker_hostname = EXCLUDED.worker_hostname,
            worker_pid = EXCLUDED.worker_pid,
            worker_process_name = EXCLUDED.worker_process_name
    ),
    upd AS (
        UPDATE horsies_tasks t
        SET status = 'COMPLETED',
            completed_at = NOW(),
            result = :result_json,
            error_code = NULL,
            finalizing_at = NULL,
            finalizing_by_worker_id = NULL,
            terminal_at = NOW(),
            updated_at = NOW()
        FROM ctx
        WHERE t.id = ctx.id
        RETURNING t.id
    )
    SELECT upd.id, pg_notify(:notify_channel, :notify_payload)
    FROM upd
""")

GET_TASK_RETRY_INFO_SQL = text("""
    SELECT retry_count, max_retries, task_options, good_until, clock_timestamp() AS db_now
    FROM horsies_tasks
    WHERE id = :id
""")

GET_TASK_RETRY_CONFIG_SQL = text("""
    SELECT retry_count, task_options, good_until, clock_timestamp() AS db_now
    FROM horsies_tasks
    WHERE id = :id
""")

GET_TASK_RETRY_POSTCHECK_SQL = text("""
    SELECT status, good_until
    FROM horsies_tasks
    WHERE id = :id
""")

SELECT_WORKER_OWNED_IN_FLIGHT_FOR_UPDATE_SQL = text("""
    SELECT id, status, retry_count, started_at, claimed_by_worker_id,
           worker_hostname, worker_pid, worker_process_name,
           queue_name, is_workflow_task, max_retries, task_options, good_until,
           task_name,
           clock_timestamp() AS db_now
    FROM horsies_tasks
    WHERE id = :id
      AND claimed_by_worker_id = CAST(:wid AS VARCHAR)
      AND status IN ('CLAIMED', 'RUNNING')
      AND (CAST(:claimed_at AS TIMESTAMPTZ) IS NULL
           OR claimed_at = CAST(:claimed_at AS TIMESTAMPTZ))
    FOR UPDATE
""")

SCHEDULE_TASK_RETRY_SQL = text("""
    UPDATE horsies_tasks
    SET status = 'PENDING',
        retry_count = :retry_count,
        next_retry_at = :next_retry_at,
        enqueued_at = :next_retry_at,
        claimed = FALSE,
        claimed_at = NULL,
        claimed_by_worker_id = NULL,
        claim_expires_at = NULL,
        started_at = NULL,
        worker_pid = NULL,
        worker_hostname = NULL,
        worker_process_name = NULL,
        finalizing_at = NULL,
        finalizing_by_worker_id = NULL,
        error_code = NULL,
        updated_at = now()
    WHERE id = :id
      AND status = 'RUNNING'
      AND claimed_by_worker_id = CAST(:wid AS VARCHAR)
      AND (good_until IS NULL OR :next_retry_at < good_until)
    RETURNING id
""")

SCHEDULE_STALE_TASK_RETRY_SQL = text("""
    UPDATE horsies_tasks AS t
    SET status = 'PENDING',
        retry_count = :retry_count,
        next_retry_at = :next_retry_at,
        enqueued_at = :next_retry_at,
        claimed = FALSE,
        claimed_at = NULL,
        claimed_by_worker_id = NULL,
        claim_expires_at = NULL,
        started_at = NULL,
        worker_pid = NULL,
        worker_hostname = NULL,
        worker_process_name = NULL,
        finalizing_at = NULL,
        finalizing_by_worker_id = NULL,
        error_code = NULL,
        updated_at = now()
    WHERE t.id = :id
      AND t.status = 'RUNNING'
      AND t.started_at IS NOT NULL
      AND (
          t.finalizing_at IS NULL
          OR t.finalizing_at < NOW() - CAST(:finalizing_stale_threshold || ' seconds' AS INTERVAL)
      )
      AND COALESCE(
          (
              SELECT h.sent_at
              FROM horsies_heartbeats h
              WHERE h.task_id = t.id AND h.role = 'runner'
              ORDER BY h.sent_at DESC
              LIMIT 1
          ),
          t.started_at
      ) < NOW() - CAST(:stale_threshold || ' seconds' AS INTERVAL)
      AND (t.good_until IS NULL OR :next_retry_at < t.good_until)
    RETURNING t.id
""")

NOTIFY_DELAYED_SQL = text("""
    SELECT pg_notify(:channel, :payload)
""")

INSERT_CLAIMER_HEARTBEAT_SQL = text("""
    INSERT INTO horsies_heartbeats (task_id, sender_id, role, sent_at, hostname, pid)
    SELECT id, CAST(:wid AS VARCHAR), 'claimer', NOW(), :host, :pid
    FROM horsies_tasks
    WHERE status = 'CLAIMED' AND claimed_by_worker_id = CAST(:wid AS VARCHAR)
""")

RENEW_CLAIM_LEASE_SQL = text("""
    UPDATE horsies_tasks
    SET claim_expires_at = NOW() + :claim_lease_ms * INTERVAL '1 millisecond',
        updated_at = NOW()
    WHERE status = 'CLAIMED'
      AND claimed_by_worker_id = CAST(:wid AS VARCHAR)
      AND claimed_at >= NOW() - :max_claim_age_ms * INTERVAL '1 millisecond'
""")

INSERT_WORKER_STATE_SQL = text("""
    INSERT INTO horsies_worker_states (
        worker_id, snapshot_at, hostname, pid,
        processes, max_claim_batch, max_claim_per_worker,
        cluster_wide_cap, queues, queue_priorities, queue_max_concurrency,
        recovery_config, tasks_running, tasks_claimed,
        memory_usage_mb, memory_percent, cpu_percent, children_memory_mb,
        worker_started_at
    )
    VALUES (
        :wid, NOW(), :host, :pid, :procs, :mcb, :mcpw, :cwc,
        :queues, :qp, :qmc, :recovery, :running, :claimed,
        :mem_mb, :mem_pct, :cpu_pct, :children_mem_mb, :started
    )
""")

# Retention deletes run in bounded batches: each statement deletes at most
# :batch_size rows selected by an id-subselect, and the reaper commits per
# batch. An unbounded DELETE turns the first pass over a large eligible
# backlog (retention newly enabled, or the window lowered) into one
# multi-minute transaction — WAL burst, task_attempts cascades, and a
# mid-pass failure rolling back everything. FOR UPDATE SKIP LOCKED lets
# concurrent ungated reaper passes drain disjoint batches instead of
# blocking. Terminal statuses are inlined as literals rather than bound
# arrays: the partial retention index is only usable while the planner can
# prove the status predicate implies the index predicate, which a bound
# array under a generic plan cannot.

DELETE_EXPIRED_HEARTBEATS_SQL = text("""
    DELETE FROM horsies_heartbeats
    WHERE id IN (
        SELECT id FROM horsies_heartbeats
        WHERE sent_at < NOW() - CAST(:retention_hours || ' hours' AS INTERVAL)
        LIMIT :batch_size
        FOR UPDATE SKIP LOCKED
    )
""")

DELETE_EXPIRED_WORKER_STATES_SQL = text("""
    DELETE FROM horsies_worker_states
    WHERE id IN (
        SELECT id FROM horsies_worker_states
        WHERE snapshot_at < NOW() - CAST(:retention_hours || ' hours' AS INTERVAL)
        LIMIT :batch_size
        FOR UPDATE SKIP LOCKED
    )
""")

# Workflow retention batches by WORKFLOW, not by node row. The previous
# shape deleted workflow_tasks rows in :batch_size slices with the
# all-backing-tasks-terminal guard re-evaluated for every candidate on
# every batch; at 972k tasks / ~36k doomed workflows that walk read
# ~686k rows per 500-row batch (~1,900 reads per deleted row, 5s per
# statement). Selecting doomed workflows once, guarding each exactly once,
# and purging its node rows set-wise does the same work in
# nodes-per-workflow reads.
#
# doomed: candidate workflows via idx_horsies_workflows_retention, capped
#   at :batch_size candidates so guard + node-count work per statement is
#   bounded even when the guard rejects most of them.
# budgeted: keeps candidates while their running node total stays within
#   :batch_size (the knob keeps its documented meaning — rows per
#   statement) and ALWAYS keeps the first candidate, so a workflow with
#   more nodes than the whole budget still drains — alone in its own
#   statement, proportional to its real size — instead of starving.
# purged_nodes: set-wise node purge (the R1 task_attempts pattern); the
#   workflow_id FK cascade still fires per parent row but finds nothing,
#   and remains the correctness net for non-retention deletes.
#
# The top-level DELETE's rowcount counts WORKFLOWS, which under the node
# budget is routinely smaller than :batch_size while backlog remains —
# the reaper therefore drives this statement with stop_on_empty_batch
# rather than the short-batch heuristic the row-batched statements use.
DELETE_EXPIRED_WORKFLOWS_SQL = text(f"""
    WITH live AS MATERIALIZED (
        -- Workflows that still have a non-terminal backing task, computed
        -- ONCE per statement from the non-terminal side: in-flight work is
        -- small by definition and probes ix_horsies_tasks_status, where the
        -- per-candidate "NOT terminal" walk scanned the tasks heap per
        -- batch. Retention keeps these workflows (and their node rows) so
        -- it never strands a live task row; orphan self-heal makes every
        -- backing task terminal eventually, so this only defers cleanup.
        SELECT DISTINCT wt.workflow_id
        FROM horsies_tasks t
        JOIN horsies_workflow_tasks wt ON wt.task_id = t.id
        WHERE t.status IN ({TASK_NONTERMINAL_STATUS_SQL_LITERALS})
    ),
    doomed AS (
        SELECT w.id,
               (SELECT count(*)
                FROM horsies_workflow_tasks wt
                WHERE wt.workflow_id = w.id) AS node_count
        FROM horsies_workflows w
        WHERE w.status IN ({WORKFLOW_TERMINAL_STATUS_SQL_LITERALS})
          AND COALESCE(w.completed_at, w.updated_at, w.created_at) < NOW() - CAST(:retention_hours || ' hours' AS INTERVAL)
          AND NOT EXISTS (
              SELECT 1 FROM live WHERE live.workflow_id = w.id
          )
        LIMIT :batch_size
        FOR UPDATE SKIP LOCKED
    ),
    budgeted AS (
        SELECT id
        FROM (
            SELECT id,
                   SUM(node_count) OVER (ORDER BY id) AS nodes_running,
                   ROW_NUMBER() OVER (ORDER BY id) AS position
            FROM doomed
        ) ranked
        WHERE nodes_running <= :batch_size OR position = 1
    ),
    purged_nodes AS (
        DELETE FROM horsies_workflow_tasks
        WHERE workflow_id IN (SELECT id FROM budgeted)
    )
    DELETE FROM horsies_workflows
    WHERE id IN (SELECT id FROM budgeted)
""")

# The status + COALESCE predicates must stay textually aligned with
# idx_horsies_tasks_retention (schemas/indexes.py).
#
# task_attempts are purged set-wise in the purged_attempts CTE rather than
# left to the FK ON DELETE CASCADE: RI triggers are row-level, so the
# cascade issues one child DELETE per doomed task inside this statement's
# transaction — costlier in aggregate than the parent delete itself. The
# CTE removes the whole child set in one indexed statement; the cascade
# trigger still fires per parent row but finds nothing, and remains the
# correctness net for non-retention deletes. doomed is referenced twice,
# so Postgres materializes the candidate scan once.
# :excluded_queues carries the queues with a per-queue override window
# (queue_terminal_record_retention_hours); an empty array excludes
# nothing. The exclusion shields PLAIN tasks only: workflow-backing rows
# (is_workflow_task = TRUE) age under the global window even on override
# queues, because the per-queue statement filters them out — an
# unconditional exclusion would leave them unreachable by both statements
# and retained forever. The exclusion is a heap filter on the
# already-bounded candidate scan, so the v11 index plan is unchanged.
DELETE_EXPIRED_TASKS_SQL = text(f"""
    WITH doomed AS (
        SELECT t.id
        FROM horsies_tasks t
        WHERE t.status IN ({TASK_TERMINAL_STATUS_SQL_LITERALS})
          AND COALESCE(t.completed_at, t.failed_at, t.updated_at, t.created_at) < NOW() - CAST(:retention_hours || ' hours' AS INTERVAL)
          AND NOT (t.queue_name = ANY(CAST(:excluded_queues AS text[]))
                   AND t.is_workflow_task = FALSE)
          AND NOT EXISTS (
              SELECT 1
              FROM horsies_workflow_tasks wt
              JOIN horsies_workflows w ON w.id = wt.workflow_id
              WHERE wt.task_id = t.id
                AND w.status NOT IN ({WORKFLOW_TERMINAL_STATUS_SQL_LITERALS})
          )
        LIMIT :batch_size
        FOR UPDATE OF t SKIP LOCKED
    ),
    purged_attempts AS (
        DELETE FROM horsies_task_attempts
        WHERE task_id IN (SELECT id FROM doomed)
    )
    DELETE FROM horsies_tasks
    WHERE id IN (SELECT id FROM doomed)
""")

# Per-queue override delete (queue_terminal_record_retention_hours). The
# queue_name equality + COALESCE predicates must stay textually aligned
# with idx_horsies_tasks_queue_retention (schemas/indexes.py) — the v11
# expression index cannot serve an override window efficiently because
# the override cutoff is far more recent than the global one, making
# every other queue's retained terminal rows heap-filter misses.
# Scoped to plain tasks (is_workflow_task = FALSE): workflow-backing rows
# age under the global window so a workflow and its task rows are
# retained as a unit. The NOT EXISTS guard is kept as defense in depth
# (plain tasks have no workflow_task linkage). Same purged_attempts
# mechanism as the global delete.
DELETE_EXPIRED_TASKS_FOR_QUEUE_SQL = text(f"""
    WITH doomed AS (
        SELECT t.id
        FROM horsies_tasks t
        WHERE t.queue_name = :queue_name
          AND t.status IN ({TASK_TERMINAL_STATUS_SQL_LITERALS})
          AND COALESCE(t.completed_at, t.failed_at, t.updated_at, t.created_at) < NOW() - CAST(:retention_hours || ' hours' AS INTERVAL)
          AND t.is_workflow_task = FALSE
          AND NOT EXISTS (
              SELECT 1
              FROM horsies_workflow_tasks wt
              JOIN horsies_workflows w ON w.id = wt.workflow_id
              WHERE wt.task_id = t.id
                AND w.status NOT IN ({WORKFLOW_TERMINAL_STATUS_SQL_LITERALS})
          )
        LIMIT :batch_size
        FOR UPDATE OF t SKIP LOCKED
    ),
    purged_attempts AS (
        DELETE FROM horsies_task_attempts
        WHERE task_id IN (SELECT id FROM doomed)
    )
    DELETE FROM horsies_tasks
    WHERE id IN (SELECT id FROM doomed)
""")

# Terminate a single orphaned workflow task this worker still holds CLAIMED.
# An orphan is a workflow task with no workflow_task row in a runnable status
# (linkage missing or already terminal) — the exact condition that makes the
# child's workflow_task->RUNNING transition fail with WORKFLOW_CHECK_FAILED.
# Scoped to this worker's own claim and re-checked here so a concurrent
# legitimate transition (TOCTOU) leaves the row untouched (0 rows -> caller
# keeps its prior skip behavior).
TERMINATE_ORPHANED_WORKFLOW_TASK_SQL = text("""
    UPDATE horsies_tasks
    SET status = 'CANCELLED',
        claimed = FALSE,
        claimed_at = NULL,
        claimed_by_worker_id = NULL,
        claim_expires_at = NULL,
        finalizing_at = NULL,
        finalizing_by_worker_id = NULL,
        error_code = 'WORKFLOW_CHECK_FAILED',
        failed_reason = 'Workflow task orphaned: no live workflow_task linkage',
        terminal_at = NOW(),
        updated_at = NOW()
    WHERE id = :id
      AND claimed_by_worker_id = :wid
      AND status = 'CLAIMED'
      AND (CAST(:claimed_at AS TIMESTAMPTZ) IS NULL
           OR claimed_at = CAST(:claimed_at AS TIMESTAMPTZ))
      AND is_workflow_task = TRUE
      AND NOT EXISTS (
          SELECT 1 FROM horsies_workflow_tasks wt
          WHERE wt.task_id = horsies_tasks.id
            AND wt.status IN ('ENQUEUED', 'READY', 'PENDING', 'RUNNING')
      )
    RETURNING id
""")


SELECT_RUNNING_TASK_CONTEXT_FOR_UPDATE_SQL = text("""
    SELECT task_name, retry_count, started_at, claimed_by_worker_id,
           worker_hostname, worker_pid, worker_process_name,
           queue_name, is_workflow_task, clock_timestamp() AS db_now
    FROM horsies_tasks
    WHERE id = :id
      AND status = 'RUNNING'
      AND claimed_by_worker_id = CAST(:wid AS VARCHAR)
      AND (CAST(:claimed_at AS TIMESTAMPTZ) IS NULL
           OR claimed_at = CAST(:claimed_at AS TIMESTAMPTZ))
    FOR UPDATE
""")

UPSERT_TASK_ATTEMPT_SQL = text("""
    INSERT INTO horsies_task_attempts (
        task_id, attempt, outcome, will_retry,
        started_at, finished_at,
        error_code, error_message, failed_reason,
        worker_id, worker_hostname, worker_pid, worker_process_name
    )
    VALUES (
        :task_id, :attempt, :outcome, :will_retry,
        :started_at, :finished_at,
        :error_code, :error_message, :failed_reason,
        :worker_id, :worker_hostname, :worker_pid, :worker_process_name
    )
    ON CONFLICT (task_id, attempt) DO UPDATE SET
        outcome = EXCLUDED.outcome,
        will_retry = EXCLUDED.will_retry,
        started_at = EXCLUDED.started_at,
        finished_at = EXCLUDED.finished_at,
        error_code = EXCLUDED.error_code,
        error_message = EXCLUDED.error_message,
        failed_reason = EXCLUDED.failed_reason,
        worker_id = EXCLUDED.worker_id,
        worker_hostname = EXCLUDED.worker_hostname,
        worker_pid = EXCLUDED.worker_pid,
        worker_process_name = EXCLUDED.worker_process_name
""")

# Sweep cadence and batch size live on RecoveryConfig
# (retention_sweep_interval_s, retention_delete_batch_size).

# Wall-clock budget for one retention pass across all five statements. A
# backlog that does not drain within the budget resumes on the next pass;
# every statement still runs at least one batch per pass so a deep backlog
# in an earlier table cannot starve the later ones indefinitely.
_RETENTION_PASS_TIME_BUDGET_S = 60.0


_FINALIZER_DRAIN_TIMEOUT_S: float = 30.0
