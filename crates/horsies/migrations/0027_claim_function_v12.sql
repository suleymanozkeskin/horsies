-- Align horsies_claim's return type with the Python implementation's schema
-- v12: OUT columns become exactly
--   (id, task_name, args, kwargs, queue_name, is_workflow_task,
--    task_options, claimed_at)
-- so both stacks can run one database — either side's ensure-schema now
-- (re)creates the identical function. A return-type change requires
-- DROP + CREATE (CREATE OR REPLACE cannot change OUT columns); the DROP pins
-- the exact 0024 signature so it also applies cleanly over a Python-managed
-- database.
--
-- claimed_at is the claim-generation fence (C10): set by the claim, cleared
-- by every requeue, so a statement fenced on it can never act on a row that
-- expired and was re-claimed — including by the SAME worker, where the
-- (status, worker_id) CAS alone cannot tell the generations apart. Unlike
-- started_at, it exists while the row is still CLAIMED, so buffered-claimed
-- recovery/cancel logic can fence on it too.
--
-- 0024's extra OUT columns (retry_count, max_retries, good_until) are gone:
-- the dispatch path now reads them from SET_RUNNING_SQL's RETURNING — the
-- statement that locks the row for the CLAIMED -> RUNNING flip — which is
-- Rust-owned SQL with no interop surface.
--
-- Body change vs 0024: claim_expires_at drops the explicit NULL-lease CASE
-- for the v12 expression `now() + p_lease_ms * INTERVAL '1 millisecond'`;
-- a NULL p_lease_ms NULL-propagates to the same NULL expiry.
DROP FUNCTION IF EXISTS horsies_claim(
    text, jsonb, jsonb, jsonb, boolean, int, int, int, int, int, bigint, jsonb
);

CREATE OR REPLACE FUNCTION horsies_claim(
    p_worker_id text,
    p_queues jsonb,
    p_queue_priority jsonb,
    p_queue_max_concurrency jsonb,
    p_hard_cap_mode boolean,
    p_processes int,
    p_prefetch_buffer int,
    p_max_claim_per_worker int,
    p_max_claim_batch int,
    p_cluster_wide_cap int,
    p_lease_ms bigint,
    p_lock_keys jsonb
)
RETURNS TABLE(
    id varchar,
    task_name varchar,
    args text,
    kwargs text,
    queue_name varchar,
    is_workflow_task boolean,
    task_options text,
    claimed_at timestamptz
)
LANGUAGE plpgsql
AS $func$
#variable_conflict use_column
DECLARE
    v_key bigint;
    v_capped text[];
    v_my_claimed int;
    v_my_running int;
    v_my_in_flight int;
    v_global_in_flight int;
    v_queue_counts jsonb;
    v_max_claimed int;
    v_local_available int;
    v_global_remaining int;
    v_total int;
BEGIN
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

    -- 2. Cap accounting under the lock snapshot.
    --    Per-queue counts: hard mode counts RUNNING+CLAIMED, soft counts RUNNING.
    WITH in_flight AS (
        SELECT queue_name, status, claimed_by_worker_id
        FROM horsies_tasks
        WHERE status = 'RUNNING'
           OR (status = 'CLAIMED'
               AND (claim_expires_at IS NULL OR claim_expires_at > now()))
    )
    SELECT
        COUNT(*) FILTER (
            WHERE claimed_by_worker_id = p_worker_id AND status = 'CLAIMED'),
        COUNT(*) FILTER (
            WHERE claimed_by_worker_id = p_worker_id AND status = 'RUNNING'),
        COUNT(*) FILTER (WHERE claimed_by_worker_id = p_worker_id),
        COUNT(*),
        COALESCE((
            SELECT jsonb_object_agg(g.queue_name, g.cnt) FROM (
                SELECT queue_name, COUNT(*) AS cnt
                FROM in_flight
                WHERE queue_name = ANY(v_capped)
                  AND (p_hard_cap_mode OR status = 'RUNNING')
                GROUP BY queue_name
            ) g
        ), '{}'::jsonb)
    INTO v_my_claimed, v_my_running, v_my_in_flight,
         v_global_in_flight, v_queue_counts
    FROM in_flight;

    -- 3. Budget math (mirrors the former client-side worker budget).
    v_max_claimed := CASE
        WHEN p_max_claim_per_worker > 0 THEN p_max_claim_per_worker
        WHEN p_prefetch_buffer > 0 THEN p_processes + p_prefetch_buffer
        ELSE p_processes END;
    IF v_my_claimed >= v_max_claimed THEN
        RETURN;
    END IF;

    IF p_hard_cap_mode THEN
        v_local_available := p_processes - v_my_in_flight;
    ELSE
        v_local_available :=
            (p_processes + p_prefetch_buffer) - v_my_running - v_my_claimed;
    END IF;
    IF v_local_available < 0 THEN
        v_local_available := 0;
    END IF;

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

    -- 4. Windowed claim: two-arm eligibility per queue (bounded lock fan-out),
    --    per-queue cap filter, global priority rank, global budget trim.
    RETURN QUERY
    WITH cand AS (
        SELECT c.id, qn.name AS queue_name, c.priority, c.enqueued_at,
               COALESCE((p_queue_priority ->> qn.name)::int, 100) AS qprio
        FROM jsonb_array_elements_text(p_queues) AS qn(name)
        CROSS JOIN LATERAL (
            -- Two eligibility arms as separate CTEs (FOR UPDATE is not allowed
            -- on UNION operands, so lock per-arm then union the references —
            -- same shape as the standalone CLAIM_SQL).
            WITH pend AS (
                SELECT t.id, t.priority, t.enqueued_at
                FROM horsies_tasks t
                WHERE t.queue_name = qn.name
                  AND t.status = 'PENDING'
                  AND t.enqueued_at <= now()
                  AND (t.next_retry_at IS NULL OR t.next_retry_at <= now())
                  AND (t.good_until IS NULL OR t.good_until > now())
                ORDER BY t.priority ASC, t.enqueued_at ASC, t.id ASC
                FOR UPDATE SKIP LOCKED
                LIMIT v_total
            ),
            expired AS (
                SELECT t.id, t.priority, t.enqueued_at
                FROM horsies_tasks t
                WHERE t.queue_name = qn.name
                  AND t.status = 'CLAIMED'
                  AND t.claim_expires_at IS NOT NULL
                  AND t.claim_expires_at < now()
                  AND t.enqueued_at <= now()
                  AND (t.next_retry_at IS NULL OR t.next_retry_at <= now())
                  AND (t.good_until IS NULL OR t.good_until > now())
                ORDER BY t.priority ASC, t.enqueued_at ASC, t.id ASC
                FOR UPDATE SKIP LOCKED
                LIMIT v_total
            )
            SELECT m.id, m.priority, m.enqueued_at
            FROM (SELECT * FROM pend UNION ALL SELECT * FROM expired) m
            ORDER BY m.priority ASC, m.enqueued_at ASC, m.id ASC
            LIMIT v_total
        ) c
    ),
    per_queue AS (
        SELECT cand.id, cand.queue_name, cand.priority, cand.enqueued_at,
               cand.qprio,
               row_number() OVER (
                   PARTITION BY cand.queue_name
                   ORDER BY cand.priority ASC, cand.enqueued_at ASC, cand.id ASC
               ) AS rn_q
        FROM cand
    ),
    capped AS (
        SELECT per_queue.id, per_queue.qprio, per_queue.priority,
               per_queue.enqueued_at
        FROM per_queue
        WHERE per_queue.rn_q <= LEAST(
            CASE WHEN p_queue_max_concurrency ? per_queue.queue_name
                THEN GREATEST(0,
                    (p_queue_max_concurrency ->> per_queue.queue_name)::int
                    - COALESCE((v_queue_counts ->> per_queue.queue_name)::int, 0))
                ELSE v_total END,
            CASE WHEN p_max_claim_batch > 0 THEN p_max_claim_batch ELSE v_total END
        )
    ),
    ranked AS (
        SELECT capped.id,
               row_number() OVER (
                   ORDER BY capped.qprio ASC, capped.priority ASC,
                            capped.enqueued_at ASC, capped.id ASC
               ) AS rn
        FROM capped
    ),
    pick AS (
        SELECT ranked.id FROM ranked WHERE ranked.rn <= v_total
    )
    UPDATE horsies_tasks t
    SET status = 'CLAIMED',
        claimed = TRUE,
        claimed_at = now(),
        claimed_by_worker_id = p_worker_id,
        claim_expires_at = now() + p_lease_ms * INTERVAL '1 millisecond',
        finalizing_at = NULL,
        finalizing_by_worker_id = NULL,
        updated_at = now()
    FROM pick
    WHERE t.id = pick.id
    RETURNING t.id, t.task_name, t.args, t.kwargs, t.queue_name,
              t.is_workflow_task, t.task_options, t.claimed_at;
END;
$func$;
