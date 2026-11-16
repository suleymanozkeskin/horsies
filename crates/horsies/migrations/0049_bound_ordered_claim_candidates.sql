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
    id text,
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
    RETURN QUERY
    WITH cand AS (
        SELECT c.id, qn.name AS queue_name, c.priority, c.enqueued_at,
               COALESCE((p_queue_priority ->> qn.name)::int, 100) AS qprio
        FROM jsonb_array_elements_text(p_queues) AS qn(name)
        CROSS JOIN LATERAL (
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
                LIMIT LEAST(
                    v_total,
                    CASE WHEN p_queue_max_concurrency ? qn.name THEN
                        GREATEST(0,
                            (p_queue_max_concurrency ->> qn.name)::int
                            - COALESCE((v_queue_counts ->> qn.name)::int, 0))
                        ELSE v_total END,
                    CASE WHEN p_max_claim_batch > 0 THEN p_max_claim_batch
                        ELSE v_total END
                )
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
                LIMIT LEAST(
                    v_total,
                    CASE WHEN p_queue_max_concurrency ? qn.name THEN
                        GREATEST(0,
                            (p_queue_max_concurrency ->> qn.name)::int
                            - COALESCE((v_queue_counts ->> qn.name)::int, 0))
                        ELSE v_total END,
                    CASE WHEN p_max_claim_batch > 0 THEN p_max_claim_batch
                        ELSE v_total END
                )
            )
            SELECT m.id, m.priority, m.enqueued_at
            FROM (SELECT * FROM pend UNION ALL SELECT * FROM expired) m
            ORDER BY m.priority ASC, m.enqueued_at ASC, m.id ASC
            LIMIT LEAST(
                    v_total,
                    CASE WHEN p_queue_max_concurrency ? qn.name THEN
                        GREATEST(0,
                            (p_queue_max_concurrency ->> qn.name)::int
                            - COALESCE((v_queue_counts ->> qn.name)::int, 0))
                        ELSE v_total END,
                    CASE WHEN p_max_claim_batch > 0 THEN p_max_claim_batch
                        ELSE v_total END
                )
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
    ),
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
        WHERE t.id = ANY(ARRAY(SELECT id FROM pick))
        RETURNING t.id::text, t.task_name, t.args, t.kwargs, t.queue_name,
                  t.is_workflow_task, t.task_options, t.claimed_at
    ),
    return_order AS (
        SELECT ranked.id, min(ranked.rn) AS rn
        FROM ranked
        GROUP BY ranked.id
    )
    SELECT updated.* FROM updated
    JOIN return_order ON return_order.id = updated.id::uuid
    ORDER BY return_order.rn;
END;
$func$;
