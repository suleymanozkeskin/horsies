-- Task counts by status with TOTAL row for dashboards
-- Always returns all statuses (even if count is 0)
-- Return schema:
-- - status (text)
-- - count (bigint)
-- - sort_order (integer)
WITH all_statuses AS (
    SELECT status, sort_order FROM (VALUES
        ('pending', 1),
        ('claimed', 2),
        ('running', 3),
        ('completed', 4),
        ('failed', 5),
        ('cancelled', 6),
        ('requeued', 7)
    ) AS s(status, sort_order)
),
task_counts AS (
    SELECT
        LOWER(status) as status,
        COUNT(*) as count
    FROM horsies_tasks
    GROUP BY LOWER(status)
)
SELECT
    all_statuses.status,
    COALESCE(task_counts.count, 0) as count,
    all_statuses.sort_order
FROM all_statuses
LEFT JOIN task_counts ON all_statuses.status = task_counts.status

UNION ALL

SELECT
    'TOTAL' as status,
    COUNT(*) as count,
    8 as sort_order
FROM horsies_tasks

ORDER BY sort_order;
