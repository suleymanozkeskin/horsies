-- Count snapshots by age (for planning retention)
SELECT age_bucket, snapshot_count, unique_workers
FROM (
    SELECT
        CASE
            WHEN snapshot_at > NOW() - INTERVAL '1 hour' THEN '< 1 hour'
            WHEN snapshot_at > NOW() - INTERVAL '24 hours' THEN '1-24 hours'
            WHEN snapshot_at > NOW() - INTERVAL '7 days' THEN '1-7 days'
            WHEN snapshot_at > NOW() - INTERVAL '30 days' THEN '7-30 days'
            ELSE '> 30 days'
        END as age_bucket,
        COUNT(*) as snapshot_count,
        COUNT(DISTINCT worker_id) as unique_workers
    FROM horsies_worker_states
    GROUP BY 1
) buckets
ORDER BY
    CASE buckets.age_bucket
        WHEN '< 1 hour' THEN 1
        WHEN '1-24 hours' THEN 2
        WHEN '1-7 days' THEN 3
        WHEN '7-30 days' THEN 4
        ELSE 5
    END;
