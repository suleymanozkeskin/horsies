-- Schema v34: give every attached history leaf an enqueued_at btree.
DO $migration$
DECLARE
    v_leaf text;
BEGIN
    FOR v_leaf IN
        SELECT c.relname
        FROM pg_partition_tree('horsies_task_history'::regclass) AS t
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
    LOOP
        IF v_leaf !~ '^[a-z_][a-z0-9_]*$' OR octet_length(v_leaf) > 63 THEN
            RAISE EXCEPTION 'task-history leaf name is not a safe identifier: %',
                v_leaf;
        END IF;
        EXECUTE format('CREATE INDEX %I ON %I (enqueued_at)',
                       v_leaf || '_enqueued_idx', v_leaf);
    END LOOP;
END
$migration$;
