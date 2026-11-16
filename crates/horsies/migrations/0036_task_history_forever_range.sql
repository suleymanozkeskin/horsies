-- Schema v35: make the forever class a RANGE parent with bounded UTC leaves.
DO $migration$
DECLARE
    v_relkind "char";
    v_today timestamptz := date_trunc('day', statement_timestamp(), 'UTC');
    v_tomorrow timestamptz := date_trunc('day', statement_timestamp(), 'UTC') + interval '1 day';
    v_today_leaf text;
    v_today_id_index text;
    v_today_order_index text;
    v_legacy_id_index text := 'horsies_task_history_forever_before_v35_task_idx';
    v_legacy_order_index text := 'horsies_task_history_forever_before_v35_enqueued_idx';
    v_bound text;
BEGIN
    v_today_leaf := 'horsies_task_history_forever_' ||
        to_char(v_today AT TIME ZONE 'UTC', 'YYYY_MM_DD');
    v_today_id_index := v_today_leaf || '_task_idx';
    v_today_order_index := v_today_leaf || '_enqueued_idx';

    SELECT relkind INTO v_relkind
    FROM pg_class
    WHERE oid = to_regclass('horsies_task_history_forever');
    IF v_relkind IS NULL OR v_relkind NOT IN ('r', 'p') THEN
        RAISE EXCEPTION
            'horsies_task_history_forever must be a table or partitioned table, found relkind %',
            v_relkind;
    END IF;

    IF v_relkind = 'r' THEN
        ALTER TABLE horsies_task_history
            DETACH PARTITION horsies_task_history_forever;
        DROP INDEX IF EXISTS horsies_task_history_forever_task_idx;
        DROP INDEX IF EXISTS horsies_task_history_forever_enqueued_idx;
        ALTER TABLE horsies_task_history_forever
            RENAME TO horsies_task_history_forever_before_v35;
        EXECUTE $horsies_p1_sql$CREATE TABLE horsies_task_history_forever
    PARTITION OF horsies_task_history
    FOR VALUES IN ('forever')
    PARTITION BY RANGE (retention_anchor_at)$horsies_p1_sql$;
    END IF;

    IF to_regclass(v_today_leaf) IS NULL THEN
        EXECUTE format(
            'CREATE TABLE %I PARTITION OF horsies_task_history_forever '
            'FOR VALUES FROM (%L) TO (%L)',
            v_today_leaf, v_today, v_tomorrow
        );
        SET LOCAL TIME ZONE 'UTC';
        SELECT pg_get_expr(c.relpartbound, c.oid)
        INTO v_bound
        FROM pg_class AS c
        WHERE c.oid = to_regclass(v_today_leaf);
        EXECUTE format(
            'INSERT INTO horsies_task_history_leaf_catalog ('
            'leaf_name, parent_name, class_key, lower_anchor, upper_anchor, '
            'index_schema_version, id_index_name, partition_bound, '
            'min_birth_at, min_birth_verified, created_at) '
            'VALUES (%L, %L, %L, %L, %L, 1, %L, %L, NULL, TRUE, statement_timestamp())',
            v_today_leaf, 'horsies_task_history_forever', 'forever',
            v_today, v_tomorrow, v_today_id_index, v_bound
        );
        EXECUTE format('CREATE INDEX %I ON %I (task_id)',
                       v_today_id_index, v_today_leaf);
        EXECUTE format('CREATE INDEX %I ON %I (enqueued_at)',
                       v_today_order_index, v_today_leaf);
        EXECUTE format('ANALYZE %I', v_today_leaf);
    ELSE
        IF NOT EXISTS (
            SELECT 1 FROM horsies_task_history_leaf_catalog
            WHERE leaf_name = v_today_leaf
              AND parent_name = 'horsies_task_history_forever'
              AND class_key = 'forever'
              AND lower_anchor = v_today
              AND upper_anchor = v_tomorrow
              AND dropped_at IS NULL
        ) THEN
            RAISE EXCEPTION 'current forever leaf % is not conformantly cataloged',
                v_today_leaf;
        END IF;
        EXECUTE format('CREATE INDEX IF NOT EXISTS %I ON %I (task_id)',
                       v_today_id_index, v_today_leaf);
        EXECUTE format('CREATE INDEX IF NOT EXISTS %I ON %I (enqueued_at)',
                       v_today_order_index, v_today_leaf);
    END IF;

    IF v_relkind = 'r' THEN
        EXECUTE format(
            'WITH moved AS ('
            'DELETE FROM horsies_task_history_forever_before_v35 '
            'WHERE retention_anchor_at >= %L RETURNING *), '
            'inserted AS (INSERT INTO horsies_task_history_forever '
            'SELECT * FROM moved RETURNING 1) '
            'SELECT count(*) FROM inserted',
            v_today
        );
        EXECUTE format(
            'ALTER TABLE horsies_task_history_forever_before_v35 '
            'ADD CONSTRAINT horsies_task_history_forever_before_v35_anchor_check '
            'CHECK (retention_anchor_at < %L)',
            v_today
        );
        EXECUTE format(
            'ALTER TABLE horsies_task_history_forever '
            'ATTACH PARTITION horsies_task_history_forever_before_v35 '
            'FOR VALUES FROM (MINVALUE) TO (%L)',
            v_today
        );
        CREATE INDEX horsies_task_history_forever_before_v35_task_idx
            ON horsies_task_history_forever_before_v35 (task_id);
        CREATE INDEX horsies_task_history_forever_before_v35_enqueued_idx
            ON horsies_task_history_forever_before_v35 (enqueued_at);
        SET LOCAL TIME ZONE 'UTC';
        SELECT pg_get_expr(c.relpartbound, c.oid)
        INTO v_bound
        FROM pg_class AS c
        WHERE c.oid = 'horsies_task_history_forever_before_v35'::regclass;
        INSERT INTO horsies_task_history_leaf_catalog (
            leaf_name, parent_name, class_key, lower_anchor, upper_anchor,
            index_schema_version, id_index_name, partition_bound,
            min_birth_at, min_birth_verified, created_at
        ) VALUES (
            'horsies_task_history_forever_before_v35',
            'horsies_task_history_forever', 'forever',
            '1970-01-01T00:00:00+00'::timestamptz, v_today,
            1, v_legacy_id_index, v_bound,
            NULL, FALSE, statement_timestamp()
        );
        ANALYZE horsies_task_history_forever_before_v35;
    END IF;
END
$migration$;
