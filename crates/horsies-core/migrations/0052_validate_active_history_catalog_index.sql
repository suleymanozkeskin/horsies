DO $migration$
DECLARE
    v_actual jsonb;
    v_expected jsonb;
BEGIN
    CREATE TEMP TABLE horsies_expected_active_catalog_index
        ON COMMIT DROP
        AS SELECT class_key, upper_anchor, dropped_at
           FROM horsies_task_history_leaf_catalog WITH NO DATA;
    CREATE INDEX horsies_expected_active_catalog_index_idx
        ON horsies_expected_active_catalog_index (class_key, upper_anchor)
        WHERE dropped_at IS NULL;

    SELECT jsonb_build_object(
               'method', am.amname,
               'valid', i.indisvalid,
               'ready', i.indisready,
               'live', i.indislive,
               'unique', i.indisunique,
               'exclusion', i.indisexclusion,
               'immediate', i.indimmediate,
               'key_count', i.indnkeyatts,
               'attribute_count', i.indnatts,
               'has_expressions', i.indexprs IS NOT NULL,
               'columns', (
                   SELECT jsonb_agg(pg_get_indexdef(i.indexrelid, n, FALSE)
                                    ORDER BY n)
                   FROM generate_series(1, i.indnatts) AS n
               ),
               'operator_classes', to_jsonb(i.indclass::oid[]),
               'collations', to_jsonb(i.indcollation::oid[]),
               'options', to_jsonb(i.indoption::smallint[]),
               'predicate', pg_get_expr(i.indpred, i.indrelid)
           )
    INTO v_expected
    FROM pg_index AS i
    JOIN pg_class AS ic ON ic.oid = i.indexrelid
    JOIN pg_am AS am ON am.oid = ic.relam
    WHERE ic.oid = to_regclass('horsies_expected_active_catalog_index_idx');

    SELECT jsonb_build_object(
               'method', am.amname,
               'valid', i.indisvalid,
               'ready', i.indisready,
               'live', i.indislive,
               'unique', i.indisunique,
               'exclusion', i.indisexclusion,
               'immediate', i.indimmediate,
               'key_count', i.indnkeyatts,
               'attribute_count', i.indnatts,
               'has_expressions', i.indexprs IS NOT NULL,
               'columns', (
                   SELECT jsonb_agg(pg_get_indexdef(i.indexrelid, n, FALSE)
                                    ORDER BY n)
                   FROM generate_series(1, i.indnatts) AS n
               ),
               'operator_classes', to_jsonb(i.indclass::oid[]),
               'collations', to_jsonb(i.indcollation::oid[]),
               'options', to_jsonb(i.indoption::smallint[]),
               'predicate', pg_get_expr(i.indpred, i.indrelid)
           )
    INTO v_actual
    FROM pg_index AS i
    JOIN pg_class AS ic ON ic.oid = i.indexrelid
    JOIN pg_am AS am ON am.oid = ic.relam
    WHERE ic.oid = to_regclass('idx_horsies_history_catalog_active')
      AND i.indrelid = 'horsies_task_history_leaf_catalog'::regclass;

    IF v_actual IS DISTINCT FROM v_expected THEN
        RAISE EXCEPTION
            'idx_horsies_history_catalog_active is absent, invalid, or noncanonical'
            USING ERRCODE = 'object_not_in_prerequisite_state';
    END IF;

END
$migration$;
