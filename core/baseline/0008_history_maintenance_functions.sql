-- horsies baseline schema, part 8: the history maintenance functions of the core design.
--
-- Each function is the one statement of a history maintenance operation in the core design: the coverage probe,
-- retention class registration, the creation of one history leaf or one heartbeat leaf, the reader publication,
-- and the miss classification of a terminal write. Each function names the Python code it replaces (paths at the
-- 0.7.0 tag, under horsies/core/).
--
-- The published readers. horsies_publish_history_readers renders four functions from the leaf catalog:
-- horsies_task_history_duplicate_staged(uuid), horsies_task_provenance_staged(uuid), horsies_task_detail_staged(uuid)
-- and horsies_task_result_staged(uuid). It is the one renderer: the Python client publishes through it, and the
-- terminal writes of 0003_interim_functions.sql and 0004_lifecycle_functions.sql pass their kind and fence kind
-- to horsies_terminalization_miss, which reads history through the provenance reader.
--
-- Results are plain rows of built-in types. Closed sets of values are text columns; each function lists its values.
-- Leaf names and bounds are UTC in any session time zone.
--
-- Part of migration 1000: the install concatenates the files of this folder in name order and computes one
-- checksum over the result.

------------------------------------------------------------------------------------------------------------------------
-- Leaf names, leaf state
------------------------------------------------------------------------------------------------------------------------

-- horsies_history_leaf_name(p_class_key text, p_lower_anchor timestamptz): the relation name of the daily leaf of a
-- PERIOD class that starts at p_lower_anchor: horsies_task_history_<class key>_YYYY_MM_DD (UTC day). The longest
-- name (standard_1825d) is 46 bytes; with _enqueued_idx it is 59, under the 63-byte identifier limit.
-- Replaces (`v0.7.0` package): core/history/partitions/catalog.py:157-164 (daily_leaf_name).
CREATE FUNCTION horsies_history_leaf_name(p_class_key text, p_lower_anchor timestamp with time zone) RETURNS text
    LANGUAGE sql STABLE STRICT
    AS $$
    SELECT 'horsies_task_history_' || p_class_key || '_' || to_char(p_lower_anchor AT TIME ZONE 'UTC', 'YYYY_MM_DD')
$$;

-- horsies_heartbeat_leaf_name(p_lower_anchor timestamptz): the relation name of the hourly heartbeat leaf that
-- starts at p_lower_anchor: horsies_heartbeats_YYYY_MM_DD_HH24 (UTC hour).
-- Replaces (`v0.7.0` package): core/history/heartbeats/partitioning.py:120-127 (hourly_leaf_name).
CREATE FUNCTION horsies_heartbeat_leaf_name(p_lower_anchor timestamp with time zone) RETURNS text
    LANGUAGE sql STABLE STRICT
    AS $$
    SELECT 'horsies_heartbeats_' || to_char(p_lower_anchor AT TIME ZONE 'UTC', 'YYYY_MM_DD_HH24')
$$;

-- horsies_history_leaf_indexes(p_leaf_kind text, p_leaf_name text): the indexes that a leaf must have, one row each,
-- with what the catalog holds under each name. The one list of leaf index names and shapes (index shape version 1):
-- - HISTORY leaves: <leaf>_task_idx (task_id) and <leaf>_enqueued_idx (enqueued_at);
-- - HEARTBEAT leaves: <leaf>_probe_idx (task_id, role, sent_at DESC).
-- Columns: index_name; index_columns, the column list for CREATE INDEX ... USING btree; holder, who holds the name:
-- NONE (no relation has it), LEAF (an index of this leaf), OTHER (another relation, or a relation that is not an
-- index); conformant: the name is a valid, ready and live btree index of this leaf, not unique, primary or exclusion,
-- without predicate or expressions, with exactly the shape's key columns, their sort options, the collation of each
-- column and the default btree operator class of each column's type: the class of the type itself, else the class of
-- a type it is binary-coercible to, the preferred type first (text_ops for varchar, as CREATE INDEX picks it). Another
-- p_leaf_kind returns no row.
-- A plain SQL table function: a caller that names it in FROM gets it inlined into its own plan.
-- Reads: pg_class, pg_index, pg_am, pg_attribute, pg_opclass, pg_type, pg_cast. Locks: none.
-- Replaces (`v0.7.0` package): the index shape checks of the coverage probe
-- (core/history/maintenance/coverage.py:271-443), core/history/partitions/catalog.py:658-839 (read_leaf_physical_state)
-- and core/history/partitions/locks.py:172-209 (read_index_relation_state).
CREATE FUNCTION horsies_history_leaf_indexes(p_leaf_kind text, p_leaf_name text) RETURNS TABLE(index_name text, index_columns text, holder text, conformant boolean)
    LANGUAGE sql STABLE
    AS $$
    SELECT
        named.index_name,
        shape.index_columns,
        CASE
            WHEN named.index_oid IS NULL THEN 'NONE'
            WHEN x.indrelid IS NOT NULL AND x.indrelid = named.leaf_oid THEN 'LEAF'
            ELSE 'OTHER'
        END,
        COALESCE(
            x.indrelid = named.leaf_oid
            AND access_method.amname = 'btree'
            AND x.indisvalid AND x.indisready AND x.indislive
            AND NOT x.indisunique AND NOT x.indisprimary AND NOT x.indisexclusion
            AND x.indpred IS NULL AND x.indexprs IS NULL
            AND x.indnkeyatts = cardinality(shape.key_columns)
            AND x.indnatts = cardinality(shape.key_columns)
            AND NOT EXISTS (
                SELECT 1
                FROM unnest(shape.key_columns, shape.sort_options) WITH ORDINALITY
                    AS key(column_name, sort_option, position)
                LEFT JOIN pg_attribute AS attribute
                  ON attribute.attrelid = named.leaf_oid AND attribute.attname = key.column_name
                WHERE attribute.attnum IS NULL
                   OR x.indkey[key.position - 1] IS DISTINCT FROM attribute.attnum
                   OR x.indoption[key.position - 1] IS DISTINCT FROM key.sort_option
                   OR x.indcollation[key.position - 1] IS DISTINCT FROM attribute.attcollation
                   OR x.indclass[key.position - 1] IS DISTINCT FROM (
                       SELECT opclass.oid
                       FROM pg_opclass AS opclass
                       JOIN pg_am AS opclass_method ON opclass_method.oid = opclass.opcmethod
                       JOIN pg_type AS input_type ON input_type.oid = opclass.opcintype
                       WHERE opclass_method.amname = 'btree'
                         AND opclass.opcdefault
                         AND (
                             opclass.opcintype = attribute.atttypid
                             OR EXISTS (
                                 SELECT 1
                                 FROM pg_cast AS binary_cast
                                 WHERE binary_cast.castsource = attribute.atttypid
                                   AND binary_cast.casttarget = opclass.opcintype
                                   AND binary_cast.castmethod = 'b'
                             )
                         )
                       ORDER BY opclass.opcintype = attribute.atttypid DESC, input_type.typispreferred DESC
                       LIMIT 1
                   )
            ),
            FALSE
        )
    FROM (
        VALUES
            ('HISTORY', '_task_idx', '(task_id)', ARRAY['task_id'], ARRAY[0]),
            ('HISTORY', '_enqueued_idx', '(enqueued_at)', ARRAY['enqueued_at'], ARRAY[0]),
            -- Sort option 3: DESC (1) with its default NULLS FIRST (2).
            ('HEARTBEAT', '_probe_idx', '(task_id, role, sent_at DESC)', ARRAY['task_id', 'role', 'sent_at'],
             ARRAY[0, 0, 3])
    ) AS shape(leaf_kind, name_suffix, index_columns, key_columns, sort_options)
    CROSS JOIN LATERAL (
        SELECT
            p_leaf_name || shape.name_suffix AS index_name,
            to_regclass(quote_ident(p_leaf_name || shape.name_suffix))::oid AS index_oid,
            to_regclass(quote_ident(p_leaf_name))::oid AS leaf_oid
    ) AS named
    LEFT JOIN pg_index AS x ON x.indexrelid = named.index_oid
    LEFT JOIN pg_class AS index_relation ON index_relation.oid = named.index_oid
    LEFT JOIN pg_am AS access_method ON access_method.oid = index_relation.relam
    WHERE shape.leaf_kind = p_leaf_kind
$$;

-- horsies_history_leaf_state(...): the state of one expected leaf against its catalog row and its relation, for
-- the coverage probe and for leaf creation. One rule for both: the probe calls a leaf conformant exactly when this
-- function answers CONFORMANT.
--
-- Parameters: p_leaf_kind HISTORY or HEARTBEAT (its indexes: horsies_history_leaf_indexes); the expected class,
-- parent, leaf name and bounds. p_lower_bound_kind OPEN (lower bound MINVALUE, p_lower_anchor NULL; only the static
-- forever leaf) or AT.
-- States, in the order they are decided:
-- - CATALOG_CONFLICT with conflict_kind:
--   - PARENT_MISSING: the parent relation does not exist;
--   - RELATION_WITHOUT_CATALOG: the leaf relation exists and has no catalog row;
--   - FOREIGN_INDEX: no catalog row and no relation, and a canonical index name of the leaf is taken; or the leaf
--     exists and another relation holds a canonical index name;
--   - METADATA_MISMATCH: the catalog row has another class, lower bound kind, anchors or index shape version, or
--     records a detach or a drop;
--   - CATALOG_WITHOUT_RELATION: the catalog row matches and the relation is gone;
--   - PHYSICAL_NONCONFORMANT: the relation is not a plain table, is not attached to the parent, has a pending
--     detach, or its partition bound differs from the expected bound.
-- - ABSENT: no catalog row, no relation, no canonical index name taken. The leaf can be created.
-- - CONFORMANT: catalog row, relation, attachment, bound and the leaf's indexes as expected.
-- - INDEX_DAMAGED: as CONFORMANT, except that an index of the leaf is missing or has another shape. The indexes
--   can be repaired.
-- The bound comparison renders both sides in the session time zone, so it does not depend on that zone.
-- A plain SQL table function, inlined into the coverage probe's plan.
-- Reads: horsies_task_history_leaf_catalog_pkey, pg_class, pg_inherits, pg_index. Locks: none. Changes nothing.
-- Returns: one row.
-- Replaces (`v0.7.0` package): core/history/partitions/manager.py:601-680 (_read_daily_leaf_creation_state),
-- core/history/heartbeats/partitioning.py:448-515 (_read_hourly_leaf_creation_state), and the conformance of the
-- coverage probe (core/history/maintenance/coverage.py:271-443).
CREATE FUNCTION horsies_history_leaf_state(p_leaf_kind text, p_class_key text, p_parent_name text, p_leaf_name text, p_lower_bound_kind text, p_lower_anchor timestamp with time zone, p_upper_anchor timestamp with time zone) RETURNS TABLE(state text, conflict_kind text)
    LANGUAGE sql STABLE
    AS $$
    SELECT
        CASE WHEN verdict.name IN ('ABSENT', 'CONFORMANT', 'INDEX_DAMAGED') THEN verdict.name ELSE 'CATALOG_CONFLICT' END,
        CASE WHEN verdict.name IN ('ABSENT', 'CONFORMANT', 'INDEX_DAMAGED') THEN NULL ELSE verdict.name END
    FROM (
        SELECT CASE
            WHEN relation.parent_oid IS NULL THEN 'PARENT_MISSING'
            WHEN catalog.leaf_name IS NULL AND relation.leaf_oid IS NOT NULL THEN 'RELATION_WITHOUT_CATALOG'
            WHEN catalog.leaf_name IS NULL AND EXISTS (
                SELECT 1 FROM horsies_history_leaf_indexes(p_leaf_kind, p_leaf_name) AS i WHERE i.holder <> 'NONE'
            ) THEN 'FOREIGN_INDEX'
            WHEN catalog.leaf_name IS NULL THEN 'ABSENT'
            WHEN catalog.class_key IS DISTINCT FROM p_class_key
              OR catalog.lower_bound_kind IS DISTINCT FROM p_lower_bound_kind
              OR catalog.lower_anchor IS DISTINCT FROM p_lower_anchor
              OR catalog.upper_anchor IS DISTINCT FROM p_upper_anchor
              OR catalog.index_schema_version <> 1
              OR catalog.detached_at IS NOT NULL
              OR catalog.dropped_at IS NOT NULL THEN 'METADATA_MISMATCH'
            WHEN relation.leaf_oid IS NULL THEN 'CATALOG_WITHOUT_RELATION'
            WHEN leaf.relkind IS DISTINCT FROM 'r'
              OR inheritance.inhparent IS DISTINCT FROM relation.parent_oid
              OR inheritance.inhdetachpending
              OR pg_get_expr(leaf.relpartbound, leaf.oid) IS DISTINCT FROM format(
                  'FOR VALUES FROM (%s) TO (%L)',
                  CASE p_lower_bound_kind WHEN 'OPEN' THEN 'MINVALUE' ELSE quote_literal(p_lower_anchor) END,
                  p_upper_anchor
              ) THEN 'PHYSICAL_NONCONFORMANT'
            WHEN EXISTS (
                SELECT 1 FROM horsies_history_leaf_indexes(p_leaf_kind, p_leaf_name) AS i WHERE i.holder = 'OTHER'
            ) THEN 'FOREIGN_INDEX'
            WHEN NOT EXISTS (
                SELECT 1 FROM horsies_history_leaf_indexes(p_leaf_kind, p_leaf_name) AS i WHERE NOT i.conformant
            ) THEN 'CONFORMANT'
            ELSE 'INDEX_DAMAGED'
        END AS name
        FROM (
            SELECT
                to_regclass(quote_ident(p_parent_name))::oid AS parent_oid,
                to_regclass(quote_ident(p_leaf_name))::oid AS leaf_oid
        ) AS relation
        LEFT JOIN horsies_task_history_leaf_catalog AS catalog ON catalog.leaf_name = p_leaf_name
        LEFT JOIN pg_class AS leaf ON leaf.oid = relation.leaf_oid
        LEFT JOIN pg_inherits AS inheritance ON inheritance.inhrelid = relation.leaf_oid
        -- A fence: the verdict is computed once, not once per output column.
        OFFSET 0
    ) AS verdict
$$;

------------------------------------------------------------------------------------------------------------------------
-- Reader publication
------------------------------------------------------------------------------------------------------------------------

-- horsies_history_reader_signatures(): the four published readers, as (reader, signature) rows. One list for the
-- renderer, the stamp writer and the freshness check.
CREATE FUNCTION horsies_history_reader_signatures() RETURNS TABLE(reader text, signature text)
    LANGUAGE sql IMMUTABLE
    AS $$
    VALUES
        ('DUPLICATE_GUARD', 'horsies_task_history_duplicate_staged(uuid)'),
        ('PROVENANCE', 'horsies_task_provenance_staged(uuid)'),
        ('DETAIL', 'horsies_task_detail_staged(uuid)'),
        ('RESULT', 'horsies_task_result_staged(uuid)')
$$;

-- horsies_history_reader_manifest(): the probe list that the readers must probe, read from the leaf catalog.
--
-- Rows: every history leaf (FOREVER and PERIOD classes; heartbeat leaves never) that the catalog does not record as
-- detached or dropped and whose relation exists, ordered by lower anchor (the OPEN bound first), then leaf name;
-- probe_position counts from 0 in that order. A cataloged leaf whose relation is gone is left out
-- (horsies_history_absent_leaves names it): a probe of a missing relation fails at execution.
-- Reads: horsies_task_history_leaf_catalog, horsies_retention_classes_pkey. Locks: none.
-- Replaces (`v0.7.0` package): core/history/partitions/catalog.py:436-507 (read_manifest_leaf_rows, which reads each
-- catalog row again) and core/history/reads/lookup_generation.py:139-187 (manifest_from_catalog).
CREATE FUNCTION horsies_history_reader_manifest() RETURNS TABLE(leaf_name text, probe_position integer, lower_bound_kind text, lower_anchor timestamp with time zone, upper_anchor timestamp with time zone)
    LANGUAGE sql STABLE
    AS $$
    SELECT
        c.leaf_name,
        (row_number() OVER (ORDER BY c.lower_anchor NULLS FIRST, c.leaf_name) - 1)::integer,
        c.lower_bound_kind::text,
        c.lower_anchor,
        c.upper_anchor
    FROM horsies_task_history_leaf_catalog AS c
    JOIN horsies_retention_classes AS r ON r.class_key = c.class_key
    WHERE r.class_kind IN ('FOREVER', 'PERIOD')
      AND c.detached_at IS NULL
      AND c.dropped_at IS NULL
      AND to_regclass(quote_ident(c.leaf_name)) IS NOT NULL
$$;

-- horsies_history_absent_leaves(): the history leaves that the catalog records as attached whose relation is gone,
-- in name order. They were removed out of band: the readers leave them out, and the catalog keeps their rows for an
-- operator. Empty on a healthy database.
-- Replaces (`v0.7.0` package): the absent_leaves column of the coverage probe
-- (core/history/maintenance/coverage.py:501-509) and the absent relations of read_manifest_leaf_rows.
CREATE FUNCTION horsies_history_absent_leaves() RETURNS text[]
    LANGUAGE sql STABLE
    AS $$
    SELECT COALESCE(array_agg(c.leaf_name ORDER BY c.leaf_name), '{}')
    FROM horsies_task_history_leaf_catalog AS c
    JOIN horsies_retention_classes AS r ON r.class_key = c.class_key
    WHERE r.class_kind IN ('FOREVER', 'PERIOD')
      AND c.detached_at IS NULL
      AND c.dropped_at IS NULL
      AND to_regclass(quote_ident(c.leaf_name)) IS NULL
$$;

-- horsies_history_reader_stamp(): the stamp of the published manifest table:
-- horsies:manifest:<renderer version>:<sha256 of the manifest rows>. The rows enter in probe order as
-- leaf_name|probe_position|lower_bound_kind|lower anchor (epoch seconds, '-' for OPEN)|upper anchor (epoch seconds),
-- joined by commas; epoch seconds do not depend on the session time zone. An empty manifest has a stamp too.
-- Renderer version 2 is the reader bodies of horsies_render_history_reader (version 1 had no command fingerprint
-- and no sent_at in the provenance row). A change of any rendered body changes the version here, so the first
-- coverage pass after the migration that ships it republishes, also when the manifest is the same.
-- Replaces (`v0.7.0` package): MANIFEST_STAMP_SQL (core/history/reads/publisher.py:85-104), which has no renderer
-- version.
CREATE FUNCTION horsies_history_reader_stamp() RETURNS text
    LANGUAGE sql STABLE
    AS $$
    SELECT 'horsies:manifest:2:' || encode(sha256(convert_to(COALESCE(
        string_agg(
            concat_ws(
                '|',
                m.leaf_name,
                m.probe_position,
                m.lower_bound_kind,
                COALESCE(extract(epoch FROM m.lower_anchor)::text, '-'),
                extract(epoch FROM m.upper_anchor)
            ),
            ',' ORDER BY m.probe_position
        ),
        ''
    ), 'UTF8')), 'hex')
    FROM horsies_task_lookup_manifest AS m
$$;

-- horsies_history_readers_current(): whether the published readers serve the current catalog.
--
-- TRUE exactly when the published manifest table equals horsies_history_reader_manifest() row for row, and each of
-- the four readers exists and carries the stamp of that manifest as its comment. A missing reader, a reader that
-- another renderer wrote (another stamp), a leaf created, detached or dropped since the last publication, and a
-- leaf whose relation is gone all answer FALSE.
-- Reads: the manifest and the catalog; pg_proc and pg_description by the four signatures. Locks: none.
-- Replaces (`v0.7.0` package): core/history/reads/publisher.py:178-188 (is_current), :212-224
-- (published_readers_stamped) and :259-309 (published_manifest_matches_catalog).
CREATE FUNCTION horsies_history_readers_current() RETURNS boolean
    LANGUAGE sql STABLE
    AS $$
    WITH published AS (
        SELECT m.leaf_name, m.probe_position, m.lower_bound_kind::text AS lower_bound_kind, m.lower_anchor,
               m.upper_anchor
        FROM horsies_task_lookup_manifest AS m
    ),
    expected AS (
        SELECT e.leaf_name, e.probe_position, e.lower_bound_kind, e.lower_anchor, e.upper_anchor
        FROM horsies_history_reader_manifest() AS e
    ),
    stamp AS (
        SELECT horsies_history_reader_stamp() AS value
    )
    SELECT
        NOT EXISTS (
            SELECT 1
            FROM horsies_history_reader_signatures() AS s
            CROSS JOIN stamp
            WHERE obj_description(to_regprocedure(s.signature), 'pg_proc') IS DISTINCT FROM stamp.value
        )
        AND NOT EXISTS (SELECT * FROM published EXCEPT SELECT * FROM expected)
        AND NOT EXISTS (SELECT * FROM expected EXCEPT SELECT * FROM published)
$$;

-- horsies_render_history_reader(p_reader text): the CREATE OR REPLACE FUNCTION statement of one published reader,
-- rendered from the published manifest table. Internal: horsies_publish_history_readers runs it after it rewrote the
-- manifest table in the same transaction.
--
-- Every reader is PL/pgSQL, STABLE, STRICT, with enable_seqscan off: PL/pgSQL keeps a generic plan per probe, and
-- on a new leaf that statistics show as empty that plan would be a sequential scan. Every probe reads one leaf by
-- its task_id index (or the live table and the clock exceptions by their primary keys); no probe names the
-- partitioned parent. Probe order for a UUIDv7 id: with b the id's birth minus 5 s, the leaves whose upper anchor is
-- later than b, oldest first; then (readers, not the guard) every other leaf, newest first, so that a row that a
-- wrong producer clock placed in an earlier leaf is still found. An id that is not a UUIDv7 probes every leaf, newest
-- first.
-- Readers (p_reader):
-- - DUPLICATE_GUARD: horsies_task_history_duplicate_staged(uuid) RETURNS boolean. TRUE when a probed leaf holds the
--   id or horsies_task_history_clock_exceptions records it. No live probe and no fallback probes: a row that a wrong
--   clock placed in an earlier leaf has its clock exception (the move writes it).
-- - PROVENANCE: horsies_task_provenance_staged(uuid) RETURNS TABLE(task_id, status, terminal_at,
--   terminalization_kind, is_workflow_task, command_fingerprint_version, command_fingerprint, sent_at): one row when
--   history holds the id, no row otherwise. No live probe. The fingerprint columns and sent_at are the facts of the
--   exact-id resend check (horsies_enqueue_task), read from the same leaf probe.
-- - DETAIL: horsies_task_detail_staged(uuid) RETURNS TABLE(location, task_row horsies_task_history): LIVE with a
--   NULL row when the live table holds the id, HISTORY with the full row, no row when absent.
-- - RESULT: horsies_task_result_staged(uuid) RETURNS TABLE(location, status, failed_reason, result_envelope_version,
--   result_payload, result_digest, task_name): LIVE with the status and task name (the live row has no result),
--   HISTORY with the stored result columns, no row when absent.
-- A change of any rendered text needs a new renderer version in horsies_history_reader_stamp.
-- Refusals: 22023 invalid_parameter_value for another p_reader. Reads: the manifest table. Changes nothing.
-- Returns: the statement text.
-- Replaces (`v0.7.0` package): core/history/reads/lookup_generation.py:190-594 (the five renderers and
-- _staged_function). Changes from it: the lookup reader is not rendered (no production caller); the provenance reader
-- has no live probe and no include_live argument, returns plain columns with is_workflow_task, the command fingerprint
-- and sent_at, and no row on absence; the fallback probes include the open-ended leaf, because a UUIDv7 can encode a
-- birth after 9999-01-01.
CREATE FUNCTION horsies_render_history_reader(p_reader text) RETURNS text
    LANGUAGE plpgsql STABLE
    SET "TimeZone" TO 'UTC'
    AS $renderer$
DECLARE
    v_header text;
    v_declare text := '';
    v_live_probe text := '';
    v_probe text;
    v_absence text;
    v_with_fallback boolean;
    v_pruned text;
    v_fallback text := '';
    v_any_id text;
    v_history_section text := '';
BEGIN
    CASE p_reader
        WHEN 'DUPLICATE_GUARD' THEN
            v_header := 'horsies_task_history_duplicate_staged(p_task_id uuid) RETURNS boolean';
            v_probe := $probe$
            PERFORM 1 FROM %1$I AS h WHERE h.task_id = p_task_id;
            IF FOUND THEN
                RETURN TRUE;
            END IF;$probe$;
            v_absence := 'RETURN EXISTS (SELECT 1 FROM horsies_task_history_clock_exceptions AS e '
                || 'WHERE e.task_id = p_task_id);';
            v_with_fallback := FALSE;
        WHEN 'PROVENANCE' THEN
            v_header := 'horsies_task_provenance_staged(p_task_id uuid) RETURNS TABLE(task_id uuid, status text, '
                || 'terminal_at timestamp with time zone, terminalization_kind text, is_workflow_task boolean, '
                || 'command_fingerprint_version smallint, command_fingerprint bytea, '
                || 'sent_at timestamp with time zone)';
            v_probe := $probe$
            RETURN QUERY SELECT h.task_id, h.status::text, h.terminal_at, h.terminalization_kind::text,
                h.is_workflow_task, h.command_fingerprint_version, h.command_fingerprint, h.sent_at
            FROM %1$I AS h WHERE h.task_id = p_task_id;
            IF FOUND THEN
                RETURN;
            END IF;$probe$;
            v_absence := 'RETURN;';
            v_with_fallback := TRUE;
        WHEN 'DETAIL' THEN
            v_header := 'horsies_task_detail_staged(p_task_id uuid) RETURNS TABLE(location text, '
                || 'task_row horsies_task_history)';
            v_declare := E'\n    v_row horsies_task_history;';
            v_live_probe := $probe$
    IF EXISTS (SELECT 1 FROM horsies_tasks AS t WHERE t.id = p_task_id) THEN
        RETURN QUERY SELECT 'LIVE'::text, NULL::horsies_task_history;
        RETURN;
    END IF;$probe$;
            v_probe := $probe$
            SELECT h.* INTO v_row FROM %1$I AS h WHERE h.task_id = p_task_id;
            IF FOUND THEN
                RETURN QUERY SELECT 'HISTORY'::text, v_row;
                RETURN;
            END IF;$probe$;
            v_absence := 'RETURN;';
            v_with_fallback := TRUE;
        WHEN 'RESULT' THEN
            v_header := 'horsies_task_result_staged(p_task_id uuid) RETURNS TABLE(location text, status text, '
                || 'failed_reason text, result_envelope_version smallint, result_payload bytea, '
                || 'result_digest bytea, task_name text)';
            v_live_probe := $probe$
    RETURN QUERY SELECT 'LIVE'::text, t.status::text, NULL::text, NULL::smallint, NULL::bytea, NULL::bytea,
        t.task_name::text
    FROM horsies_tasks AS t WHERE t.id = p_task_id;
    IF FOUND THEN
        RETURN;
    END IF;$probe$;
            v_probe := $probe$
            RETURN QUERY SELECT 'HISTORY'::text, h.status::text, h.final_failed_reason,
                h.result_envelope_version, h.result_payload, h.result_digest, h.task_name::text
            FROM %1$I AS h WHERE h.task_id = p_task_id;
            IF FOUND THEN
                RETURN;
            END IF;$probe$;
            v_absence := 'RETURN;';
            v_with_fallback := TRUE;
        ELSE
            RAISE EXCEPTION 'horsies_render_history_reader: unknown reader %', p_reader
                USING ERRCODE = 'invalid_parameter_value';
    END CASE;

    SELECT
        string_agg(
            format(E'\n        IF v_effective_birth < %L::timestamptz THEN%s\n        END IF;', m.upper_anchor,
                   format(v_probe, m.leaf_name)),
            '' ORDER BY m.probe_position
        ),
        string_agg(format(v_probe, m.leaf_name), '' ORDER BY m.probe_position DESC)
    INTO v_pruned, v_any_id
    FROM horsies_task_lookup_manifest AS m;

    IF v_with_fallback THEN
        SELECT string_agg(
            format(E'\n        IF v_effective_birth >= %L::timestamptz THEN%s\n        END IF;', m.upper_anchor,
                   format(v_probe, m.leaf_name)),
            '' ORDER BY m.probe_position DESC
        )
        INTO v_fallback
        FROM horsies_task_lookup_manifest AS m;
    END IF;

    IF v_pruned IS NOT NULL THEN
        v_history_section := format(
            $section$
    v_birth_at := horsies_uuid7_birth(p_task_id);
    IF v_birth_at IS NOT NULL THEN
        v_effective_birth := v_birth_at - interval '5 seconds';%s%s
    ELSE%s
    END IF;$section$,
            v_pruned, COALESCE(v_fallback, ''), v_any_id
        );
    END IF;

    RETURN format(
        $function$CREATE OR REPLACE FUNCTION %s
    LANGUAGE plpgsql STABLE STRICT
    SET enable_seqscan TO 'off'
    AS $reader$
#variable_conflict use_column
DECLARE%s
    v_birth_at timestamp with time zone;
    v_effective_birth timestamp with time zone;
BEGIN%s%s
    %s
END
$reader$$function$,
        v_header, v_declare, v_live_probe, v_history_section, v_absence
    );
END
$renderer$;

-- horsies_publish_history_readers(): publish the readers for the current leaf catalog, inside the caller's
-- transaction.
--
-- Guarantee: the readers and the manifest table describe one set of leaves: every history leaf of every class that
-- the catalog records as attached and whose relation exists (horsies_history_reader_manifest). A reader never names
-- a missing relation. Every reader carries the stamp of the manifest it was rendered from
-- (horsies_history_reader_stamp), and horsies_drop_detached_leaf refuses a leaf that the manifest names.
-- Serialization: the function first takes the transaction advisory lock of the publication key
-- hashtextextended('horsies:reader-publication:v1', 1601), then reads the catalog with a new snapshot (a VOLATILE
-- function under READ COMMITTED takes one per statement), so it renders every leaf that committed before the lock
-- was granted, and two publications never interleave. Under REPEATABLE READ or SERIALIZABLE the catalog read would
-- use the transaction's older snapshot, so the function refuses them.
-- Outcomes:
-- - PUBLISHED: this call rewrote the manifest table, replaced the four readers and stamped them.
-- - ALREADY_CURRENT: horsies_history_readers_current() held under the lock; nothing changed (no plan of any session
--   is invalidated).
-- Columns: manifest_stamp (the stamp of the published manifest), probed_leaves (the manifest rows), absent_leaves
-- (horsies_history_absent_leaves()).
-- Refusals (each raises; nothing changes):
-- - 25000 invalid_transaction_state: the transaction isolation is not READ COMMITTED.
-- - 42P01 undefined_table: the catalog has attached history leaves and none of their parents resolves, so this
--   session cannot see the history tables, and publishing would make every retained row look absent.
-- Lock order: the publication lock; the manifest rows (DELETE, INSERT); the four functions (CREATE OR REPLACE,
-- COMMENT). No relation lock on a leaf.
-- Returns: one row.
-- Replaces (`v0.7.0` package): core/history/reads/publisher.py:133-176 (StagedLoaderPublisher.republish): a catalog
-- read and one catalog row read per attached leaf, five renders, an overload DROP, the manifest DELETE and INSERT and
-- the stamp DO block, about 10 statements plus one per attached leaf, with no lock against a concurrent publisher.
CREATE FUNCTION horsies_publish_history_readers() RETURNS TABLE(outcome text, manifest_stamp text, probed_leaves integer, absent_leaves text[])
    LANGUAGE plpgsql
    SET "TimeZone" TO 'UTC'
    AS $$
#variable_conflict use_column
DECLARE
    c_publication_lock_key CONSTANT bigint := hashtextextended('horsies:reader-publication:v1', 1601);
    v_absent text[];
    v_probed integer;
    v_stamp text;
    v_reader record;
BEGIN
    IF current_setting('transaction_isolation') <> 'read committed' THEN
        RAISE EXCEPTION 'horsies_publish_history_readers needs READ COMMITTED, got %',
            current_setting('transaction_isolation')
            USING ERRCODE = 'invalid_transaction_state';
    END IF;
    PERFORM pg_advisory_xact_lock(c_publication_lock_key);

    IF EXISTS (
        SELECT 1
        FROM horsies_task_history_leaf_catalog AS c
        JOIN horsies_retention_classes AS r ON r.class_key = c.class_key
        WHERE r.class_kind IN ('FOREVER', 'PERIOD') AND c.detached_at IS NULL AND c.dropped_at IS NULL
    ) AND NOT EXISTS (
        SELECT 1
        FROM horsies_task_history_leaf_catalog AS c
        JOIN horsies_retention_classes AS r ON r.class_key = c.class_key
        WHERE r.class_kind IN ('FOREVER', 'PERIOD') AND c.detached_at IS NULL AND c.dropped_at IS NULL
          AND to_regclass(quote_ident(CASE r.class_kind
              WHEN 'FOREVER' THEN 'horsies_task_history_forever'
              ELSE 'horsies_task_history_' || r.class_key
          END)) IS NOT NULL
    ) THEN
        RAISE EXCEPTION 'no attached history leaf resolves its parent relation: this session cannot see the history '
            'tables, and a publication would report every retained task as absent'
            USING ERRCODE = 'undefined_table';
    END IF;

    v_absent := horsies_history_absent_leaves();
    IF horsies_history_readers_current() THEN
        SELECT count(*)::integer INTO v_probed FROM horsies_task_lookup_manifest;
        RETURN QUERY SELECT 'ALREADY_CURRENT'::text, horsies_history_reader_stamp(), v_probed, v_absent;
        RETURN;
    END IF;

    DELETE FROM horsies_task_lookup_manifest;
    INSERT INTO horsies_task_lookup_manifest (
        leaf_name, probe_position, lower_bound_kind, lower_anchor, upper_anchor, published_at
    )
    SELECT m.leaf_name, m.probe_position, m.lower_bound_kind, m.lower_anchor, m.upper_anchor, statement_timestamp()
    FROM horsies_history_reader_manifest() AS m;
    GET DIAGNOSTICS v_probed = ROW_COUNT;

    FOR v_reader IN SELECT s.reader FROM horsies_history_reader_signatures() AS s LOOP
        EXECUTE horsies_render_history_reader(v_reader.reader);
    END LOOP;
    v_stamp := horsies_history_reader_stamp();
    FOR v_reader IN SELECT s.signature FROM horsies_history_reader_signatures() AS s LOOP
        EXECUTE format('COMMENT ON FUNCTION %s IS %L', v_reader.signature, v_stamp);
    END LOOP;
    RETURN QUERY SELECT 'PUBLISHED'::text, v_stamp, v_probed, v_absent;
END
$$;

------------------------------------------------------------------------------------------------------------------------
-- Coverage probe
------------------------------------------------------------------------------------------------------------------------

-- horsies_retention_period_days(p_declared_days smallint[]): the period classes that the maintenance owner keeps:
-- the declared day counts and 30 (the default class standard_30d), distinct, ascending. One rule for the coverage
-- probe and for registration.
-- Refusals: 22023 invalid_parameter_value for a NULL array, a NULL element, an element outside 1..1825 (the
-- period bound of the class CHECK), or a day count listed twice.
-- Returns: the day counts.
CREATE FUNCTION horsies_retention_period_days(p_declared_days smallint[]) RETURNS smallint[]
    LANGUAGE plpgsql IMMUTABLE
    AS $$
BEGIN
    IF p_declared_days IS NULL THEN
        RAISE EXCEPTION 'the declared retention days must be an array; got NULL'
            USING ERRCODE = 'invalid_parameter_value';
    END IF;
    IF EXISTS (SELECT 1 FROM unnest(p_declared_days) AS d WHERE d IS NULL OR d NOT BETWEEN 1 AND 1825) THEN
        RAISE EXCEPTION 'every declared retention period must be a whole number of days from 1 to 1825; got %',
            p_declared_days
            USING ERRCODE = 'invalid_parameter_value';
    END IF;
    IF (SELECT count(DISTINCT d) FROM unnest(p_declared_days) AS d) <> cardinality(p_declared_days) THEN
        RAISE EXCEPTION 'a declared retention period is listed twice: %', p_declared_days
            USING ERRCODE = 'invalid_parameter_value';
    END IF;
    RETURN ARRAY(
        SELECT DISTINCT d FROM unnest(p_declared_days || 30::smallint) AS d ORDER BY d
    );
END
$$;

-- horsies_coverage_probe(p_declared_days smallint[], p_history_horizon_days smallint,
-- p_heartbeat_horizon_hours smallint): the check of one coverage pass, in one statement. Reads only.
--
-- Rows: one per expected leaf, ordered by leaf_kind, class_key, lower_anchor (the OPEN bound first):
-- - history leaves: every registered PERIOD class, today (UTC) and the next p_history_horizon_days days; the static
--   forever leaf (horsies_task_history_forever_open, OPEN lower and upper bounds);
-- - heartbeat leaves: the current UTC hour and the next p_heartbeat_horizon_hours hours.
-- Columns: checked_at (statement_timestamp, the same on every row); leaf_kind (history | heartbeat); class_key;
-- parent_name; leaf_name; lower_bound_kind (OPEN | AT); lower_anchor (NULL for OPEN); upper_bound_kind (OPEN | AT:
-- OPEN for the forever leaf only, whose coverage has no end; its partition bound 9999-01-01 is not reported);
-- upper_anchor (NULL for OPEN); conformant (horsies_history_leaf_state answers CONFORMANT); and, the same on every
-- row:
-- - registration_healthy: the heartbeat class with p_heartbeat_horizon_hours, one PERIOD class per day count of
--   horsies_retention_period_days(p_declared_days), and the forever class are registered;
-- - publication_healthy: horsies_history_readers_current();
-- - absent_leaves: horsies_history_absent_leaves().
-- The pass is healthy when both flags hold and every row is conformant.
-- Refusals: 22023 invalid_parameter_value for an illegal p_declared_days (horsies_retention_period_days), a
-- p_history_horizon_days outside 2..14 or a p_heartbeat_horizon_hours outside 2..48 (the configuration bounds), or a
-- NULL among them.
-- Runs with TimeZone UTC (set on the function): leaf names and bounds are UTC in any session time zone.
-- Reads: horsies_retention_classes, horsies_task_history_leaf_catalog_pkey per expected leaf, pg_class, pg_inherits,
-- pg_index; the manifest and the reader comments. Locks: none.
-- Returns: (registered PERIOD classes) x (p_history_horizon_days + 1) + 1 + (p_heartbeat_horizon_hours + 1) rows;
-- for example 16 for the default class only, 7 days and 6 hours.
-- Replaces (`v0.7.0` package): core/history/maintenance/coverage.py:103-528 (_COVERAGE_PROBE_SQL) with :655-739 (the
-- SET LOCAL TIME ZONE and the probe read): two statements.
CREATE FUNCTION horsies_coverage_probe(p_declared_days smallint[], p_history_horizon_days smallint, p_heartbeat_horizon_hours smallint) RETURNS TABLE(checked_at timestamp with time zone, leaf_kind text, class_key text, parent_name text, leaf_name text, lower_bound_kind text, lower_anchor timestamp with time zone, upper_bound_kind text, upper_anchor timestamp with time zone, conformant boolean, registration_healthy boolean, publication_healthy boolean, absent_leaves text[])
    LANGUAGE plpgsql STABLE
    SET "TimeZone" TO 'UTC'
    AS $$
#variable_conflict use_column
DECLARE
    v_period_days smallint[];
BEGIN
    v_period_days := horsies_retention_period_days(p_declared_days);
    IF p_history_horizon_days IS NULL OR p_history_horizon_days NOT BETWEEN 2 AND 14 THEN
        RAISE EXCEPTION 'p_history_horizon_days must be from 2 to 14, got %', p_history_horizon_days
            USING ERRCODE = 'invalid_parameter_value';
    END IF;
    IF p_heartbeat_horizon_hours IS NULL OR p_heartbeat_horizon_hours NOT BETWEEN 2 AND 48 THEN
        RAISE EXCEPTION 'p_heartbeat_horizon_hours must be from 2 to 48, got %', p_heartbeat_horizon_hours
            USING ERRCODE = 'invalid_parameter_value';
    END IF;

    RETURN QUERY
    WITH clock AS MATERIALIZED (
        SELECT statement_timestamp() AS checked_at
    ),
    expected_classes AS (
        SELECT 'heartbeats'::text AS class_key, 'HEARTBEAT'::text AS class_kind, NULL::smallint AS retention_days,
               p_heartbeat_horizon_hours AS heartbeat_horizon_hours
        UNION ALL
        SELECT 'standard_' || d || 'd', 'PERIOD', d, NULL::smallint
        FROM unnest(v_period_days) AS d
        UNION ALL
        SELECT 'forever', 'FOREVER', NULL::smallint, NULL::smallint
    ),
    registration AS MATERIALIZED (
        SELECT NOT EXISTS (
            SELECT 1
            FROM expected_classes AS e
            LEFT JOIN horsies_retention_classes AS r ON r.class_key = e.class_key
            WHERE r.class_key IS NULL
               OR r.class_kind IS DISTINCT FROM e.class_kind
               OR r.retention_days IS DISTINCT FROM e.retention_days
               OR r.heartbeat_horizon_hours IS DISTINCT FROM e.heartbeat_horizon_hours
        ) AS healthy
    ),
    expected_leaves AS (
        SELECT 'history'::text AS leaf_kind, r.class_key::text AS class_key,
               'horsies_task_history_' || r.class_key AS parent_name,
               horsies_history_leaf_name(r.class_key, day.lower_anchor) AS leaf_name,
               'AT'::text AS lower_bound_kind, day.lower_anchor, 'AT'::text AS upper_bound_kind,
               day.lower_anchor + interval '1 day' AS upper_anchor
        FROM horsies_retention_classes AS r
        CROSS JOIN clock
        CROSS JOIN LATERAL (
            SELECT date_trunc('day', clock.checked_at, 'UTC') + day_offset * interval '1 day' AS lower_anchor
            FROM generate_series(0, p_history_horizon_days) AS day_offset
        ) AS day
        WHERE r.class_kind = 'PERIOD'
        UNION ALL
        -- The partition bound of the forever leaf (its catalog upper_anchor), for the state check only.
        SELECT 'history', 'forever', 'horsies_task_history_forever', 'horsies_task_history_forever_open', 'OPEN',
               NULL::timestamptz, 'OPEN', '9999-01-01 00:00:00+00'::timestamptz
        UNION ALL
        SELECT 'heartbeat', 'heartbeats', 'horsies_heartbeats', horsies_heartbeat_leaf_name(hour.lower_anchor), 'AT',
               hour.lower_anchor, 'AT', hour.lower_anchor + interval '1 hour'
        FROM clock
        CROSS JOIN LATERAL (
            SELECT date_trunc('hour', clock.checked_at, 'UTC') + hour_offset * interval '1 hour' AS lower_anchor
            FROM generate_series(0, p_heartbeat_horizon_hours) AS hour_offset
        ) AS hour
    ),
    -- Single-row facts, computed once: a nested loop would otherwise evaluate them once per expected leaf.
    publication AS MATERIALIZED (
        SELECT horsies_history_readers_current() AS healthy, horsies_history_absent_leaves() AS absent_leaves
    )
    SELECT
        clock.checked_at,
        e.leaf_kind,
        e.class_key,
        e.parent_name,
        e.leaf_name,
        e.lower_bound_kind,
        e.lower_anchor,
        e.upper_bound_kind,
        CASE e.upper_bound_kind WHEN 'AT' THEN e.upper_anchor END,
        s.state = 'CONFORMANT',
        registration.healthy,
        publication.healthy,
        publication.absent_leaves
    FROM expected_leaves AS e
    CROSS JOIN LATERAL horsies_history_leaf_state(
        CASE e.leaf_kind WHEN 'heartbeat' THEN 'HEARTBEAT' ELSE 'HISTORY' END,
        e.class_key, e.parent_name, e.leaf_name, e.lower_bound_kind, e.lower_anchor, e.upper_anchor
    ) AS s
    CROSS JOIN clock
    CROSS JOIN registration
    CROSS JOIN publication
    ORDER BY e.leaf_kind, e.class_key, e.lower_anchor NULLS FIRST;
END
$$;

------------------------------------------------------------------------------------------------------------------------
-- Retention class registration
------------------------------------------------------------------------------------------------------------------------

-- horsies_register_retention_classes(p_days smallint[], p_heartbeat_horizon_hours smallint): register every
-- retention class that the maintenance owner keeps, inside the caller's transaction.
--
-- Guarantee: after the call, one PERIOD class row and its RANGE parent exist for each day count of
-- horsies_retention_period_days(p_days) (30 always included), and the heartbeat class row holds
-- p_heartbeat_horizon_hours. A class row and its parent are created in the same transaction. An existing PERIOD
-- class changes nothing: its key is a function of its period (the kind CHECK), so a class cannot conflict.
-- Rows, PERIOD classes in day order, then the heartbeat class:
-- - REGISTERED: this call inserted the row (and, for a PERIOD class, created its parent
--   horsies_task_history_<class key> PARTITION OF horsies_task_history FOR VALUES IN (<class key>) PARTITION BY
--   RANGE (terminal_at));
-- - VERIFIED: the row exists as asked; nothing changed;
-- - HORIZON_UPDATED (heartbeat only): the stored horizon differed and is now p_heartbeat_horizon_hours;
--   previous_heartbeat_horizon_hours holds the value it replaced (NULL on every other row).
-- Concurrency: two registrations of one new class insert it once; the second waits for the first, then answers
-- VERIFIED. PERIOD rows are inserted in day order, so two registrations cannot deadlock on them.
-- Refusals (each raises; nothing changes): 22023 invalid_parameter_value for an illegal p_days
-- (horsies_retention_period_days) or a p_heartbeat_horizon_hours outside 2..48 or NULL.
-- Lock order: the new class rows (INSERT); for each new PERIOD class, ACCESS EXCLUSIVE on horsies_task_history and
-- on the new parent (CREATE TABLE ... PARTITION OF), held until commit, so moves wait for the commit and the wait
-- for in-flight moves is bounded by the caller's lock_timeout; the heartbeat class row (FOR NO KEY UPDATE, which a
-- leaf catalog insert's foreign key check does not wait on).
-- Indexes: horsies_retention_classes_pkey.
-- Returns: one row per class.
-- Replaces (`v0.7.0` package): core/history/ddl/classes.py:96-163 (register_finite_retention_class: a read, an INSERT
-- and a CREATE TABLE per new class) for the default and each declared class, and
-- core/history/heartbeats/partitioning.py:200-281 (register_heartbeat_class: a pg_partitioned_table check, a read, then
-- an INSERT or an UPDATE).
CREATE FUNCTION horsies_register_retention_classes(p_days smallint[], p_heartbeat_horizon_hours smallint) RETURNS TABLE(class_key text, class_kind text, outcome text, previous_heartbeat_horizon_hours smallint)
    LANGUAGE plpgsql
    AS $$
#variable_conflict use_column
DECLARE
    v_period_days smallint[];
    v_inserted text[];
    v_key text;
    v_stored smallint;
BEGIN
    v_period_days := horsies_retention_period_days(p_days);
    IF p_heartbeat_horizon_hours IS NULL OR p_heartbeat_horizon_hours NOT BETWEEN 2 AND 48 THEN
        RAISE EXCEPTION 'p_heartbeat_horizon_hours must be from 2 to 48, got %', p_heartbeat_horizon_hours
            USING ERRCODE = 'invalid_parameter_value';
    END IF;

    WITH inserted AS (
        INSERT INTO horsies_retention_classes AS r (
            class_key, class_kind, retention_days, heartbeat_horizon_hours, created_at
        )
        SELECT 'standard_' || d || 'd', 'PERIOD', d, NULL, statement_timestamp()
        FROM unnest(v_period_days) AS d
        ORDER BY d
        ON CONFLICT (class_key) DO NOTHING
        RETURNING r.class_key::text AS class_key
    )
    SELECT COALESCE(array_agg(i.class_key), '{}') INTO v_inserted FROM inserted AS i;
    FOREACH v_key IN ARRAY v_inserted LOOP
        EXECUTE format(
            'CREATE TABLE %I PARTITION OF horsies_task_history FOR VALUES IN (%L) PARTITION BY RANGE (terminal_at)',
            'horsies_task_history_' || v_key, v_key
        );
    END LOOP;

    RETURN QUERY
    SELECT 'standard_' || d || 'd', 'PERIOD'::text,
           CASE WHEN ('standard_' || d || 'd') = ANY(v_inserted) THEN 'REGISTERED' ELSE 'VERIFIED' END,
           NULL::smallint
    FROM unnest(v_period_days) AS d
    ORDER BY d;

    SELECT r.heartbeat_horizon_hours INTO v_stored
    FROM horsies_retention_classes AS r
    WHERE r.class_key = 'heartbeats'
    FOR NO KEY UPDATE;
    IF NOT FOUND THEN
        INSERT INTO horsies_retention_classes AS r (
            class_key, class_kind, retention_days, heartbeat_horizon_hours, created_at
        ) VALUES ('heartbeats', 'HEARTBEAT', NULL, p_heartbeat_horizon_hours, statement_timestamp())
        ON CONFLICT (class_key) DO NOTHING;
        IF FOUND THEN
            RETURN QUERY SELECT 'heartbeats'::text, 'HEARTBEAT'::text, 'REGISTERED'::text, NULL::smallint;
            RETURN;
        END IF;
        -- A concurrent registration inserted the row after the read above: read it under the row lock.
        SELECT r.heartbeat_horizon_hours INTO STRICT v_stored
        FROM horsies_retention_classes AS r
        WHERE r.class_key = 'heartbeats'
        FOR NO KEY UPDATE;
    END IF;
    IF v_stored = p_heartbeat_horizon_hours THEN
        RETURN QUERY SELECT 'heartbeats'::text, 'HEARTBEAT'::text, 'VERIFIED'::text, NULL::smallint;
        RETURN;
    END IF;
    UPDATE horsies_retention_classes AS r
    SET heartbeat_horizon_hours = p_heartbeat_horizon_hours
    WHERE r.class_key = 'heartbeats';
    RETURN QUERY SELECT 'heartbeats'::text, 'HEARTBEAT'::text, 'HORIZON_UPDATED'::text, v_stored;
END
$$;

------------------------------------------------------------------------------------------------------------------------
-- Leaf creation
------------------------------------------------------------------------------------------------------------------------

-- horsies_create_partition_leaf(...): create, verify or repair one daily history leaf or one hourly heartbeat leaf
-- inside the caller's transaction. Internal: horsies_create_history_leaf and horsies_create_heartbeat_leaf validate
-- their input and call it.
--
-- Parameters: p_leaf_kind HISTORY (indexes <leaf>_task_idx (task_id) and <leaf>_enqueued_idx (enqueued_at), then
-- ANALYZE, then the reader publication) or HEARTBEAT (index <leaf>_probe_idx (task_id, role, sent_at DESC); heartbeat
-- leaves are never published); the class, parent, leaf name and bounds.
-- Steps: the leaf state (horsies_history_leaf_state); for ABSENT or INDEX_DAMAGED, the transaction advisory try-lock
-- horsies_task_history_leaf_lock_key(class, lower anchor), the state again, the relation lock NOWAIT (LOCK TABLE ONLY
-- the parent to create, the leaf to repair, so a reader of a sibling leaf does not make it busy), the state again,
-- then the work. Everything after the try-lock runs in a subtransaction with lock_timeout 2 s (set on this
-- function): a lock that is not granted rolls back the work of this call and answers BUSY.
-- Outcomes:
-- - CREATED: the leaf, its catalog row (index shape version 1) and its indexes exist; a history leaf is analyzed and
--   the readers are republished (horsies_publish_history_readers) before the call returns, so no row can land in a
--   leaf that the duplicate guard does not probe.
-- - ALREADY_CONFORMANT: nothing changed.
-- - INDEX_REPAIRED: each damaged index of the leaf was dropped (after a rename that pins it, so a concurrent rename
--   cannot make the drop hit another relation) and created again; a history leaf is analyzed.
-- - BUSY: another transaction holds the leaf lock, a lock on the parent or the leaf, or a lock that this call waited
--   on for 2 s; nothing changed.
-- - CATALOG_CONFLICT with conflict_kind (horsies_history_leaf_state); nothing changed. A conflict is never repaired:
--   it needs an operator.
-- Refusals: 25000 invalid_transaction_state from the reader publication under another isolation than READ
-- COMMITTED. A partition bound that overlaps another relation raises 42P17 from CREATE TABLE.
-- Lock order: the leaf advisory lock; ACCESS EXCLUSIVE on the parent (create) or the leaf (repair); the catalog row
-- (INSERT; KEY SHARE on the class row); the publication lock (history create).
-- Returns: one row.
CREATE FUNCTION horsies_create_partition_leaf(p_leaf_kind text, p_class_key text, p_parent_name text, p_leaf_name text, p_lower_anchor timestamp with time zone, p_upper_anchor timestamp with time zone) RETURNS TABLE(outcome text, conflict_kind text)
    LANGUAGE plpgsql
    SET "TimeZone" TO 'UTC'
    SET lock_timeout TO '2s'
    AS $$
#variable_conflict use_column
DECLARE
    v_state record;
    v_outcome text;
    v_conflict text;
    v_leaf oid;
    v_index record;
    v_temporary text;
BEGIN
    SELECT * INTO STRICT v_state
    FROM horsies_history_leaf_state(p_leaf_kind, p_class_key, p_parent_name, p_leaf_name, 'AT', p_lower_anchor,
                                    p_upper_anchor);
    IF v_state.state NOT IN ('ABSENT', 'INDEX_DAMAGED') THEN
        RETURN QUERY SELECT CASE v_state.state WHEN 'CONFORMANT' THEN 'ALREADY_CONFORMANT' ELSE v_state.state END,
            v_state.conflict_kind;
        RETURN;
    END IF;
    IF NOT pg_try_advisory_xact_lock(horsies_task_history_leaf_lock_key(p_class_key, p_lower_anchor)) THEN
        RETURN QUERY SELECT 'BUSY'::text, NULL::text;
        RETURN;
    END IF;

    BEGIN
        SELECT * INTO STRICT v_state
        FROM horsies_history_leaf_state(p_leaf_kind, p_class_key, p_parent_name, p_leaf_name, 'AT', p_lower_anchor,
                                        p_upper_anchor);
        IF v_state.state = 'ABSENT' THEN
            EXECUTE format('LOCK TABLE ONLY %I IN ACCESS EXCLUSIVE MODE NOWAIT', p_parent_name);
        ELSIF v_state.state = 'INDEX_DAMAGED' THEN
            EXECUTE format('LOCK TABLE ONLY %I IN ACCESS EXCLUSIVE MODE NOWAIT', p_leaf_name);
        END IF;
        SELECT * INTO STRICT v_state
        FROM horsies_history_leaf_state(p_leaf_kind, p_class_key, p_parent_name, p_leaf_name, 'AT', p_lower_anchor,
                                        p_upper_anchor);

        CASE v_state.state
            WHEN 'ABSENT' THEN
                EXECUTE format('CREATE TABLE %I PARTITION OF %I FOR VALUES FROM (%L) TO (%L)',
                               p_leaf_name, p_parent_name, p_lower_anchor, p_upper_anchor);
                INSERT INTO horsies_task_history_leaf_catalog (
                    leaf_name, class_key, lower_bound_kind, lower_anchor, upper_anchor, index_schema_version,
                    created_at, detached_at, dropped_at
                ) VALUES (
                    p_leaf_name, p_class_key, 'AT', p_lower_anchor, p_upper_anchor, 1, statement_timestamp(), NULL,
                    NULL
                );
                v_outcome := 'CREATED';
            WHEN 'INDEX_DAMAGED' THEN
                v_outcome := 'INDEX_REPAIRED';
            WHEN 'CONFORMANT' THEN
                v_outcome := 'ALREADY_CONFORMANT';
            ELSE
                v_outcome := v_state.state;
                v_conflict := v_state.conflict_kind;
        END CASE;

        IF v_outcome IN ('CREATED', 'INDEX_REPAIRED') THEN
            v_leaf := to_regclass(quote_ident(p_leaf_name))::oid;
            FOR v_index IN
                SELECT i.index_name, i.index_columns, i.holder
                FROM horsies_history_leaf_indexes(p_leaf_kind, p_leaf_name) AS i
                WHERE NOT i.conformant
            LOOP
                IF v_index.holder <> 'NONE' THEN
                    -- The state read under the leaf lock found this name on the leaf. The rename locks the index by
                    -- name and pins it; the drop then removes exactly that relation.
                    v_temporary := 'horsies_index_repair_' || replace(gen_random_uuid()::text, '-', '');
                    EXECUTE format('ALTER INDEX %I RENAME TO %I', v_index.index_name, v_temporary);
                    IF NOT EXISTS (
                        SELECT 1 FROM pg_index AS x
                        WHERE x.indexrelid = to_regclass(quote_ident(v_temporary)) AND x.indrelid = v_leaf
                    ) THEN
                        RAISE EXCEPTION 'index name % of leaf % belongs to another relation (changed during this call)',
                            v_index.index_name, p_leaf_name;
                    END IF;
                    EXECUTE format('DROP INDEX %I', v_temporary);
                END IF;
                EXECUTE format('CREATE INDEX %I ON %I USING btree %s', v_index.index_name, p_leaf_name,
                               v_index.index_columns);
            END LOOP;
            IF p_leaf_kind = 'HISTORY' THEN
                EXECUTE format('ANALYZE %I', p_leaf_name);
            END IF;
            IF v_outcome = 'CREATED' AND p_leaf_kind = 'HISTORY' THEN
                PERFORM 1 FROM horsies_publish_history_readers();
            END IF;
        END IF;
    EXCEPTION WHEN lock_not_available THEN
        v_outcome := 'BUSY';
        v_conflict := NULL;
    END;
    RETURN QUERY SELECT v_outcome, v_conflict;
END
$$;

-- horsies_create_history_leaf(p_class_key text, p_lower_anchor timestamptz): create, verify or repair one daily
-- history leaf, in one call inside the caller's transaction (one transaction per leaf).
--
-- PERIOD class: p_lower_anchor is a UTC day start; the leaf horsies_task_history_<class key>_YYYY_MM_DD covers
-- [p_lower_anchor, p_lower_anchor + 1 day) under horsies_task_history_<class key>
-- (horsies_create_partition_leaf). A created leaf is published in the same transaction.
-- FOREVER class: the static leaf horsies_task_history_forever_open (0001_tables.sql) covers every time and is never
-- created or repaired here. p_lower_anchor NULL names that leaf itself (its OPEN lower bound, as the coverage probe
-- reports it): ALREADY_CONFORMANT or CATALOG_CONFLICT. A day start names a daily forever leaf: COVERED_BY_OPEN_ENDED
-- when the static leaf is conformant, else CATALOG_CONFLICT. The conflict kinds of the static leaf are those of
-- horsies_history_leaf_state, plus STATIC_LEAF_MISSING (no catalog row and no relation) and PHYSICAL_NONCONFORMANT
-- for a missing or misshapen index.
-- Outcomes: CREATED, ALREADY_CONFORMANT, INDEX_REPAIRED, COVERED_BY_OPEN_ENDED, BUSY, CATALOG_CONFLICT (with
-- conflict_kind), CLASS_ABSENT (no class row for p_class_key). leaf_name is NULL only for CLASS_ABSENT.
-- Refusals (each raises; nothing changes): 22023 invalid_parameter_value for a NULL class key, a HEARTBEAT class (use
-- horsies_create_heartbeat_leaf), or a PERIOD anchor that is NULL or not a UTC day start; 25000 under another
-- isolation than READ COMMITTED (the publication).
-- Locks and indexes: horsies_create_partition_leaf; horsies_retention_classes_pkey.
-- Returns: one row.
-- Replaces (`v0.7.0` package): core/history/partitions/manager.py:323-367 (create_daily_leaf) and :423-598
-- (_create_or_verify_history_leaf): 24 statements per created leaf (the class read; two state reads of 5 statements
-- each, because the bound read pins and restores the session time zone; the try-lock; savepoint, LOCK and release;
-- CREATE TABLE; the bound capture, 4 statements; the catalog INSERT; two CREATE INDEX; ANALYZE). 0.7.0 publishes once
-- at the end of the pass.
CREATE FUNCTION horsies_create_history_leaf(p_class_key text, p_lower_anchor timestamp with time zone) RETURNS TABLE(outcome text, leaf_name text, conflict_kind text)
    LANGUAGE plpgsql
    SET "TimeZone" TO 'UTC'
    AS $$
#variable_conflict use_column
DECLARE
    v_kind text;
    v_leaf text;
    v_state record;
    v_created record;
BEGIN
    IF p_class_key IS NULL THEN
        RAISE EXCEPTION 'horsies_create_history_leaf needs a class key' USING ERRCODE = 'invalid_parameter_value';
    END IF;
    SELECT r.class_kind INTO v_kind FROM horsies_retention_classes AS r WHERE r.class_key = p_class_key;
    IF NOT FOUND THEN
        RETURN QUERY SELECT 'CLASS_ABSENT'::text, NULL::text, NULL::text;
        RETURN;
    END IF;

    CASE v_kind
        WHEN 'FOREVER' THEN
            IF p_lower_anchor IS NOT NULL AND p_lower_anchor <> date_trunc('day', p_lower_anchor, 'UTC') THEN
                RAISE EXCEPTION 'a forever day must start at a UTC midnight, got %', p_lower_anchor
                    USING ERRCODE = 'invalid_parameter_value';
            END IF;
            SELECT * INTO STRICT v_state
            FROM horsies_history_leaf_state('HISTORY', 'forever', 'horsies_task_history_forever',
                                            'horsies_task_history_forever_open', 'OPEN', NULL,
                                            '9999-01-01 00:00:00+00');
            RETURN QUERY SELECT
                CASE
                    WHEN v_state.state <> 'CONFORMANT' THEN 'CATALOG_CONFLICT'
                    WHEN p_lower_anchor IS NULL THEN 'ALREADY_CONFORMANT'
                    ELSE 'COVERED_BY_OPEN_ENDED'
                END,
                CASE WHEN p_lower_anchor IS NULL THEN 'horsies_task_history_forever_open'
                     ELSE horsies_history_leaf_name('forever', p_lower_anchor) END,
                CASE v_state.state
                    WHEN 'CONFORMANT' THEN NULL
                    WHEN 'CATALOG_CONFLICT' THEN v_state.conflict_kind
                    -- The static leaf is never created or repaired here: a missing leaf or index is a conflict.
                    WHEN 'ABSENT' THEN 'STATIC_LEAF_MISSING'
                    WHEN 'INDEX_DAMAGED' THEN 'PHYSICAL_NONCONFORMANT'
                END;
            RETURN;
        WHEN 'PERIOD' THEN
            IF p_lower_anchor IS NULL OR p_lower_anchor <> date_trunc('day', p_lower_anchor, 'UTC') THEN
                RAISE EXCEPTION 'a daily leaf must start at a UTC midnight, got %', p_lower_anchor
                    USING ERRCODE = 'invalid_parameter_value';
            END IF;
            v_leaf := horsies_history_leaf_name(p_class_key, p_lower_anchor);
            SELECT * INTO STRICT v_created
            FROM horsies_create_partition_leaf('HISTORY', p_class_key, 'horsies_task_history_' || p_class_key, v_leaf,
                                               p_lower_anchor, p_lower_anchor + interval '1 day');
            RETURN QUERY SELECT v_created.outcome, v_leaf, v_created.conflict_kind;
            RETURN;
        WHEN 'HEARTBEAT' THEN
            RAISE EXCEPTION 'class % is the heartbeat class; use horsies_create_heartbeat_leaf', p_class_key
                USING ERRCODE = 'invalid_parameter_value';
        ELSE
            RAISE EXCEPTION 'class % has unknown kind %', p_class_key, v_kind;
    END CASE;
END
$$;

-- horsies_create_heartbeat_leaf(p_lower_anchor timestamptz): create, verify or repair one hourly heartbeat leaf, in
-- one call inside the caller's transaction (one transaction per leaf).
--
-- p_lower_anchor is a UTC hour start; the leaf horsies_heartbeats_YYYY_MM_DD_HH24 covers [p_lower_anchor,
-- p_lower_anchor + 1 hour) under horsies_heartbeats, with the probe index (task_id, role, sent_at DESC) that the
-- stale probes read (horsies_create_partition_leaf). Heartbeat leaves are never published and never analyzed.
-- Outcomes: CREATED, ALREADY_CONFORMANT, INDEX_REPAIRED, BUSY, CATALOG_CONFLICT (with conflict_kind), CLASS_ABSENT
-- (no heartbeat class row; registration comes first).
-- Refusals (each raises; nothing changes): 22023 invalid_parameter_value for an anchor that is NULL or not a UTC
-- hour start.
-- Locks and indexes: horsies_create_partition_leaf; horsies_retention_classes_pkey.
-- Returns: one row.
-- Replaces (`v0.7.0` package): core/history/heartbeats/partitioning.py:319-445 (create_hourly_heartbeat_leaf): 22
-- statements per created leaf (as a history leaf, with one CREATE INDEX and no ANALYZE).
CREATE FUNCTION horsies_create_heartbeat_leaf(p_lower_anchor timestamp with time zone) RETURNS TABLE(outcome text, leaf_name text, conflict_kind text)
    LANGUAGE plpgsql
    SET "TimeZone" TO 'UTC'
    AS $$
#variable_conflict use_column
DECLARE
    v_leaf text;
    v_created record;
BEGIN
    IF p_lower_anchor IS NULL OR p_lower_anchor <> date_trunc('hour', p_lower_anchor, 'UTC') THEN
        RAISE EXCEPTION 'an hourly heartbeat leaf must start at a UTC hour, got %', p_lower_anchor
            USING ERRCODE = 'invalid_parameter_value';
    END IF;
    v_leaf := horsies_heartbeat_leaf_name(p_lower_anchor);
    IF NOT EXISTS (
        SELECT 1 FROM horsies_retention_classes AS r WHERE r.class_key = 'heartbeats' AND r.class_kind = 'HEARTBEAT'
    ) THEN
        RETURN QUERY SELECT 'CLASS_ABSENT'::text, v_leaf, NULL::text;
        RETURN;
    END IF;
    SELECT * INTO STRICT v_created
    FROM horsies_create_partition_leaf('HEARTBEAT', 'heartbeats', 'horsies_heartbeats', v_leaf, p_lower_anchor,
                                       p_lower_anchor + interval '1 hour');
    RETURN QUERY SELECT v_created.outcome, v_leaf, v_created.conflict_kind;
END
$$;

------------------------------------------------------------------------------------------------------------------------
-- Miss classification
------------------------------------------------------------------------------------------------------------------------

-- horsies_terminalization_family(p_kind text): the equivalence class of a terminalization kind. Kinds of one class
-- commit the same effect, so a replay that finds a kind of its own class in history was already applied; a kind of
-- another class is a foreign event:
-- COMPLETE (COMPLETE_LOCKED, COMPLETE_FUSED); FAIL_RUNNING; FAIL_STALE; EXPIRE (EXPIRE_CLAIMED, EXPIRE_PENDING);
-- CANCEL_ADMIN; CANCEL_ORPHAN (CANCEL_ORPHAN, CANCEL_ORPHAN_SWEEP); PAUSE_ABANDON (PAUSE_ABANDON_CLAIM,
-- PAUSE_ABANDON_CLAIM_BATCH, PAUSE_ABANDON_WORKFLOW); WORKFLOW_CANCEL (WORKFLOW_CANCEL_CLAIM,
-- WORKFLOW_CANCEL_CLAIM_BATCH, WORKFLOW_CANCEL_WORKFLOW).
-- Returns: the class name; NULL for a kind outside the 15 kinds.
-- Replaces (`v0.7.0` package): EQUIVALENCE_CLASSES (core/lifecycle/operations.py:87-119), which 0.7.0 renders into each
-- caller's array argument (core/history/terminalization/move.py:39-62). This table is the one owner of the classes in
-- SQL.
CREATE FUNCTION horsies_terminalization_family(p_kind text) RETURNS text
    LANGUAGE sql IMMUTABLE STRICT
    AS $$
    SELECT CASE p_kind
        WHEN 'COMPLETE_LOCKED' THEN 'COMPLETE'
        WHEN 'COMPLETE_FUSED' THEN 'COMPLETE'
        WHEN 'FAIL_RUNNING' THEN 'FAIL_RUNNING'
        WHEN 'FAIL_STALE' THEN 'FAIL_STALE'
        WHEN 'EXPIRE_CLAIMED' THEN 'EXPIRE'
        WHEN 'EXPIRE_PENDING' THEN 'EXPIRE'
        WHEN 'CANCEL_ADMIN' THEN 'CANCEL_ADMIN'
        WHEN 'CANCEL_ORPHAN' THEN 'CANCEL_ORPHAN'
        WHEN 'CANCEL_ORPHAN_SWEEP' THEN 'CANCEL_ORPHAN'
        WHEN 'PAUSE_ABANDON_CLAIM' THEN 'PAUSE_ABANDON'
        WHEN 'PAUSE_ABANDON_CLAIM_BATCH' THEN 'PAUSE_ABANDON'
        WHEN 'PAUSE_ABANDON_WORKFLOW' THEN 'PAUSE_ABANDON'
        WHEN 'WORKFLOW_CANCEL_CLAIM' THEN 'WORKFLOW_CANCEL'
        WHEN 'WORKFLOW_CANCEL_CLAIM_BATCH' THEN 'WORKFLOW_CANCEL'
        WHEN 'WORKFLOW_CANCEL_WORKFLOW' THEN 'WORKFLOW_CANCEL'
    END
$$;

-- horsies_terminalization_miss(p_task_id uuid, p_kind text, p_fence_kind text, p_worker_id text,
-- p_claimed_at timestamptz): the one outcome of a terminal write of kind p_kind that did not apply. Internal: the
-- terminal writes call it, in their transaction, after their own fenced read missed.
--
-- p_fence_kind names the caller's fence, a closed set:
-- - NONE: the caller fences on no claim (p_worker_id and p_claimed_at NULL);
-- - OWNER: the caller fences on the worker only (p_worker_id given, p_claimed_at NULL);
-- - OWNER_AND_GENERATION: the caller fences on the worker and the claim generation (both given).
-- No NULL argument carries a meaning: the fence kind decides the fence.
--
-- Outcomes (horsies_terminalization_outcome rows, one row), in the order they are decided:
-- - TASK_ABSENT: no live row and no history row (published provenance reader).
-- - ALREADY_APPLIED: history holds the task with a kind of p_kind's class (horsies_terminalization_family); the row
--   carries its terminal time, kind and status.
-- - SOURCE_STATE_CONFLICT with guard_kind FOREIGN_TERMINALIZATION: history holds the task with a kind of another
--   class.
-- - LOST_CLAIM: the live row exists and the fence does not hold: OWNER, the row's claim owner differs from
--   p_worker_id; OWNER_AND_GENERATION, the owner differs or the row's claimed_at differs from p_claimed_at.
-- - SOURCE_STATE_CONFLICT: the live row exists in another state and the fence holds (always for NONE); the row
--   carries its status and claim.
-- History is read through the published reader horsies_task_provenance_staged(uuid), never through the parent. The
-- live row is read FOR UPDATE.
-- Refusals: 22023 invalid_parameter_value for a NULL task id, a kind outside the 15 kinds, a fence kind outside the
-- three, or a worker or claim generation that does not fit the fence kind. A missing provenance reader raises 42883
-- undefined_function: the readers are published at install and by every coverage pass.
-- Lock order: the live row FOR UPDATE (the caller holds the task advisory lock).
-- Indexes: horsies_tasks_pkey; the leaf task_id indexes (provenance reader).
-- Returns: one row.
-- Replaces (`v0.7.0` package): core/history/terminalization/outcome.py:43-114 (MISS_CLASSIFIER_DDL). Changes from it:
-- the caller passes its kind and this function owns the equivalence classes, instead of each caller writing the class
-- as an array literal; the caller names its fence kind, instead of a NULL argument turning a part of the fence off; the
-- provenance reader takes no include_live argument and has no live probe, so the LIVE-after-miss refusal has nothing to
-- check.
CREATE FUNCTION horsies_terminalization_miss(p_task_id uuid, p_kind text, p_fence_kind text, p_worker_id text, p_claimed_at timestamp with time zone) RETURNS SETOF horsies_terminalization_outcome
    LANGUAGE plpgsql
    AS $$
DECLARE
    v_family text;
    v_row horsies_tasks%ROWTYPE;
    v_provenance record;
    v_held boolean;
BEGIN
    v_family := horsies_terminalization_family(p_kind);
    IF p_task_id IS NULL OR v_family IS NULL THEN
        RAISE EXCEPTION 'horsies_terminalization_miss needs a task id and one of the 15 kinds, got % and %',
            p_task_id, p_kind
            USING ERRCODE = 'invalid_parameter_value';
    END IF;
    CASE p_fence_kind
        WHEN 'NONE' THEN
            IF num_nulls(p_worker_id, p_claimed_at) <> 2 THEN
                RAISE EXCEPTION 'fence NONE takes no worker and no claim generation'
                    USING ERRCODE = 'invalid_parameter_value';
            END IF;
        WHEN 'OWNER' THEN
            IF p_worker_id IS NULL OR p_claimed_at IS NOT NULL THEN
                RAISE EXCEPTION 'fence OWNER takes a worker and no claim generation'
                    USING ERRCODE = 'invalid_parameter_value';
            END IF;
        WHEN 'OWNER_AND_GENERATION' THEN
            IF num_nulls(p_worker_id, p_claimed_at) <> 0 THEN
                RAISE EXCEPTION 'fence OWNER_AND_GENERATION takes a worker and a claim generation'
                    USING ERRCODE = 'invalid_parameter_value';
            END IF;
        ELSE
            RAISE EXCEPTION 'unknown fence kind %', p_fence_kind
                USING ERRCODE = 'invalid_parameter_value';
    END CASE;

    SELECT * INTO v_row
    FROM horsies_tasks AS t
    WHERE t.id = p_task_id
    FOR UPDATE;

    IF NOT FOUND THEN
        SELECT p.* INTO v_provenance FROM horsies_task_provenance_staged(p_task_id) AS p;
        IF NOT FOUND THEN
            RETURN QUERY SELECT
                p_task_id, NULL::bigint, 'TASK_ABSENT'::text,
                NULL::timestamptz, NULL::text,
                NULL::text, NULL::varchar, NULL::timestamptz,
                NULL::text, NULL::jsonb;
            RETURN;
        END IF;
        IF horsies_terminalization_family(v_provenance.terminalization_kind) IS NOT DISTINCT FROM v_family THEN
            RETURN QUERY SELECT
                p_task_id, NULL::bigint, 'ALREADY_APPLIED'::text,
                v_provenance.terminal_at, v_provenance.terminalization_kind,
                v_provenance.status, NULL::varchar, NULL::timestamptz,
                NULL::text, NULL::jsonb;
            RETURN;
        END IF;
        RETURN QUERY SELECT
            p_task_id, NULL::bigint, 'SOURCE_STATE_CONFLICT'::text,
            v_provenance.terminal_at, v_provenance.terminalization_kind,
            v_provenance.status, NULL::varchar, NULL::timestamptz,
            'FOREIGN_TERMINALIZATION'::text, NULL::jsonb;
        RETURN;
    END IF;

    v_held := CASE p_fence_kind
        WHEN 'NONE' THEN TRUE
        WHEN 'OWNER' THEN v_row.claimed_by_worker_id IS NOT DISTINCT FROM p_worker_id::varchar
        WHEN 'OWNER_AND_GENERATION' THEN
            v_row.claimed_by_worker_id IS NOT DISTINCT FROM p_worker_id::varchar
            AND v_row.claimed_at IS NOT DISTINCT FROM p_claimed_at
    END;
    IF NOT v_held THEN
        RETURN QUERY SELECT
            p_task_id, NULL::bigint, 'LOST_CLAIM'::text,
            NULL::timestamptz, NULL::text,
            v_row.status::text, v_row.claimed_by_worker_id, v_row.claimed_at,
            NULL::text, NULL::jsonb;
        RETURN;
    END IF;

    RETURN QUERY SELECT
        p_task_id, NULL::bigint, 'SOURCE_STATE_CONFLICT'::text,
        NULL::timestamptz, NULL::text,
        v_row.status::text, v_row.claimed_by_worker_id, v_row.claimed_at,
        NULL::text, NULL::jsonb;
END
$$;
