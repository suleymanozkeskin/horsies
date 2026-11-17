-- The reference side of the dual-run in test_history_readers.py: the statements that StagedLoaderPublisher.republish
-- sent (horsies/core/history/reads/publisher.py and lookup_generation.py at commit abdc49d4) for a catalog of two
-- attached history leaves: horsies_task_history_forever_open and horsies_task_history_standard_30d_2030_01_01. The
-- renderer output is copied as it was rendered.
--
-- The file runs after interim_miss_classifier.sql (the composite types that the readers return), on a database whose
-- readers were never published. Do not edit a statement: the file is the record of the statements before the switch.

CREATE OR REPLACE FUNCTION horsies_task_lookup_staged(p_task_id uuid)
    RETURNS horsies_task_lookup
    LANGUAGE plpgsql
    STABLE
    SET enable_seqscan = off
    AS $function$
    DECLARE
        v_task_id uuid;
        v_fingerprint_version smallint;
        v_fingerprint bytea;
        v_uuid_bytes bytea;
        v_birth_milliseconds bigint;
        v_birth_at timestamptz;
        v_effective_birth timestamptz;
    BEGIN

        SELECT id, command_fingerprint_version, command_fingerprint
        INTO v_task_id, v_fingerprint_version, v_fingerprint
        FROM horsies_tasks
        WHERE id = p_task_id;
        IF FOUND THEN
            RETURN ROW(
                TRUE, 'LIVE', v_task_id,
                v_fingerprint_version, v_fingerprint
            )::horsies_task_lookup;
        END IF;


        v_uuid_bytes := uuid_send(p_task_id);
        IF (get_byte(v_uuid_bytes, 6) >> 4) = 7
           AND (get_byte(v_uuid_bytes, 8) & 192) = 128
        THEN
            v_birth_milliseconds :=
                (get_byte(v_uuid_bytes, 0)::bigint << 40)
                | (get_byte(v_uuid_bytes, 1)::bigint << 32)
                | (get_byte(v_uuid_bytes, 2)::bigint << 24)
                | (get_byte(v_uuid_bytes, 3)::bigint << 16)
                | (get_byte(v_uuid_bytes, 4)::bigint << 8)
                | get_byte(v_uuid_bytes, 5)::bigint;
            v_birth_at := to_timestamp(
                v_birth_milliseconds::double precision / 1000.0
            );
            v_effective_birth :=
                v_birth_at - INTERVAL '5 seconds';

            IF v_effective_birth < TIMESTAMPTZ '9999-01-01T00:00:00Z' THEN

        SELECT task_id, command_fingerprint_version, command_fingerprint
        INTO v_task_id, v_fingerprint_version, v_fingerprint
        FROM horsies_task_history_forever_open
        WHERE task_id = p_task_id;
        IF FOUND THEN
            RETURN ROW(
                TRUE, 'HISTORY', v_task_id,
                v_fingerprint_version, v_fingerprint
            )::horsies_task_lookup;
        END IF;

            END IF;


            IF v_effective_birth < TIMESTAMPTZ '2030-01-02T00:00:00Z' THEN

        SELECT task_id, command_fingerprint_version, command_fingerprint
        INTO v_task_id, v_fingerprint_version, v_fingerprint
        FROM horsies_task_history_standard_30d_2030_01_01
        WHERE task_id = p_task_id;
        IF FOUND THEN
            RETURN ROW(
                TRUE, 'HISTORY', v_task_id,
                v_fingerprint_version, v_fingerprint
            )::horsies_task_lookup;
        END IF;

            END IF;

            -- Birth time is an optimization hint, not an integrity
            -- constraint. Probe every leaf skipped above before declaring
            -- absence so a caller-clock violation cannot hide a retained row.

            IF v_effective_birth >= TIMESTAMPTZ '2030-01-02T00:00:00Z' THEN

        SELECT task_id, command_fingerprint_version, command_fingerprint
        INTO v_task_id, v_fingerprint_version, v_fingerprint
        FROM horsies_task_history_standard_30d_2030_01_01
        WHERE task_id = p_task_id;
        IF FOUND THEN
            RETURN ROW(
                TRUE, 'HISTORY', v_task_id,
                v_fingerprint_version, v_fingerprint
            )::horsies_task_lookup;
        END IF;

            END IF;

        ELSE

        SELECT task_id, command_fingerprint_version, command_fingerprint
        INTO v_task_id, v_fingerprint_version, v_fingerprint
        FROM horsies_task_history_standard_30d_2030_01_01
        WHERE task_id = p_task_id;
        IF FOUND THEN
            RETURN ROW(
                TRUE, 'HISTORY', v_task_id,
                v_fingerprint_version, v_fingerprint
            )::horsies_task_lookup;
        END IF;


        SELECT task_id, command_fingerprint_version, command_fingerprint
        INTO v_task_id, v_fingerprint_version, v_fingerprint
        FROM horsies_task_history_forever_open
        WHERE task_id = p_task_id;
        IF FOUND THEN
            RETURN ROW(
                TRUE, 'HISTORY', v_task_id,
                v_fingerprint_version, v_fingerprint
            )::horsies_task_lookup;
        END IF;

        END IF;

        RETURN ROW(FALSE, NULL, NULL, NULL, NULL)::horsies_task_lookup;
    END
    $function$;

DROP FUNCTION IF EXISTS horsies_task_provenance_staged(uuid);

CREATE OR REPLACE FUNCTION horsies_task_provenance_staged(p_task_id uuid, p_include_live boolean DEFAULT TRUE)
    RETURNS horsies_task_provenance
    LANGUAGE plpgsql
    STABLE
    SET enable_seqscan = off
    AS $function$
    DECLARE
        v_task_id uuid;
        v_status text;
        v_terminal_at timestamptz;
        v_kind text;
        v_uuid_bytes bytea;
        v_birth_milliseconds bigint;
        v_birth_at timestamptz;
        v_effective_birth timestamptz;
    BEGIN

        IF p_include_live THEN

        SELECT id INTO v_task_id
        FROM horsies_tasks
        WHERE id = p_task_id;
        IF FOUND THEN
            RETURN ROW(
                TRUE, 'LIVE', v_task_id, NULL, NULL, NULL
            )::horsies_task_provenance;
        END IF;

        END IF;


        v_uuid_bytes := uuid_send(p_task_id);
        IF (get_byte(v_uuid_bytes, 6) >> 4) = 7
           AND (get_byte(v_uuid_bytes, 8) & 192) = 128
        THEN
            v_birth_milliseconds :=
                (get_byte(v_uuid_bytes, 0)::bigint << 40)
                | (get_byte(v_uuid_bytes, 1)::bigint << 32)
                | (get_byte(v_uuid_bytes, 2)::bigint << 24)
                | (get_byte(v_uuid_bytes, 3)::bigint << 16)
                | (get_byte(v_uuid_bytes, 4)::bigint << 8)
                | get_byte(v_uuid_bytes, 5)::bigint;
            v_birth_at := to_timestamp(
                v_birth_milliseconds::double precision / 1000.0
            );
            v_effective_birth :=
                v_birth_at - INTERVAL '5 seconds';

            IF v_effective_birth < TIMESTAMPTZ '9999-01-01T00:00:00Z' THEN

        SELECT task_id, status, terminal_at, terminalization_kind
        INTO v_task_id, v_status, v_terminal_at, v_kind
        FROM horsies_task_history_forever_open
        WHERE task_id = p_task_id;
        IF FOUND THEN
            RETURN ROW(
                TRUE, 'HISTORY', v_task_id,
                v_status, v_terminal_at, v_kind
            )::horsies_task_provenance;
        END IF;

            END IF;


            IF v_effective_birth < TIMESTAMPTZ '2030-01-02T00:00:00Z' THEN

        SELECT task_id, status, terminal_at, terminalization_kind
        INTO v_task_id, v_status, v_terminal_at, v_kind
        FROM horsies_task_history_standard_30d_2030_01_01
        WHERE task_id = p_task_id;
        IF FOUND THEN
            RETURN ROW(
                TRUE, 'HISTORY', v_task_id,
                v_status, v_terminal_at, v_kind
            )::horsies_task_provenance;
        END IF;

            END IF;

            -- Birth time is an optimization hint, not an integrity
            -- constraint. Probe every leaf skipped above before declaring
            -- absence so a caller-clock violation cannot hide a retained row.

            IF v_effective_birth >= TIMESTAMPTZ '2030-01-02T00:00:00Z' THEN

        SELECT task_id, status, terminal_at, terminalization_kind
        INTO v_task_id, v_status, v_terminal_at, v_kind
        FROM horsies_task_history_standard_30d_2030_01_01
        WHERE task_id = p_task_id;
        IF FOUND THEN
            RETURN ROW(
                TRUE, 'HISTORY', v_task_id,
                v_status, v_terminal_at, v_kind
            )::horsies_task_provenance;
        END IF;

            END IF;

        ELSE

        SELECT task_id, status, terminal_at, terminalization_kind
        INTO v_task_id, v_status, v_terminal_at, v_kind
        FROM horsies_task_history_standard_30d_2030_01_01
        WHERE task_id = p_task_id;
        IF FOUND THEN
            RETURN ROW(
                TRUE, 'HISTORY', v_task_id,
                v_status, v_terminal_at, v_kind
            )::horsies_task_provenance;
        END IF;


        SELECT task_id, status, terminal_at, terminalization_kind
        INTO v_task_id, v_status, v_terminal_at, v_kind
        FROM horsies_task_history_forever_open
        WHERE task_id = p_task_id;
        IF FOUND THEN
            RETURN ROW(
                TRUE, 'HISTORY', v_task_id,
                v_status, v_terminal_at, v_kind
            )::horsies_task_provenance;
        END IF;

        END IF;

        RETURN ROW(FALSE, NULL, NULL, NULL, NULL, NULL)::horsies_task_provenance;
    END
    $function$;

CREATE OR REPLACE FUNCTION horsies_task_detail_staged(p_task_id uuid)
    RETURNS TABLE (location text, task_row horsies_task_history)
    LANGUAGE plpgsql
    STABLE
    SET enable_seqscan = off
    AS $function$
    DECLARE
        v_row horsies_task_history%ROWTYPE;
        v_uuid_bytes bytea;
        v_birth_milliseconds bigint;
        v_birth_at timestamptz;
        v_effective_birth timestamptz;
    BEGIN

        IF EXISTS (SELECT 1 FROM horsies_tasks WHERE id = p_task_id) THEN
            RETURN QUERY SELECT
                'LIVE'::text, NULL::horsies_task_history;
            RETURN;
        END IF;


        v_uuid_bytes := uuid_send(p_task_id);
        IF (get_byte(v_uuid_bytes, 6) >> 4) = 7
           AND (get_byte(v_uuid_bytes, 8) & 192) = 128
        THEN
            v_birth_milliseconds :=
                (get_byte(v_uuid_bytes, 0)::bigint << 40)
                | (get_byte(v_uuid_bytes, 1)::bigint << 32)
                | (get_byte(v_uuid_bytes, 2)::bigint << 24)
                | (get_byte(v_uuid_bytes, 3)::bigint << 16)
                | (get_byte(v_uuid_bytes, 4)::bigint << 8)
                | get_byte(v_uuid_bytes, 5)::bigint;
            v_birth_at := to_timestamp(
                v_birth_milliseconds::double precision / 1000.0
            );
            v_effective_birth :=
                v_birth_at - INTERVAL '5 seconds';

            IF v_effective_birth < TIMESTAMPTZ '9999-01-01T00:00:00Z' THEN

        SELECT h.* INTO v_row FROM horsies_task_history_forever_open h WHERE h.task_id = p_task_id;
        IF FOUND THEN
            RETURN QUERY SELECT 'HISTORY'::text, v_row;
            RETURN;
        END IF;

            END IF;


            IF v_effective_birth < TIMESTAMPTZ '2030-01-02T00:00:00Z' THEN

        SELECT h.* INTO v_row FROM horsies_task_history_standard_30d_2030_01_01 h WHERE h.task_id = p_task_id;
        IF FOUND THEN
            RETURN QUERY SELECT 'HISTORY'::text, v_row;
            RETURN;
        END IF;

            END IF;

            -- Birth time is an optimization hint, not an integrity
            -- constraint. Probe every leaf skipped above before declaring
            -- absence so a caller-clock violation cannot hide a retained row.

            IF v_effective_birth >= TIMESTAMPTZ '2030-01-02T00:00:00Z' THEN

        SELECT h.* INTO v_row FROM horsies_task_history_standard_30d_2030_01_01 h WHERE h.task_id = p_task_id;
        IF FOUND THEN
            RETURN QUERY SELECT 'HISTORY'::text, v_row;
            RETURN;
        END IF;

            END IF;

        ELSE

        SELECT h.* INTO v_row FROM horsies_task_history_standard_30d_2030_01_01 h WHERE h.task_id = p_task_id;
        IF FOUND THEN
            RETURN QUERY SELECT 'HISTORY'::text, v_row;
            RETURN;
        END IF;


        SELECT h.* INTO v_row FROM horsies_task_history_forever_open h WHERE h.task_id = p_task_id;
        IF FOUND THEN
            RETURN QUERY SELECT 'HISTORY'::text, v_row;
            RETURN;
        END IF;

        END IF;

        RETURN;
    END
    $function$;

CREATE OR REPLACE FUNCTION horsies_task_result_staged(p_task_id uuid)
    RETURNS TABLE (location text, status text, live_result text, failed_reason text, result_envelope_version smallint, result_payload bytea, result_digest bytea, task_name text)
    LANGUAGE plpgsql
    STABLE
    SET enable_seqscan = off
    AS $function$
    DECLARE

        v_uuid_bytes bytea;
        v_birth_milliseconds bigint;
        v_birth_at timestamptz;
        v_effective_birth timestamptz;
    BEGIN

        RETURN QUERY SELECT
            'LIVE'::text, t.status::text, NULL::text,
            NULL::text, NULL::smallint,
            NULL::bytea, NULL::bytea, t.task_name::text
        FROM horsies_tasks t WHERE t.id = p_task_id;
        IF FOUND THEN
            RETURN;
        END IF;


        v_uuid_bytes := uuid_send(p_task_id);
        IF (get_byte(v_uuid_bytes, 6) >> 4) = 7
           AND (get_byte(v_uuid_bytes, 8) & 192) = 128
        THEN
            v_birth_milliseconds :=
                (get_byte(v_uuid_bytes, 0)::bigint << 40)
                | (get_byte(v_uuid_bytes, 1)::bigint << 32)
                | (get_byte(v_uuid_bytes, 2)::bigint << 24)
                | (get_byte(v_uuid_bytes, 3)::bigint << 16)
                | (get_byte(v_uuid_bytes, 4)::bigint << 8)
                | get_byte(v_uuid_bytes, 5)::bigint;
            v_birth_at := to_timestamp(
                v_birth_milliseconds::double precision / 1000.0
            );
            v_effective_birth :=
                v_birth_at - INTERVAL '5 seconds';

            IF v_effective_birth < TIMESTAMPTZ '9999-01-01T00:00:00Z' THEN

        RETURN QUERY SELECT
            'HISTORY'::text, h.status::text, NULL::text,
            h.final_failed_reason::text, h.result_envelope_version,
            h.result_payload, h.result_digest, h.task_name::text
        FROM horsies_task_history_forever_open h WHERE h.task_id = p_task_id;
        IF FOUND THEN
            RETURN;
        END IF;

            END IF;


            IF v_effective_birth < TIMESTAMPTZ '2030-01-02T00:00:00Z' THEN

        RETURN QUERY SELECT
            'HISTORY'::text, h.status::text, NULL::text,
            h.final_failed_reason::text, h.result_envelope_version,
            h.result_payload, h.result_digest, h.task_name::text
        FROM horsies_task_history_standard_30d_2030_01_01 h WHERE h.task_id = p_task_id;
        IF FOUND THEN
            RETURN;
        END IF;

            END IF;

            -- Birth time is an optimization hint, not an integrity
            -- constraint. Probe every leaf skipped above before declaring
            -- absence so a caller-clock violation cannot hide a retained row.

            IF v_effective_birth >= TIMESTAMPTZ '2030-01-02T00:00:00Z' THEN

        RETURN QUERY SELECT
            'HISTORY'::text, h.status::text, NULL::text,
            h.final_failed_reason::text, h.result_envelope_version,
            h.result_payload, h.result_digest, h.task_name::text
        FROM horsies_task_history_standard_30d_2030_01_01 h WHERE h.task_id = p_task_id;
        IF FOUND THEN
            RETURN;
        END IF;

            END IF;

        ELSE

        RETURN QUERY SELECT
            'HISTORY'::text, h.status::text, NULL::text,
            h.final_failed_reason::text, h.result_envelope_version,
            h.result_payload, h.result_digest, h.task_name::text
        FROM horsies_task_history_standard_30d_2030_01_01 h WHERE h.task_id = p_task_id;
        IF FOUND THEN
            RETURN;
        END IF;


        RETURN QUERY SELECT
            'HISTORY'::text, h.status::text, NULL::text,
            h.final_failed_reason::text, h.result_envelope_version,
            h.result_payload, h.result_digest, h.task_name::text
        FROM horsies_task_history_forever_open h WHERE h.task_id = p_task_id;
        IF FOUND THEN
            RETURN;
        END IF;

        END IF;

        RETURN;
    END
    $function$;

CREATE OR REPLACE FUNCTION horsies_task_history_duplicate_staged(p_task_id uuid)
    RETURNS boolean
    LANGUAGE plpgsql
    STABLE
    SET enable_seqscan = off
    AS $function$
    DECLARE

        v_uuid_bytes bytea;
        v_birth_milliseconds bigint;
        v_birth_at timestamptz;
        v_effective_birth timestamptz;
    BEGIN


        v_uuid_bytes := uuid_send(p_task_id);
        IF (get_byte(v_uuid_bytes, 6) >> 4) = 7
           AND (get_byte(v_uuid_bytes, 8) & 192) = 128
        THEN
            v_birth_milliseconds :=
                (get_byte(v_uuid_bytes, 0)::bigint << 40)
                | (get_byte(v_uuid_bytes, 1)::bigint << 32)
                | (get_byte(v_uuid_bytes, 2)::bigint << 24)
                | (get_byte(v_uuid_bytes, 3)::bigint << 16)
                | (get_byte(v_uuid_bytes, 4)::bigint << 8)
                | get_byte(v_uuid_bytes, 5)::bigint;
            v_birth_at := to_timestamp(
                v_birth_milliseconds::double precision / 1000.0
            );
            v_effective_birth :=
                v_birth_at - INTERVAL '5 seconds';

            IF v_effective_birth < TIMESTAMPTZ '9999-01-01T00:00:00Z' THEN

        PERFORM 1 FROM horsies_task_history_forever_open h WHERE h.task_id = p_task_id;
        IF FOUND THEN
            RETURN TRUE;
        END IF;

            END IF;


            IF v_effective_birth < TIMESTAMPTZ '2030-01-02T00:00:00Z' THEN

        PERFORM 1 FROM horsies_task_history_standard_30d_2030_01_01 h WHERE h.task_id = p_task_id;
        IF FOUND THEN
            RETURN TRUE;
        END IF;

            END IF;


        ELSE

        PERFORM 1 FROM horsies_task_history_standard_30d_2030_01_01 h WHERE h.task_id = p_task_id;
        IF FOUND THEN
            RETURN TRUE;
        END IF;


        PERFORM 1 FROM horsies_task_history_forever_open h WHERE h.task_id = p_task_id;
        IF FOUND THEN
            RETURN TRUE;
        END IF;

        END IF;

        RETURN EXISTS (SELECT 1 FROM horsies_task_history_clock_exceptions WHERE task_id = p_task_id);
    END
    $function$;

DELETE FROM horsies_task_lookup_manifest;

INSERT INTO horsies_task_lookup_manifest (
    leaf_name, probe_position, lower_bound_kind, lower_anchor, upper_anchor, published_at
)
VALUES
    ('horsies_task_history_forever_open', 0, 'OPEN', NULL, '9999-01-01 00:00:00+00', statement_timestamp()),
    ('horsies_task_history_standard_30d_2030_01_01', 1, 'AT', '2030-01-01 00:00:00+00', '2030-01-02 00:00:00+00',
     statement_timestamp());

DO $stamp$
DECLARE
    manifest_stamp text := (
    SELECT 'horsies:manifest:' || encode(sha256(convert_to(COALESCE(
        string_agg(
            concat_ws(
                '|',
                manifest.leaf_name,
                manifest.probe_position,
                manifest.lower_bound_kind,
                COALESCE(extract(epoch FROM manifest.lower_anchor)::text, '-'),
                extract(epoch FROM manifest.upper_anchor)
            ),
            ',' ORDER BY manifest.probe_position
        ),
        ''
    ), 'UTF8')), 'hex')
    FROM horsies_task_lookup_manifest AS manifest
);
    signature text;
BEGIN
    FOREACH signature IN ARRAY CAST(ARRAY['horsies_task_lookup_staged(uuid)', 'horsies_task_provenance_staged(uuid, boolean)', 'horsies_task_detail_staged(uuid)', 'horsies_task_result_staged(uuid)', 'horsies_task_history_duplicate_staged(uuid)'] AS text[]) LOOP
        EXECUTE format(
            'COMMENT ON FUNCTION %s IS %L', signature, manifest_stamp
        );
    END LOOP;
END
$stamp$;
