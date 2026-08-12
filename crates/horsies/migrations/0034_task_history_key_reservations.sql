-- Schema v28: final reservation registry and its owning program.
CREATE TABLE horsies_key_reservations (
    idempotency_key_digest bytea PRIMARY KEY,
    key_scope_version smallint NOT NULL CHECK (key_scope_version > 0),
    fingerprint_version smallint NOT NULL CHECK (fingerprint_version > 0),
    command_fingerprint bytea NOT NULL
        CHECK (octet_length(command_fingerprint) = 32),
    task_id uuid NOT NULL,
    disposition text NOT NULL CHECK (disposition IN ('LIVE', 'TERMINAL')),
    reservation_window interval NOT NULL CHECK (
        reservation_window > interval '0'
        AND reservation_window <= interval '30 days'
    ),
    expires_at timestamptz,
    CHECK (octet_length(idempotency_key_digest) = 32),
    CHECK (
        (disposition = 'LIVE' AND expires_at IS NULL)
        OR (disposition = 'TERMINAL' AND expires_at IS NOT NULL)
    )
);
CREATE TYPE horsies_key_reservation_outcome AS (
    outcome text,
    task_id uuid,
    observed_fingerprint_version smallint
);
CREATE FUNCTION horsies_key_reservation_claim(
    p_key_digest bytea,
    p_key_scope_version smallint,
    p_reservation_window interval,
    p_fingerprint_version smallint,
    p_fingerprint bytea,
    p_task_id uuid
) RETURNS horsies_key_reservation_outcome
LANGUAGE plpgsql
AS $function$
DECLARE
    v_task_id uuid;
    v_fingerprint_version smallint;
    v_fingerprint bytea;
BEGIN
    IF octet_length(p_key_digest) <> 32 THEN
        RAISE EXCEPTION USING ERRCODE = 'invalid_parameter_value',
            MESSAGE = 'key digest must be 32 bytes';
    END IF;
    IF octet_length(p_fingerprint) <> 32 THEN
        RAISE EXCEPTION USING ERRCODE = 'invalid_parameter_value',
            MESSAGE = 'fingerprint must be 32 bytes';
    END IF;
    IF p_reservation_window <= interval '0'
       OR p_reservation_window > interval '30 days' THEN
        RAISE EXCEPTION USING ERRCODE = 'invalid_parameter_value',
            MESSAGE = 'reservation window must be positive and at most 30 days';
    END IF;

    SELECT task_id, fingerprint_version, command_fingerprint
    INTO v_task_id, v_fingerprint_version, v_fingerprint
    FROM horsies_key_reservations
    WHERE idempotency_key_digest = p_key_digest
      AND (disposition = 'LIVE' OR expires_at > statement_timestamp())
    FOR UPDATE;
    IF FOUND THEN
        IF v_fingerprint = p_fingerprint
           AND v_fingerprint_version = p_fingerprint_version THEN
            RETURN ROW('REPLAY', v_task_id, v_fingerprint_version)
                ::horsies_key_reservation_outcome;
        END IF;
        RETURN ROW('CONFLICT', v_task_id, v_fingerprint_version)
            ::horsies_key_reservation_outcome;
    END IF;

    DELETE FROM horsies_key_reservations
    WHERE idempotency_key_digest = p_key_digest
      AND disposition = 'TERMINAL'
      AND expires_at <= statement_timestamp();

    INSERT INTO horsies_key_reservations (
        idempotency_key_digest, key_scope_version,
        fingerprint_version, command_fingerprint, task_id,
        disposition, reservation_window, expires_at
    ) VALUES (
        p_key_digest, p_key_scope_version,
        p_fingerprint_version, p_fingerprint, p_task_id,
        'LIVE', p_reservation_window, NULL
    );
    RETURN ROW('APPLIED', p_task_id, NULL)
        ::horsies_key_reservation_outcome;
END
$function$;
CREATE FUNCTION horsies_key_reservation_terminalize(
    p_key_digest bytea,
    p_task_id uuid,
    p_terminal_at timestamptz
) RETURNS boolean
LANGUAGE plpgsql
AS $function$
DECLARE
    v_updated integer;
BEGIN
    UPDATE horsies_key_reservations
    SET disposition = 'TERMINAL',
        expires_at = p_terminal_at + reservation_window
    WHERE idempotency_key_digest = p_key_digest
      AND task_id = p_task_id
      AND disposition = 'LIVE';
    GET DIAGNOSTICS v_updated = ROW_COUNT;
    RETURN v_updated = 1;
END
$function$;
CREATE FUNCTION horsies_key_reservation_terminalize_batch(
    p_key_digests bytea[],
    p_task_ids uuid[],
    p_terminal_at timestamptz
) RETURNS integer
LANGUAGE plpgsql
AS $function$
DECLARE
    v_updated integer;
BEGIN
    IF cardinality(p_key_digests) <> cardinality(p_task_ids) THEN
        RAISE EXCEPTION USING ERRCODE = 'invalid_parameter_value',
            MESSAGE = 'digest and task arrays must pair element-wise';
    END IF;
    UPDATE horsies_key_reservations r
    SET disposition = 'TERMINAL',
        expires_at = p_terminal_at + r.reservation_window
    FROM unnest(p_key_digests, p_task_ids) AS pair(key_digest, task_id)
    WHERE r.idempotency_key_digest = pair.key_digest
      AND r.task_id = pair.task_id
      AND r.disposition = 'LIVE';
    GET DIAGNOSTICS v_updated = ROW_COUNT;
    RETURN v_updated;
END
$function$;
CREATE FUNCTION horsies_key_reservation_cleanup(
    p_batch_size integer
) RETURNS integer
LANGUAGE plpgsql
AS $function$
DECLARE
    v_deleted integer;
BEGIN
    IF p_batch_size <= 0 THEN
        RAISE EXCEPTION USING ERRCODE = 'invalid_parameter_value',
            MESSAGE = 'cleanup batch size must be positive';
    END IF;
    WITH targets AS (
        SELECT idempotency_key_digest
        FROM horsies_key_reservations
        WHERE disposition = 'TERMINAL'
          AND expires_at <= statement_timestamp()
        ORDER BY expires_at
        LIMIT p_batch_size
        FOR UPDATE SKIP LOCKED
    )
    DELETE FROM horsies_key_reservations AS reservations
    USING targets
    WHERE reservations.idempotency_key_digest = targets.idempotency_key_digest;
    GET DIAGNOSTICS v_deleted = ROW_COUNT;
    RETURN v_deleted;
END
$function$;
