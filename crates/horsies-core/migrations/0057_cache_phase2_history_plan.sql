DO $migration$
BEGIN
    IF to_regprocedure('horsies_phase2_consume(uuid,text)') IS NULL THEN
        RETURN;
    END IF;

    ALTER FUNCTION horsies_phase2_consume(uuid,text)
        SET plan_cache_mode = force_generic_plan;
END
$migration$;
