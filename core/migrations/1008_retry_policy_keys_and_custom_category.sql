-- horsies schema migration 1008: the retry policy keys intervals_seconds and retry_for, and the code category CUSTOM.
--
-- The install applies this text after the migration before it in the chain (MIGRATION_SOURCES,
-- core/src/broker/migrations.rs), in one READ COMMITTED transaction with its bookkeeping row. A database that applied
-- an older text of migration 1008 is refused by the checksum.
--
-- Changes:
-- - The retry policy of a workflow node: the four validation functions of core/baseline/0005_workflow_functions.sql
--   read the keys intervals_seconds and retry_for of the stored retry_policy object, in place of intervals and
--   auto_retry_for, and their validation texts name the new keys: horsies_workflow_retry_interval_entries,
--   horsies_workflow_retry_code_entries, horsies_workflow_retry_policy_entries and
--   horsies_workflow_retry_policy_value_error. The clients write the new keys into task_options from this build on.
-- - The monitoring code category of a custom code: the five functions of
--   core/baseline/0007_scheduler_monitoring_functions.sql that hold the category text take CUSTOM in place of
--   DOMAIN: horsies_monitoring_check_scope, horsies_monitoring_task_list, horsies_monitoring_task_facets,
--   horsies_monitoring_task_stats and horsies_monitoring_task_breakdown. A caller that sends DOMAIN in
--   p_error_categories gets the 22023 refusal of horsies_monitoring_check_scope.
--
-- Rows written before this migration do not change, and the migration writes no row. A stored retry_policy with the
-- keys intervals or auto_retry_for does not validate after it: a workflow node with such options fails at promotion,
-- and the worker refuses such a task before its body runs.
--
-- Rules of the file, as migration 1000: no database default (every insert names every value), objects in the current
-- schema, no transaction control. Each function keeps its argument list and its result, so each one is replaced in
-- place. The comment above each function of this file is its contract from this migration on; the comment in
-- core/baseline/ is the contract before it.
--
-- Locks: none on a table. CREATE OR REPLACE FUNCTION locks only the function row of the catalog.

------------------------------------------------------------------------------------------------------------------------
-- Retry policy keys of a workflow node (intervals_seconds, retry_for)
------------------------------------------------------------------------------------------------------------------------

-- The validation entries of the intervals_seconds member p_intervals of a retry_policy object, as validation_summary writes
-- them: none when absent; one list_type entry for a value that is not a list; one too_long entry (with the length) for
-- a list longer than 20, whatever its items; one too_short entry for an empty list; else one entry per item that is
-- not an int from 1 to 86400, at 'intervals_seconds.<position from 0>'. Strict mode refuses a bool or a float for an int.
-- A list whose text holds only int literals from 1 to 86400 has no item entry, and its items are not walked.
-- Locks: none. Python: RetryPolicy.intervals_seconds (core/models/tasks.py:549-557).
CREATE OR REPLACE FUNCTION horsies_workflow_retry_interval_entries(p_intervals json) RETURNS text[]
    LANGUAGE plpgsql IMMUTABLE
AS $$
BEGIN
    CASE
        WHEN p_intervals IS NULL THEN
            RETURN ARRAY[]::text[];
        WHEN json_typeof(p_intervals) <> 'array' THEN
            RETURN ARRAY['intervals_seconds: Input should be a valid list [list_type]'];
        WHEN json_array_length(p_intervals) > 20 THEN
            RETURN ARRAY[format(
                'intervals_seconds: List should have at most 20 items after validation, not %s [too_long]',
                json_array_length(p_intervals)
            )];
        WHEN json_array_length(p_intervals) = 0 THEN
            RETURN ARRAY['intervals_seconds: List should have at least 1 item after validation, not 0 [too_short]'];
        -- An int literal from 1 to 86400 is one of 1-9999, 10000-79999, 80000-85999, 86000-86399 and 86400.
        WHEN p_intervals::text ~ (
            '^[[][[:space:]]*([1-9][0-9]{0,3}|[1-7][0-9]{4}|8[0-5][0-9]{3}|86[0-3][0-9]{2}|86400)'
            '([[:space:]]*,[[:space:]]*([1-9][0-9]{0,3}|[1-7][0-9]{4}|8[0-5][0-9]{3}|86[0-3][0-9]{2}|86400))*'
            '[[:space:]]*[]]$'
        ) THEN
            RETURN ARRAY[]::text[];
        ELSE
            RETURN ARRAY(
                SELECT format('intervals_seconds.%s: %s', i.position - 1, CASE
                    WHEN horsies_workflow_json_int(i.value) IS NULL THEN 'Input should be a valid integer [int_type]'
                    WHEN horsies_workflow_json_int(i.value) < 1 THEN 'Input should be greater than 0 [greater_than]'
                    ELSE 'Input should be less than or equal to 86400 [less_than_equal]'
                END)
                FROM json_array_elements(p_intervals) WITH ORDINALITY AS i(value, position)
                WHERE horsies_workflow_json_int(i.value) IS NULL
                   OR horsies_workflow_json_int(i.value) NOT BETWEEN 1 AND 86400
                ORDER BY i.position
            );
    END CASE;
END;
$$;

-- The validation entries of the retry_for member p_codes of a retry_policy object, as validation_summary writes
-- them: one missing entry when absent; one list_type entry for a value that is not a list; one too_short entry for an
-- empty list; else five entries per item that is not a str, at 'retry_for.<position from 0>': the str member and
-- the four str enums of BuiltInTaskCode of Union[str, BuiltInTaskCode], in pydantic's order. Plain codes
-- (horsies_workflow_retry_codes_plain) have no item entry, and their items are not walked.
-- Locks: none. Python: RetryPolicy.retry_for (core/models/tasks.py:564-567), BuiltInTaskCode (:58).
CREATE OR REPLACE FUNCTION horsies_workflow_retry_code_entries(p_codes json) RETURNS text[]
    LANGUAGE plpgsql IMMUTABLE
AS $$
BEGIN
    CASE
        WHEN p_codes IS NULL THEN
            RETURN ARRAY['retry_for: Field required [missing]'];
        WHEN json_typeof(p_codes) <> 'array' THEN
            RETURN ARRAY['retry_for: Input should be a valid list [list_type]'];
        WHEN json_array_length(p_codes) = 0 THEN
            RETURN ARRAY['retry_for: List should have at least 1 item after validation, not 0 [too_short]'];
        WHEN horsies_workflow_retry_codes_plain(p_codes) THEN
            RETURN ARRAY[]::text[];
        ELSE
            RETURN ARRAY(
                SELECT format('retry_for.%s.%s', c.position - 1, m.entry)
                FROM json_array_elements(p_codes) WITH ORDINALITY AS c(value, position)
                CROSS JOIN (
                    SELECT 0 AS member, 'str: Input should be a valid string [string_type]' AS entry
                    UNION ALL
                    SELECT e.member, format(
                        'union[str-enum[OperationalErrorCode],str-enum[ContractCode],str-enum[RetrievalCode],'
                        'str-enum[OutcomeCode]].str-enum[%1$s]: Input should be an instance of %1$s [is_instance_of]',
                        e.enum_name
                    )
                    FROM unnest(ARRAY['OperationalErrorCode', 'ContractCode', 'RetrievalCode', 'OutcomeCode'])
                        WITH ORDINALITY AS e(enum_name, member)
                ) AS m
                WHERE json_typeof(c.value) <> 'string'
                ORDER BY c.position, m.member
            );
    END CASE;
END;
$$;

-- The field entries of a retry_policy object p_policy, as Python's validation_summary writes the field errors of
-- RetryPolicy.model_validate(policy, strict=True) (core/codec/validation_errors.py:95-115): one '<loc>: <msg> [<type>]'
-- per error, the fields in declaration order (max_retries: an int from 1 to 20; intervals_seconds; backoff_strategy: 'fixed'
-- or 'exponential'; jitter: a bool; max_delay_seconds: null or an int above 0; retry_for), then one entry per
-- member that is not a field, in the order of the text (an empty key has no '<loc>: '). Empty when the fields
-- validate. Precondition: p_policy is an object with no repeated key.
-- Locks: none. Python: RetryPolicy (core/models/tasks.py:522-567, extra='forbid').
CREATE OR REPLACE FUNCTION horsies_workflow_retry_policy_entries(p_policy json) RETURNS text[]
    LANGUAGE plpgsql IMMUTABLE
AS $$
DECLARE
    c_fields CONSTANT text[] := ARRAY[
        'max_retries', 'intervals_seconds', 'backoff_strategy', 'jitter', 'max_delay_seconds', 'retry_for'
    ];
    v_max_retries json := p_policy -> 'max_retries';
    v_backoff_strategy json := p_policy -> 'backoff_strategy';
    v_jitter json := p_policy -> 'jitter';
    v_max_delay_seconds json := p_policy -> 'max_delay_seconds';
    v_extras text[] := ARRAY[]::text[];
BEGIN
    IF p_policy::jsonb - c_fields <> '{}'::jsonb THEN
        v_extras := ARRAY(
            SELECT CASE WHEN e.key = '' THEN '' ELSE e.key || ': ' END
                || 'Extra inputs are not permitted [extra_forbidden]'
            FROM json_each(p_policy) WITH ORDINALITY AS e(key, value, position)
            WHERE e.key <> ALL (c_fields)
            ORDER BY e.position
        );
    END IF;
    RETURN array_remove(ARRAY[
               CASE
                   WHEN v_max_retries IS NULL THEN NULL
                   WHEN horsies_workflow_json_int(v_max_retries) IS NULL
                       THEN 'max_retries: Input should be a valid integer [int_type]'
                   WHEN horsies_workflow_json_int(v_max_retries) < 1
                       THEN 'max_retries: Input should be greater than or equal to 1 [greater_than_equal]'
                   WHEN horsies_workflow_json_int(v_max_retries) > 20
                       THEN 'max_retries: Input should be less than or equal to 20 [less_than_equal]'
               END
           ], NULL)
        || horsies_workflow_retry_interval_entries(p_policy -> 'intervals_seconds')
        || array_remove(ARRAY[
               CASE
                   WHEN v_backoff_strategy IS NULL THEN NULL
                   WHEN json_typeof(v_backoff_strategy) = 'string'
                        AND (v_backoff_strategy #>> '{}') IN ('fixed', 'exponential') THEN NULL
                   ELSE 'backoff_strategy: Input should be ''fixed'' or ''exponential'' [literal_error]'
               END,
               CASE
                   WHEN v_jitter IS NULL OR json_typeof(v_jitter) = 'boolean' THEN NULL
                   ELSE 'jitter: Input should be a valid boolean [bool_type]'
               END,
               CASE
                   WHEN v_max_delay_seconds IS NULL OR json_typeof(v_max_delay_seconds) = 'null' THEN NULL
                   WHEN horsies_workflow_json_int(v_max_delay_seconds) IS NULL
                       THEN 'max_delay_seconds: Input should be a valid integer [int_type]'
                   WHEN horsies_workflow_json_int(v_max_delay_seconds) < 1
                       THEN 'max_delay_seconds: Input should be greater than 0 [greater_than]'
               END
           ], NULL)
        || horsies_workflow_retry_code_entries(p_policy -> 'retry_for')
        || v_extras;
END;
$$;

-- The model validator error of a retry_policy object p_policy whose fields validate, as validation_summary writes it
-- ('Value error, <text> [value_error]'), or NULL. The validators run in Python's order; the first error ends the
-- validation:
-- 1. validate_strategy_consistency: 'fixed' needs as many intervals_seconds items as max_retries, 'exponential'
--    needs one.
--    An absent field has its default: max_retries 3, intervals_seconds [60, 300, 900], backoff_strategy 'fixed'.
-- 2. validate_error_code_fields: each retry_for code in order, by validate_error_code_string
--    (core/exception_mapper.py:64-84): not empty, not an exception class name (EXCEPTION_NAME_RE), UPPER_SNAKE_CASE
--    (ERROR_CODE_RE), and not in RESERVED_NON_TASK_CODES (WORKFLOW_STOPPED). Plain codes
--    (horsies_workflow_retry_codes_plain) other than WORKFLOW_STOPPED pass all four checks and are not walked.
-- Precondition: horsies_workflow_retry_policy_entries(p_policy) is empty.
-- Locks: none. Python: RetryPolicy (core/models/tasks.py:569-600).
CREATE OR REPLACE FUNCTION horsies_workflow_retry_policy_value_error(p_policy json) RETURNS text
    LANGUAGE plpgsql IMMUTABLE
AS $$
DECLARE
    v_backoff_strategy text := coalesce(p_policy ->> 'backoff_strategy', 'fixed');
    v_max_retries integer := coalesce((p_policy ->> 'max_retries')::integer, 3);
    v_interval_count integer := coalesce(json_array_length(p_policy -> 'intervals_seconds'), 3);
    v_codes json := p_policy -> 'retry_for';
    v_error text;
BEGIN
    v_error := CASE v_backoff_strategy
        WHEN 'fixed' THEN CASE WHEN v_interval_count <> v_max_retries THEN format(
            'Fixed backoff strategy requires intervals_seconds length (%s) to match max_retries (%s). '
            'Either adjust intervals_seconds list or use exponential strategy.', v_interval_count, v_max_retries) END
        WHEN 'exponential' THEN CASE WHEN v_interval_count <> 1 THEN format(
            'Exponential backoff strategy requires exactly one base interval, got %s intervals. '
            'Use intervals_seconds=[base_seconds] for exponential backoff.', v_interval_count) END
    END;
    IF v_error IS NULL AND NOT (
        horsies_workflow_retry_codes_plain(v_codes) AND strpos(v_codes::text, '"WORKFLOW_STOPPED"') = 0
    ) THEN
        SELECT CASE
            WHEN c.code = '' THEN 'retry_for must be a non-empty string, got '''''
            WHEN c.code ~ '^[A-Z][A-Za-z0-9_]*(Error|Exception)$' THEN format(
                'retry_for ''%s'' looks like an exception class name; retry matching is '
                'error-code-only, use UPPER_SNAKE_CASE code names', c.code)
            WHEN c.code !~ '^[A-Z][A-Z0-9_]*$' THEN format(
                'retry_for ''%s'' is invalid; expected UPPER_SNAKE_CASE (e.g. RATE_LIMITED)', c.code)
            ELSE format(
                'retry_for ''%s'' is a reserved error code name and is not a task code; '
                'use another UPPER_SNAKE_CASE code', c.code)
        END
        INTO v_error
        FROM json_array_elements_text(v_codes) WITH ORDINALITY AS c(code, position)
        WHERE c.code = '' OR c.code ~ '^[A-Z][A-Za-z0-9_]*(Error|Exception)$' OR c.code !~ '^[A-Z][A-Z0-9_]*$'
           OR c.code = 'WORKFLOW_STOPPED'
        ORDER BY c.position
        LIMIT 1;
    END IF;
    RETURN 'Value error, ' || v_error || ' [value_error]';
END;
$$;

------------------------------------------------------------------------------------------------------------------------
-- Code category of a custom code (CUSTOM)
------------------------------------------------------------------------------------------------------------------------

-- horsies_monitoring_check_scope: the precondition check of the task list and facet reads. Internal.
--
-- Guarantee: returns only when every argument holds its precondition:
-- - p_builtin_codes and p_builtin_categories are parallel arrays of one length: code i belongs to category i. Each
--   code is non-empty and appears once; each category is OPERATIONAL, CONTRACT, RETRIEVAL or OUTCOME. The caller
--   passes the built-in error-code taxonomy of the library; a code outside it is a CUSTOM code.
-- - p_statuses holds only task statuses; p_error_categories holds only the four built-in categories and CUSTOM.
-- - the window is non-empty: p_window_lower < p_window_upper.
-- Lock order: none. Reads no table.
-- Returns: nothing. Raises 22004 or 22023 naming the argument.
CREATE OR REPLACE FUNCTION horsies_monitoring_check_scope(
    p_window_lower timestamp with time zone,
    p_window_upper timestamp with time zone,
    p_statuses text[],
    p_error_categories text[],
    p_builtin_codes text[],
    p_builtin_categories text[]
) RETURNS void
    LANGUAGE plpgsql IMMUTABLE PARALLEL SAFE
AS $$
BEGIN
    IF p_window_lower IS NULL OR p_window_upper IS NULL OR p_statuses IS NULL OR p_error_categories IS NULL
       OR p_builtin_codes IS NULL OR p_builtin_categories IS NULL THEN
        RAISE EXCEPTION 'monitoring scope: window bounds, filter arrays and the code taxonomy are required'
            USING ERRCODE = 'null_value_not_allowed';
    END IF;
    IF p_window_lower >= p_window_upper THEN
        RAISE EXCEPTION 'monitoring scope: window lower bound % is not before upper bound %',
            p_window_lower, p_window_upper
            USING ERRCODE = 'invalid_parameter_value';
    END IF;
    IF EXISTS (
        SELECT 1 FROM unnest(p_statuses) AS s(status)
        WHERE s.status IS NULL
           OR s.status NOT IN ('PENDING', 'CLAIMED', 'RUNNING', 'COMPLETED', 'FAILED', 'CANCELLED', 'EXPIRED')
    ) THEN
        RAISE EXCEPTION 'monitoring scope: p_statuses % holds a value that is not a task status', p_statuses
            USING ERRCODE = 'invalid_parameter_value';
    END IF;
    IF EXISTS (
        SELECT 1 FROM unnest(p_error_categories) AS c(category)
        WHERE c.category IS NULL
           OR c.category NOT IN ('OPERATIONAL', 'CONTRACT', 'RETRIEVAL', 'OUTCOME', 'CUSTOM')
    ) THEN
        RAISE EXCEPTION 'monitoring scope: p_error_categories % holds a value that is not an error category',
            p_error_categories
            USING ERRCODE = 'invalid_parameter_value';
    END IF;
    IF cardinality(p_builtin_codes) <> cardinality(p_builtin_categories) THEN
        RAISE EXCEPTION 'monitoring scope: % built-in codes but % categories', cardinality(p_builtin_codes),
            cardinality(p_builtin_categories)
            USING ERRCODE = 'invalid_parameter_value';
    END IF;
    IF EXISTS (
        SELECT 1 FROM unnest(p_builtin_codes, p_builtin_categories) AS m(code, category)
        WHERE m.code IS NULL OR m.code = ''
           OR m.category IS NULL OR m.category NOT IN ('OPERATIONAL', 'CONTRACT', 'RETRIEVAL', 'OUTCOME')
    ) OR (SELECT count(DISTINCT m.code) FROM unnest(p_builtin_codes) AS m(code)) <> cardinality(p_builtin_codes) THEN
        RAISE EXCEPTION 'monitoring scope: the built-in code taxonomy has an empty, repeated or unknown entry'
            USING ERRCODE = 'invalid_parameter_value';
    END IF;
END;
$$;

-- horsies_monitoring_task_list: one page of the monitoring task list.
--
-- Guarantee: one page of tasks from the live table and the history window [p_window_lower, p_window_upper) on
-- terminal_at, read in one statement and so in one snapshot: a task that moves to history during the read is on
-- the page once or, past the page, not at all; never twice.
-- - Filters: values in one dimension OR together; dimensions AND together; an empty array is no filter. An error
--   category selects the codes of its built-in family; CUSTOM selects every non-empty code outside the built-in
--   taxonomy. A live row has no error code, so an error-code or category filter drops the live side.
--   p_retried_only keeps rows with retry_count > 0.
-- - Sort: p_sort_key is one of enqueued_at, started_at, completed_at, failed_at, status, task_name, queue_name,
--   priority, retry_count, queue_s (started_at - enqueued_at), exec_s (terminal_at - started_at); p_sort_direction
--   is ASC or DESC. NULL keys come last in both directions. A live row sorts completed_at, failed_at and exec_s as
--   NULL. The order is total, so a page does not reorder between polls: the sort value, then the side rank (LIVE 0
--   before HISTORY 1, in both directions), then the task id ascending, inside each side and in the merge. Text keys
--   sort in the database collation on both sides and in the merge.
-- - Each side reads at most p_offset + p_limit rows in sort order; only nullable keys carry NULLS LAST, so the
--   default key rides idx_horsies_tasks_enqueued_at (live) and the leaf enqueued_at index (history) in both
--   directions. The id tie-break is an incremental sort on that index order: it sorts only rows that share one
--   enqueued_at, and the scan still stops at the page reach.
-- - Bounds: p_limit 1..200, p_offset >= 0, p_offset + p_limit <= 500. No payload column is read.
-- - Totals: with any filter, live_total and history_total are exact counts of the matches, in the same
--   statement. With no filter, live_total is the planner's reltuples of horsies_tasks (an exact count when the
--   table was never sampled, reltuples < 0), and the history side is 0 when the history table has no row, else
--   the EXPLAIN (FORMAT JSON) text of the window read, which the caller decodes. EXPLAIN is a utility statement,
--   which PostgreSQL refuses inside a STABLE function; this is why the function is VOLATILE. It writes nothing.
-- Lock order: none (a plain read).
-- Returns: row 0 (row_kind 'TOTALS'), then the page rows (row_kind 'TASK', row_position 1..n, in page order).
-- total_kind of the TOTALS row: 'EXACT' (live_total, history_total), 'ESTIMATE_HISTORY_EMPTY' (live_total,
-- history_total = 0), or 'ESTIMATE_HISTORY_PLAN' (live_total, history_estimate_plan). A TASK row has location
-- 'LIVE' (error_code and terminal_at NULL; worker columns from the current claim) or 'HISTORY' (worker columns from
-- the last claim). status is the stored text; the caller decodes it.
-- Replaces: horsies/monitoring/queries.py:1279-1401 (list_tasks), :1190-1221 (list_rows_statement), :1229-1276 (the
-- totals) and horsies/core/history/reads/pages.py (history_page_statement): one session of 6 (filtered) or 7
-- (unfiltered) round trips, four snapshots, and a merge in client code-point order.
CREATE OR REPLACE FUNCTION horsies_monitoring_task_list(
    p_window_lower timestamp with time zone,
    p_window_upper timestamp with time zone,
    p_statuses text[],
    p_task_names text[],
    p_queue_names text[],
    p_worker_ids text[],
    p_error_codes text[],
    p_error_categories text[],
    p_retried_only boolean,
    p_builtin_codes text[],
    p_builtin_categories text[],
    p_sort_key text,
    p_sort_direction text,
    p_offset integer,
    p_limit integer
) RETURNS TABLE (
    row_kind text,
    row_position integer,
    total_kind text,
    live_total bigint,
    history_total bigint,
    history_estimate_plan text,
    location text,
    task_id uuid,
    task_name character varying,
    queue_name character varying,
    priority integer,
    status text,
    retry_count integer,
    max_retries integer,
    is_workflow_task boolean,
    error_code text,
    worker_id character varying,
    worker_hostname character varying,
    enqueued_at timestamp with time zone,
    started_at timestamp with time zone,
    terminal_at timestamp with time zone
)
    LANGUAGE plpgsql VOLATILE
AS $$
DECLARE
    c_max_limit CONSTANT integer := 200;
    c_max_reach CONSTANT integer := 500;
    v_live_key text;
    v_history_key text;
    v_nullable boolean;
    v_direction text;
    v_live_order text;
    v_history_order text;
    v_live_where text[] := ARRAY[]::text[];
    v_history_where text[] := ARRAY['h.terminal_at >= $1', 'h.terminal_at < $2'];
    v_category_arms text[] := ARRAY[]::text[];
    v_category_codes text[];
    v_filtered boolean;
    v_total_kind text;
    v_history_total bigint;
    v_plan text;
    v_totals text;
    v_live_where_sql text;
    v_history_where_sql text;
BEGIN
    PERFORM horsies_monitoring_check_scope(
        p_window_lower, p_window_upper, p_statuses, p_error_categories, p_builtin_codes, p_builtin_categories
    );
    IF p_task_names IS NULL OR p_queue_names IS NULL OR p_worker_ids IS NULL OR p_error_codes IS NULL
       OR p_retried_only IS NULL OR p_sort_key IS NULL OR p_sort_direction IS NULL
       OR p_offset IS NULL OR p_limit IS NULL THEN
        RAISE EXCEPTION 'horsies_monitoring_task_list: every argument is required'
            USING ERRCODE = 'null_value_not_allowed';
    END IF;
    IF p_limit < 1 OR p_limit > c_max_limit OR p_offset < 0 OR p_offset > c_max_reach - p_limit THEN
        RAISE EXCEPTION 'horsies_monitoring_task_list: offset % and limit % are outside the page bounds',
            p_offset, p_limit
            USING ERRCODE = 'invalid_parameter_value';
    END IF;
    CASE p_sort_direction
        WHEN 'ASC' THEN v_direction := 'ASC';
        WHEN 'DESC' THEN v_direction := 'DESC';
        ELSE
            RAISE EXCEPTION 'horsies_monitoring_task_list: sort direction % is not ASC or DESC', p_sort_direction
                USING ERRCODE = 'invalid_parameter_value';
    END CASE;
    -- (live expression, history expression, nullable). Only these fixed texts reach the statement.
    CASE p_sort_key
        WHEN 'enqueued_at' THEN
            v_live_key := 't.enqueued_at'; v_history_key := 'h.enqueued_at'; v_nullable := FALSE;
        WHEN 'started_at' THEN
            v_live_key := 't.started_at'; v_history_key := 'h.started_at'; v_nullable := TRUE;
        WHEN 'completed_at' THEN
            v_live_key := 'NULL::timestamptz';
            v_history_key := $k$CASE WHEN h.status = 'COMPLETED' THEN h.terminal_at END$k$;
            v_nullable := TRUE;
        WHEN 'failed_at' THEN
            v_live_key := 'NULL::timestamptz';
            v_history_key := $k$CASE WHEN h.status <> 'COMPLETED' THEN h.terminal_at END$k$;
            v_nullable := TRUE;
        WHEN 'status' THEN
            v_live_key := 't.status'; v_history_key := 'h.status'; v_nullable := FALSE;
        WHEN 'task_name' THEN
            v_live_key := 't.task_name'; v_history_key := 'h.task_name'; v_nullable := FALSE;
        WHEN 'queue_name' THEN
            v_live_key := 't.queue_name'; v_history_key := 'h.queue_name'; v_nullable := FALSE;
        WHEN 'priority' THEN
            v_live_key := 't.priority'; v_history_key := 'h.priority'; v_nullable := FALSE;
        WHEN 'retry_count' THEN
            v_live_key := 't.retry_count'; v_history_key := 'h.retry_count'; v_nullable := FALSE;
        WHEN 'queue_s' THEN
            v_live_key := '(t.started_at - t.enqueued_at)';
            v_history_key := '(h.started_at - h.enqueued_at)';
            v_nullable := TRUE;
        WHEN 'exec_s' THEN
            v_live_key := 'NULL::interval';
            v_history_key := '(h.terminal_at - h.started_at)';
            v_nullable := TRUE;
        ELSE
            RAISE EXCEPTION 'horsies_monitoring_task_list: % is not a sort key', p_sort_key
                USING ERRCODE = 'invalid_parameter_value';
    END CASE;
    v_live_order := v_live_key || ' ' || v_direction || CASE WHEN v_nullable THEN ' NULLS LAST' ELSE '' END;
    v_history_order := v_history_key || ' ' || v_direction || CASE WHEN v_nullable THEN ' NULLS LAST' ELSE '' END;

    -- Filters. Values travel as parameters; only fixed texts are concatenated.
    IF cardinality(p_statuses) > 0 THEN
        v_live_where := v_live_where || 't.status = ANY($3)'::text;
        v_history_where := v_history_where || 'h.status = ANY($3)'::text;
    END IF;
    IF cardinality(p_task_names) > 0 THEN
        v_live_where := v_live_where || 't.task_name = ANY($4)'::text;
        v_history_where := v_history_where || 'h.task_name = ANY($4)'::text;
    END IF;
    IF cardinality(p_queue_names) > 0 THEN
        v_live_where := v_live_where || 't.queue_name = ANY($5)'::text;
        v_history_where := v_history_where || 'h.queue_name = ANY($5)'::text;
    END IF;
    IF cardinality(p_worker_ids) > 0 THEN
        v_live_where := v_live_where || 't.claimed_by_worker_id = ANY($6)'::text;
        v_history_where := v_history_where || 'h.last_claimed_worker_id = ANY($6)'::text;
    END IF;
    IF cardinality(p_error_codes) > 0 THEN
        v_history_where := v_history_where || 'h.error_code = ANY($7)'::text;
    END IF;
    IF cardinality(p_error_categories) > 0 THEN
        v_category_codes := ARRAY(
            SELECT m.code
            FROM unnest(p_builtin_codes, p_builtin_categories) AS m(code, category)
            WHERE m.category = ANY(p_error_categories)
        );
        IF cardinality(v_category_codes) > 0 THEN
            v_category_arms := v_category_arms || 'h.error_code = ANY($8)'::text;
        END IF;
        IF 'CUSTOM' = ANY(p_error_categories) THEN
            v_category_arms := v_category_arms || $c$(h.error_code <> '' AND h.error_code <> ALL($9))$c$::text;
        END IF;
        IF cardinality(v_category_arms) = 0 THEN
            v_category_arms := ARRAY['FALSE'];
        END IF;
        v_history_where := v_history_where || ('(' || array_to_string(v_category_arms, ' OR ') || ')');
    END IF;
    IF cardinality(p_error_codes) > 0 OR cardinality(p_error_categories) > 0 THEN
        v_live_where := v_live_where || 'FALSE'::text;
    END IF;
    IF p_retried_only THEN
        v_live_where := v_live_where || 't.retry_count > 0'::text;
        v_history_where := v_history_where || 'h.retry_count > 0'::text;
    END IF;
    v_filtered := cardinality(p_statuses) > 0 OR cardinality(p_task_names) > 0 OR cardinality(p_queue_names) > 0
        OR cardinality(p_worker_ids) > 0 OR cardinality(p_error_codes) > 0 OR cardinality(p_error_categories) > 0
        OR p_retried_only;
    v_live_where_sql := CASE WHEN cardinality(v_live_where) = 0 THEN 'TRUE'
                             ELSE array_to_string(v_live_where, ' AND ') END;
    v_history_where_sql := array_to_string(v_history_where, ' AND ');

    IF v_filtered THEN
        v_totals := format(
            $t$'EXACT'::text,
               (SELECT count(*) FROM horsies_tasks AS t WHERE %1$s),
               (SELECT count(*) FROM horsies_task_history AS h WHERE %2$s),
               NULL::text$t$,
            v_live_where_sql, v_history_where_sql
        );
    ELSE
        IF EXISTS (SELECT 1 FROM horsies_task_history) THEN
            EXECUTE 'EXPLAIN (FORMAT JSON) SELECT 1 FROM horsies_task_history AS h '
                    'WHERE h.terminal_at >= $1 AND h.terminal_at < $2'
                INTO v_plan
                USING p_window_lower, p_window_upper;
            v_total_kind := 'ESTIMATE_HISTORY_PLAN';
            v_history_total := NULL;
        ELSE
            v_total_kind := 'ESTIMATE_HISTORY_EMPTY';
            v_history_total := 0;
        END IF;
        v_totals := $t$ $13::text,
               (SELECT CASE WHEN c.reltuples < 0 THEN (SELECT count(*) FROM horsies_tasks)
                            ELSE c.reltuples::bigint END
                FROM pg_class AS c WHERE c.oid = 'horsies_tasks'::regclass),
               $14::bigint,
               $15::text$t$;
    END IF;

    RETURN QUERY EXECUTE format(
        $q$
        WITH live AS (
            SELECT 'LIVE'::text AS location, t.id AS task_id, t.task_name, t.queue_name, t.priority,
                   t.status::text AS status, t.retry_count, t.max_retries, t.is_workflow_task,
                   NULL::text AS error_code, t.claimed_by_worker_id AS worker_id, t.worker_hostname,
                   t.enqueued_at, t.started_at, NULL::timestamptz AS terminal_at, %1$s AS sort_value,
                   0 AS location_rank
            FROM horsies_tasks AS t
            WHERE %3$s
            ORDER BY %5$s, t.id
            LIMIT $10
        ), history AS (
            SELECT 'HISTORY'::text, h.task_id, h.task_name, h.queue_name, h.priority,
                   h.status::text, h.retry_count, h.max_retries, h.is_workflow_task,
                   h.error_code, h.last_claimed_worker_id, h.last_worker_hostname,
                   h.enqueued_at, h.started_at, h.terminal_at, %2$s, 1
            FROM horsies_task_history AS h
            WHERE %4$s
            ORDER BY %6$s, h.task_id
            LIMIT $10
        ), page AS (
            SELECT u.*,
                   row_number() OVER (
                       ORDER BY u.sort_value %7$s NULLS LAST, u.location_rank, u.task_id
                   ) AS list_position
            FROM (SELECT * FROM live UNION ALL SELECT * FROM history) AS u
        )
        SELECT 'TOTALS'::text, 0, %8$s,
               NULL::text, NULL::uuid, NULL::character varying, NULL::character varying, NULL::integer,
               NULL::text, NULL::integer, NULL::integer, NULL::boolean, NULL::text, NULL::character varying,
               NULL::character varying, NULL::timestamptz, NULL::timestamptz, NULL::timestamptz
        UNION ALL
        SELECT 'TASK'::text, (p.list_position - $12)::integer, NULL::text, NULL::bigint, NULL::bigint, NULL::text,
               p.location, p.task_id, p.task_name, p.queue_name, p.priority, p.status, p.retry_count,
               p.max_retries, p.is_workflow_task, p.error_code, p.worker_id, p.worker_hostname, p.enqueued_at,
               p.started_at, p.terminal_at
        FROM page AS p
        WHERE p.list_position > $12 AND p.list_position <= $12 + $11
        ORDER BY 2
        $q$,
        v_live_key, v_history_key, v_live_where_sql, v_history_where_sql, v_live_order, v_history_order,
        v_direction, v_totals
    )
    USING p_window_lower, p_window_upper, p_statuses, p_task_names, p_queue_names, p_worker_ids, p_error_codes,
          v_category_codes, p_builtin_codes, p_offset + p_limit, p_limit, p_offset, v_total_kind, v_history_total,
          v_plan;
END;
$$;

-- horsies_monitoring_task_facets: the filter options of the monitoring task list.
--
-- Guarantee: the distinct workers, task names and queues (at most 50 each, c_facet_value_cap) and error codes (at
-- most 30 in each category, c_error_facet_cap) with counts, over the live table and the history window
-- [p_window_lower, p_window_upper) on terminal_at, in one statement. Live and history counts sum before the rank;
-- ties follow the database collation. Each error code carries its category (its built-in family, else CUSTOM), and
-- each category carries the total of every code in it, also codes past the cap, so the result stays bounded for
-- any number of distinct codes. Only p_statuses and p_retried_only scope the facets. NULL values are not facet
-- values; an empty error code is not either (a user error code is free text, and the empty string passes the
-- writer's check). The live table has no error code, so the live side counts three facets.
-- Lock order: none (a plain read).
-- Returns: one row per facet value: facet ('worker', 'task_name', 'queue_name', 'error_code'), facet_value,
-- facet_count; category and category_total are set on error_code rows only. Ordered by facet, then rank.
-- Replaces: horsies/monitoring/queries.py:725-849 (facets_statement) and :942-1007 (task_facets): the same
-- statement inside a session (BEGIN, SELECT, ROLLBACK), with the taxonomy written into the text.
CREATE OR REPLACE FUNCTION horsies_monitoring_task_facets(
    p_window_lower timestamp with time zone,
    p_window_upper timestamp with time zone,
    p_statuses text[],
    p_retried_only boolean,
    p_builtin_codes text[],
    p_builtin_categories text[]
) RETURNS TABLE (
    facet text,
    facet_value text,
    facet_count bigint,
    category text,
    category_total bigint
)
    LANGUAGE plpgsql STABLE
AS $$
DECLARE
    c_facet_value_cap CONSTANT integer := 50;
    c_error_facet_cap CONSTANT integer := 30;
BEGIN
    PERFORM horsies_monitoring_check_scope(
        p_window_lower, p_window_upper, p_statuses, ARRAY[]::text[], p_builtin_codes, p_builtin_categories
    );
    IF p_retried_only IS NULL THEN
        RAISE EXCEPTION 'horsies_monitoring_task_facets: p_retried_only is required'
            USING ERRCODE = 'null_value_not_allowed';
    END IF;
    -- EXECUTE plans the statement with the argument values, so an empty filter folds away.
    RETURN QUERY EXECUTE $q$
        WITH lifecycle_facets AS (
            SELECT CASE GROUPING(claimed_by_worker_id, task_name, queue_name)
                       WHEN 3 THEN 'worker' WHEN 5 THEN 'task_name' WHEN 6 THEN 'queue_name'
                   END AS facet,
                   CASE GROUPING(claimed_by_worker_id, task_name, queue_name)
                       WHEN 3 THEN claimed_by_worker_id::text WHEN 5 THEN task_name::text
                       WHEN 6 THEN queue_name::text
                   END AS facet_value,
                   sum(group_count) AS facet_count
            FROM (
                SELECT t.claimed_by_worker_id, t.task_name, t.queue_name, count(*) AS group_count
                FROM horsies_tasks AS t
                WHERE (cardinality($3) = 0 OR t.status = ANY($3)) AND (NOT $4 OR t.retry_count > 0)
                GROUP BY t.claimed_by_worker_id, t.task_name, t.queue_name
            ) AS live_groups
            GROUP BY GROUPING SETS ((claimed_by_worker_id), (task_name), (queue_name))
            UNION ALL
            SELECT CASE GROUPING(last_claimed_worker_id, task_name, queue_name, error_code)
                       WHEN 7 THEN 'worker' WHEN 11 THEN 'task_name' WHEN 13 THEN 'queue_name'
                       WHEN 14 THEN 'error_code'
                   END,
                   CASE GROUPING(last_claimed_worker_id, task_name, queue_name, error_code)
                       WHEN 7 THEN last_claimed_worker_id::text WHEN 11 THEN task_name::text
                       WHEN 13 THEN queue_name::text WHEN 14 THEN error_code
                   END,
                   sum(group_count)
            FROM (
                SELECT h.last_claimed_worker_id, h.task_name, h.queue_name, h.error_code, count(*) AS group_count
                FROM horsies_task_history AS h
                WHERE h.terminal_at >= $1 AND h.terminal_at < $2
                  AND (cardinality($3) = 0 OR h.status = ANY($3)) AND (NOT $4 OR h.retry_count > 0)
                GROUP BY h.last_claimed_worker_id, h.task_name, h.queue_name, h.error_code
            ) AS history_groups
            GROUP BY GROUPING SETS ((last_claimed_worker_id), (task_name), (queue_name), (error_code))
        ), combined AS (
            SELECT f.facet, f.facet_value, sum(f.facet_count)::bigint AS facet_count
            FROM lifecycle_facets AS f
            WHERE f.facet_value IS NOT NULL
              AND (f.facet <> 'error_code' OR f.facet_value <> '')
            GROUP BY f.facet, f.facet_value
        ), ranked AS (
            SELECT c.facet, c.facet_value, c.facet_count,
                   row_number() OVER (PARTITION BY c.facet ORDER BY c.facet_count DESC, c.facet_value) AS facet_rank,
                   CASE WHEN c.facet = 'error_code' THEN COALESCE(m.category, 'CUSTOM') END AS category
            FROM combined AS c
            LEFT JOIN unnest($5::text[], $6::text[]) AS m(code, category)
                ON c.facet = 'error_code' AND m.code = c.facet_value
        ), scoped AS (
            SELECT r.facet, r.facet_value, r.facet_count, r.facet_rank, r.category,
                   row_number() OVER (PARTITION BY r.facet, r.category ORDER BY r.facet_rank) AS category_rank,
                   sum(r.facet_count) OVER (PARTITION BY r.facet, r.category)::bigint AS category_total
            FROM ranked AS r
        )
        SELECT s.facet, s.facet_value, s.facet_count, s.category,
               CASE WHEN s.facet = 'error_code' THEN s.category_total END
        FROM scoped AS s
        WHERE CASE s.facet WHEN 'error_code' THEN s.category_rank <= $8 ELSE s.facet_rank <= $7 END
        ORDER BY s.facet, s.facet_rank
    $q$
    USING p_window_lower, p_window_upper, p_statuses, p_retried_only, p_builtin_codes, p_builtin_categories,
          c_facet_value_cap, c_error_facet_cap;
END;
$$;

-- horsies_monitoring_task_stats: the task count of each status, for the status cards.
--
-- Guarantee: one count per status over the live table and the history window [p_window_lower, p_window_upper) on
-- terminal_at, read in one statement and so in one snapshot: a task that moves to history during the read counts
-- once. Every filter of the task list except status scopes the counts, with the rules of the task list: values in
-- one dimension OR together; dimensions AND together; an empty array is no filter; an error category selects the
-- codes of its built-in family, and CUSTOM every non-empty code outside the built-in taxonomy; a live row has no
-- error code, so an error-code or category filter drops the live side; p_retried_only keeps rows with
-- retry_count > 0. It changes nothing.
-- - Fails closed on a broken history contract: a history row in the scope whose status is none of the seven (the
--   history table has no status CHECK) raises data_corrupted (XX001) and returns no count. The caller reports it as
--   a broken history contract.
-- Lock order: none (a plain read).
-- Returns: rows 1..7 for PENDING, CLAIMED, RUNNING, COMPLETED, FAILED, CANCELLED and EXPIRED in this order, with
-- task_count 0 for a status that no task has.
-- Raises 22004 for a NULL argument, 22023 for a broken scope precondition (horsies_monitoring_check_scope), and
-- XX001 for a stored status that is not a task status (the message names it).
-- Replaces (`v0.7.0` package): monitoring/queries.py:869-939 (task_stats) and core/history/reads/aggregates.py:63-76
-- (history_scoped_status_counts_statement): a session of 4 round trips with two statements, so two snapshots, and the
-- sum and the zero fill in client code.
CREATE OR REPLACE FUNCTION horsies_monitoring_task_stats(
    p_window_lower timestamp with time zone,
    p_window_upper timestamp with time zone,
    p_task_names text[],
    p_queue_names text[],
    p_worker_ids text[],
    p_error_codes text[],
    p_error_categories text[],
    p_retried_only boolean,
    p_builtin_codes text[],
    p_builtin_categories text[]
) RETURNS TABLE (
    row_position integer,
    status text,
    task_count bigint
)
    LANGUAGE plpgsql STABLE
AS $$
#variable_conflict use_column
DECLARE
    v_category_codes text[];
    v_position integer;
    v_status text;
    v_count bigint;
BEGIN
    PERFORM horsies_monitoring_check_scope(
        p_window_lower, p_window_upper, ARRAY[]::text[], p_error_categories, p_builtin_codes, p_builtin_categories
    );
    IF num_nulls(p_task_names, p_queue_names, p_worker_ids, p_error_codes, p_retried_only) <> 0 THEN
        RAISE EXCEPTION 'horsies_monitoring_task_stats: every argument is required'
            USING ERRCODE = 'null_value_not_allowed';
    END IF;
    v_category_codes := ARRAY(
        SELECT m.code
        FROM unnest(p_builtin_codes, p_builtin_categories) AS m(code, category)
        WHERE m.category = ANY(p_error_categories)
    );
    -- EXECUTE plans the statement with the argument values, so an empty filter folds away. A stored status that is
    -- none of the seven comes first, with no position, so the refusal comes before any row.
    FOR v_position, v_status, v_count IN EXECUTE $q$
        WITH side_counts AS (
            SELECT t.status::text AS status, count(*) AS task_count
            FROM horsies_tasks AS t
            WHERE cardinality($6) = 0 AND cardinality($7) = 0
              AND (cardinality($3) = 0 OR t.task_name = ANY($3))
              AND (cardinality($4) = 0 OR t.queue_name = ANY($4))
              AND (cardinality($5) = 0 OR t.claimed_by_worker_id = ANY($5))
              AND (NOT $8 OR t.retry_count > 0)
            GROUP BY t.status
            UNION ALL
            SELECT h.status::text, count(*)
            FROM horsies_task_history AS h
            WHERE h.terminal_at >= $1 AND h.terminal_at < $2
              AND (cardinality($3) = 0 OR h.task_name = ANY($3))
              AND (cardinality($4) = 0 OR h.queue_name = ANY($4))
              AND (cardinality($5) = 0 OR h.last_claimed_worker_id = ANY($5))
              AND (cardinality($6) = 0 OR h.error_code = ANY($6))
              AND (
                  cardinality($7) = 0
                  OR h.error_code = ANY($9)
                  OR ('CUSTOM' = ANY($7) AND h.error_code <> '' AND h.error_code <> ALL($10))
              )
              AND (NOT $8 OR h.retry_count > 0)
            GROUP BY h.status
        ), merged AS (
            SELECT c.status, sum(c.task_count)::bigint AS task_count
            FROM side_counts AS c
            GROUP BY c.status
        ), fixed AS (
            SELECT f.status, f.position::integer AS position
            FROM unnest(ARRAY['PENDING', 'CLAIMED', 'RUNNING', 'COMPLETED', 'FAILED', 'CANCELLED', 'EXPIRED'])
                WITH ORDINALITY AS f(status, position)
        )
        SELECT f.position, f.status, COALESCE(m.task_count, 0)::bigint
        FROM fixed AS f
        LEFT JOIN merged AS m ON m.status = f.status
        UNION ALL
        SELECT NULL::integer, m.status, m.task_count
        FROM merged AS m
        WHERE NOT EXISTS (SELECT 1 FROM fixed AS f WHERE f.status = m.status)
        ORDER BY 1 NULLS FIRST
    $q$
    USING p_window_lower, p_window_upper, p_task_names, p_queue_names, p_worker_ids, p_error_codes,
          p_error_categories, p_retried_only, v_category_codes, p_builtin_codes
    LOOP
        IF v_position IS NULL THEN
            RAISE EXCEPTION 'horsies_monitoring_task_stats: % history row(s) in the window hold status %, which is '
                'not a task status', v_count, v_status
                USING ERRCODE = 'data_corrupted';
        END IF;
        row_position := v_position;
        status := v_status;
        task_count := v_count;
        RETURN NEXT;
    END LOOP;
END;
$$;

-- horsies_monitoring_task_breakdown: the task counts per worker, task name or queue, with a total.
--
-- Guarantee: one group per value of the group column, over the live table and the history window
-- [p_window_lower, p_window_upper) on terminal_at, read in one statement and so in one snapshot. The group column
-- is the current claim (claimed_by_worker_id) on the live side and the last claim (last_claimed_worker_id) in
-- history for 'worker', task_name for 'task_name', queue_name for 'queue'. Every filter of the task list scopes the
-- rows, with the rules of the task list (see horsies_monitoring_task_stats; here the status filter applies too).
-- - A group is a stored value (group_kind 'VALUE', group_value the worker id, task name or queue name) or, for
--   'worker' only, the rows with no claim on either side (group_kind 'UNCLAIMED', group_value NULL). A worker whose
--   id is the text 'unknown' is a VALUE group of its own, apart from the UNCLAIMED group. Task and queue names are
--   never NULL, so the other two columns give VALUE groups only.
-- - Each group has total, a count per status (a row whose stored status is none of the seven counts only in total)
--   and retried (rows with retry_count > 0).
-- - The TOTAL row sums every group, also the groups past the limit; group_count is the number of groups.
-- - Groups come by total, highest first, then by the group text in byte order (UTF-8 byte order is code-point
--   order). The UNCLAIMED group takes the place of its label 'unknown' in that order; a VALUE group 'unknown' with
--   the same total comes before it. At most p_limit (1..500) groups. An empty scope gives no group and a TOTAL row
--   of zeros.
-- It changes nothing.
-- Lock order: none (a plain read).
-- Returns: first the row_kind 'TOTAL' row (row_position 0, group_kind and group_value NULL, group_count set), then
-- one row_kind 'GROUP' row per group (row_position 1..n, group_kind set, group_count NULL).
-- Raises 22004 for a NULL argument and 22023 for an unknown group column, a limit outside 1..500 or a broken scope
-- precondition (horsies_monitoring_check_scope).
-- Replaces (`v0.7.0` package): monitoring/queries.py:1010-1187 (task_breakdown) and
-- core/history/reads/aggregates.py:85-105 (history_breakdown_statement): a session of 4 round trips with two
-- statements, so two snapshots, every group of both sides read into the client, and the merge, the order, the cap and
-- the total in client code. 0.7.0 labels the unclaimed rows 'unknown' and so merges them with a worker of that name.
CREATE OR REPLACE FUNCTION horsies_monitoring_task_breakdown(
    p_window_lower timestamp with time zone,
    p_window_upper timestamp with time zone,
    p_group_by text,
    p_statuses text[],
    p_task_names text[],
    p_queue_names text[],
    p_worker_ids text[],
    p_error_codes text[],
    p_error_categories text[],
    p_retried_only boolean,
    p_builtin_codes text[],
    p_builtin_categories text[],
    p_limit integer
) RETURNS TABLE (
    row_kind text,
    row_position integer,
    group_kind text,
    group_value text,
    group_count bigint,
    total bigint,
    pending bigint,
    claimed bigint,
    running bigint,
    completed bigint,
    failed bigint,
    cancelled bigint,
    expired bigint,
    retried bigint
)
    LANGUAGE plpgsql STABLE
AS $$
#variable_conflict use_column
DECLARE
    c_max_limit CONSTANT integer := 500;
    v_live_column text;
    v_history_column text;
    v_category_codes text[];
BEGIN
    PERFORM horsies_monitoring_check_scope(
        p_window_lower, p_window_upper, p_statuses, p_error_categories, p_builtin_codes, p_builtin_categories
    );
    IF num_nulls(p_group_by, p_task_names, p_queue_names, p_worker_ids, p_error_codes, p_retried_only, p_limit) <> 0
    THEN
        RAISE EXCEPTION 'horsies_monitoring_task_breakdown: every argument is required'
            USING ERRCODE = 'null_value_not_allowed';
    END IF;
    IF p_limit < 1 OR p_limit > c_max_limit THEN
        RAISE EXCEPTION 'horsies_monitoring_task_breakdown: limit % is outside 1..%', p_limit, c_max_limit
            USING ERRCODE = 'invalid_parameter_value';
    END IF;
    -- Only these fixed texts reach the statement.
    CASE p_group_by
        WHEN 'worker' THEN
            v_live_column := 't.claimed_by_worker_id'; v_history_column := 'h.last_claimed_worker_id';
        WHEN 'task_name' THEN
            v_live_column := 't.task_name'; v_history_column := 'h.task_name';
        WHEN 'queue' THEN
            v_live_column := 't.queue_name'; v_history_column := 'h.queue_name';
        ELSE
            RAISE EXCEPTION 'horsies_monitoring_task_breakdown: % is not a group column', p_group_by
                USING ERRCODE = 'invalid_parameter_value';
    END CASE;
    v_category_codes := ARRAY(
        SELECT m.code
        FROM unnest(p_builtin_codes, p_builtin_categories) AS m(code, category)
        WHERE m.category = ANY(p_error_categories)
    );
    RETURN QUERY EXECUTE format(
        $q$
        WITH side_groups AS (
            SELECT CASE WHEN %1$s IS NULL THEN 'UNCLAIMED' ELSE 'VALUE' END AS group_kind,
                   %1$s::text AS group_value, t.status::text AS status,
                   count(*) AS status_count, count(*) FILTER (WHERE t.retry_count > 0) AS retried_count
            FROM horsies_tasks AS t
            WHERE cardinality($7) = 0 AND cardinality($8) = 0
              AND (cardinality($3) = 0 OR t.status = ANY($3))
              AND (cardinality($4) = 0 OR t.task_name = ANY($4))
              AND (cardinality($5) = 0 OR t.queue_name = ANY($5))
              AND (cardinality($6) = 0 OR t.claimed_by_worker_id = ANY($6))
              AND (NOT $9 OR t.retry_count > 0)
            GROUP BY 1, 2, 3
            UNION ALL
            SELECT CASE WHEN %2$s IS NULL THEN 'UNCLAIMED' ELSE 'VALUE' END,
                   %2$s::text, h.status::text,
                   count(*), count(*) FILTER (WHERE h.retry_count > 0)
            FROM horsies_task_history AS h
            WHERE h.terminal_at >= $1 AND h.terminal_at < $2
              AND (cardinality($3) = 0 OR h.status = ANY($3))
              AND (cardinality($4) = 0 OR h.task_name = ANY($4))
              AND (cardinality($5) = 0 OR h.queue_name = ANY($5))
              AND (cardinality($6) = 0 OR h.last_claimed_worker_id = ANY($6))
              AND (cardinality($7) = 0 OR h.error_code = ANY($7))
              AND (
                  cardinality($8) = 0
                  OR h.error_code = ANY($10)
                  OR ('CUSTOM' = ANY($8) AND h.error_code <> '' AND h.error_code <> ALL($11))
              )
              AND (NOT $9 OR h.retry_count > 0)
            GROUP BY 1, 2, 3
        ), groups AS MATERIALIZED (
            SELECT s.group_kind, s.group_value,
                   COALESCE(s.group_value, 'unknown') AS sort_text,
                   sum(s.status_count)::bigint AS total,
                   COALESCE(sum(s.status_count) FILTER (WHERE s.status = 'PENDING'), 0)::bigint AS pending,
                   COALESCE(sum(s.status_count) FILTER (WHERE s.status = 'CLAIMED'), 0)::bigint AS claimed,
                   COALESCE(sum(s.status_count) FILTER (WHERE s.status = 'RUNNING'), 0)::bigint AS running,
                   COALESCE(sum(s.status_count) FILTER (WHERE s.status = 'COMPLETED'), 0)::bigint AS completed,
                   COALESCE(sum(s.status_count) FILTER (WHERE s.status = 'FAILED'), 0)::bigint AS failed,
                   COALESCE(sum(s.status_count) FILTER (WHERE s.status = 'CANCELLED'), 0)::bigint AS cancelled,
                   COALESCE(sum(s.status_count) FILTER (WHERE s.status = 'EXPIRED'), 0)::bigint AS expired,
                   sum(s.retried_count)::bigint AS retried
            FROM side_groups AS s
            GROUP BY s.group_kind, s.group_value
        ), top_groups AS (
            SELECT g.*
            FROM groups AS g
            ORDER BY g.total DESC, g.sort_text COLLATE "C", g.group_kind = 'UNCLAIMED'
            LIMIT $12
        )
        SELECT 'TOTAL'::text, 0, NULL::text, NULL::text, count(*)::bigint,
               COALESCE(sum(g.total), 0)::bigint, COALESCE(sum(g.pending), 0)::bigint,
               COALESCE(sum(g.claimed), 0)::bigint, COALESCE(sum(g.running), 0)::bigint,
               COALESCE(sum(g.completed), 0)::bigint, COALESCE(sum(g.failed), 0)::bigint,
               COALESCE(sum(g.cancelled), 0)::bigint, COALESCE(sum(g.expired), 0)::bigint,
               COALESCE(sum(g.retried), 0)::bigint
        FROM groups AS g
        UNION ALL
        SELECT 'GROUP'::text,
               (row_number() OVER (
                   ORDER BY tg.total DESC, tg.sort_text COLLATE "C", tg.group_kind = 'UNCLAIMED'
               ))::integer,
               tg.group_kind, tg.group_value, NULL::bigint, tg.total, tg.pending, tg.claimed, tg.running,
               tg.completed, tg.failed, tg.cancelled, tg.expired, tg.retried
        FROM top_groups AS tg
        ORDER BY 2
        $q$,
        v_live_column, v_history_column
    )
    USING p_window_lower, p_window_upper, p_statuses, p_task_names, p_queue_names, p_worker_ids, p_error_codes,
          p_error_categories, p_retried_only, v_category_codes, p_builtin_codes, p_limit;
END;
$$;
