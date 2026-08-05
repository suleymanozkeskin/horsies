# app/core/brokers/postgres.py
from __future__ import annotations
import asyncio, hashlib, contextlib, os, random, threading, uuid
from typing import Any, Optional, TYPE_CHECKING, assert_never, cast
from datetime import datetime, timedelta, timezone
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    create_async_engine,
    async_sessionmaker,
)
from sqlalchemy import text, select
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.exc import DBAPIError
from horsies.core.brokers.listener import PostgresListener
from horsies.core.brokers.result_types import (
    BrokerErrorCode,
    BrokerOperationError,
    BrokerResult,
    RawResultRecord,
)
from horsies.core.models.broker import PostgresConfig
from horsies.core.models.task_pg import TaskModel, Base
from horsies.core.models.workflow_pg import (
    WorkflowModel as _WorkflowModel,
    WorkflowTaskModel as _WorkflowTaskModel,
)
from horsies.core.types.status import TaskStatus, TaskAttemptOutcome
from horsies.core.types.result import Err, Ok, is_err
from horsies.core.codec.json_io import loads_json, dumps_json
from horsies.core.models.tasks import TaskInfo, TaskAttemptInfo
from horsies.core.lifecycle.commands import (
    CancelOrphanedTasks,
    ExpirePendingTasks,
    FailStaleTask,
)
from horsies.core.lifecycle.outcomes import (
    AlreadyApplied,
    Applied,
    LostClaim,
    SourceStateConflict,
    TaskAbsent,
)
from horsies.core.lifecycle.persistence import apply_async, apply_batch_async
from horsies.core.models.health import (
    WORKER_PING_CHANNEL,
    DatabasePing,
    WorkerPong,
    WorkerPongPayload,
    WorkerPingRequest,
    WorkerStateSnapshot,
)
from horsies.core.utils.db import is_retryable_connection_error
from horsies.core.utils.loop_runner import LoopRunner
from horsies.core.utils.url import to_psycopg_url

if TYPE_CHECKING:
    from horsies.core.app import Horsies
from horsies.core.logging import get_logger

if TYPE_CHECKING:
    from horsies.core.models.tasks import TaskResult, TaskError

# Ensure workflow tables are registered in SQLAlchemy metadata.
_ = (_WorkflowModel, _WorkflowTaskModel)


def _broker_err(
    code: BrokerErrorCode,
    message: str,
    exc: BaseException,
) -> Err[BrokerOperationError]:
    """Build an Err with retryable classification from the raw exception."""
    return Err(
        BrokerOperationError(
            code=code,
            message=message,
            retryable=is_retryable_connection_error(exc),
            exception=exc,
        )
    )


def _dbapi_sqlstate(exc: BaseException) -> str | None:
    orig = getattr(exc, 'orig', exc)
    sqlstate = getattr(orig, 'sqlstate', None)
    if isinstance(sqlstate, str):
        return sqlstate
    pgcode = getattr(orig, 'pgcode', None)
    if isinstance(pgcode, str):
        return pgcode
    return None


def _is_schema_deadlock(exc: BaseException) -> bool:
    return _dbapi_sqlstate(exc) == _SCHEMA_INIT_DEADLOCK_SQLSTATE


def _is_missing_schema_version_table(exc: BaseException) -> bool:
    return _dbapi_sqlstate(exc) == _SCHEMA_VERSION_MISSING_SQLSTATE


# ---- Schema DDL (triggers, indexes, migrations) ----
from horsies.core.schemas.triggers import (
    CREATE_TASK_NOTIFY_FUNCTION_SQL,
    CREATE_TASK_NOTIFY_TRIGGER_SQL,
    CREATE_TASK_STATUS_NOTIFY_FUNCTION_SQL,
    CREATE_TASK_STATUS_NOTIFY_TRIGGER_SQL,
    CREATE_WORKER_STATE_NOTIFY_FUNCTION_SQL,
    CREATE_WORKER_STATE_NOTIFY_TRIGGER_SQL,
    CREATE_WORKFLOW_NOTIFY_FUNCTION_SQL,
    CREATE_WORKFLOW_NOTIFY_TRIGGER_SQL,
    CREATE_WORKFLOW_STATUS_NOTIFY_FUNCTION_SQL,
    CREATE_WORKFLOW_STATUS_NOTIFY_TRIGGER_SQL,
)
from horsies.core.worker.sql import (
    UPSERT_TASK_ATTEMPT_SQL,
    SCHEDULE_STALE_TASK_RETRY_SQL,
)
from horsies.core.schemas.indexes import (
    CREATE_HEARTBEATS_TASK_ROLE_SENT_INDEX_SQL,
    CREATE_TASK_ATTEMPTS_ERROR_CODE_INDEX_SQL,
    CREATE_TASK_ATTEMPTS_FINISHED_AT_INDEX_SQL,
    CREATE_TASKS_CLAIM_EXPIRED_INDEX_SQL,
    CREATE_TASKS_CLAIM_PENDING_INDEX_SQL,
    CREATE_HEARTBEATS_SENT_AT_INDEX_SQL,
    CREATE_TASKS_CLAIM_EXPIRED_ORDERED_INDEX_SQL,
    CREATE_TASKS_ENQUEUED_AT_INDEX_SQL,
    CREATE_TASKS_ERROR_CODE_INDEX_SQL,
    CREATE_TASKS_RETENTION_INDEX_SQL,
    CREATE_TASKS_QUEUE_RETENTION_INDEX_SQL,
    CREATE_TASKS_TASK_NAME_INDEX_SQL,
    CREATE_TASKS_WORKER_STATUS_INDEX_SQL,
    CREATE_WORKER_STATES_SNAPSHOT_AT_INDEX_SQL,
    CREATE_WORKER_STATES_WORKER_SNAPSHOT_INDEX_SQL,
    CREATE_WORKFLOW_TASKS_DEPS_INDEX_SQL,
    CREATE_WORKFLOW_TASKS_WF_STATUS_IDX_INDEX_SQL,
    CREATE_WORKFLOWS_RETENTION_INDEX_SQL,
)
from horsies.core.schemas.migrations import (
    ANALYZE_EXPRESSION_INDEXED_TABLES_SQL,
    ADD_TERMINAL_AT_CHECK_SQL,
    ADD_TERMINALIZATION_KIND_CHECK_SQL,
    ADD_TERMINALIZATION_KIND_COLUMN_SQL,
    BACKFILL_TERMINAL_AT_SQL,
    CREATE_CLAIM_FUNCTION_SQL,
    CREATE_OUTCOME_TYPE_SQL,
    CREATE_TERMINALIZATION_FUNCTIONS_SQL,
    CREATE_TASKS_RETENTION_STATISTICS_SQL,
    CREATE_WORKFLOWS_RETENTION_STATISTICS_SQL,
    DROP_CLAIM_FUNCTION_SQL,
    DROP_TERMINALIZATION_FUNCTIONS_SQL,
    VALIDATE_TERMINAL_AT_CHECK_SQL,
    VALIDATE_TERMINALIZATION_KIND_CHECK_SQL,
    CREATE_TASK_ATTEMPTS_TABLE_SQL,
    CREATE_SCHEMA_VERSION_TABLE_SQL,
    ADD_DEPTH_COLUMN_SQL,
    ADD_ENQUEUE_SHA_COLUMN_SQL,
    ADD_ENQUEUED_AT_COLUMN_SQL,
    ADD_ERROR_CODE_COLUMN_SQL,
    ADD_TASK_FINALIZING_COLUMNS_SQL,
    ADD_TASK_TERMINAL_AT_COLUMN_SQL,
    ADD_TASK_IS_WORKFLOW_TASK_COLUMN_SQL,
    ADD_IS_SUBWORKFLOW_COLUMN_SQL,
    ADD_JOIN_TYPE_COLUMN_SQL,
    ADD_MIN_SUCCESS_COLUMN_SQL,
    ADD_NODE_ID_COLUMN_SQL,
    ADD_PARENT_TASK_INDEX_COLUMN_SQL,
    ADD_PARENT_WORKFLOW_ID_COLUMN_SQL,
    ADD_ROOT_WORKFLOW_ID_COLUMN_SQL,
    ADD_SUB_WORKFLOW_ID_COLUMN_SQL,
    ADD_SUB_WORKFLOW_NAME_COLUMN_SQL,
    ADD_SUB_WORKFLOW_SUMMARY_COLUMN_SQL,
    ADD_SUB_DEFINITION_KEY_COLUMN_SQL,
    ADD_SUCCESS_POLICY_COLUMN_SQL,
    ADD_TASK_OPTIONS_COLUMN_SQL,
    ADD_WORKFLOW_SENT_AT_COLUMN_SQL,
    ADD_WORKER_STATES_CHILDREN_MEMORY_COLUMN_SQL,
    ADD_DEFINITION_KEY_COLUMN_SQL,
    BACKFILL_TASK_IS_WORKFLOW_TASK_SQL,
    BACKFILL_ENQUEUE_SHA_SQL,
    BACKFILL_ENQUEUED_AT_SQL,
    BACKFILL_WORKFLOW_SENT_AT_SQL,
    DROP_SUB_WORKFLOW_RETRY_MODE_COLUMN_SQL,
    DROP_SUB_WORKFLOW_MODULE_COLUMN_SQL,
    DROP_SUB_WORKFLOW_QUALNAME_COLUMN_SQL,
    DROP_WORKFLOW_DEF_MODULE_COLUMN_SQL,
    DROP_WORKFLOW_DEF_QUALNAME_COLUMN_SQL,
    SCHEMA_ADVISORY_LOCK_SQL,
    SCHEMA_VERSION,
    SCHEMA_VERSION_TABLE_EXISTS_SQL,
    INSERT_SCHEMA_VERSION_SQL,
    READ_SCHEMA_VERSION_SQL,
    SET_ENQUEUE_SHA_NOT_NULL_SQL,
    SET_ENQUEUED_AT_DEFAULT_SQL,
    SET_ENQUEUED_AT_NOT_NULL_SQL,
    CREATE_TASKS_GOOD_UNTIL_PARTIAL_INDEX_SQL,
    DROP_REDUNDANT_TASK_INDEXES_SQL,
    SET_TASK_COLUMN_DEFAULTS_SQL,
    SET_WORKFLOW_SENT_AT_DEFAULT_SQL,
    SET_WORKFLOW_SENT_AT_NOT_NULL_SQL,
    WIDEN_HEARTBEATS_ID_TO_BIGINT_SQL,
    WIDEN_WORKER_STATES_ID_TO_BIGINT_SQL,
)

_SCHEMA_INIT_MAX_ATTEMPTS = 5
_SCHEMA_INIT_DEADLOCK_SQLSTATE = '40P01'
_SCHEMA_VERSION_MISSING_SQLSTATE = '42P01'

# ---- Monitoring queries ----

GET_STALE_TASKS_SQL = text("""
    SELECT
        t.id,
        t.worker_hostname,
        t.worker_pid,
        t.worker_process_name,
        hb.last_heartbeat,
        t.started_at,
        t.task_name
    FROM horsies_tasks t
    LEFT JOIN LATERAL (
        SELECT sent_at AS last_heartbeat
        FROM horsies_heartbeats h
        WHERE h.task_id = t.id AND h.role = 'runner'
        ORDER BY sent_at DESC
        LIMIT 1
    ) hb ON TRUE
    WHERE t.status = 'RUNNING'
      AND t.started_at IS NOT NULL
      AND COALESCE(hb.last_heartbeat, t.started_at) < NOW() - CAST(:stale_threshold || ' minutes' AS INTERVAL)
    ORDER BY hb.last_heartbeat NULLS FIRST
""")

# Latest snapshot per worker (includes idle workers). The retired
# get_worker_stats only saw workers with RUNNING tasks; this reads the
# worker-states timeseries instead. DISTINCT ON keeps the newest row per
# worker_id.
_WORKER_STATE_COLUMNS = """
        worker_id,
        snapshot_at,
        hostname,
        pid,
        processes,
        max_claim_batch,
        max_claim_per_worker,
        cluster_wide_cap,
        queues,
        queue_priorities,
        queue_max_concurrency,
        recovery_config,
        tasks_running,
        tasks_claimed,
        memory_usage_mb,
        memory_percent,
        cpu_percent,
        children_memory_mb,
        worker_started_at
"""

# Recursive skip-scan: one (worker_id, snapshot_at DESC) index probe per
# worker instead of a DISTINCT ON pass over the whole snapshot timeseries.
# The timeseries holds days of per-worker rows (5s-30s cadence), so the
# DISTINCT ON form reads every retained row to return one per worker —
# measured at 10s on a 118k-row production table.
LIST_WORKER_STATES_SQL = text(f"""
    WITH RECURSIVE latest AS (
        (
            SELECT
{_WORKER_STATE_COLUMNS}
            FROM horsies_worker_states
            ORDER BY worker_id, snapshot_at DESC
            LIMIT 1
        )
        UNION ALL
        SELECT nxt.* FROM latest l
        CROSS JOIN LATERAL (
            SELECT
{_WORKER_STATE_COLUMNS}
            FROM horsies_worker_states w
            WHERE w.worker_id > l.worker_id
            ORDER BY w.worker_id, w.snapshot_at DESC
            LIMIT 1
        ) nxt
    )
    SELECT * FROM latest
""")

GET_WORKER_STATE_LATEST_SQL = text(f"""
    SELECT
{_WORKER_STATE_COLUMNS}
    FROM horsies_worker_states
    WHERE worker_id = :worker_id
    ORDER BY snapshot_at DESC
    LIMIT 1
""")

# History for a single worker, newest first. ``:limit`` of NULL returns all
# retained rows; callers pass an explicit cap to bound the fetch.
GET_WORKER_STATE_HISTORY_SQL = text(f"""
    SELECT
{_WORKER_STATE_COLUMNS}
    FROM horsies_worker_states
    WHERE worker_id = :worker_id
    ORDER BY snapshot_at DESC
    LIMIT :limit
""")

GET_EXPIRED_TASKS_SQL = text("""
    SELECT
        id,
        task_name,
        queue_name,
        priority,
        sent_at,
        enqueued_at,
        good_until,
        NOW() - good_until as expired_for
    FROM horsies_tasks
    WHERE status = 'PENDING'
      AND good_until <= NOW()
    ORDER BY good_until ASC
""")

# ---- Cleanup queries ----

SELECT_STALE_RUNNING_TASKS_SQL = text("""
    SELECT t2.id, t2.worker_pid, t2.worker_hostname, t2.claimed_by_worker_id,
           t2.started_at, hb.last_heartbeat,
           t2.retry_count, t2.worker_process_name,
           t2.max_retries, t2.task_options, t2.good_until,
           clock_timestamp() AS db_now
    FROM horsies_tasks t2
    LEFT JOIN LATERAL (
        SELECT sent_at AS last_heartbeat
        FROM horsies_heartbeats h
        WHERE h.task_id = t2.id AND h.role = 'runner'
        ORDER BY sent_at DESC
        LIMIT 1
    ) hb ON TRUE
    WHERE t2.status = 'RUNNING'
      AND t2.started_at IS NOT NULL
      AND (
          t2.finalizing_at IS NULL
          OR t2.finalizing_at < NOW() - CAST(:finalizing_stale_threshold || ' seconds' AS INTERVAL)
      )
      AND COALESCE(hb.last_heartbeat, t2.started_at) < NOW() - CAST(:stale_threshold || ' seconds' AS INTERVAL)
    FOR UPDATE OF t2 SKIP LOCKED
""")

SELECT_STALE_TASK_FOR_UPDATE_SQL = text("""
    SELECT t.id, t.worker_pid, t.worker_hostname, t.claimed_by_worker_id,
           t.started_at, hb.last_heartbeat, t.retry_count, t.worker_process_name,
           t.max_retries, t.task_options, t.good_until, t.queue_name,
           clock_timestamp() AS db_now
    FROM horsies_tasks t
    LEFT JOIN LATERAL (
        SELECT sent_at AS last_heartbeat
        FROM horsies_heartbeats h
        WHERE h.task_id = t.id AND h.role = 'runner'
        ORDER BY sent_at DESC
        LIMIT 1
    ) hb ON TRUE
    WHERE t.id = :id
      AND t.status = 'RUNNING'
      AND t.started_at IS NOT NULL
      AND (
          t.finalizing_at IS NULL
          OR t.finalizing_at < NOW() - CAST(:finalizing_stale_threshold || ' seconds' AS INTERVAL)
      )
      AND COALESCE(hb.last_heartbeat, t.started_at) < NOW() - CAST(:stale_threshold || ' seconds' AS INTERVAL)
    FOR UPDATE OF t
""")

MARK_STALE_TASK_FAILED_SQL = text("""
    UPDATE horsies_tasks AS t
    SET status = 'FAILED',
        failed_at = NOW(),
        failed_reason = :failed_reason,
        result = :result,
        error_code = :error_code,
        finalizing_at = NULL,
        finalizing_by_worker_id = NULL,
        terminal_at = NOW(),
        updated_at = NOW()
    WHERE t.id = :task_id
      AND t.status = 'RUNNING'
      AND t.started_at IS NOT NULL
      AND (
          t.finalizing_at IS NULL
          OR t.finalizing_at < NOW() - CAST(:finalizing_stale_threshold || ' seconds' AS INTERVAL)
      )
      AND COALESCE(
          (
              SELECT h.sent_at
              FROM horsies_heartbeats h
              WHERE h.task_id = t.id AND h.role = 'runner'
              ORDER BY h.sent_at DESC
              LIMIT 1
          ),
          t.started_at
      ) < NOW() - CAST(:stale_threshold || ' seconds' AS INTERVAL)
    RETURNING t.id
""")

# Batched: a mass expiry as one statement is a single long transaction whose
# commit flushes two NOTIFYs per row at once, overflowing listener queues.
# SKIP LOCKED steps around rows a concurrent claim pass holds; the claim
# itself re-checks good_until, so the race resolves consistently either way.
EXPIRE_PENDING_TASKS_SQL = text("""
    UPDATE horsies_tasks t
    SET status = 'EXPIRED',
        failed_at = NOW(),
        result = :result,
        error_code = :error_code,
        terminal_at = NOW(),
        updated_at = NOW()
    FROM (
        SELECT id FROM horsies_tasks
        WHERE status = 'PENDING'
          AND good_until IS NOT NULL
          AND good_until <= NOW()
        ORDER BY good_until ASC
        LIMIT :batch_size
        FOR UPDATE SKIP LOCKED
    ) s
    WHERE t.id = s.id
""")

_EXPIRE_BATCH_SIZE = 500
# Backstop against an unbounded pass; the remainder expires next interval.
_EXPIRE_MAX_BATCHES_PER_PASS = 200
_ORPHAN_BATCH_SIZE = 500
# Match expiry's bounded drain: each transaction is small and one reaper pass
# cannot run forever if orphan production remains above its drain rate.
_ORPHAN_MAX_BATCHES_PER_PASS = 200

SELECT_TASK_ATTEMPTS_BY_TASK_ID_SQL = text("""
    SELECT task_id, attempt, outcome, will_retry,
           started_at, finished_at,
           error_code, error_message, failed_reason,
           worker_id, worker_hostname, worker_pid, worker_process_name
    FROM horsies_task_attempts
    WHERE task_id = :task_id
    ORDER BY attempt DESC
""")

# Slim status probe for result-wait polling: the full row drags the
# (potentially TOASTed) args/kwargs/result payload columns along on every
# poll iteration; status+name is all the loop needs until terminal.
GET_TASK_STATUS_NAME_SQL = text("""
    SELECT status, task_name FROM horsies_tasks WHERE id = :id
""")

# Terminal fetch for the result-wait loop: exactly the columns
# RawResultRecord consumes. The full entity would also ship args/kwargs —
# multi-MB for payload-heavy tasks — to read a result envelope.
GET_TASK_RESULT_RECORD_SQL = text("""
    SELECT task_name, status, result FROM horsies_tasks WHERE id = :id
""")

# Sentinel returned by _probe_result_row while the row is non-terminal.
_STILL_WAITING = object()

REQUEUE_STALE_CLAIMED_SQL = text("""
    UPDATE horsies_tasks AS t
    SET status = 'PENDING',
        claimed = FALSE,
        claimed_at = NULL,
        claimed_by_worker_id = NULL,
        claim_expires_at = NULL,
        finalizing_at = NULL,
        finalizing_by_worker_id = NULL,
        updated_at = NOW()
    FROM (
        -- Staleness predicate sits inside the locking subquery so only
        -- genuinely stale rows are row-locked; locking every CLAIMED row
        -- stalled concurrent lease renewals and made the claim path skip
        -- reclaimable rows for the duration of this scan.
        SELECT t2.id
        FROM horsies_tasks t2
        LEFT JOIN LATERAL (
            SELECT sent_at AS last_heartbeat
            FROM horsies_heartbeats h
            WHERE h.task_id = t2.id AND h.role = 'claimer'
            ORDER BY sent_at DESC
            LIMIT 1
        ) hb ON TRUE
        WHERE t2.status = 'CLAIMED'
          AND (
            (hb.last_heartbeat IS NULL AND t2.claimed_at IS NOT NULL AND t2.claimed_at < NOW() - CAST(:stale_threshold || ' seconds' AS INTERVAL))
            OR (hb.last_heartbeat IS NOT NULL AND hb.last_heartbeat < NOW() - CAST(:stale_threshold || ' seconds' AS INTERVAL))
          )
          -- Never requeue an orphaned workflow task (no workflow_task row in a
          -- runnable status): re-dispatch can only fail WORKFLOW_CHECK_FAILED
          -- again, so requeuing is the churn engine. Orphans are cancelled by
          -- terminate_orphaned_workflow_tasks instead.
          AND NOT (
              t2.is_workflow_task = TRUE
              AND NOT EXISTS (
                  SELECT 1 FROM horsies_workflow_tasks wt
                  WHERE wt.task_id = t2.id
                    AND wt.status IN ('ENQUEUED', 'READY', 'PENDING', 'RUNNING')
              )
          )
        FOR UPDATE OF t2 SKIP LOCKED
    ) s
    WHERE t.id = s.id
""")


# Cancel orphaned workflow tasks the reaper finds stuck non-terminal: a
# workflow task (is_workflow_task) with no workflow_task row in a runnable
# status (linkage missing or terminal). These can never progress — the child's
# workflow_task->RUNNING transition always fails — so they are made terminal
# (CANCELLED) and their claim released, which frees in-flight budget and lets
# retention sweep them. RUNNING is excluded (handled by auto_fail_stale_running
# and a real orphan never reaches RUNNING). FOR UPDATE SKIP LOCKED avoids racing
# an in-flight dispatch transaction.
TERMINATE_ORPHANED_CLAIMED_WORKFLOW_TASKS_SQL = text("""
    UPDATE horsies_tasks AS t
    SET status = 'CANCELLED',
        claimed = FALSE,
        claimed_at = NULL,
        claimed_by_worker_id = NULL,
        claim_expires_at = NULL,
        finalizing_at = NULL,
        finalizing_by_worker_id = NULL,
        error_code = 'WORKFLOW_CHECK_FAILED',
        failed_reason = 'Workflow task orphaned: no live workflow_task linkage',
        terminal_at = NOW(),
        updated_at = NOW()
    FROM (
        SELECT t2.id
        FROM horsies_tasks t2
        WHERE t2.is_workflow_task = TRUE
          AND t2.status IN ('CLAIMED', 'PENDING', 'READY', 'ENQUEUED')
          AND NOT EXISTS (
              SELECT 1 FROM horsies_workflow_tasks wt
              WHERE wt.task_id = t2.id
                AND wt.status IN ('ENQUEUED', 'READY', 'PENDING', 'RUNNING')
          )
        FOR UPDATE OF t2 SKIP LOCKED
    ) s
    WHERE t.id = s.id
""")


class PostgresBroker:
    """
    PostgreSQL-based task broker with LISTEN/NOTIFY for real-time updates.

    Provides both async and sync APIs:
      - Async: enqueue_async(), get_raw_result_record_async()
      - Sync: enqueue(), get_raw_result_record() (run in background loop)

    Features:
      - Real-time notifications via PostgreSQL triggers
      - Automatic task status tracking
      - Connection pooling and health monitoring
      - Operational monitoring (stale tasks, worker stats)
    """

    def __init__(
        self,
        config: PostgresConfig,
        *,
        assume_initialized: bool = False,
        run_schema_migrations: bool = True,
    ):
        self.config = config
        # False makes this broker incapable of executing DDL. Read-only
        # tooling that constructs its own broker sets it so that pointing
        # the tool at a database can never migrate that database. Owning
        # the schema stays the app's and the worker's job.
        self.run_schema_migrations = run_schema_migrations
        self.logger = get_logger('broker')
        self._app: Horsies | None = None  # Set by Horsies.get_broker()

        engine_cfg = self._runtime_engine_config()
        self.async_engine = create_async_engine(
            self.config.database_url.get_secret_value(), **engine_cfg,
        )
        self.session_factory = async_sessionmaker(
            self.async_engine, expire_on_commit=False
        )

        if assume_initialized:
            self._listener = None
        else:
            psycopg_url = to_psycopg_url(self.config.effective_session_database_url)
            self._listener = PostgresListener(psycopg_url)

        self._initialized = assume_initialized
        self._loop_runner = LoopRunner()  # for sync facades

        # Each worker child builds its own broker, so this fires on every
        # recycle (max_tasks_per_child); keep it at DEBUG in child processes.
        if os.getenv('HORSIES_CHILD_PROCESS') == '1':
            self.logger.debug('PostgresBroker initialized')
        else:
            self.logger.info('PostgresBroker initialized')

    def _base_engine_config(self) -> dict[str, Any]:
        return self.config.sqlalchemy_engine_kwargs()

    def _runtime_engine_config(self) -> dict[str, Any]:
        engine_cfg = self._base_engine_config()
        connect_args = self.config.pooled_connect_args
        if connect_args:
            # TCP keepalives (keep idle pooled sockets warm) plus the
            # PgBouncer prepared-statement knob, built from the typed
            # keepalive fields on PostgresConfig.
            engine_cfg['connect_args'] = connect_args
        return engine_cfg

    def _schema_engine_config(self) -> dict[str, Any]:
        return self._base_engine_config()

    @property
    def app(self) -> 'Horsies | None':
        """Get the attached Horsies app instance (if any)."""
        return self._app

    @app.setter
    def app(self, value: 'Horsies | None') -> None:
        """Set the Horsies app instance."""
        self._app = value

    @property
    def listener(self) -> PostgresListener:
        if self._listener is None:
            raise RuntimeError('Postgres listener is disabled for this broker')
        return self._listener

    @listener.setter
    def listener(self, value: PostgresListener | None) -> None:
        self._listener = value

    def _schema_advisory_key(self) -> int:
        """
        Compute a stable 64-bit advisory lock key for schema initialization.

        PostgreSQL advisory locks are scoped to the current database, so this
        URL-independent key serializes Horsies schema initializers per database.
        """
        h = hashlib.sha256(b'horsies:schema:v1').digest()
        return int.from_bytes(h[:8], byteorder='big', signed=True)

    def _legacy_schema_advisory_key_for_url(self, url: str) -> int:
        basis = url.encode('utf-8', errors='ignore')
        h = hashlib.sha256(b'horsies-schema:' + basis).digest()
        return int.from_bytes(h[:8], byteorder='big', signed=True)

    def _legacy_schema_advisory_key(self) -> int:
        return self._legacy_schema_advisory_key_for_url(
            self.config.database_url.get_secret_value()
        )

    def _legacy_schema_advisory_keys(self) -> tuple[int, ...]:
        urls = {
            self.config.database_url.get_secret_value(),
            self.config.effective_session_database_url,
        }
        return tuple(
            sorted(self._legacy_schema_advisory_key_for_url(url) for url in urls)
        )

    async def _create_triggers(self, conn: Any) -> None:
        """
        Set up PostgreSQL triggers for automatic task notifications.

        Creates triggers that send NOTIFY messages on:
        - INSERT: Sends task_new + task_queue_{queue_name} notifications
        - UPDATE to terminal status (COMPLETED/FAILED/CANCELLED/EXPIRED): Sends task_done notification

        This enables real-time task processing without polling.
        """
        # Create trigger function
        await conn.execute(CREATE_TASK_NOTIFY_FUNCTION_SQL)

        # Create trigger
        await conn.execute(CREATE_TASK_NOTIFY_TRIGGER_SQL)

        # TUI notification triggers (broader: fires on ANY status change)
        await conn.execute(CREATE_TASK_STATUS_NOTIFY_FUNCTION_SQL)
        await conn.execute(CREATE_TASK_STATUS_NOTIFY_TRIGGER_SQL)
        await conn.execute(CREATE_WORKER_STATE_NOTIFY_FUNCTION_SQL)
        await conn.execute(CREATE_WORKER_STATE_NOTIFY_TRIGGER_SQL)

    async def _create_workflow_schema(self, conn: Any) -> None:
        """
        Set up workflow-specific schema elements.

        Creates:
        - GIN index on workflow_tasks.dependencies for efficient dependency lookups
        - Trigger for workflow completion notifications
        - Migration: adds task_options column if missing (for existing installs)
        """
        # GIN index for efficient dependency array lookups
        await conn.execute(CREATE_WORKFLOW_TASKS_DEPS_INDEX_SQL)

        # Migration (v8): first-failed lookups inside the completion lock.
        await conn.execute(CREATE_WORKFLOW_TASKS_WF_STATUS_IDX_INDEX_SQL)

        # Migration: add task_options column for existing installs
        await conn.execute(ADD_TASK_OPTIONS_COLUMN_SQL)

        # Migration: add success_policy column for existing installs
        await conn.execute(ADD_SUCCESS_POLICY_COLUMN_SQL)

        # Migration: add join_type and min_success columns for existing installs
        await conn.execute(ADD_JOIN_TYPE_COLUMN_SQL)
        await conn.execute(ADD_MIN_SUCCESS_COLUMN_SQL)
        await conn.execute(ADD_NODE_ID_COLUMN_SQL)

        # Subworkflow support columns
        await conn.execute(ADD_PARENT_WORKFLOW_ID_COLUMN_SQL)
        await conn.execute(ADD_PARENT_TASK_INDEX_COLUMN_SQL)
        await conn.execute(ADD_DEPTH_COLUMN_SQL)
        await conn.execute(ADD_ROOT_WORKFLOW_ID_COLUMN_SQL)
        await conn.execute(ADD_DEFINITION_KEY_COLUMN_SQL)
        await conn.execute(DROP_WORKFLOW_DEF_MODULE_COLUMN_SQL)
        await conn.execute(DROP_WORKFLOW_DEF_QUALNAME_COLUMN_SQL)

        await conn.execute(ADD_IS_SUBWORKFLOW_COLUMN_SQL)
        await conn.execute(ADD_SUB_WORKFLOW_ID_COLUMN_SQL)
        await conn.execute(ADD_SUB_WORKFLOW_NAME_COLUMN_SQL)
        await conn.execute(DROP_SUB_WORKFLOW_RETRY_MODE_COLUMN_SQL)
        await conn.execute(ADD_SUB_WORKFLOW_SUMMARY_COLUMN_SQL)
        await conn.execute(ADD_SUB_DEFINITION_KEY_COLUMN_SQL)
        await conn.execute(DROP_SUB_WORKFLOW_MODULE_COLUMN_SQL)
        await conn.execute(DROP_SUB_WORKFLOW_QUALNAME_COLUMN_SQL)

        # Workflow notification trigger function
        await conn.execute(CREATE_WORKFLOW_NOTIFY_FUNCTION_SQL)

        # Create workflow trigger
        await conn.execute(CREATE_WORKFLOW_NOTIFY_TRIGGER_SQL)

        # TUI notification trigger (broader: fires on ANY workflow status change)
        await conn.execute(CREATE_WORKFLOW_STATUS_NOTIFY_FUNCTION_SQL)
        await conn.execute(CREATE_WORKFLOW_STATUS_NOTIFY_TRIGGER_SQL)

    async def _run_with_schema_engine(self, fn: Any) -> None:
        schema_url = self.config.effective_session_database_url
        if schema_url == self.config.database_url.get_secret_value():
            await fn(self.async_engine)
            return

        schema_engine = create_async_engine(schema_url, **self._schema_engine_config())
        try:
            await fn(schema_engine)
        finally:
            await schema_engine.dispose()

    async def _read_schema_version(self, conn: Any) -> int:
        result = await conn.execute(READ_SCHEMA_VERSION_SQL)
        version = result.scalar_one()
        return int(version or 0)

    async def _read_schema_version_if_exists(self, engine: AsyncEngine) -> int:
        try:
            async with engine.begin() as conn:
                exists_result = await conn.execute(SCHEMA_VERSION_TABLE_EXISTS_SQL)
                if not bool(exists_result.scalar()):
                    return 0
                return await self._read_schema_version(conn)
        except DBAPIError as exc:
            if _is_missing_schema_version_table(exc):
                return 0
            raise

    async def _maybe_run_schema_init(self, engine: AsyncEngine) -> None:
        if await self._read_schema_version_if_exists(engine) >= SCHEMA_VERSION:
            return
        await self._run_schema_migrations_with_retry(engine)

    async def _run_schema_migrations_with_retry(self, engine: AsyncEngine) -> None:
        for attempt in range(1, _SCHEMA_INIT_MAX_ATTEMPTS + 1):
            try:
                await self._run_schema_migrations(engine)
                return
            except Exception as exc:
                if (
                    not _is_schema_deadlock(exc)
                    or attempt == _SCHEMA_INIT_MAX_ATTEMPTS
                ):
                    raise
                backoff = 0.05 + random.uniform(0, 0.2) * (2 ** (attempt - 1))
                self.logger.warning(
                    'schema init hit deadlock; retrying attempt=%s backoff_s=%.3f',
                    attempt,
                    backoff,
                )
                await asyncio.sleep(backoff)

    async def _run_schema_migrations(self, engine: AsyncEngine) -> None:
        if not self.run_schema_migrations:
            self.logger.info(
                'Schema migrations are disabled on this broker; skipping DDL'
            )
            return
        async with engine.begin() as conn:
            # Acquire every relevant legacy URL-derived key before the new
            # constant key. Sorting keeps new-process lock ordering stable,
            # while also protecting rolling deploys that change database_url
            # from a direct URL to a pooled URL and move direct access to
            # session_database_url.
            for key in self._legacy_schema_advisory_keys():
                await conn.execute(
                    SCHEMA_ADVISORY_LOCK_SQL,
                    {'key': key},
                )
            await conn.execute(
                SCHEMA_ADVISORY_LOCK_SQL,
                {'key': self._schema_advisory_key()},
            )

            await conn.execute(CREATE_SCHEMA_VERSION_TABLE_SQL)
            if await self._read_schema_version(conn) >= SCHEMA_VERSION:
                return

            await conn.run_sync(Base.metadata.create_all)

            # Migration: ensure NOT NULL columns have server-side DEFAULTs
            # for existing tables created before server_default was added.
            await conn.execute(SET_TASK_COLUMN_DEFAULTS_SQL)
            await conn.execute(CREATE_HEARTBEATS_TASK_ROLE_SENT_INDEX_SQL)

            # Migration: add enqueued_at column, backfill from sent_at,
            # and enforce NOT NULL for existing databases.
            await conn.execute(ADD_ENQUEUED_AT_COLUMN_SQL)
            await conn.execute(BACKFILL_ENQUEUED_AT_SQL)
            await conn.execute(SET_ENQUEUED_AT_NOT_NULL_SQL)
            await conn.execute(SET_ENQUEUED_AT_DEFAULT_SQL)

            # Migration: add enqueue_sha column, backfill NULLs, enforce NOT NULL.
            await conn.execute(ADD_ENQUEUE_SHA_COLUMN_SQL)
            await conn.execute(BACKFILL_ENQUEUE_SHA_SQL)
            await conn.execute(SET_ENQUEUE_SHA_NOT_NULL_SQL)

            # Migration: add error_code column for task failure observability.
            await conn.execute(ADD_ERROR_CODE_COLUMN_SQL)
            await conn.execute(ADD_TASK_IS_WORKFLOW_TASK_COLUMN_SQL)
            await conn.execute(BACKFILL_TASK_IS_WORKFLOW_TASK_SQL)
            await conn.execute(ADD_TASK_FINALIZING_COLUMNS_SQL)

            # Migration (v17): canonical terminal_at. Catalog-only ALTER;
            # the backfill of pre-existing terminal rows is separate.
            await conn.execute(ADD_TASK_TERMINAL_AT_COLUMN_SQL)
            await conn.execute(CREATE_TASKS_ERROR_CODE_INDEX_SQL)

            # Migration (v3): claim-path indexes.
            await conn.execute(CREATE_TASKS_CLAIM_PENDING_INDEX_SQL)
            await conn.execute(CREATE_TASKS_CLAIM_EXPIRED_INDEX_SQL)
            # Migration (v7): ordered walk for deep expired backlogs.
            await conn.execute(CREATE_TASKS_CLAIM_EXPIRED_ORDERED_INDEX_SQL)
            await conn.execute(CREATE_TASKS_WORKER_STATUS_INDEX_SQL)
            await conn.execute(CREATE_WORKER_STATES_WORKER_SNAPSHOT_INDEX_SQL)

            # Migration (v4): widen timeseries PKs to BIGINT.
            await conn.execute(WIDEN_HEARTBEATS_ID_TO_BIGINT_SQL)
            await conn.execute(WIDEN_WORKER_STATES_ID_TO_BIGINT_SQL)

            # Migration (v9): executor-child memory in worker telemetry.
            await conn.execute(ADD_WORKER_STATES_CHILDREN_MEMORY_COLUMN_SQL)

            # Migration (v5): drop write-amplifying single-column indexes;
            # partial replacement for good_until.
            await conn.execute(DROP_REDUNDANT_TASK_INDEXES_SQL)
            await conn.execute(CREATE_TASKS_GOOD_UNTIL_PARTIAL_INDEX_SQL)

            # Migration: create horsies_task_attempts table and indexes.
            await conn.execute(CREATE_TASK_ATTEMPTS_TABLE_SQL)
            await conn.execute(CREATE_TASK_ATTEMPTS_ERROR_CODE_INDEX_SQL)
            await conn.execute(CREATE_TASK_ATTEMPTS_FINISHED_AT_INDEX_SQL)

            # Migration: add workflow sent_at column (immutable workflow start
            # call-site timestamp), backfill existing rows, and enforce NOT NULL.
            # Keep this in the advisory-locked schema transaction to avoid DDL
            # lock races with concurrent workflow writes.
            await conn.execute(ADD_WORKFLOW_SENT_AT_COLUMN_SQL)
            await conn.execute(BACKFILL_WORKFLOW_SENT_AT_SQL)
            await conn.execute(SET_WORKFLOW_SENT_AT_NOT_NULL_SQL)
            await conn.execute(SET_WORKFLOW_SENT_AT_DEFAULT_SQL)

            await self._create_triggers(conn)
            await self._create_workflow_schema(conn)

            # Migration (v10): single-statement claim function.
            # Migration (v12): claimed_at added to the function's OUT columns
            # (claim-generation fence, C10); the return-type change requires
            # dropping the v10/v11 definition first.
            await conn.execute(DROP_CLAIM_FUNCTION_SQL)
            await conn.execute(CREATE_CLAIM_FUNCTION_SQL)

            # Migration (v18): terminal_at completeness. Backfill first, then
            # constrain, in that order and in this transaction — the
            # constraint's precondition has to be true at the moment it is
            # enforced. This ships inside the v19 artifact rather than ahead of
            # it: the apply path exits early once the stored version is at or
            # above SCHEMA_VERSION, so a database that reached v19 without this
            # would never run it afterwards.
            await conn.execute(BACKFILL_TERMINAL_AT_SQL)
            await conn.execute(ADD_TERMINAL_AT_CHECK_SQL)
            await conn.execute(VALIDATE_TERMINAL_AT_CHECK_SQL)

            # Migration (v19): terminalization kind, and the operations that
            # write it. The column, the type and the constraint are installed
            # only when absent; the functions are dropped and recreated on
            # every apply, following the claim precedent, because a function
            # body is program rather than state.
            await conn.execute(ADD_TERMINALIZATION_KIND_COLUMN_SQL)
            await conn.execute(ADD_TERMINALIZATION_KIND_CHECK_SQL)
            await conn.execute(VALIDATE_TERMINALIZATION_KIND_CHECK_SQL)
            await conn.execute(CREATE_OUTCOME_TYPE_SQL)
            for drop_function in DROP_TERMINALIZATION_FUNCTIONS_SQL:
                await conn.execute(drop_function)
            for create_function in CREATE_TERMINALIZATION_FUNCTIONS_SQL:
                await conn.execute(create_function)

            # Migration (v11): retention eligibility indexes.
            await conn.execute(CREATE_TASKS_RETENTION_INDEX_SQL)
            await conn.execute(CREATE_WORKER_STATES_SNAPSHOT_AT_INDEX_SQL)

            # Migration (v12): heartbeat retention eligibility index (the
            # v11 pass covered tasks and worker_states, not heartbeats).
            await conn.execute(CREATE_HEARTBEATS_SENT_AT_INDEX_SQL)

            # Migration (v13): workflow retention eligibility index — both
            # workflow retention deletes filter horsies_workflows on the
            # indexed predicate; completes the v11/v12 retention-index set.
            await conn.execute(CREATE_WORKFLOWS_RETENTION_INDEX_SQL)

            # Migration (v14): whole-table expression statistics for the
            # retention predicates. Partial-index statistics are never
            # used for whole-table selectivity, so without these objects
            # the planner costs the retention cutoff at default
            # selectivity and may keep a full-table walk despite the
            # v11/v13 indexes. ANALYZE populates them in this transaction.
            await conn.execute(CREATE_TASKS_RETENTION_STATISTICS_SQL)
            await conn.execute(CREATE_WORKFLOWS_RETENTION_STATISTICS_SQL)
            await conn.execute(ANALYZE_EXPRESSION_INDEXED_TABLES_SQL)

            # Migration (v15): queue-leading retention index for the
            # per-queue override deletes
            # (queue_terminal_record_retention_hours).
            await conn.execute(CREATE_TASKS_QUEUE_RETENTION_INDEX_SQL)

            # Migration (v16): monitoring read-path indexes — task list
            # default sort and task-name facet.
            await conn.execute(CREATE_TASKS_ENQUEUED_AT_INDEX_SQL)
            await conn.execute(CREATE_TASKS_TASK_NAME_INDEX_SQL)

            await conn.execute(
                INSERT_SCHEMA_VERSION_SQL,
                {'version': SCHEMA_VERSION},
            )

    async def _initialize_schema(self, engine: AsyncEngine) -> None:
        await self._run_schema_migrations_with_retry(engine)

    async def _ensure_initialized(self) -> None:
        if self._initialized:
            return
        await self._run_with_schema_engine(self._maybe_run_schema_init)
        self._initialized = True

    async def ensure_schema_initialized(self) -> BrokerResult[None]:
        """
        Public entry point to ensure tables and triggers exist.

        Safe to call multiple times and from multiple processes; internally
        guarded by a PostgreSQL advisory lock to avoid DDL races.

        Returns Ok(None) on success, Err(BrokerOperationError) on failure.
        """
        try:
            await self._ensure_initialized()
            return Ok(None)
        except Exception as exc:
            return _broker_err(
                BrokerErrorCode.SCHEMA_INIT_FAILED,
                f'Schema initialization failed: {exc}',
                exc,
            )

    # ----------------- Async API -----------------

    async def enqueue_async(
        self,
        task_name: str,
        queue_name: str = 'default',
        *,
        task_id: str,
        enqueue_sha: str,
        args_json: str | None = None,
        kwargs_json: str | None = None,
        priority: int = 100,
        sent_at: Optional[datetime] = None,
        enqueued_at: Optional[datetime] = None,
        enqueue_delay_seconds: Optional[int] = None,
        good_until: Optional[datetime] = None,
        task_options: Optional[str] = None,
    ) -> BrokerResult[str]:
        if enqueued_at is not None and enqueue_delay_seconds is not None:
            return _broker_err(
                BrokerErrorCode.ENQUEUE_FAILED,
                'Cannot specify both enqueued_at and enqueue_delay_seconds',
                ValueError('Cannot specify both enqueued_at and enqueue_delay_seconds'),
            )
        # Guard: sent_at is an immutable call-site timestamp, not a scheduling
        # mechanism.  A future sent_at without an explicit enqueued_at or
        # enqueue_delay_seconds is almost certainly legacy ETA usage that would
        # silently run immediately (enqueued_at defaults to NOW()).
        # 5-second tolerance absorbs trivial clock drift.
        _SENT_AT_FUTURE_TOLERANCE = timedelta(seconds=5)
        if (
            sent_at is not None
            and sent_at > datetime.now(timezone.utc) + _SENT_AT_FUTURE_TOLERANCE
            and enqueued_at is None
            and enqueue_delay_seconds is None
        ):
            return _broker_err(
                BrokerErrorCode.ENQUEUE_FAILED,
                'sent_at is in the future without enqueued_at or enqueue_delay_seconds; '
                'sent_at is a call-site timestamp, use enqueued_at or '
                'enqueue_delay_seconds to schedule deferred execution',
                ValueError('future sent_at without explicit scheduling parameter'),
            )
        try:
            await self._ensure_initialized()

            call_site_sent_at = sent_at or datetime.now(timezone.utc)

            # Parse retry configuration from task_options.
            # task_options is always produced by serialize_task_options() — malformed JSON
            # is a bug, not a runtime condition, and must not be silently swallowed.
            max_retries = 0
            if task_options:
                opts_r = loads_json(task_options)
                if is_err(opts_r):
                    return _broker_err(
                        BrokerErrorCode.ENQUEUE_FAILED,
                        f'task_options JSON corrupt: {opts_r.err_value}',
                        opts_r.err_value,
                    )
                options_data = opts_r.ok_value
                if isinstance(options_data, dict):
                    retry_policy = options_data.get('retry_policy')
                    if isinstance(retry_policy, dict):
                        max_retries = retry_policy.get('max_retries', 3)

            # Determine enqueued_at value for the INSERT.
            # enqueue_delay_seconds: DB-side NOW() + interval.
            # explicit enqueued_at: caller-provided value.
            # default: DB-side NOW() via text('NOW()').
            if enqueue_delay_seconds is not None:
                enqueued_at_value = text(
                    "NOW() + CAST(:delay || ' seconds' AS INTERVAL)",
                ).bindparams(delay=str(enqueue_delay_seconds))
            elif enqueued_at is not None:
                enqueued_at_value = enqueued_at
            else:
                enqueued_at_value = text('NOW()')

            # Single SQLAlchemy Core INSERT ... ON CONFLICT DO NOTHING ... RETURNING id.
            # Replaces both the ORM session.add() and raw SQL paths.
            stmt = (
                pg_insert(TaskModel)
                .values(
                    id=task_id,
                    task_name=task_name,
                    queue_name=queue_name,
                    priority=priority,
                    args=args_json,
                    kwargs=kwargs_json,
                    status=TaskStatus.PENDING,
                    sent_at=call_site_sent_at,
                    enqueued_at=enqueued_at_value,
                    good_until=good_until,
                    max_retries=max_retries,
                    task_options=task_options,
                    enqueue_sha=enqueue_sha,
                    is_workflow_task=False,
                    created_at=text('NOW()'),
                    updated_at=text('NOW()'),
                )
                .on_conflict_do_nothing(index_elements=['id'])
                .returning(TaskModel.id)
            )

            async with self.session_factory() as session:
                result = await session.execute(stmt)
                row = result.fetchone()
                await session.commit()

            if row is not None:
                # Row inserted — fresh enqueue succeeded.
                return Ok(task_id)

            # Conflict: task_id already exists. Verify payload identity via stored SHA.
            return await self._verify_enqueue_conflict(task_id, enqueue_sha, task_name)

        except Exception as exc:
            return _broker_err(
                BrokerErrorCode.ENQUEUE_FAILED,
                f'Failed to enqueue task {task_name}: {exc}',
                exc,
            )

    async def _verify_enqueue_conflict(
        self,
        task_id: str,
        enqueue_sha: str,
        task_name: str,
    ) -> BrokerResult[str]:
        """Verify a conflicting task_id has the same payload.

        Called when INSERT ... ON CONFLICT DO NOTHING returns no row.
        """
        try:
            async with self.session_factory() as session:
                row = (
                    await session.execute(
                        select(TaskModel.enqueue_sha).where(TaskModel.id == task_id),
                    )
                ).fetchone()
        except Exception as exc:
            # SELECT failed — task exists (conflict proves it) but we cannot
            # confirm payload identity. Retryable so auto-retry or manual
            # retry can reattempt when DB recovers.
            return Err(
                BrokerOperationError(
                    code=BrokerErrorCode.ENQUEUE_FAILED,
                    message=(
                        f'task_id {task_id} conflict detected but verification '
                        f'query failed for {task_name}: {exc}'
                    ),
                    retryable=True,
                    exception=exc,
                )
            )

        if row is None:
            # The insert observed a conflict, but the row disappeared before we
            # could compare enqueue_sha. Without the stored fingerprint, this
            # cannot be proven idempotent.
            return Err(
                BrokerOperationError(
                    code=BrokerErrorCode.ENQUEUE_FAILED,
                    message=(
                        f'task_id {task_id} conflict detected but row disappeared '
                        f'before verification for {task_name}; cannot verify payload identity'
                    ),
                    retryable=False,
                    exception=None,
                )
            )

        existing_sha: str = row.enqueue_sha
        # enqueue_sha is NOT NULL — defensive assertion against data corruption.
        assert (
            existing_sha is not None
        ), f'enqueue_sha is NULL for task_id={task_id} — column is NOT NULL'

        if existing_sha == enqueue_sha:
            # Idempotent success — same payload already enqueued.
            return Ok(task_id)

        # Different payload with same task_id — always a bug.
        return Err(
            BrokerOperationError(
                code=BrokerErrorCode.PAYLOAD_MISMATCH,
                message=(
                    f'task_id {task_id} already exists with different payload '
                    f'for {task_name} (sha mismatch)'
                ),
                retryable=False,
                exception=None,
            )
        )


    def _build_raw_result_record(
        self,
        task_id: str,
        task_name: str,
        status: TaskStatus,
        result_json: 'str | None',
    ) -> BrokerResult[RawResultRecord | None]:
        """Decode the result column into a ``RawResultRecord``.

        Returns ``Err(INVALID_JSON_PAYLOAD)`` when the stored JSON is
        malformed (parser failure, NaN / Infinity rejected by the
        parse-constant guard) or when the top-level payload is neither
        ``None`` nor a JSON object (the envelope grammar requires a
        ``dict``). Otherwise wraps the loaded value into the record.
        """
        _lr = loads_json(result_json)
        if is_err(_lr):
            return Err(BrokerOperationError(
                code=BrokerErrorCode.INVALID_JSON_PAYLOAD,
                message=(
                    f'Result JSON parse failed for task {task_id}: '
                    f'{_lr.err_value}'
                ),
                retryable=False,
            ))
        raw_value = _lr.ok_value
        if raw_value is not None and not isinstance(raw_value, dict):
            return Err(BrokerOperationError(
                code=BrokerErrorCode.INVALID_JSON_PAYLOAD,
                message=(
                    f'Result for task {task_id} is not a JSON object; '
                    f'got {type(raw_value).__name__}'
                ),
                retryable=False,
            ))
        return Ok(RawResultRecord(
            task_id=task_id,
            task_name=task_name,
            status=status,
            raw_result=raw_value,
        ))

    async def _probe_result_row(
        self,
        session: AsyncSession,
        task_id: str,
        *,
        non_terminal_snapshot: bool = False,
    ) -> 'BrokerResult[RawResultRecord | None] | object':
        """Slim status probe for the result-wait loop.

        Polls status+name only; the full row (with potentially TOASTed
        payload columns) is fetched once, when a terminal status is
        observed.

        Returns the loop's final ``BrokerResult`` when the wait is over,
        or the ``_STILL_WAITING`` sentinel to keep polling. With
        ``non_terminal_snapshot=True`` (timeout path) a non-terminal row is
        returned as a payload-less record instead of the sentinel.
        """
        probe = await session.execute(
            GET_TASK_STATUS_NAME_SQL, {'id': task_id},
        )
        probe_row = probe.fetchone()
        if probe_row is None:
            return Ok(None)
        status = TaskStatus(str(probe_row.status))
        if status in (
            TaskStatus.COMPLETED,
            TaskStatus.FAILED,
            TaskStatus.EXPIRED,
        ):
            record_row = (await session.execute(
                GET_TASK_RESULT_RECORD_SQL, {'id': task_id},
            )).fetchone()
            if record_row is None:
                return Ok(None)
            return self._build_raw_result_record(
                task_id,
                str(record_row.task_name),
                TaskStatus(str(record_row.status)),
                cast('str | None', record_row.result),
            )
        if status == TaskStatus.CANCELLED or non_terminal_snapshot:
            return Ok(RawResultRecord(
                task_id=task_id,
                task_name=str(probe_row.task_name),
                status=status,
                raw_result=None,
            ))
        return _STILL_WAITING

    async def get_raw_result_record_async(
        self,
        task_id: str,
        timeout_ms: Optional[int] = None,
    ) -> BrokerResult[RawResultRecord | None]:
        """Raw broker fetch of a task's stored result envelope.

        Strict-serde phase 6a primitive. The broker performs no typed
        decoding; callers at the handle/app layer derive ``ok_type``
        from ``record.task_name`` and call
        ``decode_task_result(record.raw_result, ok_type)``.

        Semantics:

        - ``Ok(None)``: row truly absent (no ``TaskModel`` row for
          ``task_id``).
        - ``Ok(RawResultRecord(status=COMPLETED|FAILED|EXPIRED, raw_result=<dict>))``:
          terminal row with a stored payload.
        - ``Ok(RawResultRecord(status=CANCELLED, raw_result=None))``:
          task was cancelled before completion.
        - ``Ok(RawResultRecord(status=<non-terminal>, raw_result=None))``:
          timeout fired before the row reached a terminal state.
        - ``Err(BrokerOperationError(INVALID_JSON_PAYLOAD, ...))``:
          stored JSON failed strict parse (malformed, non-finite
          float, non-object envelope).
        - ``Err(...)`` with other codes for DB / listener failures.
        """
        try:
            await self._ensure_initialized()

            start_time = asyncio.get_event_loop().time()

            timeout_seconds: Optional[float] = None
            if timeout_ms is not None:
                timeout_seconds = timeout_ms / 1000.0

            # Quick path: row may already be terminal.
            async with self.session_factory() as session:
                outcome = await self._probe_result_row(session, task_id)
                if outcome is not _STILL_WAITING:
                    return cast(
                        'BrokerResult[RawResultRecord | None]', outcome,
                    )

            # Listen + poll loop.
            q: asyncio.Queue[Any] | None = None
            try:
                listen_r = await self.listener.listen_payload(
                    'task_done', task_id,
                )
            except RuntimeError as e:
                self.logger.debug(
                    'LISTEN unavailable; falling back to polling for '
                    'task_done. Original error: %s',
                    e,
                )
            else:
                match listen_r:
                    case Ok(queue):
                        q = queue
                    case Err(listen_err):
                        self.logger.debug(
                            'LISTEN unavailable; falling back to polling '
                            'for task_done. Original error: %s',
                            listen_err.message,
                        )

            try:
                poll_interval = 5.0 if q is not None else 0.2
                while True:
                    remaining_timeout: float | None = None
                    if timeout_seconds is not None:
                        elapsed = (
                            asyncio.get_event_loop().time() - start_time
                        )
                        remaining_timeout = timeout_seconds - elapsed
                        if remaining_timeout <= 0:
                            # Timeout. Surface current row state so the
                            # caller can map to WAIT_TIMEOUT / outcome
                            # codes per their UX. The row may have moved
                            # to terminal between checks; one more read
                            # to capture the latest snapshot.
                            async with self.session_factory() as session:
                                outcome = await self._probe_result_row(
                                    session,
                                    task_id,
                                    non_terminal_snapshot=True,
                                )
                                return cast(
                                    'BrokerResult[RawResultRecord | None]',
                                    outcome,
                                )

                    wait_time = (
                        min(poll_interval, remaining_timeout)
                        if remaining_timeout is not None
                        else poll_interval
                    )

                    if q is not None:
                        try:

                            async def _wait_for_task() -> None:
                                while True:
                                    note = await q.get()
                                    if note.payload == task_id:
                                        return

                            await asyncio.wait_for(
                                _wait_for_task(), timeout=wait_time,
                            )
                        except asyncio.TimeoutError:
                            pass
                    else:
                        await asyncio.sleep(wait_time)

                    async with self.session_factory() as session:
                        outcome = await self._probe_result_row(
                            session, task_id,
                        )
                        if outcome is not _STILL_WAITING:
                            return cast(
                                'BrokerResult[RawResultRecord | None]',
                                outcome,
                            )
            finally:
                if q is not None:
                    await self._unsubscribe_task_done_safely(task_id, q)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            self.logger.exception(
                'Broker error while retrieving raw task result record',
            )
            return _broker_err(
                BrokerErrorCode.TASK_INFO_QUERY_FAILED,
                f'Broker error while retrieving task {task_id}: {exc}',
                exc,
            )

    def get_raw_result_record(
        self,
        task_id: str,
        timeout_ms: Optional[int] = None,
    ) -> BrokerResult[RawResultRecord | None]:
        """Synchronous wrapper around ``get_raw_result_record_async``.

        Runs the async path on the broker's background event loop via
        ``LoopRunner``.
        """
        try:
            return self._loop_runner.call(
                self.get_raw_result_record_async, task_id, timeout_ms,
            )
        except Exception as exc:
            return _broker_err(
                BrokerErrorCode.TASK_INFO_QUERY_FAILED,
                f'Sync bridge failed for raw result fetch ({task_id}): '
                f'{exc}',
                exc,
            )

    async def _unsubscribe_task_done_safely(
        self, task_id: str, q: asyncio.Queue[Any],
    ) -> None:
        """Ensure task_done unsubscribe completes even under repeated cancellation."""
        unsubscribe_task = asyncio.create_task(
            self.listener.unsubscribe_payload('task_done', task_id, q)
        )
        cancelled_during_cleanup = False
        while not unsubscribe_task.done():
            try:
                await asyncio.shield(unsubscribe_task)
            except asyncio.CancelledError:
                cancelled_during_cleanup = True
                continue

        # Preserve existing semantics: cross-loop RuntimeError on unsubscribe is non-fatal.
        with contextlib.suppress(RuntimeError):
            await unsubscribe_task

        # Propagate cancellation only after cleanup has completed.
        if cancelled_during_cleanup:
            raise asyncio.CancelledError

    async def close_async(self) -> BrokerResult[None]:
        """Close listener/engine and stop sync loop runner if it was started."""
        errors: list[BaseException] = []
        try:
            if self._listener is not None:
                await self._listener.close()
        except Exception as exc:
            errors.append(exc)
        try:
            await self.async_engine.dispose()
        except Exception as exc:
            errors.append(exc)
        # If sync APIs started the LoopRunner, close_async should also tear it down
        # when called from any thread except the loop-runner thread itself.
        loop_thread = self._loop_runner._thread
        if (
            self._loop_runner._started
            and loop_thread is not None
            and threading.current_thread() is not loop_thread
        ):
            try:
                self._loop_runner.stop()
            except Exception as exc:
                errors.append(exc)
        if errors:
            return _broker_err(
                BrokerErrorCode.CLOSE_FAILED,
                f'Close failed ({len(errors)} error(s)): {errors[0]}',
                errors[0],
            )
        return Ok(None)

    # ------------- Operational & Monitoring Methods -------------

    async def get_stale_tasks(
        self,
        stale_threshold_minutes: int = 2,
    ) -> BrokerResult[list[dict[str, Any]]]:
        """Identify potentially crashed tasks based on heartbeat absence."""
        try:
            async with self.session_factory() as session:
                result = await session.execute(
                    GET_STALE_TASKS_SQL,
                    {'stale_threshold': stale_threshold_minutes},
                )
                columns = result.keys()
                return Ok([dict(zip(columns, row)) for row in result.fetchall()])
        except Exception as exc:
            return _broker_err(
                BrokerErrorCode.MONITORING_QUERY_FAILED,
                f'get_stale_tasks failed: {exc}',
                exc,
            )

    async def ping_database_async(self) -> BrokerResult[DatabasePing]:
        """Probe Postgres reachability with ``SELECT 1`` through the live pool.

        Goes through the broker's own pool (not an isolated engine), so a
        success also confirms a connection could be checked out. Returns the
        measured round-trip latency.
        """
        loop = asyncio.get_running_loop()
        try:
            start = loop.time()
            async with self.session_factory() as session:
                await session.execute(text('SELECT 1'))
            latency_ms = (loop.time() - start) * 1000.0
            return Ok(DatabasePing(latency_ms=latency_ms))
        except Exception as exc:
            return _broker_err(
                BrokerErrorCode.DB_PING_FAILED,
                f'ping_database failed: {exc}',
                exc,
            )

    async def ping_workers_async(
        self,
        *,
        target_worker_id: str | None = None,
        timeout_seconds: float = 2.0,
        min_responses: int | None = None,
    ) -> BrokerResult[list[WorkerPong]]:
        """Active ping-pong: NOTIFY workers and collect replies within a window.

        Subscribes to a unique reply channel, broadcasts a ping on
        ``WORKER_PING_CHANNEL``, then collects pongs until a stop condition:

        - ``target_worker_id`` set: returns as soon as that worker replies.
        - ``min_responses`` set: returns as soon as that many distinct workers
          reply (fast fail-open liveness, e.g. ``min_responses=1`` for a
          ``/health`` gate — a healthy fleet answers in milliseconds; only a
          degraded fleet pays the full ``timeout_seconds``).
        - neither set: waits the full window and enumerates every responder.

        A pong proves the replying worker's event loop is responsive *and*
        that it can reach Postgres. Workers present in
        ``list_worker_states_async`` but absent here are non-responsive.
        """
        if timeout_seconds <= 0:
            return Err(
                BrokerOperationError(
                    code=BrokerErrorCode.WORKER_PING_FAILED,
                    message=f'timeout_seconds must be positive, got {timeout_seconds}',
                    retryable=False,
                )
            )
        if min_responses is not None and min_responses < 1:
            return Err(
                BrokerOperationError(
                    code=BrokerErrorCode.WORKER_PING_FAILED,
                    message=f'min_responses must be >= 1 when set, got {min_responses}',
                    retryable=False,
                )
            )

        correlation_id = uuid.uuid4().hex
        reply_channel = f'horsies_worker_pong_{correlation_id}'

        # The `listener` property raises if the broker has no listener
        # (assume_initialized), and listen() raises on cross-loop misuse.
        # Convert both to an Err so the Result contract holds on every path.
        try:
            listen_r = await self.listener.listen(reply_channel)
        except RuntimeError as exc:
            return _broker_err(
                BrokerErrorCode.WORKER_PING_FAILED,
                f'ping_workers listener unavailable: {exc}',
                exc,
            )
        if is_err(listen_r):
            err = listen_r.err_value
            return Err(
                BrokerOperationError(
                    code=BrokerErrorCode.WORKER_PING_FAILED,
                    message=f'ping_workers reply subscribe failed: {err.message}',
                    retryable=err.retryable,
                    exception=err.exception,
                )
            )
        queue = listen_r.ok_value

        try:
            request = WorkerPingRequest(
                correlation_id=correlation_id,
                reply_channel=reply_channel,
                target_worker_id=target_worker_id,
            )
            payload_r = dumps_json(request.model_dump())
            if is_err(payload_r):
                return Err(
                    BrokerOperationError(
                        code=BrokerErrorCode.WORKER_PING_FAILED,
                        message=f'ping_workers payload encode failed: {payload_r.err_value}',
                        retryable=False,
                    )
                )
            payload = payload_r.ok_value

            loop = asyncio.get_running_loop()
            async with self.session_factory() as session:
                await session.execute(
                    text('SELECT pg_notify(:ch, :p)'),
                    {'ch': WORKER_PING_CHANNEL, 'p': payload},
                )
                await session.commit()
            sent_at = loop.time()

            pongs: list[WorkerPong] = []
            seen: set[str] = set()
            deadline = sent_at + timeout_seconds
            while True:
                remaining = deadline - loop.time()
                if remaining <= 0:
                    break
                try:
                    notify = await asyncio.wait_for(queue.get(), timeout=remaining)
                except asyncio.TimeoutError:
                    break
                pong = self._decode_pong(
                    notify.payload, correlation_id, loop.time() - sent_at
                )
                if pong is None or pong.worker_id in seen:
                    continue  # malformed, mismatched, or duplicate worker
                seen.add(pong.worker_id)
                pongs.append(pong)
                if target_worker_id is not None and pong.worker_id == target_worker_id:
                    break
                if min_responses is not None and len(pongs) >= min_responses:
                    break
            return Ok(pongs)
        except Exception as exc:
            return _broker_err(
                BrokerErrorCode.WORKER_PING_FAILED,
                f'ping_workers failed: {exc}',
                exc,
            )
        finally:
            await self._unsubscribe_ping_safely(reply_channel, queue)

    async def _unsubscribe_ping_safely(
        self, channel: str, q: asyncio.Queue[Any]
    ) -> None:
        """Ensure reply-channel unsubscribe completes even under repeated cancellation.

        Mirrors ``_unsubscribe_task_done_safely``: shields the unsubscribe so a
        cancelled ``ping_workers_async`` does not leak the server-side LISTEN,
        suppresses the cross-loop RuntimeError, and re-raises cancellation only
        after cleanup finishes.
        """
        unsubscribe_task = asyncio.create_task(self.listener.unsubscribe(channel, q))
        cancelled_during_cleanup = False
        while not unsubscribe_task.done():
            try:
                await asyncio.shield(unsubscribe_task)
            except asyncio.CancelledError:
                cancelled_during_cleanup = True
                continue
        with contextlib.suppress(RuntimeError):
            await unsubscribe_task
        if cancelled_during_cleanup:
            raise asyncio.CancelledError

    def _decode_pong(
        self,
        raw_payload: str,
        correlation_id: str,
        elapsed_seconds: float,
    ) -> WorkerPong | None:
        """Decode a pong notification, discarding malformed or mismatched replies."""
        parsed = loads_json(raw_payload)
        if is_err(parsed):
            self.logger.warning(
                'Discarding unparseable pong payload: %s', parsed.err_value
            )
            return None
        body = parsed.ok_value
        if not isinstance(body, dict):
            return None
        try:
            payload = WorkerPongPayload.model_validate(body)
        except Exception as exc:
            self.logger.warning('Discarding invalid pong payload: %s', exc)
            return None
        if payload.correlation_id != correlation_id:
            return None
        return WorkerPong(
            worker_id=payload.worker_id,
            hostname=payload.hostname,
            pid=payload.pid,
            round_trip_ms=elapsed_seconds * 1000.0,
        )

    @staticmethod
    def _row_to_worker_snapshot(row: Any) -> WorkerStateSnapshot:
        """Map a worker-states row to a typed snapshot."""
        m = row._mapping
        return WorkerStateSnapshot(
            worker_id=m['worker_id'],
            snapshot_at=m['snapshot_at'],
            hostname=m['hostname'],
            pid=m['pid'],
            processes=m['processes'],
            max_claim_batch=m['max_claim_batch'],
            max_claim_per_worker=m['max_claim_per_worker'],
            cluster_wide_cap=m['cluster_wide_cap'],
            queues=list(m['queues']),
            queue_priorities=m['queue_priorities'],
            queue_max_concurrency=m['queue_max_concurrency'],
            recovery_config=m['recovery_config'],
            tasks_running=m['tasks_running'],
            tasks_claimed=m['tasks_claimed'],
            memory_usage_mb=m['memory_usage_mb'],
            memory_percent=m['memory_percent'],
            cpu_percent=m['cpu_percent'],
            children_memory_mb=m['children_memory_mb'],
            worker_started_at=m['worker_started_at'],
        )

    async def list_worker_states_async(self) -> BrokerResult[list[WorkerStateSnapshot]]:
        """Latest state snapshot per worker, including idle workers.

        Unlike the retired ``get_worker_stats`` (RUNNING tasks only), this
        reads ``horsies_worker_states`` so every worker that has reported a
        snapshot appears, regardless of current load.
        """
        try:
            async with self.session_factory() as session:
                result = await session.execute(LIST_WORKER_STATES_SQL)
                return Ok(
                    [self._row_to_worker_snapshot(row) for row in result.fetchall()]
                )
        except Exception as exc:
            return _broker_err(
                BrokerErrorCode.MONITORING_QUERY_FAILED,
                f'list_worker_states failed: {exc}',
                exc,
            )

    async def get_worker_state_async(
        self,
        worker_id: str,
    ) -> BrokerResult[WorkerStateSnapshot | None]:
        """Latest state snapshot for one worker, or ``None`` if unknown."""
        try:
            async with self.session_factory() as session:
                result = await session.execute(
                    GET_WORKER_STATE_LATEST_SQL,
                    {'worker_id': worker_id},
                )
                row = result.fetchone()
                if row is None:
                    return Ok(None)
                return Ok(self._row_to_worker_snapshot(row))
        except Exception as exc:
            return _broker_err(
                BrokerErrorCode.MONITORING_QUERY_FAILED,
                f'get_worker_state failed: {exc}',
                exc,
            )

    async def get_worker_state_history_async(
        self,
        worker_id: str,
        *,
        limit: int | None = None,
    ) -> BrokerResult[list[WorkerStateSnapshot]]:
        """Timeseries snapshots for one worker, newest first.

        ``limit`` of ``None`` returns all retained rows; pass an explicit cap
        to bound the fetch (the table grows ~1 row per worker per interval).
        """
        if limit is not None and limit <= 0:
            return Err(
                BrokerOperationError(
                    code=BrokerErrorCode.MONITORING_QUERY_FAILED,
                    message=f'limit must be positive when set, got {limit}',
                    retryable=False,
                )
            )
        try:
            async with self.session_factory() as session:
                result = await session.execute(
                    GET_WORKER_STATE_HISTORY_SQL,
                    {'worker_id': worker_id, 'limit': limit},
                )
                return Ok(
                    [self._row_to_worker_snapshot(row) for row in result.fetchall()]
                )
        except Exception as exc:
            return _broker_err(
                BrokerErrorCode.MONITORING_QUERY_FAILED,
                f'get_worker_state_history failed: {exc}',
                exc,
            )

    async def get_expired_tasks(self) -> BrokerResult[list[dict[str, Any]]]:
        """Find tasks that expired before worker processing."""
        try:
            async with self.session_factory() as session:
                result = await session.execute(GET_EXPIRED_TASKS_SQL)
                columns = result.keys()
                return Ok([dict(zip(columns, row)) for row in result.fetchall()])
        except Exception as exc:
            return _broker_err(
                BrokerErrorCode.MONITORING_QUERY_FAILED,
                f'get_expired_tasks failed: {exc}',
                exc,
            )

    async def mark_stale_tasks_as_failed(
        self,
        stale_threshold_ms: int = 300_000,
        finalizing_stale_threshold_ms: int = 300_000,
    ) -> BrokerResult[int]:
        """Clean up crashed worker tasks: retry if policy allows, otherwise mark FAILED.

        Two-phase approach:
        1. Lightweight scan: identify stale task IDs (no lock hold after scan).
        2. Per-task processing: re-acquire FOR UPDATE lock, re-read fresh state,
           then upsert attempt + transition — all within one transaction per task.

        This avoids holding row locks across the entire batch while still
        preventing races with concurrent worker finalizers.
        """
        try:
            from horsies.core.models.tasks import (
                TaskResult,
                TaskError,
                OperationalErrorCode,
            )
            from horsies.core.codec.json_io import dumps_json
            from horsies.core.codec.typed import encode_task_result
            from horsies.core.utils.retry import (
                check_retry_eligibility,
                calculate_retry_delay,
                parse_retry_policy,
            )

            stale_threshold_seconds = stale_threshold_ms / 1000.0
            finalizing_stale_threshold_seconds = finalizing_stale_threshold_ms / 1000.0

            # Phase 1: lightweight scan to collect candidate task IDs.
            # FOR UPDATE SKIP LOCKED prevents picking up rows already being finalized,
            # but we release locks immediately — the IDs are just candidates.
            async with self.session_factory() as session:
                stale_tasks_result = await session.execute(
                    SELECT_STALE_RUNNING_TASKS_SQL,
                    {
                        'stale_threshold': stale_threshold_seconds,
                        'finalizing_stale_threshold': finalizing_stale_threshold_seconds,
                    },
                )
                candidate_ids = [row.id for row in stale_tasks_result.fetchall()]
                await session.rollback()

            if not candidate_ids:
                return Ok(0)

            # Phase 2: process each candidate in its own transaction.
            # Re-acquire FOR UPDATE per task to get fresh state and prevent races.
            processed = 0
            for task_id in candidate_ids:
                try:
                    async with self.session_factory() as session:
                        # Lock the row and re-read current state (fresh db_now).
                        # If the row is no longer RUNNING, a worker finalized it
                        # between scan and now — skip it.
                        ctx_result = await session.execute(
                            SELECT_STALE_TASK_FOR_UPDATE_SQL,
                            {
                                'id': task_id,
                                'stale_threshold': stale_threshold_seconds,
                                'finalizing_stale_threshold': finalizing_stale_threshold_seconds,
                            },
                        )
                        ctx_row = ctx_result.fetchone()
                        if ctx_row is None:
                            # Task no longer RUNNING — worker or another reaper handled it.
                            await session.rollback()
                            continue

                        retry_count = ctx_row.retry_count or 0
                        max_retries = ctx_row.max_retries or 0
                        started_at = ctx_row.started_at
                        worker_pid = ctx_row.worker_pid
                        worker_hostname = ctx_row.worker_hostname
                        worker_id = ctx_row.claimed_by_worker_id
                        worker_process_name = ctx_row.worker_process_name
                        task_options_json: str | None = ctx_row.task_options
                        good_until: datetime | None = ctx_row.good_until
                        db_now: datetime = ctx_row.db_now

                        failed_reason_str = (
                            f'Worker process crashed (no runner heartbeat '
                            f'for {stale_threshold_ms}ms = {stale_threshold_ms/1000:.1f}s)'
                        )
                        attempt_num = retry_count + 1
                        now_utc = db_now
                        attempt_worker = {
                            'worker_id': worker_id,
                            'worker_hostname': worker_hostname,
                            'worker_pid': worker_pid,
                            'worker_process_name': worker_process_name,
                        }

                        should_retry = check_retry_eligibility(
                            retry_count=retry_count,
                            max_retries=max_retries,
                            task_options_json=task_options_json,
                            error_code=OperationalErrorCode.WORKER_CRASHED,
                            good_until=good_until,
                            db_now=db_now,
                        )

                        if should_retry:
                            retry_policy_data = (
                                parse_retry_policy(task_options_json) or {}
                            )
                            new_retry_count = retry_count + 1
                            delay_seconds = calculate_retry_delay(
                                new_retry_count, retry_policy_data
                            )
                            next_retry_at = db_now + timedelta(seconds=delay_seconds)

                            # Guard: don't schedule retry past good_until
                            if good_until is not None:
                                _gu = (
                                    good_until.replace(tzinfo=timezone.utc)
                                    if good_until.tzinfo is None
                                    else good_until
                                )
                                _nra = (
                                    next_retry_at.replace(tzinfo=timezone.utc)
                                    if next_retry_at.tzinfo is None
                                    else next_retry_at
                                )
                                if _nra >= _gu:
                                    should_retry = False

                        if should_retry:
                            await session.execute(
                                UPSERT_TASK_ATTEMPT_SQL,
                                {
                                    'task_id': task_id,
                                    'attempt': attempt_num,
                                    'outcome': 'FAILED',
                                    'will_retry': True,
                                    'started_at': started_at or now_utc,
                                    'finished_at': now_utc,
                                    'error_code': OperationalErrorCode.WORKER_CRASHED.value,
                                    'error_message': failed_reason_str,
                                    'failed_reason': failed_reason_str,
                                    **attempt_worker,
                                },
                            )
                            res = await session.execute(
                                SCHEDULE_STALE_TASK_RETRY_SQL,
                                {
                                    'id': task_id,
                                    'retry_count': new_retry_count,
                                    'next_retry_at': next_retry_at,
                                    'stale_threshold': stale_threshold_seconds,
                                    'finalizing_stale_threshold': finalizing_stale_threshold_seconds,
                                },
                            )
                            if res.fetchone() is None:
                                # good_until SQL guard rejected — rollback attempt too.
                                await session.rollback()
                                continue
                            await session.commit()
                            self.logger.info(
                                'Reaper scheduled retry #%d for stale task %s at %s',
                                new_retry_count,
                                task_id,
                                next_retry_at,
                            )

                            # Best-effort: notify workers so they wake at next_retry_at
                            # rather than waiting for the next poll cycle.
                            # Reuses the already-open session post-commit — a
                            # second pool checkout per retried task is real
                            # churn during mass crash recovery.
                            queue_name = ctx_row.queue_name or 'default'
                            try:
                                await session.execute(
                                    text('SELECT pg_notify(:ch, :p)'),
                                    {
                                        'ch': f'task_queue_{queue_name}',
                                        'p': f'retry:{task_id}',
                                    },
                                )
                                await session.commit()
                            except Exception:
                                pass  # Non-fatal; polling will pick it up
                        else:
                            task_error = TaskError(
                                error_code=OperationalErrorCode.WORKER_CRASHED,
                                message=failed_reason_str,
                                data={
                                    'stale_threshold_ms': stale_threshold_ms,
                                    'stale_threshold_seconds': stale_threshold_seconds,
                                    'worker_pid': worker_pid,
                                    'worker_hostname': worker_hostname,
                                    'worker_id': worker_id,
                                    'started_at': started_at.isoformat()
                                    if started_at
                                    else None,
                                    'detected_at': now_utc.isoformat(),
                                },
                            )
                            task_result: TaskResult[None, TaskError] = TaskResult(
                                err=task_error
                            )
                            # Strict-serde phase 7: write the wire envelope so
                            # downstream readers (worker finalizers, workflow
                            # decoders) see ``__h_task_result__``. The crashed
                            # task carries no ok payload (TaskResult[None, ...]),
                            # so ``type(None)`` is the truthful ok_type.
                            ser_r = dumps_json(
                                encode_task_result(task_result, type(None)),
                            )
                            if is_err(ser_r):
                                self.logger.error(
                                    'Failed to serialize crash result for task %s: %s',
                                    task_id,
                                    ser_r.err_value,
                                )
                                await session.rollback()
                                continue
                            result_json = ser_r.ok_value

                            await session.execute(
                                UPSERT_TASK_ATTEMPT_SQL,
                                {
                                    'task_id': task_id,
                                    'attempt': attempt_num,
                                    'outcome': 'FAILED',
                                    'will_retry': False,
                                    'started_at': started_at or now_utc,
                                    'finished_at': now_utc,
                                    'error_code': OperationalErrorCode.WORKER_CRASHED.value,
                                    'error_message': task_error.message,
                                    'failed_reason': failed_reason_str,
                                    **attempt_worker,
                                },
                            )
                            terminalization = await apply_async(
                                await session.connection(),
                                FailStaleTask(
                                    task_id=task_id,
                                    stale_after_ms=stale_threshold_ms,
                                    finalizing_stale_after_ms=(
                                        finalizing_stale_threshold_ms
                                    ),
                                    result_json=result_json,
                                    error_code=(
                                        OperationalErrorCode.WORKER_CRASHED.value
                                    ),
                                    failed_reason=failed_reason_str,
                                ),
                            )
                            match terminalization:
                                case Applied():
                                    await session.commit()
                                case (
                                    AlreadyApplied()
                                    | LostClaim()
                                    | SourceStateConflict()
                                    | TaskAbsent()
                                ):
                                    # The attempt and transition are one unit.
                                    # A refusal must discard the attempt written
                                    # immediately above as well.
                                    await session.rollback()
                                    continue
                                case _ as unreachable:
                                    assert_never(unreachable)

                        processed += 1
                except Exception as task_exc:
                    self.logger.error(
                        'Reaper failed to process stale task %s: %s',
                        task_id,
                        task_exc,
                    )
                    continue

            return Ok(processed)
        except Exception as exc:
            return _broker_err(
                BrokerErrorCode.CLEANUP_FAILED,
                f'mark_stale_tasks_as_failed failed: {exc}',
                exc,
            )

    async def expire_pending_tasks(self) -> BrokerResult[int]:
        """Transition PENDING tasks whose good_until deadline has passed to EXPIRED.

        Only affects tasks that were never claimed. Writes a TaskResult with
        TASK_EXPIRED outcome code so callers have a meaningful result payload.
        No attempt row is written (the task never started).
        """
        try:
            from horsies.core.models.tasks import TaskResult, TaskError, OutcomeCode
            from horsies.core.codec.json_io import dumps_json
            from horsies.core.codec.typed import encode_task_result

            task_error = TaskError(
                error_code=OutcomeCode.TASK_EXPIRED,
                message='Task expired: good_until deadline passed before execution started',
            )
            task_result: TaskResult[None, TaskError] = TaskResult(err=task_error)
            # Strict-serde phase 7: emit the wire envelope so downstream
            # readers see ``__h_task_result__`` and the err slot decodes
            # cleanly. Expired tasks carry no ok payload.
            ser_r = dumps_json(encode_task_result(task_result, type(None)))
            if is_err(ser_r):
                return _broker_err(
                    BrokerErrorCode.CLEANUP_FAILED,
                    f'expire_pending_tasks: failed to serialize TASK_EXPIRED result: {ser_r.err_value}',
                    ser_r.err_value,
                )
            result_json = ser_r.ok_value

            total_expired = 0
            for _ in range(_EXPIRE_MAX_BATCHES_PER_PASS):
                async with self.session_factory() as session:
                    outcomes = await apply_batch_async(
                        await session.connection(),
                        ExpirePendingTasks(
                            batch_size=_EXPIRE_BATCH_SIZE,
                            result_json=result_json,
                            error_code=OutcomeCode.TASK_EXPIRED.value,
                        ),
                    )
                    for outcome in outcomes:
                        match outcome:
                            case Applied():
                                continue
                            case (
                                AlreadyApplied()
                                | LostClaim()
                                | SourceStateConflict()
                                | TaskAbsent()
                            ):
                                raise RuntimeError(
                                    'pending expiry operation returned '
                                    f'{type(outcome).__name__}; discovery batches '
                                    'report transitioned rows only'
                                )
                            case _ as unreachable:
                                assert_never(unreachable)
                    await session.commit()
                batch_expired = len(outcomes)
                total_expired += batch_expired
                if batch_expired < _EXPIRE_BATCH_SIZE:
                    return Ok(total_expired)
            self.logger.warning(
                'expire_pending_tasks reached the per-pass batch cap after '
                'expiring %s task(s); the remainder expires next interval',
                total_expired,
            )
            return Ok(total_expired)
        except Exception as exc:
            return _broker_err(
                BrokerErrorCode.CLEANUP_FAILED,
                f'expire_pending_tasks failed: {exc}',
                exc,
            )

    async def requeue_stale_claimed(
        self,
        stale_threshold_ms: int = 120_000,
    ) -> BrokerResult[int]:
        """Requeue tasks stuck in CLAIMED without recent claimer heartbeat."""
        try:
            stale_threshold_seconds = stale_threshold_ms / 1000.0

            async with self.session_factory() as session:
                result = await session.execute(
                    REQUEUE_STALE_CLAIMED_SQL,
                    {'stale_threshold': stale_threshold_seconds},
                )
                await session.commit()
                return Ok(getattr(result, 'rowcount', 0))
        except Exception as exc:
            return _broker_err(
                BrokerErrorCode.CLEANUP_FAILED,
                f'requeue_stale_claimed failed: {exc}',
                exc,
            )

    async def terminate_orphaned_workflow_tasks(self) -> BrokerResult[int]:
        """Cancel orphaned workflow tasks (no live workflow_task linkage).

        These can never reach RUNNING, so requeuing them only churns. Marking
        them CANCELLED releases the claim and lets retention sweep them.
        """
        try:
            total_terminated = 0
            for _ in range(_ORPHAN_MAX_BATCHES_PER_PASS):
                async with self.session_factory() as session:
                    outcomes = await apply_batch_async(
                        await session.connection(),
                        CancelOrphanedTasks(batch_size=_ORPHAN_BATCH_SIZE),
                    )
                    for outcome in outcomes:
                        match outcome:
                            case Applied():
                                continue
                            case (
                                AlreadyApplied()
                                | LostClaim()
                                | SourceStateConflict()
                                | TaskAbsent()
                            ):
                                raise RuntimeError(
                                    'orphan sweep returned '
                                    f'{type(outcome).__name__}; discovery '
                                    'batches report transitioned rows only'
                                )
                            case _ as unreachable:
                                assert_never(unreachable)
                    await session.commit()
                batch_terminated = len(outcomes)
                total_terminated += batch_terminated
                if batch_terminated < _ORPHAN_BATCH_SIZE:
                    return Ok(total_terminated)
            self.logger.warning(
                'terminate_orphaned_workflow_tasks reached the per-pass '
                'batch cap after terminating %s task(s); the remainder '
                'will be handled next interval',
                total_terminated,
            )
            return Ok(total_terminated)
        except Exception as exc:
            return _broker_err(
                BrokerErrorCode.CLEANUP_FAILED,
                f'terminate_orphaned_workflow_tasks failed: {exc}',
                exc,
            )

    # ----------------- Sync API Facades -----------------

    def enqueue(
        self,
        task_name: str,
        queue_name: str = 'default',
        *,
        task_id: str,
        enqueue_sha: str,
        args_json: str | None = None,
        kwargs_json: str | None = None,
        priority: int = 100,
        sent_at: Optional[datetime] = None,
        enqueued_at: Optional[datetime] = None,
        enqueue_delay_seconds: Optional[int] = None,
        good_until: Optional[datetime] = None,
        task_options: Optional[str] = None,
    ) -> BrokerResult[str]:
        """Synchronous task submission (runs enqueue_async in background loop)."""
        try:
            return self._loop_runner.call(
                self.enqueue_async,
                task_name,
                queue_name,
                task_id=task_id,
                enqueue_sha=enqueue_sha,
                args_json=args_json,
                kwargs_json=kwargs_json,
                priority=priority,
                sent_at=sent_at,
                enqueued_at=enqueued_at,
                enqueue_delay_seconds=enqueue_delay_seconds,
                good_until=good_until,
                task_options=task_options,
            )
        except Exception as exc:
            return _broker_err(
                BrokerErrorCode.ENQUEUE_FAILED,
                f'Failed to enqueue task {task_name} (sync): {exc}',
                exc,
            )


    async def get_task_info_async(
        self,
        task_id: str,
        *,
        include_result: bool = False,
        include_failed_reason: bool = False,
        include_attempts: bool = False,
    ) -> BrokerResult[TaskInfo | None]:
        """Fetch metadata for a task by ID.

        Returns Ok(TaskInfo) if found, Ok(None) if not found,
        Err(BrokerOperationError) on infrastructure failure.
        """
        try:
            await self._ensure_initialized()

            async with self.session_factory() as session:
                base_columns = [
                    'id',
                    'task_name',
                    'status',
                    'queue_name',
                    'priority',
                    'retry_count',
                    'max_retries',
                    'next_retry_at',
                    'sent_at',
                    'enqueued_at',
                    'claimed_at',
                    'started_at',
                    'completed_at',
                    'failed_at',
                    'worker_hostname',
                    'worker_pid',
                    'worker_process_name',
                    'error_code',
                ]
                if include_result:
                    base_columns.append('result')
                if include_failed_reason:
                    base_columns.append('failed_reason')

                query = text(
                    f"""
                    SELECT {', '.join(base_columns)}
                    FROM horsies_tasks
                    WHERE id = :id
                """
                )
                result = await session.execute(query, {'id': task_id})
                row = result.fetchone()
                if row is None:
                    return Ok(None)

                raw_result_value: dict[str, Any] | None = None
                failed_reason = None

                idx = 0
                task_id_value = row[idx]
                idx += 1
                task_name = row[idx]
                idx += 1
                status = TaskStatus(row[idx])
                idx += 1
                queue_name = row[idx]
                idx += 1
                priority = row[idx]
                idx += 1
                retry_count = row[idx] or 0
                idx += 1
                max_retries = row[idx] or 0
                idx += 1
                next_retry_at = row[idx]
                idx += 1
                sent_at = row[idx]
                idx += 1
                enqueued_at = row[idx]
                idx += 1
                claimed_at = row[idx]
                idx += 1
                started_at = row[idx]
                idx += 1
                completed_at = row[idx]
                idx += 1
                failed_at = row[idx]
                idx += 1
                worker_hostname = row[idx]
                idx += 1
                worker_pid = row[idx]
                idx += 1
                worker_process_name = row[idx]
                idx += 1
                error_code_value = row[idx]
                idx += 1

                if include_result:
                    raw_result_text = row[idx]
                    idx += 1
                    if raw_result_text:
                        _lr = loads_json(raw_result_text)
                        if is_err(_lr):
                            return Err(BrokerOperationError(
                                code=BrokerErrorCode.INVALID_JSON_PAYLOAD,
                                message=(
                                    f'Result JSON parse failed for task '
                                    f'{task_id}: {_lr.err_value}'
                                ),
                                retryable=False,
                            ))
                        loaded = _lr.ok_value
                        if loaded is not None and not isinstance(loaded, dict):
                            return Err(BrokerOperationError(
                                code=BrokerErrorCode.INVALID_JSON_PAYLOAD,
                                message=(
                                    f'Result for task {task_id} is not a '
                                    f'JSON object; got '
                                    f'{type(loaded).__name__}'
                                ),
                                retryable=False,
                            ))
                        # Broker stays infrastructure: no typed decode
                        # here. The app-level `get_task_info` does the
                        # `decode_task_result` step using the local task
                        # catalog. (Strict-serde design §6.)
                        raw_result_value = loaded
                    else:
                        raw_result_value = None
                else:
                    raw_result_value = None

                if include_failed_reason:
                    failed_reason = row[idx]

                # Load attempt history if requested
                attempts: list[TaskAttemptInfo] | None = None
                if include_attempts:
                    att_result = await session.execute(
                        SELECT_TASK_ATTEMPTS_BY_TASK_ID_SQL,
                        {'task_id': task_id},
                    )
                    attempts = [
                        TaskAttemptInfo(
                            task_id=att_row[0],
                            attempt=att_row[1],
                            outcome=TaskAttemptOutcome(att_row[2]),
                            will_retry=att_row[3],
                            started_at=att_row[4],
                            finished_at=att_row[5],
                            error_code=att_row[6],
                            error_message=att_row[7],
                            failed_reason=att_row[8],
                            worker_id=att_row[9],
                            worker_hostname=att_row[10],
                            worker_pid=att_row[11],
                            worker_process_name=att_row[12],
                        )
                        for att_row in att_result.fetchall()
                    ]

                return Ok(
                    TaskInfo(
                        task_id=task_id_value,
                        task_name=task_name,
                        status=status,
                        queue_name=queue_name,
                        priority=priority,
                        retry_count=retry_count,
                        max_retries=max_retries,
                        next_retry_at=next_retry_at,
                        sent_at=sent_at,
                        enqueued_at=enqueued_at,
                        claimed_at=claimed_at,
                        started_at=started_at,
                        completed_at=completed_at,
                        failed_at=failed_at,
                        worker_hostname=worker_hostname,
                        worker_pid=worker_pid,
                        worker_process_name=worker_process_name,
                        error_code=error_code_value,
                        raw_result=raw_result_value,
                        decoded_result=None,
                        result_decoded=False,
                        failed_reason=failed_reason,
                        attempts=attempts,
                    )
                )
        except Exception as exc:
            return _broker_err(
                BrokerErrorCode.TASK_INFO_QUERY_FAILED,
                f'get_task_info failed for {task_id}: {exc}',
                exc,
            )

    def get_task_info(
        self,
        task_id: str,
        *,
        include_result: bool = False,
        include_failed_reason: bool = False,
        include_attempts: bool = False,
    ) -> BrokerResult[TaskInfo | None]:
        """Synchronous wrapper for get_task_info_async()."""
        try:
            return self._loop_runner.call(
                self.get_task_info_async,
                task_id,
                include_result=include_result,
                include_failed_reason=include_failed_reason,
                include_attempts=include_attempts,
            )
        except Exception as exc:
            return _broker_err(
                BrokerErrorCode.TASK_INFO_QUERY_FAILED,
                f'get_task_info failed for {task_id} (sync): {exc}',
                exc,
            )

    def close(self) -> BrokerResult[None]:
        """Synchronous cleanup (runs close_async in background loop)."""
        try:
            return self._loop_runner.call(self.close_async)
        except Exception as exc:
            return _broker_err(
                BrokerErrorCode.CLOSE_FAILED,
                f'close failed (sync): {exc}',
                exc,
            )
        finally:
            self._loop_runner.stop()
