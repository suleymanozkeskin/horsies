# app/core/brokers/postgres.py
from __future__ import annotations
import uuid, asyncio, hashlib
from typing import Any, Optional, TYPE_CHECKING
from datetime import datetime, timezone
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker
from sqlalchemy import text
from horsies.core.brokers.listener import PostgresListener
from horsies.core.models.broker import PostgresConfig
from horsies.core.models.task_pg import TaskModel, Base
from horsies.core.models.workflow_pg import WorkflowModel, WorkflowTaskModel  # noqa: F401
from horsies.core.types.status import TaskStatus
from horsies.core.codec.serde import (
    args_to_json,
    kwargs_to_json,
    loads_json,
    task_result_from_json,
)
from horsies.core.models.tasks import TaskInfo
from horsies.core.utils.loop_runner import LoopRunner
from horsies.core.logging import get_logger

if TYPE_CHECKING:
    from horsies.core.models.tasks import TaskResult, TaskError


class PostgresBroker:
    """
    PostgreSQL-based task broker with LISTEN/NOTIFY for real-time updates.

    Provides both async and sync APIs:
      - Async: enqueue_async(), get_result_async()
      - Sync: enqueue(), get_result() (run in background event loop)

    Features:
      - Real-time notifications via PostgreSQL triggers
      - Automatic task status tracking
      - Connection pooling and health monitoring
      - Operational monitoring (stale tasks, worker stats)
    """

    def __init__(self, config: PostgresConfig):
        self.config = config
        self.logger = get_logger('broker')
        self._app: Any = None  # Set by Horsies.get_broker()

        engine_cfg = self.config.model_dump(exclude={'database_url'}, exclude_none=True)
        self.async_engine = create_async_engine(self.config.database_url, **engine_cfg)
        self.session_factory = async_sessionmaker(
            self.async_engine, expire_on_commit=False
        )

        psycopg_url = self.config.database_url.replace('+asyncpg', '').replace(
            '+psycopg', ''
        )
        self.listener = PostgresListener(psycopg_url)

        self._initialized = False
        self._loop_runner = LoopRunner()  # for sync facades

        self.logger.info('PostgresBroker initialized')

    @property
    def app(self) -> Any:
        """Get the attached Horsies app instance (if any)."""
        return self._app

    @app.setter
    def app(self, value: Any) -> None:
        """Set the Horsies app instance."""
        self._app = value

    def _schema_advisory_key(self) -> int:
        """
        Compute a stable 64-bit advisory lock key for schema initialization.

        Uses the database URL as a basis so that different clusters do not
        contend on the same advisory lock key.
        """
        basis = self.config.database_url.encode('utf-8', errors='ignore')
        h = hashlib.sha256(b'horsies-schema:' + basis).digest()
        return int.from_bytes(h[:8], byteorder='big', signed=True)

    async def _create_triggers(self) -> None:
        """
        Set up PostgreSQL triggers for automatic task notifications.

        Creates triggers that send NOTIFY messages on:
        - INSERT: Sends task_new + task_queue_{queue_name} notifications
        - UPDATE to COMPLETED/FAILED: Sends task_done notification

        This enables real-time task processing without polling.
        """
        async with self.async_engine.begin() as conn:
            # Create trigger function
            await conn.execute(
                text("""
                CREATE OR REPLACE FUNCTION horsies_notify_task_changes()
                RETURNS trigger AS $$
                BEGIN
                    IF TG_OP = 'INSERT' AND NEW.status = 'PENDING' THEN
                        -- New task notifications: wake up workers
                        PERFORM pg_notify('task_new', NEW.id);  -- Global worker notification
                        PERFORM pg_notify('task_queue_' || NEW.queue_name, NEW.id);  -- Queue-specific notification
                    ELSIF TG_OP = 'UPDATE' AND OLD.status != NEW.status THEN
                        -- Task completion notifications: wake up result waiters
                        IF NEW.status IN ('COMPLETED', 'FAILED') THEN
                            PERFORM pg_notify('task_done', NEW.id);  -- Send task_id as payload
                        END IF;
                    END IF;
                    RETURN NEW;
                END;
                $$ LANGUAGE plpgsql;
            """)
            )

            # Create trigger
            await conn.execute(
                text("""
                DROP TRIGGER IF EXISTS horsies_task_notify_trigger ON horsies_tasks;
                CREATE TRIGGER horsies_task_notify_trigger
                    AFTER INSERT OR UPDATE ON horsies_tasks
                    FOR EACH ROW
                    EXECUTE FUNCTION horsies_notify_task_changes();
            """)
            )

    async def _create_workflow_schema(self) -> None:
        """
        Set up workflow-specific schema elements.

        Creates:
        - GIN index on workflow_tasks.dependencies for efficient dependency lookups
        - Trigger for workflow completion notifications
        - Migration: adds task_options column if missing (for existing installs)
        """
        async with self.async_engine.begin() as conn:
            # GIN index for efficient dependency array lookups
            await conn.execute(
                text("""
                CREATE INDEX IF NOT EXISTS idx_horsies_workflow_tasks_deps
                ON horsies_workflow_tasks USING GIN(dependencies);
            """)
            )

            # Migration: add task_options column for existing installs
            await conn.execute(
                text("""
                ALTER TABLE horsies_workflow_tasks
                ADD COLUMN IF NOT EXISTS task_options TEXT;
            """)
            )

            # Migration: add success_policy column for existing installs
            await conn.execute(
                text("""
                ALTER TABLE horsies_workflows
                ADD COLUMN IF NOT EXISTS success_policy JSONB;
            """)
            )

            # Migration: add join_type and min_success columns for existing installs
            await conn.execute(
                text("""
                ALTER TABLE horsies_workflow_tasks
                ADD COLUMN IF NOT EXISTS join_type VARCHAR(10) NOT NULL DEFAULT 'all';
            """)
            )
            await conn.execute(
                text("""
                ALTER TABLE horsies_workflow_tasks
                ADD COLUMN IF NOT EXISTS min_success INTEGER;
            """)
            )
            await conn.execute(
                text("""
                ALTER TABLE horsies_workflow_tasks
                ADD COLUMN IF NOT EXISTS node_id VARCHAR(128);
            """)
            )
            await conn.execute(
                text("""
                ALTER TABLE horsies_workflow_tasks
                ALTER COLUMN workflow_ctx_from
                TYPE VARCHAR(128)[]
                USING workflow_ctx_from::VARCHAR(128)[];
            """)
            )

            # Subworkflow support columns
            await conn.execute(
                text("""
                ALTER TABLE horsies_workflows
                ADD COLUMN IF NOT EXISTS parent_workflow_id VARCHAR(36);
            """)
            )
            await conn.execute(
                text("""
                ALTER TABLE horsies_workflows
                ADD COLUMN IF NOT EXISTS parent_task_index INTEGER;
            """)
            )
            await conn.execute(
                text("""
                ALTER TABLE horsies_workflows
                ADD COLUMN IF NOT EXISTS depth INTEGER NOT NULL DEFAULT 0;
            """)
            )
            await conn.execute(
                text("""
                ALTER TABLE horsies_workflows
                ADD COLUMN IF NOT EXISTS root_workflow_id VARCHAR(36);
            """)
            )
            await conn.execute(
                text("""
                ALTER TABLE horsies_workflows
                ADD COLUMN IF NOT EXISTS workflow_def_module VARCHAR(512);
            """)
            )
            await conn.execute(
                text("""
                ALTER TABLE horsies_workflows
                ADD COLUMN IF NOT EXISTS workflow_def_qualname VARCHAR(512);
            """)
            )

            await conn.execute(
                text("""
                ALTER TABLE horsies_workflow_tasks
                ADD COLUMN IF NOT EXISTS is_subworkflow BOOLEAN NOT NULL DEFAULT FALSE;
            """)
            )
            await conn.execute(
                text("""
                ALTER TABLE horsies_workflow_tasks
                ADD COLUMN IF NOT EXISTS sub_workflow_id VARCHAR(36);
            """)
            )
            await conn.execute(
                text("""
                ALTER TABLE horsies_workflow_tasks
                ADD COLUMN IF NOT EXISTS sub_workflow_name VARCHAR(255);
            """)
            )
            await conn.execute(
                text("""
                ALTER TABLE horsies_workflow_tasks
                ADD COLUMN IF NOT EXISTS sub_workflow_retry_mode VARCHAR(50);
            """)
            )
            await conn.execute(
                text("""
                ALTER TABLE horsies_workflow_tasks
                ADD COLUMN IF NOT EXISTS sub_workflow_summary TEXT;
            """)
            )
            await conn.execute(
                text("""
                ALTER TABLE horsies_workflow_tasks
                ADD COLUMN IF NOT EXISTS sub_workflow_module VARCHAR(512);
            """)
            )
            await conn.execute(
                text("""
                ALTER TABLE horsies_workflow_tasks
                ADD COLUMN IF NOT EXISTS sub_workflow_qualname VARCHAR(512);
            """)
            )

            # Workflow notification trigger function
            await conn.execute(
                text("""
                CREATE OR REPLACE FUNCTION horsies_notify_workflow_changes()
                RETURNS trigger AS $$
                BEGIN
                    IF TG_OP = 'UPDATE' AND OLD.status != NEW.status THEN
                        -- Workflow completion notifications
                        IF NEW.status IN ('COMPLETED', 'FAILED', 'CANCELLED', 'PAUSED') THEN
                            PERFORM pg_notify('workflow_done', NEW.id);
                        END IF;
                    END IF;
                    RETURN NEW;
                END;
                $$ LANGUAGE plpgsql;
            """)
            )

            # Create workflow trigger
            await conn.execute(
                text("""
                DROP TRIGGER IF EXISTS horsies_workflow_notify_trigger ON horsies_workflows;
                CREATE TRIGGER horsies_workflow_notify_trigger
                    AFTER UPDATE ON horsies_workflows
                    FOR EACH ROW
                    EXECUTE FUNCTION horsies_notify_workflow_changes();
            """)
            )

    async def _ensure_initialized(self) -> None:
        if self._initialized:
            return
        async with self.async_engine.begin() as conn:
            # Take a short-lived, cluster-wide advisory lock to serialize
            # schema creation across workers and producers.
            await conn.execute(
                text('SELECT pg_advisory_xact_lock(CAST(:key AS BIGINT))'),
                {'key': self._schema_advisory_key()},
            )
            await conn.run_sync(Base.metadata.create_all)

        await self._create_triggers()
        await self._create_workflow_schema()
        await self.listener.start()
        self._initialized = True

    async def ensure_schema_initialized(self) -> None:
        """
        Public entry point to ensure tables and triggers exist.

        Safe to call multiple times and from multiple processes; internally
        guarded by a PostgreSQL advisory lock to avoid DDL races.
        """
        await self._ensure_initialized()

    # ----------------- Async API -----------------

    async def enqueue_async(
        self,
        task_name: str,
        args: tuple[Any, ...],
        kwargs: dict[str, Any],
        queue_name: str = 'default',
        *,
        priority: int = 100,
        sent_at: Optional[datetime] = None,
        good_until: Optional[datetime] = None,
        task_options: Optional[str] = None,
    ) -> str:
        await self._ensure_initialized()

        task_id = str(uuid.uuid4())
        now = datetime.now(timezone.utc)
        sent = sent_at or now

        # Parse retry configuration from task_options
        max_retries = 0
        if task_options:
            try:
                options_data = loads_json(task_options)
                if isinstance(options_data, dict):
                    retry_policy = options_data.get('retry_policy')
                    if isinstance(retry_policy, dict):
                        max_retries = retry_policy.get('max_retries', 3)
            except Exception:
                pass

        async with self.session_factory() as session:
            task = TaskModel(
                id=task_id,
                task_name=task_name,
                queue_name=queue_name,
                priority=priority,
                args=args_to_json(args) if args else None,
                kwargs=kwargs_to_json(kwargs) if kwargs else None,
                status=TaskStatus.PENDING,
                sent_at=sent,
                good_until=good_until,
                max_retries=max_retries,
                task_options=task_options,
                created_at=now,
                updated_at=now,
            )
            session.add(task)
            await session.commit()

        # PostgreSQL trigger automatically sends task_new + task_queue_{queue_name} notifications
        return task_id

    async def get_result_async(
        self, task_id: str, timeout_ms: Optional[int] = None
    ) -> 'TaskResult[Any, TaskError]':
        """
        Get task result, waiting if necessary.

        Returns TaskResult for task completion and retrieval outcomes:
        - Success: TaskResult(ok=value) from task execution
        - Task error: TaskResult(err=TaskError) from task execution
        - Retrieval error: TaskResult(err=TaskError) with WAIT_TIMEOUT, TASK_NOT_FOUND, or TASK_CANCELLED

        Broker failures (e.g., database errors) are returned as BROKER_ERROR.
        """
        from horsies.core.models.tasks import TaskResult, TaskError, LibraryErrorCode

        try:
            await self._ensure_initialized()

            start_time = asyncio.get_event_loop().time()

            # Convert milliseconds to seconds for internal use
            timeout_seconds: Optional[float] = None
            if timeout_ms is not None:
                timeout_seconds = timeout_ms / 1000.0

            # Quick path - check if task is already completed
            async with self.session_factory() as session:
                row = await session.get(TaskModel, task_id)
                if row is None:
                    self.logger.error(f'Task {task_id} not found')
                    return TaskResult(
                        err=TaskError(
                            error_code=LibraryErrorCode.TASK_NOT_FOUND,
                            message=f'Task {task_id} not found in database',
                            data={'task_id': task_id},
                        )
                    )
                if row.status in (TaskStatus.COMPLETED, TaskStatus.FAILED):
                    self.logger.info(f'Task {task_id} already completed')
                    return task_result_from_json(loads_json(row.result))
                if row.status == TaskStatus.CANCELLED:
                    self.logger.info(f'Task {task_id} was cancelled')
                    return TaskResult(
                        err=TaskError(
                            error_code=LibraryErrorCode.TASK_CANCELLED,
                            message=f'Task {task_id} was cancelled before completion',
                            data={'task_id': task_id},
                        )
                    )

            # Listen for task completion notifications with polling fallback
            # Multiple clients can listen to task_done; each filters for their specific task_id
            q = await self.listener.listen('task_done')
            try:
                poll_interval = (
                    5.0  # Fallback polling interval (handles lost notifications)
                )

                while True:
                    # Calculate remaining timeout
                    remaining_timeout = None
                    if timeout_seconds:
                        elapsed = asyncio.get_event_loop().time() - start_time
                        remaining_timeout = timeout_seconds - elapsed
                        if remaining_timeout <= 0:
                            return TaskResult(
                                err=TaskError(
                                    error_code=LibraryErrorCode.WAIT_TIMEOUT,
                                    message=f'Timed out waiting for task {task_id} after {timeout_ms}ms. Task may still be running.',
                                    data={'task_id': task_id, 'timeout_ms': timeout_ms},
                                )
                            )

                    # Wait for NOTIFY or timeout (whichever comes first)
                    wait_time = (
                        min(poll_interval, remaining_timeout)
                        if remaining_timeout
                        else poll_interval
                    )

                    try:

                        async def _wait_for_task() -> None:
                            # Filter notifications: only process our specific task_id
                            while True:
                                note = (
                                    await q.get()
                                )  # Blocks until any task_done notification
                                if (
                                    note.payload == task_id
                                ):  # Check if it's for our task
                                    return  # Found our task completion!

                        # Wait for our specific task notification with timeout
                        await asyncio.wait_for(_wait_for_task(), timeout=wait_time)
                        break  # Got our task completion notification

                    except asyncio.TimeoutError:
                        # Fallback polling: handles lost notifications or crashed workers
                        # Ensures eventual consistency even if NOTIFY system fails
                        async with self.session_factory() as session:
                            row = await session.get(TaskModel, task_id)
                            if row is None:
                                return TaskResult(
                                    err=TaskError(
                                        error_code=LibraryErrorCode.TASK_NOT_FOUND,
                                        message=f'Task {task_id} not found in database',
                                        data={'task_id': task_id},
                                    )
                                )
                            if row.status in (TaskStatus.COMPLETED, TaskStatus.FAILED):
                                self.logger.debug(
                                    f'Task {task_id} completed, polling database'
                                )
                                return task_result_from_json(loads_json(row.result))
                            if row.status == TaskStatus.CANCELLED:
                                self.logger.error(f'Task {task_id} was cancelled')
                                return TaskResult(
                                    err=TaskError(
                                        error_code=LibraryErrorCode.TASK_CANCELLED,
                                        message=f'Task {task_id} was cancelled before completion',
                                        data={'task_id': task_id},
                                    )
                                )
                        # Task still not done, continue waiting...

            finally:
                # Clean up subscription (keeps server-side LISTEN active for other waiters)
                await self.listener.unsubscribe('task_done', q)

            # Final database check to get actual task result after notification
            async with self.session_factory() as session:
                row = await session.get(TaskModel, task_id)
                if row is None:
                    self.logger.error(f'Task {task_id} not found')
                    return TaskResult(
                        err=TaskError(
                            error_code=LibraryErrorCode.TASK_NOT_FOUND,
                            message=f'Task {task_id} not found in database',
                            data={'task_id': task_id},
                        )
                    )
                if row.status == TaskStatus.CANCELLED:
                    self.logger.error(f'Task {task_id} was cancelled')
                    return TaskResult(
                        err=TaskError(
                            error_code=LibraryErrorCode.TASK_CANCELLED,
                            message=f'Task {task_id} was cancelled before completion',
                            data={'task_id': task_id},
                        )
                    )
                if row.status not in (TaskStatus.COMPLETED, TaskStatus.FAILED):
                    return TaskResult(
                        err=TaskError(
                            error_code=LibraryErrorCode.WAIT_TIMEOUT,
                            message=f'Task {task_id} not completed after notification (status: {row.status}). Task may still be running.',
                            data={'task_id': task_id, 'status': str(row.status)},
                        )
                    )
                return task_result_from_json(loads_json(row.result))
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            self.logger.exception('Broker error while retrieving task result')
            return TaskResult(
                err=TaskError(
                    error_code=LibraryErrorCode.BROKER_ERROR,
                    message='Broker error while retrieving task result',
                    data={'task_id': task_id, 'timeout_ms': timeout_ms},
                    exception=exc,
                )
            )

    async def close_async(self) -> None:
        await self.listener.close()
        await self.async_engine.dispose()

    # ------------- Operational & Monitoring Methods -------------

    async def get_stale_tasks(
        self, stale_threshold_minutes: int = 2
    ) -> list[dict[str, Any]]:
        """
        Identify potentially crashed tasks based on heartbeat absence.

        Finds RUNNING tasks whose workers haven't sent heartbeats within the threshold,
        indicating the worker process may have crashed or become unresponsive.

        Args:
            stale_threshold_minutes: Minutes without heartbeat to consider stale

        Returns:
            List of task info dicts: id, worker_hostname, worker_pid, last_heartbeat
        """
        async with self.session_factory() as session:
            result = await session.execute(
                text("""
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
            """),
                {'stale_threshold': stale_threshold_minutes},
            )
            columns = result.keys()
            return [dict(zip(columns, row)) for row in result.fetchall()]

    async def get_worker_stats(self) -> list[dict[str, Any]]:
        """
        Gather statistics about active worker processes.

        Groups RUNNING tasks by worker to show load distribution and health.
        Useful for monitoring worker performance and identifying bottlenecks.

        Returns:
            List of worker stats: worker_hostname, worker_pid, active_tasks, oldest_task_start
        """
        async with self.session_factory() as session:
            result = await session.execute(
                text("""
                SELECT 
                    t.worker_hostname,
                    t.worker_pid,
                    t.worker_process_name,
                    COUNT(*) AS active_tasks,
                    MIN(t.started_at) AS oldest_task_start,
                    MAX(hb.last_heartbeat) AS latest_heartbeat
                FROM horsies_tasks t
                LEFT JOIN LATERAL (
                    SELECT sent_at AS last_heartbeat
                    FROM horsies_heartbeats h
                    WHERE h.task_id = t.id AND h.role = 'runner'
                    ORDER BY sent_at DESC
                    LIMIT 1
                ) hb ON TRUE
                WHERE t.status = 'RUNNING'
                  AND t.worker_hostname IS NOT NULL
                GROUP BY t.worker_hostname, t.worker_pid, t.worker_process_name
                ORDER BY active_tasks DESC
            """)
            )

            columns = result.keys()
            return [dict(zip(columns, row)) for row in result.fetchall()]

    async def get_expired_tasks(self) -> list[dict[str, Any]]:
        """
        Find tasks that expired before worker processing.

        Identifies PENDING tasks that exceeded their good_until deadline,
        indicating potential worker capacity issues or scheduling problems.

        Returns:
            List of expired task info: id, task_name, queue_name, good_until, expired_for
        """
        async with self.session_factory() as session:
            result = await session.execute(
                text("""
                SELECT 
                    id,
                    task_name,
                    queue_name,
                    priority,
                    sent_at,
                    good_until,
                    NOW() - good_until as expired_for
                FROM horsies_tasks
                WHERE status = 'PENDING'
                  AND good_until < NOW()
                ORDER BY good_until ASC
            """)
            )

            columns = result.keys()
            return [dict(zip(columns, row)) for row in result.fetchall()]

    async def mark_stale_tasks_as_failed(
        self, stale_threshold_ms: int = 300_000
    ) -> int:
        """
        Clean up crashed worker tasks by marking them as FAILED.

        Updates RUNNING tasks that haven't received heartbeats within the threshold.
        This is typically called by a cleanup process to handle worker crashes.
        Creates a proper TaskResult with WORKER_CRASHED error code.

        Args:
            stale_threshold_ms: Milliseconds without heartbeat to consider crashed (default: 300000 = 5 minutes)

        Returns:
            Number of tasks marked as failed
        """
        from horsies.core.models.tasks import TaskResult, TaskError, LibraryErrorCode
        from horsies.core.codec.serde import dumps_json

        # Convert milliseconds to seconds for PostgreSQL INTERVAL
        stale_threshold_seconds = stale_threshold_ms / 1000.0

        async with self.session_factory() as session:
            # First, find stale tasks and get their metadata
            stale_tasks_result = await session.execute(
                text("""
                SELECT t2.id, t2.worker_pid, t2.worker_hostname, t2.claimed_by_worker_id,
                       t2.started_at, hb.last_heartbeat
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
                  AND COALESCE(hb.last_heartbeat, t2.started_at) < NOW() - CAST(:stale_threshold || ' seconds' AS INTERVAL)
            """),
                {'stale_threshold': stale_threshold_seconds},
            )

            stale_tasks = stale_tasks_result.fetchall()
            if not stale_tasks:
                return 0

            # Mark each stale task as failed with proper TaskResult
            for task_row in stale_tasks:
                task_id = task_row[0]
                worker_pid = task_row[1]
                worker_hostname = task_row[2]
                worker_id = task_row[3]
                started_at = task_row[4]
                last_heartbeat = task_row[5]

                # Create TaskResult with WORKER_CRASHED error
                task_error = TaskError(
                    error_code=LibraryErrorCode.WORKER_CRASHED,
                    message=f'Worker process crashed (no runner heartbeat for {stale_threshold_ms}ms = {stale_threshold_ms/1000:.1f}s)',
                    data={
                        'stale_threshold_ms': stale_threshold_ms,
                        'stale_threshold_seconds': stale_threshold_seconds,
                        'worker_pid': worker_pid,
                        'worker_hostname': worker_hostname,
                        'worker_id': worker_id,
                        'started_at': started_at.isoformat() if started_at else None,
                        'last_heartbeat': last_heartbeat.isoformat()
                        if last_heartbeat
                        else None,
                        'detected_at': datetime.now(timezone.utc).isoformat(),
                    },
                )
                task_result: TaskResult[None, TaskError] = TaskResult(err=task_error)
                result_json = dumps_json(task_result)

                # Update task with proper result
                await session.execute(
                    text("""
                    UPDATE horsies_tasks
                    SET status = 'FAILED',
                        failed_at = NOW(),
                        failed_reason = :failed_reason,
                        result = :result,
                        updated_at = NOW()
                    WHERE id = :task_id
                """),
                    {
                        'task_id': task_id,
                        'failed_reason': f'Worker process crashed (no runner heartbeat for {stale_threshold_ms}ms = {stale_threshold_ms/1000:.1f}s)',
                        'result': result_json,
                    },
                )

            await session.commit()
            return len(stale_tasks)

    async def requeue_stale_claimed(self, stale_threshold_ms: int = 120_000) -> int:
        """
        Requeue tasks stuck in CLAIMED without recent claimer heartbeat.

        Args:
            stale_threshold_ms: Milliseconds without heartbeat to consider stale (default: 120000 = 2 minutes)

        Returns:
            Number of tasks requeued
        """
        # Convert milliseconds to seconds for PostgreSQL INTERVAL
        stale_threshold_seconds = stale_threshold_ms / 1000.0

        async with self.session_factory() as session:
            result = await session.execute(
                text("""
                UPDATE horsies_tasks AS t
                SET status = 'PENDING',
                    claimed = FALSE,
                    claimed_at = NULL,
                    claimed_by_worker_id = NULL,
                    updated_at = NOW()
                FROM (
                    SELECT t2.id, hb.last_heartbeat, t2.claimed_at
                    FROM horsies_tasks t2
                    LEFT JOIN LATERAL (
                        SELECT sent_at AS last_heartbeat
                        FROM horsies_heartbeats h
                        WHERE h.task_id = t2.id AND h.role = 'claimer'
                        ORDER BY sent_at DESC
                        LIMIT 1
                    ) hb ON TRUE
                    WHERE t2.status = 'CLAIMED'
                ) s
                WHERE t.id = s.id
                  AND (
                    (s.last_heartbeat IS NULL AND s.claimed_at IS NOT NULL AND s.claimed_at < NOW() - CAST(:stale_threshold || ' seconds' AS INTERVAL))
                    OR (s.last_heartbeat IS NOT NULL AND s.last_heartbeat < NOW() - CAST(:stale_threshold || ' seconds' AS INTERVAL))
                  )
            """),
                {'stale_threshold': stale_threshold_seconds},
            )
            await session.commit()
            return getattr(result, 'rowcount', 0)

    # ----------------- Sync API Facades -----------------

    def enqueue(
        self,
        task_name: str,
        args: tuple[Any, ...],
        kwargs: dict[str, Any],
        queue_name: str = 'default',
        *,
        priority: int = 100,
        sent_at: Optional[datetime] = None,
        good_until: Optional[datetime] = None,
        task_options: Optional[str] = None,
    ) -> str:
        """
        Synchronous task submission (runs enqueue_async in background loop).
        """
        return self._loop_runner.call(
            self.enqueue_async,
            task_name,
            args,
            kwargs,
            queue_name,
            priority=priority,
            sent_at=sent_at,
            good_until=good_until,
            task_options=task_options,
        )

    def get_result(
        self, task_id: str, timeout_ms: Optional[int] = None
    ) -> 'TaskResult[Any, TaskError]':
        """
        Synchronous result retrieval (runs get_result_async in background loop).

        Args:
            task_id: The task ID to retrieve result for
            timeout_ms: Maximum time to wait for result (milliseconds)

        Returns:
            TaskResult - success, task error, retrieval error, or broker error
        """
        return self._loop_runner.call(self.get_result_async, task_id, timeout_ms)

    async def get_task_info_async(
        self,
        task_id: str,
        *,
        include_result: bool = False,
        include_failed_reason: bool = False,
    ) -> 'TaskInfo | None':
        """Fetch metadata for a task by ID."""
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
                'claimed_at',
                'started_at',
                'completed_at',
                'failed_at',
                'worker_hostname',
                'worker_pid',
                'worker_process_name',
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
                return None

            result_value = None
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

            if include_result:
                raw_result = row[idx]
                idx += 1
                if raw_result:
                    result_value = task_result_from_json(loads_json(raw_result))

            if include_failed_reason:
                failed_reason = row[idx]

            return TaskInfo(
                task_id=task_id_value,
                task_name=task_name,
                status=status,
                queue_name=queue_name,
                priority=priority,
                retry_count=retry_count,
                max_retries=max_retries,
                next_retry_at=next_retry_at,
                sent_at=sent_at,
                claimed_at=claimed_at,
                started_at=started_at,
                completed_at=completed_at,
                failed_at=failed_at,
                worker_hostname=worker_hostname,
                worker_pid=worker_pid,
                worker_process_name=worker_process_name,
                result=result_value,
                failed_reason=failed_reason,
            )

    def get_task_info(
        self,
        task_id: str,
        *,
        include_result: bool = False,
        include_failed_reason: bool = False,
    ) -> 'TaskInfo | None':
        """Synchronous wrapper for get_task_info_async()."""
        return self._loop_runner.call(
            self.get_task_info_async,
            task_id,
            include_result=include_result,
            include_failed_reason=include_failed_reason,
        )

    def close(self) -> None:
        """
        Synchronous cleanup (runs close_async in background loop).
        """
        try:
            self._loop_runner.call(self.close_async)
        finally:
            self._loop_runner.stop()
