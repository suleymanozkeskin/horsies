from __future__ import annotations
from datetime import datetime, timezone
from typing import Any, Optional
from sqlalchemy import (
    String,
    Text,
    Boolean,
    DateTime,
    Integer,
    Index,
    Enum as SQLAlchemyEnum,
    Float,
    ARRAY,
    false as sa_false,
    text,
)
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column
from sqlalchemy.dialects.postgresql import JSONB

from horsies.core.types.status import TaskStatus


class Base(DeclarativeBase):
    """SQLAlchemy declarative base for PostgreSQL models"""

    pass


class TaskModel(Base):
    """
    SQLAlchemy model for storing tasks in the database.

    - id: str # uuid4
    - task_name: str # name of the task, coming from the task decorator
    - queue_name: str # name of the queue, coming from the task decorator, defaulting to "default"
    - priority: int # 1..100, set by the queue's priority value, defaulting to 100, least important
    - args: str # task arguments, serialized as json
    - kwargs: str # task keyword arguments, serialized as json
    - status: TaskStatus # PENDING, CLAIMED, RUNNING, COMPLETED, FAILED
    - sent_at: datetime # immutable call-site timestamp (when .send()/.schedule()/.start() was called)
    - enqueued_at: datetime # mutable dispatch timestamp (when task becomes eligible for claiming)
    - claimed_at: datetime # when task was claimed by a worker
    - started_at: datetime # when task actually started running in the worker's process
    - completed_at: datetime # when task was completed, set by the process
    - failed_at: datetime # when task failed, set by the process
    - result: str # task result, serialized as json
    - failed_reason: str # reason for task failure, serialized as json
    - claimed: bool # whether the task is claimed by a worker
    - claim_expires_at: datetime # when a prefetched claim expires and can be reclaimed
    - good_until: datetime # when the task will be considered expired and retried
    - retry_count: int # current number of retry attempts
    - max_retries: int # maximum number of retry attempts allowed
    - next_retry_at: datetime # when the task should be retried next
    - task_options: str # serialized TaskOptions configuration for retry policies
    - worker_pid: int # process ID of the worker executing this task
    - worker_hostname: str # hostname of the machine running the worker
    - worker_process_name: str # name/identifier of the worker process
    - created_at: datetime # when the task was created
    - updated_at: datetime # when the task was last updated
    """

    __tablename__ = 'horsies_tasks'

    # Basic task information
    id: Mapped[str] = mapped_column(String(36), primary_key=True)
    task_name: Mapped[str] = mapped_column(String(255), nullable=False)
    queue_name: Mapped[str] = mapped_column(String(100), nullable=False, index=True)
    priority: Mapped[int] = mapped_column(
        nullable=False, default=100, server_default=text('100'),
    )  # 1..100

    # Function arguments (stored as JSON)
    args: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    kwargs: Mapped[Optional[str]] = mapped_column(Text, nullable=True)

    # Task status and execution tracking
    status: Mapped[TaskStatus] = mapped_column(
        SQLAlchemyEnum(TaskStatus, native_enum=False),
        nullable=False,
        default=TaskStatus.PENDING,
        index=True,
    )

    # Timestamps
    sent_at: Mapped[Optional[datetime]] = mapped_column(
        DateTime(timezone=True), nullable=True
    )  # immutable call-site timestamp (Python clock); never mutated after insert
    enqueued_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=text('NOW()'),
    )  # mutable dispatch/eligibility timestamp; updated on retry
    claimed_at: Mapped[Optional[datetime]] = mapped_column(
        DateTime(timezone=True), nullable=True
    )  # when the task was claimed by a worker
    started_at: Mapped[Optional[datetime]] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    completed_at: Mapped[Optional[datetime]] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    failed_at: Mapped[Optional[datetime]] = mapped_column(
        DateTime(timezone=True), nullable=True
    )

    # Results and error handling
    result: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    failed_reason: Mapped[Optional[str]] = mapped_column(Text, nullable=True)

    # Task claiming and lifecycle
    claimed: Mapped[bool] = mapped_column(
        Boolean, default=False, server_default=sa_false(), index=True,
    )
    claimed_by_worker_id: Mapped[Optional[str]] = mapped_column(
        String(255), nullable=True
    )
    # Claim lease expiry: prefetched claims expire after this time and can be reclaimed.
    # NULL means no expiry (task is within running capacity, not prefetched).
    claim_expires_at: Mapped[Optional[datetime]] = mapped_column(
        DateTime(timezone=True), nullable=True, index=True
    )
    good_until: Mapped[Optional[datetime]] = mapped_column(
        DateTime(timezone=True), nullable=True, index=True
    )

    # Retry configuration and tracking
    retry_count: Mapped[int] = mapped_column(
        Integer, default=0, server_default=text('0'), nullable=False,
    )
    max_retries: Mapped[int] = mapped_column(
        Integer, default=0, server_default=text('0'), nullable=False,
    )
    next_retry_at: Mapped[Optional[datetime]] = mapped_column(
        DateTime(timezone=True), nullable=True, index=True
    )
    task_options: Mapped[Optional[str]] = mapped_column(Text, nullable=True)

    # Worker process tracking
    worker_pid: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    worker_hostname: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)
    worker_process_name: Mapped[Optional[str]] = mapped_column(
        String(255), nullable=True
    )

    # Metadata
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=datetime.now(timezone.utc),
        server_default=text('NOW()'),
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=datetime.now(timezone.utc),
        server_default=text('NOW()'),
        onupdate=datetime.now(timezone.utc),
    )


class TaskHeartbeatModel(Base):
    """Normalized heartbeat entries for task liveness tracking."""

    __tablename__ = 'horsies_heartbeats'
    __table_args__ = (
        Index(
            'idx_horsies_heartbeats_task_role_sent',
            'task_id',
            'role',
            text('sent_at DESC'),
        ),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    task_id: Mapped[str] = mapped_column(String(36), nullable=False)
    sender_id: Mapped[str] = mapped_column(String(255), nullable=False)
    role: Mapped[str] = mapped_column(String(20), nullable=False)
    sent_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=datetime.now(timezone.utc), nullable=False
    )
    hostname: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)
    pid: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)


class WorkerStateModel(Base):
    """Worker instance state tracking for monitoring and cluster management.

    Timeseries table - each row is a snapshot taken every 5 seconds.
    Enables historical analysis and trend visualization for TUI/web monitoring.

    Retention: Recommend deleting snapshots older than 7-30 days to prevent unbounded growth.
    """

    __tablename__ = 'horsies_worker_states'

    # Timeseries primary key
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    worker_id: Mapped[str] = mapped_column(String(255), nullable=False, index=True)
    snapshot_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        index=True,
        default=datetime.now(timezone.utc),
    )

    # Identity
    hostname: Mapped[str] = mapped_column(String(255), nullable=False)
    pid: Mapped[int] = mapped_column(Integer, nullable=False)

    # Configuration snapshot (from WorkerConfig)
    processes: Mapped[int] = mapped_column(Integer, nullable=False)
    max_claim_batch: Mapped[int] = mapped_column(Integer, nullable=False)
    max_claim_per_worker: Mapped[int] = mapped_column(Integer, nullable=False)
    cluster_wide_cap: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    queues: Mapped[list[str]] = mapped_column(ARRAY(String), nullable=False)

    # Queue configuration (CUSTOM mode)
    queue_priorities: Mapped[Optional[dict[str, int]]] = mapped_column(
        JSONB, nullable=True
    )
    queue_max_concurrency: Mapped[Optional[dict[str, int]]] = mapped_column(
        JSONB, nullable=True
    )

    # Recovery configuration snapshot
    recovery_config: Mapped[Optional[dict[str, Any]]] = mapped_column(
        JSONB, nullable=True
    )

    # Current load at snapshot time
    tasks_running: Mapped[int] = mapped_column(
        Integer, default=0, nullable=False, index=True
    )
    tasks_claimed: Mapped[int] = mapped_column(
        Integer, default=0, nullable=False, index=True
    )

    # System metrics (via psutil)
    memory_usage_mb: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    memory_percent: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    cpu_percent: Mapped[Optional[float]] = mapped_column(Float, nullable=True)

    # Worker lifecycle metadata
    worker_started_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False
    )


class ScheduleStateModel(Base):
    """Schedule execution state tracking for scheduler service.

    Tracks the last and next execution times for each scheduled task.
    Ensures schedules don't run multiple times and enables catch-up logic.

    Fields:
        - schedule_name: Unique schedule identifier (from TaskSchedule.name)
        - last_run_at: When the schedule last executed successfully
        - next_run_at: When the schedule should run next (calculated by scheduler)
        - last_task_id: Task ID of the most recently enqueued task
        - run_count: Total number of times this schedule has executed
        - config_hash: Hash of schedule configuration (pattern + timezone) for change detection
        - updated_at: Last state update timestamp
    """

    __tablename__ = 'horsies_schedule_state'

    schedule_name: Mapped[str] = mapped_column(String(255), primary_key=True)
    last_run_at: Mapped[Optional[datetime]] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    next_run_at: Mapped[Optional[datetime]] = mapped_column(
        DateTime(timezone=True), nullable=True, index=True
    )
    last_task_id: Mapped[Optional[str]] = mapped_column(String(36), nullable=True)
    run_count: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    config_hash: Mapped[Optional[str]] = mapped_column(String(64), nullable=True)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=datetime.now(timezone.utc),
        onupdate=datetime.now(timezone.utc),
        nullable=False,
    )


"""
NOTE:
- When a task returns a `TaskError`, the task is not necessarily failed.
  It could be that be that running the task itself was successful from library's perspective,
  but the task function itself wanted to return an error for a given condition.

    Scenario 1:
    ```python
    @app.task()
    def task_function(input: int) -> TaskResult[int, TaskError]:
        if input < 0:
            return TaskResult(err=TaskError(
            error_code="NEGATIVE_INPUT",
            message="Input cannot be negative",
        ))
        return TaskResult(ok=input * 2)

    # In this case, task did not fail, rather returns an error as expected.
    handle = task_function.send(-1)  # This will return a TaskError
    result = handle.get()
    ```

  Scenario 2:
  ```python
    class Dog:
        def __init__(self, name: str):
            self.name = name

    def return_dog(input: str) -> Dog:
        return Dog(input)


    @app.task()
    def example_task(input: str) -> TaskResult[Dog, TaskError]:
        try:
            unserializable_return = return_dog(input)
            return TaskResult(ok=unserializable_return)
        except Exception as e:
            return TaskResult(err=TaskError(
                error_code="YOUR_ERROR_CODE", # this is still the case you as the developer, catch in the task function
                message="Your error message",
                data={"error": str(e)},
            ))

    handle = example_task.send("Rex")
    result = handle.get()
    if result.is_err():
        print(f"✓ Task execution failed: {result.unwrap_err()}") # Since a python class of Dog is not serializable by the library, 
        # this will return a TaskError with error_code="WORKER_SERIALIZATION_ERROR" and message="Cannot serialize value of type Dog"
        # And the task will be marked as failed. 
    ```
  
  """
