"""SQLAlchemy models for workflow persistence."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Optional

from sqlalchemy import String, Text, Integer, DateTime, ForeignKey, UniqueConstraint, text
from sqlalchemy.dialects.postgresql import ARRAY, JSONB
from sqlalchemy.orm import Mapped, mapped_column

from horsies.core.models.task_pg import Base


class WorkflowModel(Base):
    """
    SQLAlchemy model for workflow instances.

    Tracks the overall state of a workflow execution, including:
    - Current status (PENDING, RUNNING, COMPLETED, FAILED, PAUSED, CANCELLED)
    - Error handling policy (fail or pause on task error)
    - Explicit output task (if specified)
    - Final result and any errors
    - Parent workflow relationship (for nested/subworkflows)
    """

    __tablename__ = 'horsies_workflows'

    # Primary key
    id: Mapped[str] = mapped_column(
        String(36), primary_key=True
    )  # UUID stored as string for consistency with tasks

    # Workflow metadata
    name: Mapped[str] = mapped_column(String(255), nullable=False)
    status: Mapped[str] = mapped_column(
        String(50), nullable=False, default='PENDING', index=True
    )
    on_error: Mapped[str] = mapped_column(String(50), nullable=False, default='fail')

    # Output task configuration
    output_task_index: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)

    # Success policy (serialized as JSONB with task indices)
    # Format: {"cases": [{"required_indices": [0, 2]}], "optional_indices": [1]}
    success_policy: Mapped[Optional[dict[str, list[int]]]] = mapped_column(
        JSONB, nullable=True
    )

    # Results and errors
    result: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    error: Mapped[Optional[str]] = mapped_column(Text, nullable=True)

    # -------------------------------------------------------------------------
    # Workflow definition identity (for import-based recovery/conditions)
    # -------------------------------------------------------------------------

    workflow_def_module: Mapped[Optional[str]] = mapped_column(
        String(512), nullable=True
    )
    workflow_def_qualname: Mapped[Optional[str]] = mapped_column(
        String(512), nullable=True
    )

    # -------------------------------------------------------------------------
    # Subworkflow support: parent-child relationship
    # -------------------------------------------------------------------------

    # Parent workflow (if this is a subworkflow)
    parent_workflow_id: Mapped[Optional[str]] = mapped_column(
        String(36),
        ForeignKey('horsies_workflows.id', ondelete='CASCADE'),
        nullable=True,
        index=True,
    )

    # Index of the SubWorkflowNode in the parent workflow
    parent_task_index: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)

    # Nesting depth (0 = root, 1 = child, 2 = grandchild, etc.)
    depth: Mapped[int] = mapped_column(Integer, nullable=False, default=0)

    # Root workflow ID for efficient queries across nesting levels
    root_workflow_id: Mapped[Optional[str]] = mapped_column(
        String(36), nullable=True, index=True
    )

    # -------------------------------------------------------------------------
    # Timestamps
    # -------------------------------------------------------------------------

    sent_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=text('NOW()'),
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=datetime.now(timezone.utc)
    )
    started_at: Mapped[Optional[datetime]] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    completed_at: Mapped[Optional[datetime]] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        default=datetime.now(timezone.utc),
        onupdate=datetime.now(timezone.utc),
        index=True,
    )


class WorkflowTaskModel(Base):
    """
    SQLAlchemy model for workflow task nodes.

    Represents a single task within a workflow DAG, including:
    - Task specification (name, args, kwargs, queue, priority)
    - Dependencies (array of task indices this task waits for)
    - Data flow configuration (args_from mapping, workflow_ctx_from)
    - Execution state and result
    - Link to actual task in tasks table once enqueued
    """

    __tablename__ = 'horsies_workflow_tasks'

    # Primary key
    id: Mapped[str] = mapped_column(String(36), primary_key=True)

    # Workflow reference
    workflow_id: Mapped[str] = mapped_column(
        String(36),
        ForeignKey('horsies_workflows.id', ondelete='CASCADE'),
        nullable=False,
        index=True,
    )

    # Position in workflow
    task_index: Mapped[int] = mapped_column(Integer, nullable=False)
    node_id: Mapped[Optional[str]] = mapped_column(
        String(128), nullable=True, index=True
    )

    # Task specification
    task_name: Mapped[str] = mapped_column(String(255), nullable=False)
    # Compat: new writes always store [] (kwargs-only contract).
    # Column retained for read compatibility with pre-kwargs-only rows.
    # Pending future column drop once all persisted rows have been migrated.
    task_args: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    task_kwargs: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    queue_name: Mapped[str] = mapped_column(
        String(100), nullable=False, default='default'
    )
    priority: Mapped[int] = mapped_column(Integer, nullable=False, default=100)

    # DAG structure: indices of tasks this task waits for
    dependencies: Mapped[list[int]] = mapped_column(
        ARRAY(Integer), nullable=False, default=[]
    )

    # Data flow: {"kwarg_name": task_index, ...}
    args_from: Mapped[Optional[dict[str, int]]] = mapped_column(JSONB, nullable=True)

    # Context injection: node_ids to include in WorkflowContext
    workflow_ctx_from: Mapped[Optional[list[str]]] = mapped_column(
        ARRAY(String), nullable=True
    )

    # If True, task runs even if dependencies failed (receives failed TaskResults)
    allow_failed_deps: Mapped[bool] = mapped_column(default=False, nullable=False)

    # Join semantics: "all" (default), "any", or "quorum"
    join_type: Mapped[str] = mapped_column(String(10), nullable=False, default='all')

    # For join_type="quorum": minimum number of dependencies that must succeed
    min_success: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)

    # Task options (retry policy, auto_retry_for, etc.) - serialized JSON
    task_options: Mapped[Optional[str]] = mapped_column(Text, nullable=True)

    # Execution state
    status: Mapped[str] = mapped_column(
        String(50), nullable=False, default='PENDING', index=True
    )

    # Link to actual task once enqueued (for TaskNode only)
    task_id: Mapped[Optional[str]] = mapped_column(
        String(36), nullable=True, index=True
    )

    # -------------------------------------------------------------------------
    # SubWorkflowNode support
    # -------------------------------------------------------------------------

    # True if this node is a SubWorkflowNode (not a TaskNode)
    is_subworkflow: Mapped[bool] = mapped_column(default=False, nullable=False)

    # Link to child workflow (for SubWorkflowNode)
    sub_workflow_id: Mapped[Optional[str]] = mapped_column(
        String(36),
        ForeignKey('horsies_workflows.id', ondelete='SET NULL'),
        nullable=True,
        index=True,
    )

    # Child workflow definition name (for SubWorkflowNode)
    sub_workflow_name: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)

    # Import path for subworkflow definition (fallback if registry not loaded)
    sub_workflow_module: Mapped[Optional[str]] = mapped_column(
        String(512), nullable=True
    )
    sub_workflow_qualname: Mapped[Optional[str]] = mapped_column(
        String(512), nullable=True
    )

    # Retry mode for subworkflow (rerun_failed_only, rerun_all, no_rerun)
    sub_workflow_retry_mode: Mapped[Optional[str]] = mapped_column(
        String(50), nullable=True
    )

    # Summary of subworkflow execution (serialized SubWorkflowSummary)
    sub_workflow_summary: Mapped[Optional[str]] = mapped_column(Text, nullable=True)

    # -------------------------------------------------------------------------
    # Results and errors
    # -------------------------------------------------------------------------

    result: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    error: Mapped[Optional[str]] = mapped_column(Text, nullable=True)

    # Timestamps
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, default=datetime.now(timezone.utc)
    )
    started_at: Mapped[Optional[datetime]] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    completed_at: Mapped[Optional[datetime]] = mapped_column(
        DateTime(timezone=True), nullable=True
    )

    # Unique constraint: one task per index per workflow
    __table_args__ = (
        UniqueConstraint(
            'workflow_id', 'task_index', name='uq_horsies_workflow_task_index'
        ),
    )


# Note: The following index should be created via raw SQL for optimal performance:
# CREATE INDEX IF NOT EXISTS idx_horsies_workflow_tasks_deps ON horsies_workflow_tasks USING GIN(dependencies);
# This is handled in the broker's schema initialization.
