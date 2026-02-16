"""Tests for PostgreSQL task persistence models."""

from __future__ import annotations

import pytest

from horsies.core.models.task_pg import TaskHeartbeatModel


@pytest.mark.unit
def test_task_heartbeat_model_has_composite_latest_heartbeat_index() -> None:
    """Heartbeat table keeps an index aligned with latest-heartbeat lookups."""
    index = next(
        idx
        for idx in TaskHeartbeatModel.__table__.indexes
        if idx.name == 'idx_horsies_heartbeats_task_role_sent'
    )
    indexed_columns = [col.name for col in index.columns]
    assert indexed_columns == ['task_id', 'role']
    assert any(
        'sent_at' in str(expr).lower() and 'desc' in str(expr).lower()
        for expr in index.expressions
    )
