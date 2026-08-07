"""Every chain-installed function declares its two-worlds side.

The chain serves uuid-born and varchar-born databases; a function
whose declared or returned types reference the live table's identity
must take a position — type-agnostic (works on both shapes),
varchar-world-only (installed inside the fork's else-arm and replaced
by the move family everywhere else), or era-correct (its callers
exist only where the shape is already uuid). The enumeration is
closed the kind-table way: a chain function absent from the table
fails here, so a future function must declare its side before it can
ship.
"""

from __future__ import annotations

import inspect
import re

import pytest

import horsies.core.schemas.migrations as migrations_module
import horsies.core.schemas.terminalization as terminalization_module
import horsies.core.schemas.triggers as triggers_module
from horsies.core.history.identity import reservations as reservations_module

pytestmark = [pytest.mark.unit]

_CREATE = re.compile(r'CREATE (?:OR REPLACE )?FUNCTION\s+([a-z_]+)')

# name -> (disposition, reason)
DISPOSITIONS: dict[str, tuple[str, str]] = {
    'horsies_claim': (
        'type-agnostic',
        'identity declared text and returned through an explicit cast; '
        'both birth shapes satisfy it',
    ),
    'horsies_terminalization_miss': (
        'varchar-world-only',
        'installed inside the fork else-arm; the move family carries '
        'its own classifier',
    ),
    **{
        name: (
            'varchar-world-only',
            'in-place terminalization; the fresh world installs the '
            'move family instead',
        )
        for name in (
            'horsies_complete_locked_task',
            'horsies_complete_task_fused',
            'horsies_fail_locked_task',
            'horsies_fail_stale_task',
            'horsies_expire_owned_claim',
            'horsies_expire_pending_tasks',
            'horsies_cancel_locked_task',
            'horsies_cancel_owned_orphan',
            'horsies_cancel_orphaned_tasks',
            'horsies_abandon_owned_node',
            'horsies_abandon_owned_nodes',
            'horsies_abandon_nodes_of_paused_workflows',
            'horsies_cancel_owned_node',
            'horsies_cancel_owned_nodes',
            'horsies_cancel_nodes_of_cancelled_workflow',
        )
    },
    **{
        name: (
            'type-agnostic',
            'trigger payloads cast the identity to text explicitly',
        )
        for name in (
            'horsies_notify_task_changes',
            'horsies_notify_task_status_change',
            'horsies_notify_worker_state_change',
            'horsies_notify_workflow_changes',
            'horsies_notify_workflow_status_change',
        )
    },
    **{
        name: (
            'era-correct',
            'uuid declarations; callers exist only where the identity '
            'shape is already uuid (fresh installs and post-cutover)',
        )
        for name in (
            'horsies_key_reservation_claim',
            'horsies_key_reservation_terminalize',
            'horsies_key_reservation_terminalize_batch',
            'horsies_key_reservation_cleanup',
        )
    },
}


class TestChainFunctionsDeclareTheirWorld:
    def test_every_chain_function_has_a_disposition(self) -> None:
        found: set[str] = set()
        for module in (
            migrations_module,
            terminalization_module,
            triggers_module,
        ):
            found.update(_CREATE.findall(inspect.getsource(module)))
        # Reservation DDL interpolates its names; scan the RENDERED
        # fragments, which is what actually installs.
        for fragment in reservations_module.reservation_function_fragments():
            found.update(_CREATE.findall(fragment))
        undeclared = sorted(found - set(DISPOSITIONS))
        assert not undeclared, (
            f'chain functions without a two-worlds disposition: '
            f'{undeclared}'
        )
        vanished = sorted(set(DISPOSITIONS) - found)
        assert not vanished, (
            f'dispositions for functions no longer installed: {vanished}'
        )

    def test_the_claim_identity_is_declared_agnostic(self) -> None:
        source = inspect.getsource(migrations_module)
        assert 'id text,' in source
        assert 'RETURNING t.id::text' in source
