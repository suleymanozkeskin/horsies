"""Claim candidate bounds, dispatch order, and the non-runnable lookup.

- A claim locks only the candidates its queue cap and batch limit let it
  return; the rest stay claimable by other workers while it is open.
- Rows come back in the global dispatch rank (queue priority, task
  priority, enqueue time, id), across the pending and expired arms.
- Candidate selection (the plan and the take) reuses a generic plan; the
  parent does not.
- The bounded non-runnable lookup returns what the plain join returns.
"""

from __future__ import annotations

import pytest
from sqlalchemy import text

from horsies.core.brokers.postgres import PostgresBroker
from tests.broker_resources_support import current_resources

pytestmark = [pytest.mark.integration]


@pytest.mark.asyncio(loop_scope='function')
async def test_only_candidate_selection_forces_a_generic_plan(
    broker: PostgresBroker,
) -> None:
    async with current_resources(broker).engine.connect() as connection:
        settings = dict(
            (
                await connection.execute(
                    text("""
            SELECT proname, coalesce(array_to_string(proconfig, ','), '')
            FROM pg_proc
            WHERE proname IN (
                'horsies_claim', 'horsies_claim_plan', 'horsies_claim_candidates',
                'horsies_claim_in_flight'
            )
        """)
                )
            )
            .tuples()
            .all()
        )
    # Both run without JIT: the generic plan's cost estimate would compile
    # the statement on every pass.
    # The in-flight count turns off bitmap and sequential scans, so its plan
    # is the scan of the status index, which marks the dead entries it passes.
    assert settings == {
        'horsies_claim': 'jit=off',
        'horsies_claim_plan': 'plan_cache_mode=force_generic_plan,jit=off',
        'horsies_claim_candidates': 'plan_cache_mode=force_generic_plan,jit=off',
        'horsies_claim_in_flight': 'jit=off,enable_bitmapscan=off,enable_seqscan=off',
    }
