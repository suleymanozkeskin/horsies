"""The retry limit that enqueue stores, and the paths that refuse it.

`stored_max_retries` is the one reader of `retry_policy.max_retries` in
decoded task options. Enqueue, workflow node promotion, and the workflow
fast-path root all use it, so a limit outside 0..MAX_RETRY_ATTEMPTS never
reaches `horsies_tasks.max_retries`.
"""

from __future__ import annotations

from typing import Any
from unittest.mock import MagicMock

import pytest

from horsies.core.codec.json_io import Json, SerializationError
from horsies.core.codec.task_options import (
    InvalidRetryLimit,
    stored_max_retries,
)
from horsies.core.models.tasks import MAX_RETRY_ATTEMPTS, RetryPolicy
from horsies.core.types.result import Err, Ok
from horsies.core.workflows.engine import _build_enqueued_task_params
from horsies.core.workflows.lifecycle import root_retry_and_deadline

pytestmark = [pytest.mark.unit]

REFUSED: list[tuple[Json, str]] = [
    ({'retry_policy': {'max_retries': -1}}, 'max_retries is not an integer in'),
    (
        {'retry_policy': {'max_retries': MAX_RETRY_ATTEMPTS + 1}},
        'max_retries is not an integer in',
    ),
    ({'retry_policy': {'max_retries': 2.0}}, 'max_retries is not an integer in'),
    ({'retry_policy': {'max_retries': '3'}}, 'max_retries is not an integer in'),
    ({'retry_policy': {'max_retries': True}}, 'max_retries is not an integer:'),
]


class TestStoredMaxRetries:
    def test_options_without_a_policy_store_zero(self) -> None:
        assert stored_max_retries({'timeout_ms': None}) == Ok(0)

    @pytest.mark.parametrize('policy', [None, [3], 'fixed'])
    def test_a_policy_that_is_not_an_object_stores_zero(self, policy: Json) -> None:
        assert stored_max_retries({'retry_policy': policy}) == Ok(0)

    def test_options_that_are_not_an_object_store_zero(self) -> None:
        assert stored_max_retries([]) == Ok(0)

    def test_a_policy_without_a_limit_stores_the_policy_default(self) -> None:
        assert stored_max_retries({'retry_policy': {}}) == Ok(3)

    @pytest.mark.parametrize('limit', [0, MAX_RETRY_ATTEMPTS])
    def test_limits_at_the_bounds_are_stored(self, limit: int) -> None:
        assert stored_max_retries({'retry_policy': {'max_retries': limit}}) == Ok(
            limit
        )

    @pytest.mark.parametrize(('options', 'detail'), REFUSED)
    def test_a_limit_that_cannot_be_stored_is_refused(
        self, options: Json, detail: str
    ) -> None:
        outcome = stored_max_retries(options)
        assert isinstance(outcome, Err)
        assert isinstance(outcome.err_value, InvalidRetryLimit)
        assert detail in outcome.err_value.detail

    def test_the_retry_policy_bound_is_the_stored_bound(self) -> None:
        policy = RetryPolicy.exponential(
            base_seconds=1,
            max_retries=MAX_RETRY_ATTEMPTS,
            auto_retry_for=['TRANSIENT'],
        )
        assert policy.max_retries == MAX_RETRY_ATTEMPTS
        with pytest.raises(ValueError, match='less than or equal to 20'):
            RetryPolicy.exponential(
                base_seconds=1,
                max_retries=MAX_RETRY_ATTEMPTS + 1,
                auto_retry_for=['TRANSIENT'],
            )


class TestWorkflowPaths:
    def test_the_fast_path_root_refuses_a_negative_limit(self) -> None:
        with pytest.raises(SerializationError, match='not an integer in'):
            root_retry_and_deadline(
                '{"retry_policy": {"max_retries": -1}}', 'root'
            )

    def test_the_fast_path_root_stores_a_valid_limit(self) -> None:
        max_retries, _, _ = root_retry_and_deadline(
            '{"retry_policy": {"max_retries": 4}}', 'root'
        )
        assert max_retries == 4

    @pytest.mark.asyncio
    async def test_node_promotion_refuses_a_negative_limit(self) -> None:
        arguments: dict[str, Any] = {
            'task_name': 'node.task',
            'task_args': None,
            'task_kwargs': None,
            'queue_name': 'default',
            'priority': 50,
            'args_from': None,
            'workflow_ctx_from': None,
            'task_options_str': '{"retry_policy": {"max_retries": -1}}',
            'all_dep_results': {},
            'all_dep_task_names': None,
            'broker': None,
            'all_dep_definition_keys': None,
        }
        outcome = await _build_enqueued_task_params(
            MagicMock(), 'workflow-1', 3, **arguments
        )
        assert isinstance(outcome, Err)
        assert outcome.err_value.message.startswith(
            'Invalid retry_policy for task 3:'
        )
        assert 'not an integer in' in outcome.err_value.message
