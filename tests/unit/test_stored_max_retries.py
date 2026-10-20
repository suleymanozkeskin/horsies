"""The retry limit that enqueue stores, and the paths that refuse it.

`stored_max_retries` is the one reader of `retry_policy` in decoded task
options. Enqueue, workflow node promotion, and the workflow fast-path root
all use it, so only a policy that validates as `RetryPolicy` reaches
`horsies_tasks.max_retries`.
"""

from __future__ import annotations

import json
from typing import Any
from unittest.mock import MagicMock

import pytest

from horsies.core.codec.json_io import Json, SerializationError
from horsies.core.codec.task_options import (
    InvalidRetryPolicy,
    stored_max_retries,
)
from horsies.core.models.tasks import MAX_RETRY_ATTEMPTS, RetryPolicy
from horsies.core.types.result import Err, Ok
from horsies.core.workflows.engine import _build_enqueued_task_params
from horsies.core.workflows.lifecycle import root_retry_and_deadline

pytestmark = [pytest.mark.unit]


def _policy(**fields: Any) -> dict[str, Any]:
    """A valid exponential policy with `fields` replaced."""
    policy: dict[str, Any] = {
        'max_retries': 3,
        'intervals': [5],
        'backoff_strategy': 'exponential',
        'jitter': True,
        'auto_retry_for': ['TRANSIENT'],
    }
    policy.update(fields)
    return policy


REFUSED: list[tuple[Json, str]] = [
    ({'retry_policy': [3]}, 'retry_policy is not an object: list'),
    ({'retry_policy': 'fixed'}, 'retry_policy is not an object: str'),
    ({'retry_policy': {}}, 'auto_retry_for: Field required'),
    ({'retry_policy': _policy(max_retries=0)}, 'max_retries: Input should be greater'),
    ({'retry_policy': _policy(max_retries=-1)}, 'max_retries: Input should be greater'),
    (
        {'retry_policy': _policy(max_retries=MAX_RETRY_ATTEMPTS + 1)},
        'max_retries: Input should be less',
    ),
    (
        {'retry_policy': _policy(max_retries=2.0)},
        'max_retries: Input should be a valid integer',
    ),
    (
        {'retry_policy': _policy(max_retries='3')},
        'max_retries: Input should be a valid integer',
    ),
    (
        {'retry_policy': _policy(max_retries=True)},
        'max_retries: Input should be a valid integer',
    ),
    (
        {'retry_policy': _policy(intervals=['5'])},
        'intervals.0: Input should be a valid integer',
    ),
    (
        {'retry_policy': _policy(intervals=[0])},
        'intervals.0: Input should be greater than 0',
    ),
    (
        {'retry_policy': _policy(intervals=[5, 10])},
        'requires exactly one base interval',
    ),
    (
        {'retry_policy': _policy(backoff_strategy='fixed', intervals=[5])},
        'requires intervals length (1) to match max_retries (3)',
    ),
    (
        {'retry_policy': _policy(backoff_strategy='linear')},
        'backoff_strategy: Input should be',
    ),
    (
        {'retry_policy': _policy(jitter='yes')},
        'jitter: Input should be a valid boolean',
    ),
    (
        {'retry_policy': _policy(max_delay_seconds=0)},
        'max_delay_seconds: Input should be greater',
    ),
    (
        {'retry_policy': _policy(auto_retry_for=[])},
        'auto_retry_for: List should have at least 1',
    ),
    ({'retry_policy': _policy(auto_retry_for=['bad code'])}, 'auto_retry_for'),
    ({'retry_policy': _policy(surprise=1)}, 'surprise: Extra inputs are not permitted'),
]


class TestStoredMaxRetries:
    def test_options_without_a_policy_store_zero(self) -> None:
        assert stored_max_retries({'timeout_ms': None}) == Ok(0)

    def test_a_null_policy_stores_zero(self) -> None:
        assert stored_max_retries({'retry_policy': None}) == Ok(0)

    def test_options_that_are_not_an_object_store_zero(self) -> None:
        assert stored_max_retries([]) == Ok(0)

    @pytest.mark.parametrize(
        'policy',
        [
            RetryPolicy.fixed([1, 2], auto_retry_for=['TRANSIENT']),
            RetryPolicy.exponential(
                base_seconds=1,
                max_retries=1,
                auto_retry_for=['TRANSIENT'],
            ),
            RetryPolicy.exponential(
                base_seconds=1,
                max_retries=MAX_RETRY_ATTEMPTS,
                auto_retry_for=['TASK_EXCEPTION'],
                jitter=False,
                max_delay_seconds=60,
            ),
        ],
        ids=['fixed', 'exponential_lower_bound', 'exponential_upper_bound'],
    )
    def test_a_serialized_policy_stores_its_limit(self, policy: RetryPolicy) -> None:
        stored = json.loads(policy.model_dump_json(exclude_none=True))
        assert stored_max_retries({'retry_policy': stored}) == Ok(policy.max_retries)

    @pytest.mark.parametrize(('options', 'detail'), REFUSED)
    def test_a_malformed_policy_is_refused(self, options: Json, detail: str) -> None:
        outcome = stored_max_retries(options)
        assert isinstance(outcome, Err)
        assert isinstance(outcome.err_value, InvalidRetryPolicy)
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
        with pytest.raises(SerializationError, match='max_retries: Input should be'):
            root_retry_and_deadline(
                json.dumps({'retry_policy': _policy(max_retries=-1)}), 'root'
            )

    def test_the_fast_path_root_stores_a_valid_limit(self) -> None:
        max_retries, _, _ = root_retry_and_deadline(
            json.dumps({'retry_policy': _policy(max_retries=4)}), 'root'
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
            'task_options_str': json.dumps(
                {'retry_policy': _policy(max_retries=-1)},
            ),
            'all_dep_results': {},
            'all_dep_task_names': None,
            'broker': None,
            'all_dep_definition_keys': None,
        }
        outcome = await _build_enqueued_task_params(
            MagicMock(), 'workflow-1', 3, **arguments
        )
        assert isinstance(outcome, Err)
        assert outcome.err_value.message.startswith('Invalid retry_policy for task 3:')
        assert 'max_retries: Input should be' in outcome.err_value.message
