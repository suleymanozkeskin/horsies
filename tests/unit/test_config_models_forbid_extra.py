"""Every config model refuses unknown keys.

The walk starts at ``AppConfig`` and follows each field annotation into every
pydantic model it reaches (``broker``, ``recovery``, ``schedule`` and its
patterns, ...). A new config model without ``extra='forbid'`` fails here.
"""

from __future__ import annotations

from typing import Any, get_args

import pytest
from pydantic import BaseModel, SecretStr, ValidationError

from horsies.core.models.app import AppConfig
from horsies.core.models.broker import PostgresConfig
from horsies.core.models.recovery import RecoveryConfig
from horsies.core.models.schedule import CronSchedule, ScheduleConfig

MAX_ANNOTATION_DEPTH = 16


def _models_in_annotation(annotation: Any, depth: int) -> list[type[BaseModel]]:
    assert (
        depth <= MAX_ANNOTATION_DEPTH
    ), f'annotation nesting deeper than {MAX_ANNOTATION_DEPTH}'
    if isinstance(annotation, type) and issubclass(annotation, BaseModel):
        return [annotation]
    found: list[type[BaseModel]] = []
    for arg in get_args(annotation):
        found.extend(_models_in_annotation(arg, depth + 1))
    return found


def _reachable_models(root: type[BaseModel]) -> list[type[BaseModel]]:
    seen: list[type[BaseModel]] = []
    pending: list[type[BaseModel]] = [root]
    while pending:
        model = pending.pop()
        if model in seen:
            continue
        seen.append(model)
        for field in model.model_fields.values():
            pending.extend(_models_in_annotation(field.annotation, depth=0))
    return seen


@pytest.mark.unit
class TestConfigModelsForbidExtra:
    def test_walk_reaches_nested_config_models(self) -> None:
        """The walk reaches the leaf models, so the forbid check covers them."""
        names = {model.__name__ for model in _reachable_models(AppConfig)}

        assert {
            'PostgresConfig',
            'RecoveryConfig',
            'ScheduleConfig',
            'CronSchedule',
            'ByWeekday',
        } <= names
        assert any(name.startswith('CronEnumStep[') for name in names)

    def test_every_reachable_config_model_forbids_extra(self) -> None:
        """Each model reachable from AppConfig has extra='forbid'."""
        missing = [
            model.__name__
            for model in _reachable_models(AppConfig)
            if model.model_config.get('extra') != 'forbid'
        ]

        assert missing == []

    def test_unknown_key_in_nested_config_is_refused(self) -> None:
        """A misspelled key inside a nested config raises extra_forbidden, not a silent drop."""
        with pytest.raises(ValidationError) as exc_info:
            AppConfig(
                broker=PostgresConfig(
                    database_url=SecretStr(
                        'postgresql+psycopg://user:pass@localhost/db'
                    ),
                ),
                recovery={
                    'auto_requeue_stale_claimed': True,
                    'stale_claim_threshold': 5,
                },  # type: ignore[arg-type]
            )

        errors = exc_info.value.errors(include_input=False)
        assert [(error['type'], error['loc']) for error in errors] == [
            ('extra_forbidden', ('recovery', 'stale_claim_threshold')),
        ]

    @pytest.mark.parametrize(
        ('model', 'kwargs'),
        [
            (RecoveryConfig, {'unknown_key': 1}),
            (ScheduleConfig, {'unknown_key': 1}),
            (CronSchedule, {'unknown_key': 1}),
        ],
        ids=['recovery', 'schedule_config', 'cron_schedule'],
    )
    def test_unknown_top_level_key_is_refused(
        self,
        model: type[BaseModel],
        kwargs: dict[str, Any],
    ) -> None:
        """A key that is not a field raises extra_forbidden."""
        with pytest.raises(ValidationError) as exc_info:
            model(**kwargs)

        assert 'extra_forbidden' in {
            error['type'] for error in exc_info.value.errors(include_input=False)
        }
