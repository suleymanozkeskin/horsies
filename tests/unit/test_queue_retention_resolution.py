"""What retention class a send stamps, given a queue mapping.

Precedence is explicit-argument, then the queue's mapping, then the
30-day default. The middle rung is the new one and the top rung is why
absence had to become its own value: `standard_30d` passed by hand and
the parameter omitted are the same string, and they must resolve
differently once a queue is mapped.

The stamped key is read off the broker call because that is what the
enqueue actually writes; asserting on a resolver's return value would
pass whether or not the send used it.
"""

from __future__ import annotations

import json
from datetime import timedelta
from typing import Any
from unittest.mock import MagicMock

import pytest

from horsies.core.models.payload import PayloadPolicy
from horsies.core.models.tasks import TaskError, TaskResult
from horsies.core.models.retention import (
    RetentionClassConfig,
    RetentionConfig,
)
from horsies.core.task_decorator import create_task_wrapper
from horsies.core.types.result import Ok

pytestmark = [pytest.mark.unit]

MAPPED_QUEUE = 'emails'
DERIVED_KEY = 'q_emails_7d'


def good_fn(*, x: int) -> TaskResult[int, TaskError]:
    return TaskResult(ok=x)


def _make_app(retention: RetentionConfig) -> MagicMock:
    """A mock app carrying a REAL retention config.

    Real, not a MagicMock attribute, for the reason the neighbouring
    payload policy is real: an auto-created attribute answers every
    lookup with a truthy mock, and the resolution would read a mapping
    that does not exist.
    """
    app = MagicMock()
    app.config.queue_mode.name = 'DEFAULT'
    app.config.custom_queues = None
    app.config.exception_mapper = {}
    app.config.default_unhandled_error_code = 'UNHANDLED_EXCEPTION'
    app.config.resend_on_transient_err = False
    app.config.payload = PayloadPolicy()
    app.config.retention = retention
    app.are_sends_suppressed.return_value = False
    app.validate_queue_name.return_value = MAPPED_QUEUE
    return app


def _stamp(
    app: MagicMock, path: str, **send_options: Any
) -> dict[str, Any]:
    """What the broker was actually asked to store, for one send path.

    Read off the broker call rather than from a resolver, so a path that
    resolves correctly and then fails to carry the result still fails.
    """
    broker = MagicMock()
    broker.enqueue.return_value = Ok('task-abc')
    app.get_broker.return_value = broker
    wrapper = create_task_wrapper(good_fn, app, 'test.good_fn')
    target = wrapper.with_options(**send_options) if send_options else wrapper

    match path:
        case 'send':
            result = target.send(x=1)
        case 'schedule':
            result = target.schedule(60, x=1)
        case unknown:
            raise AssertionError(f'unknown send path {unknown!r}')
    assert result.is_ok(), result

    return broker.enqueue.call_args.kwargs


def _stamped_key(app: MagicMock, **send_options: Any) -> str | None:
    """The retention class an immediate send stores."""
    return _stamp(app, 'send', **send_options)['retention_class_key']


_MAPPED = RetentionConfig(queue_retention={MAPPED_QUEUE: timedelta(days=7)})


def test_an_unmapped_queue_stamps_the_default() -> None:
    key = _stamped_key(_make_app(RetentionConfig()))

    assert key == 'standard_30d'


def test_a_mapped_queue_stamps_its_derived_class() -> None:
    key = _stamped_key(_make_app(_MAPPED))

    assert key == DERIVED_KEY


def test_a_queue_mapped_to_none_stamps_forever() -> None:
    app = _make_app(RetentionConfig(queue_retention={MAPPED_QUEUE: None}))

    assert _stamped_key(app) is None, 'None is the explicit forever choice'


def test_an_explicit_default_beats_the_mapping() -> None:
    """The distinction the sentinel exists to carry.

    Naming the default class is a choice, and a choice outranks the
    queue's mapping. Before absence became its own value this send was
    indistinguishable from one that said nothing.
    """
    key = _stamped_key(_make_app(_MAPPED), retention_class_key='standard_30d')

    assert key == 'standard_30d', (
        'an explicitly named class must beat the queue mapping; the '
        'mapping is a default for sends that express no preference'
    )


def test_an_explicit_none_beats_the_mapping() -> None:
    key = _stamped_key(_make_app(_MAPPED), retention_class_key=None)

    assert key is None


def test_an_explicit_declared_class_beats_the_mapping() -> None:
    config = RetentionConfig(
        retention_classes=(
            RetentionClassConfig(key='audit_1y', duration=timedelta(days=365)),
        ),
        queue_retention={MAPPED_QUEUE: timedelta(days=7)},
    )

    key = _stamped_key(_make_app(config), retention_class_key='audit_1y')

    assert key == 'audit_1y'


def test_a_derived_class_may_be_named_explicitly() -> None:
    """A queue's own class is a valid target by name.

    Send-time validation refuses keys this process does not know, and a
    class the mapping creates is one it knows.
    """
    key = _stamped_key(_make_app(_MAPPED), retention_class_key=DERIVED_KEY)

    assert key == DERIVED_KEY


def test_an_unknown_class_is_still_refused() -> None:
    """Widening the known set must not have opened it."""
    broker = MagicMock()
    broker.enqueue.return_value = Ok('task-abc')
    app = _make_app(_MAPPED)
    app.get_broker.return_value = broker
    wrapper = create_task_wrapper(good_fn, app, 'test.good_fn')

    result = wrapper.with_options(retention_class_key='q_other_9d').send(x=1)

    assert result.is_err(), 'an unmapped derived key is not a known class'
    assert not broker.enqueue.called, 'the refusal must precede the enqueue'


def test_the_mapping_is_read_per_send_not_at_decoration() -> None:
    """An edit governs the next send, not only the next process.

    The resolution reads the app's config at send time, so a mapping
    changed after the task was defined still applies.
    """
    app = _make_app(RetentionConfig())
    assert _stamped_key(app) == 'standard_30d'

    app.config.retention = _MAPPED

    assert _stamped_key(app) == DERIVED_KEY


def test_json_task_options_do_not_carry_the_class() -> None:
    """The class rides its own column, not the options blob.

    Recorded because the stamped key must reach the enqueue as a field
    the archive can route on.
    """
    broker = MagicMock()
    broker.enqueue.return_value = Ok('task-abc')
    app = _make_app(_MAPPED)
    app.get_broker.return_value = broker
    wrapper = create_task_wrapper(good_fn, app, 'test.good_fn')

    wrapper.send(x=1)

    options = broker.enqueue.call_args.kwargs.get('task_options')
    if options is not None:
        assert 'retention' not in json.loads(options)


# --- The delayed path resolves exactly as the immediate one does -------------
#
# Every row below was WRONG before this fix: `.schedule()` never touched
# retention, so the payload kept its dataclass default and the broker was
# handed `standard_30d` no matter what the queue mapped or the caller asked
# for. Parametrized against `.send()` so the two paths cannot drift: a rule
# that holds for one and not the other fails here.


@pytest.mark.parametrize('path', ['send', 'schedule'])
class TestEveryPathResolvesAlike:
    def test_a_mapped_queue_resolves(self, path: str) -> None:
        assert _stamp(_make_app(_MAPPED), path)['retention_class_key'] == (
            DERIVED_KEY
        )

    def test_an_unmapped_queue_takes_the_default(self, path: str) -> None:
        app = _make_app(RetentionConfig())

        assert _stamp(app, path)['retention_class_key'] == 'standard_30d'

    def test_an_explicit_forever_is_honored(self, path: str) -> None:
        """The sub-case that silently deleted records at 30 days.

        A caller asking for forever on the delayed path had the request
        discarded and the record dropped on the default schedule.
        """
        stamped = _stamp(
            _make_app(_MAPPED), path, retention_class_key=None
        )['retention_class_key']

        assert stamped is None

    def test_an_explicit_default_beats_the_mapping(self, path: str) -> None:
        stamped = _stamp(
            _make_app(_MAPPED), path, retention_class_key='standard_30d'
        )['retention_class_key']

        assert stamped == 'standard_30d'

    def test_an_explicit_derived_key_is_honored(self, path: str) -> None:
        stamped = _stamp(
            _make_app(_MAPPED), path, retention_class_key=DERIVED_KEY
        )['retention_class_key']

        assert stamped == DERIVED_KEY

    def test_a_queue_mapped_to_none_resolves_to_forever(
        self, path: str
    ) -> None:
        app = _make_app(RetentionConfig(queue_retention={MAPPED_QUEUE: None}))

        assert _stamp(app, path)['retention_class_key'] is None

    def test_an_unknown_class_is_refused_before_the_enqueue(
        self, path: str
    ) -> None:
        broker = MagicMock()
        broker.enqueue.return_value = Ok('task-abc')
        app = _make_app(_MAPPED)
        app.get_broker.return_value = broker
        wrapper = create_task_wrapper(good_fn, app, 'test.good_fn')
        options = wrapper.with_options(retention_class_key='q_other_9d')

        result = (
            options.send(x=1) if path == 'send' else options.schedule(60, x=1)
        )

        assert result.is_err(), result
        assert not broker.enqueue.called, (
            'the refusal must precede the enqueue on every path'
        )

    def test_the_idempotency_key_is_carried(self, path: str) -> None:
        """Rides the same bypass; fixed with it rather than left behind."""
        stamped = _stamp(_make_app(_MAPPED), path, idempotency_key='k1')

        assert stamped['idempotency_key'] == 'k1'
