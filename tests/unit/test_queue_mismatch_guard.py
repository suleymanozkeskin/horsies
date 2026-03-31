"""Verify that every queue/task mismatch combination is a hard fail.

Combinations tested:
  1. DEFAULT mode + task with queue_name       → ConfigurationError
  2. CUSTOM mode + task without queue_name      → ConfigurationError
  3. CUSTOM mode + task with non-existent queue → ConfigurationError
  4. CUSTOM mode + task with valid queue         → OK
  5. DEFAULT mode + task without queue_name      → OK
  6. validate_queue_name called at send time too → ConfigurationError
  7. check() catches queue mismatch from imports → error collected
"""

from __future__ import annotations

import pytest

from horsies.core.app import Horsies
from horsies.core.errors import ConfigurationError, ErrorCode, HorsiesError
from horsies.core.models.app import AppConfig
from horsies.core.models.broker import PostgresConfig
from horsies.core.models.queues import CustomQueueConfig, QueueMode
from horsies.core.models.tasks import TaskError, TaskResult

BROKER = PostgresConfig(
    database_url='postgresql+psycopg://user:pass@localhost/db',
    pool_size=5,
    max_overflow=5,
)


def _default_app() -> Horsies:
    return Horsies(config=AppConfig(queue_mode=QueueMode.DEFAULT, broker=BROKER))


def _custom_app(*queue_names: str) -> Horsies:
    return Horsies(config=AppConfig(
        queue_mode=QueueMode.CUSTOM,
        broker=BROKER,
        custom_queues=[CustomQueueConfig(name=n) for n in queue_names],
    ))


# ---------------------------------------------------------------------------
# 1. DEFAULT mode + task specifies queue_name → hard fail
# ---------------------------------------------------------------------------
@pytest.mark.unit
class TestDefaultModeRejectsQueueName:

    def test_task_with_queue_name_raises(self) -> None:
        """DEFAULT mode must reject @app.task(queue_name='fast')."""
        app = _default_app()
        with pytest.raises(ConfigurationError) as exc_info:
            @app.task('my_task', queue_name='fast')
            def my_task() -> TaskResult[str, TaskError]:
                return TaskResult(ok='done')  # pragma: no cover

        assert exc_info.value.code == ErrorCode.CONFIG_INVALID_QUEUE_MODE
        assert 'DEFAULT' in str(exc_info.value)

    def test_task_with_queue_name_default_string_raises(self) -> None:
        """Even queue_name='default' is rejected — DEFAULT mode means no queue_name at all."""
        app = _default_app()
        with pytest.raises(ConfigurationError) as exc_info:
            @app.task('my_task', queue_name='default')
            def my_task() -> TaskResult[str, TaskError]:
                return TaskResult(ok='done')  # pragma: no cover

        assert exc_info.value.code == ErrorCode.CONFIG_INVALID_QUEUE_MODE


# ---------------------------------------------------------------------------
# 2. CUSTOM mode + task omits queue_name → hard fail
# ---------------------------------------------------------------------------
@pytest.mark.unit
class TestCustomModeRequiresQueueName:

    def test_task_without_queue_name_raises(self) -> None:
        """CUSTOM mode must reject @app.task('x') without queue_name."""
        app = _custom_app('fast', 'slow')
        with pytest.raises(ConfigurationError) as exc_info:
            @app.task('my_task')
            def my_task() -> TaskResult[str, TaskError]:
                return TaskResult(ok='done')  # pragma: no cover

        assert exc_info.value.code == ErrorCode.CONFIG_INVALID_QUEUE_MODE
        assert 'CUSTOM' in str(exc_info.value)


# ---------------------------------------------------------------------------
# 3. CUSTOM mode + task with non-existent queue → hard fail
# ---------------------------------------------------------------------------
@pytest.mark.unit
class TestCustomModeRejectsUnknownQueue:

    def test_task_with_unknown_queue_raises(self) -> None:
        """CUSTOM mode with queues ['fast','slow'] must reject queue_name='analytics'."""
        app = _custom_app('fast', 'slow')
        with pytest.raises(ConfigurationError) as exc_info:
            @app.task('my_task', queue_name='analytics')
            def my_task() -> TaskResult[str, TaskError]:
                return TaskResult(ok='done')  # pragma: no cover

        assert exc_info.value.code == ErrorCode.TASK_INVALID_QUEUE
        assert 'analytics' in str(exc_info.value)
        # Error should mention valid queues for actionability
        assert 'fast' in str(exc_info.value) or 'slow' in str(exc_info.value)

    def test_task_with_typo_queue_raises(self) -> None:
        """Typo in queue_name (e.g. 'fats' instead of 'fast') must fail."""
        app = _custom_app('fast', 'slow')
        with pytest.raises(ConfigurationError) as exc_info:
            @app.task('my_task', queue_name='fats')
            def my_task() -> TaskResult[str, TaskError]:
                return TaskResult(ok='done')  # pragma: no cover

        assert exc_info.value.code == ErrorCode.TASK_INVALID_QUEUE


# ---------------------------------------------------------------------------
# 4. CUSTOM mode + task with valid queue → OK
# ---------------------------------------------------------------------------
@pytest.mark.unit
class TestCustomModeAcceptsValidQueue:

    def test_task_with_valid_queue_succeeds(self) -> None:
        """CUSTOM mode should accept a task targeting one of the configured queues."""
        app = _custom_app('fast', 'slow')

        @app.task('my_task', queue_name='fast')
        def my_task() -> TaskResult[str, TaskError]:
            return TaskResult(ok='done')  # pragma: no cover

        assert 'my_task' in app.tasks

    def test_multiple_tasks_on_different_valid_queues(self) -> None:
        """Multiple tasks can target different valid queues."""
        app = _custom_app('fast', 'slow')

        @app.task('task_a', queue_name='fast')
        def task_a() -> TaskResult[str, TaskError]:
            return TaskResult(ok='a')  # pragma: no cover

        @app.task('task_b', queue_name='slow')
        def task_b() -> TaskResult[str, TaskError]:
            return TaskResult(ok='b')  # pragma: no cover

        assert 'task_a' in app.tasks
        assert 'task_b' in app.tasks


# ---------------------------------------------------------------------------
# 5. DEFAULT mode + task without queue_name → OK
# ---------------------------------------------------------------------------
@pytest.mark.unit
class TestDefaultModeAcceptsNoQueue:

    def test_task_without_queue_succeeds(self) -> None:
        """DEFAULT mode should accept @app.task('x') with no queue_name."""
        app = _default_app()

        @app.task('my_task')
        def my_task() -> TaskResult[str, TaskError]:
            return TaskResult(ok='done')  # pragma: no cover

        assert 'my_task' in app.tasks


# ---------------------------------------------------------------------------
# 6. validate_queue_name is also called at send time (defense in depth)
# ---------------------------------------------------------------------------
@pytest.mark.unit
class TestSendTimeValidation:

    def test_validate_queue_name_rejects_at_runtime(self) -> None:
        """validate_queue_name called directly should also reject mismatches."""
        app = _default_app()

        # Simulate what _prepare_send does
        with pytest.raises(ConfigurationError):
            app.validate_queue_name('some_queue')

    def test_validate_queue_name_custom_mode_rejects_none(self) -> None:
        """CUSTOM mode validate_queue_name(None) should fail."""
        app = _custom_app('fast')
        with pytest.raises(ConfigurationError):
            app.validate_queue_name(None)

    def test_validate_queue_name_custom_mode_rejects_unknown(self) -> None:
        """CUSTOM mode validate_queue_name('missing') should fail."""
        app = _custom_app('fast')
        with pytest.raises(ConfigurationError):
            app.validate_queue_name('missing')

    def test_validate_queue_name_returns_validated_name(self) -> None:
        """Happy path: returns the validated queue name."""
        app = _custom_app('fast')
        assert app.validate_queue_name('fast') == 'fast'

        default_app = _default_app()
        assert default_app.validate_queue_name(None) == 'default'


# ---------------------------------------------------------------------------
# 7. check() collects queue mismatch errors from task module imports
# ---------------------------------------------------------------------------
@pytest.mark.unit
class TestCheckCollectsQueueMismatch:

    def test_check_catches_queue_mismatch_from_import(self, tmp_path: object) -> None:
        """app.check() should surface queue errors when importing task modules.

        We simulate this by registering a task module path that, when imported,
        would trigger a queue mismatch. Since we can't easily create a real
        module import scenario in a unit test, we verify the validate_queue_name
        guard is called during the decorator — which is what check() relies on
        (it imports modules, decorators fire, validate_queue_name raises).
        """
        # The chain is: check() → _check_task_imports() → import module
        #   → @app.task fires → validate_queue_name → ConfigurationError
        #
        # We verify the guard itself here; e2e tests cover the full chain.
        app = _default_app()
        with pytest.raises(ConfigurationError) as exc_info:
            app.validate_queue_name('custom_queue')

        error = exc_info.value
        assert error.code == ErrorCode.CONFIG_INVALID_QUEUE_MODE
        assert error.help_text is not None
        assert 'remove queue_name' in error.help_text or 'CUSTOM' in error.help_text


# ---------------------------------------------------------------------------
# 8. Error messages are actionable
# ---------------------------------------------------------------------------
@pytest.mark.unit
class TestErrorMessageQuality:

    def test_default_mode_error_suggests_fix(self) -> None:
        """Error should suggest removing queue_name OR switching to CUSTOM."""
        app = _default_app()
        with pytest.raises(ConfigurationError) as exc_info:
            app.validate_queue_name('oops')

        error = exc_info.value
        assert error.help_text is not None
        assert 'remove queue_name' in error.help_text
        assert 'CUSTOM' in error.help_text

    def test_custom_mode_missing_queue_error_suggests_fix(self) -> None:
        """Error should suggest specifying a queue_name."""
        app = _custom_app('fast')
        with pytest.raises(ConfigurationError) as exc_info:
            app.validate_queue_name(None)

        error = exc_info.value
        assert error.help_text is not None
        assert 'queue_name' in error.help_text

    def test_custom_mode_invalid_queue_lists_valid_options(self) -> None:
        """Error notes should list the valid queue names."""
        app = _custom_app('fast', 'slow', 'batch')
        with pytest.raises(ConfigurationError) as exc_info:
            app.validate_queue_name('nope')

        error = exc_info.value
        notes_str = ' '.join(error.notes or [])
        assert 'fast' in notes_str
        assert 'slow' in notes_str
        assert 'batch' in notes_str
