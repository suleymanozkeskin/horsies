"""Unit tests for TaskRegistry, NotRegistered, and DuplicateTaskNameError."""

from __future__ import annotations

import pytest

from horsies.core.errors import ErrorCode
from horsies.core.registry.tasks import (
    DuplicateTaskNameError,
    NotRegistered,
    TaskRegistry,
)


@pytest.mark.unit
class TestNotRegistered:
    """Tests for the NotRegistered exception."""

    def test_message_contains_task_name(self) -> None:
        """Error message should include the missing task name."""
        err = NotRegistered('my_task')
        assert 'my_task' in str(err)

    def test_error_code(self) -> None:
        """Error code should be TASK_NOT_REGISTERED."""
        err = NotRegistered('my_task')
        assert err.code == ErrorCode.TASK_NOT_REGISTERED

    def test_task_name_attribute(self) -> None:
        """task_name attribute should be set on the exception."""
        err = NotRegistered('some_task')
        assert err.task_name == 'some_task'

    def test_help_text_present(self) -> None:
        """Help text should guide users to register the task."""
        err = NotRegistered('missing')
        assert 'ensure the task is defined' in err.help_text


@pytest.mark.unit
class TestDuplicateTaskNameError:
    """Tests for the DuplicateTaskNameError exception."""

    def test_message_contains_task_name(self) -> None:
        """Error message should include the duplicate task name."""
        err = DuplicateTaskNameError('dup_task')
        assert 'dup_task' in str(err)

    def test_error_code(self) -> None:
        """Error code should be TASK_DUPLICATE_NAME."""
        err = DuplicateTaskNameError('dup_task')
        assert err.code == ErrorCode.TASK_DUPLICATE_NAME

    def test_task_name_attribute(self) -> None:
        """task_name attribute should be set on the exception."""
        err = DuplicateTaskNameError('dup_task')
        assert err.task_name == 'dup_task'

    def test_context_included_in_notes(self) -> None:
        """Context string should appear in error notes when provided."""
        err = DuplicateTaskNameError('dup_task', context='from module X')
        assert any('from module X' in n for n in err.notes)

    def test_empty_context_omitted(self) -> None:
        """Empty context should not produce notes."""
        err = DuplicateTaskNameError('dup_task')
        assert err.notes == []


@pytest.mark.unit
class TestTaskRegistryGetItem:
    """Tests for TaskRegistry.__getitem__ (bracket access)."""

    def test_get_existing_key(self) -> None:
        """Existing key should return the registered value."""
        reg: TaskRegistry[str] = TaskRegistry()
        reg._data['task_a'] = 'handler_a'
        assert reg['task_a'] == 'handler_a'

    def test_get_missing_key_raises_not_registered(self) -> None:
        """Missing key should raise NotRegistered with correct error code."""
        reg: TaskRegistry[str] = TaskRegistry()
        with pytest.raises(NotRegistered) as exc_info:
            _ = reg['nonexistent']
        assert exc_info.value.code == ErrorCode.TASK_NOT_REGISTERED
        assert exc_info.value.task_name == 'nonexistent'


@pytest.mark.unit
class TestTaskRegistrySetItem:
    """Tests for TaskRegistry.__setitem__ (bracket assignment)."""

    def test_set_new_key(self) -> None:
        """New key should be inserted."""
        reg: TaskRegistry[str] = TaskRegistry()
        reg['task_a'] = 'handler_a'
        assert reg['task_a'] == 'handler_a'

    def test_set_duplicate_key_raises(self) -> None:
        """Setting an existing key should raise DuplicateTaskNameError."""
        reg: TaskRegistry[str] = TaskRegistry()
        reg['task_a'] = 'handler_a'
        with pytest.raises(DuplicateTaskNameError) as exc_info:
            reg['task_a'] = 'handler_b'
        assert exc_info.value.code == ErrorCode.TASK_DUPLICATE_NAME
        assert 'direct assignment' in str(exc_info.value)


@pytest.mark.unit
class TestTaskRegistryDelItem:
    """Tests for TaskRegistry.__delitem__."""

    def test_delete_existing_key(self) -> None:
        """Deleting existing key should remove it from data and sources."""
        reg: TaskRegistry[str] = TaskRegistry()
        reg.register('handler', name='task_a', source='file.py:1')
        del reg['task_a']
        assert 'task_a' not in reg
        assert 'task_a' not in reg._sources

    def test_delete_missing_key_raises_key_error(self) -> None:
        """Deleting missing key should raise KeyError."""
        reg: TaskRegistry[str] = TaskRegistry()
        with pytest.raises(KeyError):
            del reg['nonexistent']


@pytest.mark.unit
class TestTaskRegistryRegister:
    """Tests for TaskRegistry.register()."""

    def test_register_new_task(self) -> None:
        """New task should be inserted and returned."""
        reg: TaskRegistry[str] = TaskRegistry()
        result = reg.register('handler', name='my_task', source='mod.py:10')
        assert result == 'handler'
        assert reg['my_task'] == 'handler'
        assert reg._sources['my_task'] == 'mod.py:10'

    def test_register_without_source(self) -> None:
        """Registering without source should work but not track source."""
        reg: TaskRegistry[str] = TaskRegistry()
        reg.register('handler', name='my_task')
        assert reg['my_task'] == 'handler'
        assert 'my_task' not in reg._sources

    def test_reimport_same_source_returns_existing(self) -> None:
        """Re-registering same name+source should silently return existing task."""
        reg: TaskRegistry[str] = TaskRegistry()
        reg.register('handler_v1', name='my_task', source='mod.py:10')
        result = reg.register('handler_v2', name='my_task', source='mod.py:10')
        assert result == 'handler_v1'  # returns original, not the new one
        assert reg['my_task'] == 'handler_v1'

    def test_duplicate_different_source_raises(self) -> None:
        """Re-registering same name from different source should raise."""
        reg: TaskRegistry[str] = TaskRegistry()
        reg.register('handler_v1', name='my_task', source='mod_a.py:10')
        with pytest.raises(DuplicateTaskNameError) as exc_info:
            reg.register('handler_v2', name='my_task', source='mod_b.py:20')
        assert exc_info.value.code == ErrorCode.TASK_DUPLICATE_NAME

    def test_duplicate_no_source_on_existing_raises(self) -> None:
        """Duplicate name when existing has no source should raise (can't confirm re-import)."""
        reg: TaskRegistry[str] = TaskRegistry()
        reg.register('handler_v1', name='my_task')
        with pytest.raises(DuplicateTaskNameError):
            reg.register('handler_v2', name='my_task', source='mod.py:1')

    def test_duplicate_no_source_on_new_raises(self) -> None:
        """Duplicate name when new registration has no source should raise."""
        reg: TaskRegistry[str] = TaskRegistry()
        reg.register('handler_v1', name='my_task', source='mod.py:1')
        with pytest.raises(DuplicateTaskNameError):
            reg.register('handler_v2', name='my_task')


@pytest.mark.unit
class TestTaskRegistryUnregister:
    """Tests for TaskRegistry.unregister()."""

    def test_unregister_existing(self) -> None:
        """Unregistering existing task should remove it from data and sources."""
        reg: TaskRegistry[str] = TaskRegistry()
        reg.register('handler', name='my_task', source='mod.py:1')
        reg.unregister('my_task')
        assert 'my_task' not in reg
        assert 'my_task' not in reg._sources

    def test_unregister_missing_is_noop(self) -> None:
        """Unregistering missing task should not raise."""
        reg: TaskRegistry[str] = TaskRegistry()
        reg.unregister('nonexistent')  # should not raise


@pytest.mark.unit
class TestTaskRegistryMutableMapping:
    """Tests for MutableMapping protocol (__iter__, __len__, keys_list)."""

    def test_iter_yields_keys(self) -> None:
        """__iter__ should yield all registered task names."""
        reg: TaskRegistry[str] = TaskRegistry()
        reg.register('a', name='task_a')
        reg.register('b', name='task_b')
        assert set(reg) == {'task_a', 'task_b'}

    def test_len(self) -> None:
        """__len__ should return the number of registered tasks."""
        reg: TaskRegistry[str] = TaskRegistry()
        assert len(reg) == 0
        reg.register('a', name='task_a')
        assert len(reg) == 1
        reg.register('b', name='task_b')
        assert len(reg) == 2

    def test_keys_list(self) -> None:
        """keys_list() should return a list of all registered task names."""
        reg: TaskRegistry[str] = TaskRegistry()
        reg.register('a', name='task_a')
        reg.register('b', name='task_b')
        keys = reg.keys_list()
        assert isinstance(keys, list)
        assert set(keys) == {'task_a', 'task_b'}

    def test_initial_data(self) -> None:
        """TaskRegistry should accept initial data dict."""
        reg: TaskRegistry[str] = TaskRegistry({'task_a': 'handler_a'})
        assert reg['task_a'] == 'handler_a'
        assert len(reg) == 1

    def test_contains_existing_key(self) -> None:
        """'in' operator should return True for existing keys."""
        reg: TaskRegistry[str] = TaskRegistry()
        reg.register('handler', name='task_a')
        assert 'task_a' in reg

    def test_contains_missing_key_returns_false(self) -> None:
        """'in' operator should return False for missing keys.

        NotRegistered inherits from both RegistryError and KeyError,
        so MutableMapping.__contains__ catches it correctly.
        """
        reg: TaskRegistry[str] = TaskRegistry()
        assert 'nonexistent' not in reg
