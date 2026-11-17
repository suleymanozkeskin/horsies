# app/core/registry/tasks.py
from __future__ import annotations
from typing import Dict, Iterator, MutableMapping, Generic, TypeVar
from horsies.core.errors import RegistryError, ErrorCode

T = TypeVar('T')


class NotRegistered(RegistryError, KeyError):
    """Raised when a task name is not present in the registry.

    Inherits from KeyError so MutableMapping.__contains__ works correctly
    (it catches KeyError to implement the ``in`` operator).
    """

    def __init__(self, task_name: str) -> None:
        RegistryError.__init__(
            self,
            message=f"task '{task_name}' not registered",
            code=ErrorCode.TASK_NOT_REGISTERED,
            notes=[f"requested task: '{task_name}'"],
            help_text='ensure the task is defined with @app.task() before use\nor make sure that task is discovered by the app',
        )
        self.task_name = task_name


class DuplicateTaskNameError(RegistryError):
    """Raised when a task name is registered more than once within the same app."""

    def __init__(self, task_name: str, context: str = '') -> None:
        super().__init__(
            message=f"duplicate task name '{task_name}'",
            code=ErrorCode.TASK_DUPLICATE_NAME,
            notes=[context] if context else [],
            help_text='each task name must be unique within a horsies instance',
        )
        self.task_name = task_name


class TaskRegistry(MutableMapping[str, T], Generic[T]):
    """Registry mapping task name -> task object.

    Tracks source locations to detect duplicate registrations:
    - Same name + same source: silently skip (re-import scenario)
    - Same name + different source: raise DuplicateTaskNameError
    """

    def __init__(self, initial: Dict[str, T] | None = None) -> None:
        self._data: Dict[str, T] = dict(initial or {})
        self._sources: Dict[str, str] = {}  # task_name -> "file:lineno"

    def __getitem__(self, key: str) -> T:
        try:
            return self._data[key]
        except KeyError:
            raise NotRegistered(key)

    def __setitem__(self, key: str, value: T) -> None:
        """Discourage direct assignment; enforce uniqueness like register()."""
        if key in self._data:
            raise DuplicateTaskNameError(key, 'detected via direct assignment')
        self._data[key] = value

    def __delitem__(self, key: str) -> None:
        del self._data[key]
        self._sources.pop(key, None)

    def __iter__(self) -> Iterator[str]:
        return iter(self._data)

    def __len__(self) -> int:
        return len(self._data)

    # --- convenience ---
    def register(self, task: T, *, name: str, source: str | None = None) -> T:
        """Insert a task under `name`, enforcing uniqueness per app.

        Args:
            task: The task object to register.
            name: The unique name for the task.
            source: Optional source location string (e.g., "file.py:42").
                    Used to detect re-imports vs. true duplicates.

        Returns:
            The registered task (existing if re-import, new otherwise).

        Raises:
            DuplicateTaskNameError: If same name registered from different source.
        """
        if name in self._data:
            existing_source = self._sources.get(name)
            if existing_source and source and existing_source == source:
                # Same source location - this is a re-import, skip silently
                return self._data[name]
            raise DuplicateTaskNameError(name, 'task with this name already exists')
        self._data[name] = task
        if source:
            self._sources[name] = source
        return task

    def unregister(self, name: str) -> None:
        self._data.pop(name, None)
        self._sources.pop(name, None)

    def keys_list(self) -> list[str]:
        return list(self._data.keys())
