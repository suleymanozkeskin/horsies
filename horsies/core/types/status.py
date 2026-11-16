# core/types/status.py
"""
Core types and enums used throughout the application.
This module imports no other application module but the generated catalogs
(`horsies._catalog`), which import nothing from the package.
"""

from horsies._catalog.status import TASK_TERMINAL_STATES as TASK_TERMINAL_STATES
from horsies._catalog.status import TaskAttemptOutcome as TaskAttemptOutcome
from horsies._catalog.status import TaskStatus as TaskStatus
