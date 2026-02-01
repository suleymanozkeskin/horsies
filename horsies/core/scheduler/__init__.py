# horsies/core/scheduler/__init__.py
"""
Scheduler module for executing scheduled tasks.

Main components:
- Scheduler: Main service for running scheduled tasks
- ScheduleStateManager: Database state management
- calculate_next_run: Next run time calculation

Example usage:
    from horsies.core.scheduler import Scheduler

    scheduler = Scheduler(app)
    await scheduler.run_forever()
"""

from horsies.core.scheduler.service import Scheduler
from horsies.core.scheduler.state import ScheduleStateManager
from horsies.core.scheduler.calculator import calculate_next_run, should_run_now

__all__ = [
    'Scheduler',
    'ScheduleStateManager',
    'calculate_next_run',
    'should_run_now',
]
