# horsies/core/models/schedule.py
from __future__ import annotations
from datetime import time as datetime_time
from typing import Literal, Union, Optional, Any
from pydantic import BaseModel, Field, model_validator
from typing_extensions import Self
from enum import Enum
from horsies.core.errors import (
    ConfigurationError,
    ErrorCode,
    ValidationReport,
    raise_collected,
)


class Weekday(str, Enum):
    """Enum for days of the week."""

    MONDAY = 'monday'
    TUESDAY = 'tuesday'
    WEDNESDAY = 'wednesday'
    THURSDAY = 'thursday'
    FRIDAY = 'friday'
    SATURDAY = 'saturday'
    SUNDAY = 'sunday'


class IntervalSchedule(BaseModel):
    """
    Run task every N seconds/minutes/hours/days.

    At least one time unit must be specified.
    Total interval is the sum of all specified units.

    Examples:
        - Every 30 seconds: IntervalSchedule(seconds=30)
        - Every 5 minutes: IntervalSchedule(minutes=5)
        - Every 6 hours: IntervalSchedule(hours=6)
        - Every 2 days: IntervalSchedule(days=2)
        - Every 1.5 hours: IntervalSchedule(hours=1, minutes=30)
    """

    type: Literal['interval'] = 'interval'
    seconds: Optional[int] = Field(
        default=None, ge=1, le=86400, description='Seconds component (1-86400)'
    )
    minutes: Optional[int] = Field(
        default=None, ge=1, le=1440, description='Minutes component (1-1440)'
    )
    hours: Optional[int] = Field(
        default=None, ge=1, le=168, description='Hours component (1-168)'
    )
    days: Optional[int] = Field(
        default=None, ge=1, le=365, description='Days component (1-365)'
    )

    @model_validator(mode='after')
    def validate_at_least_one_unit(self) -> Self:
        """Ensure at least one time unit is specified."""
        report = ValidationReport('schedule')
        if not any([self.seconds, self.minutes, self.hours, self.days]):
            report.add(
                ConfigurationError(
                    message='IntervalSchedule requires at least one time unit',
                    code=ErrorCode.CONFIG_INVALID_SCHEDULE,
                    notes=['all time units (seconds, minutes, hours, days) are None'],
                    help_text='specify at least one: seconds, minutes, hours, or days',
                )
            )
        raise_collected(report)
        return self

    def total_seconds(self) -> int:
        """Calculate total interval in seconds."""
        total = 0
        if self.seconds:
            total += self.seconds
        if self.minutes:
            total += self.minutes * 60
        if self.hours:
            total += self.hours * 3600
        if self.days:
            total += self.days * 86400
        return total


class HourlySchedule(BaseModel):
    """
    Run task every hour at a specific minute and second.

    Examples:
        - Every hour at XX:30:00 -> HourlySchedule(minute=30)
        - Every hour at XX:15:30 -> HourlySchedule(minute=15, second=30)
    """

    type: Literal['hourly'] = 'hourly'
    minute: int = Field(ge=0, le=59, description='Minute of the hour (0-59)')
    second: int = Field(
        default=0, ge=0, le=59, description='Second of the minute (0-59)'
    )


class DailySchedule(BaseModel):
    """
    Run task every day at a specific time.

    Examples:
        - Daily at 3:00 AM -> DailySchedule(time=time(3, 0, 0))
        - Daily at 15:30:00 -> DailySchedule(time=time(15, 30, 0))
    """

    type: Literal['daily'] = 'daily'
    time: datetime_time = Field(description='Time of day to run (HH:MM:SS)')


class WeeklySchedule(BaseModel):
    """
    Run task on specific days of the week at a specific time.

    Examples:
        - Monday and Friday at 9 AM:
          WeeklySchedule(days=[Weekday.MONDAY, Weekday.FRIDAY], time=time(9, 0, 0))
        - Weekdays at 5 PM:
          WeeklySchedule(days=[...weekdays...], time=time(17, 0, 0))
    """

    type: Literal['weekly'] = 'weekly'
    days: list[Weekday] = Field(min_length=1, description='Days of week to run')
    time: datetime_time = Field(description='Time of day to run (HH:MM:SS)')

    @model_validator(mode='after')
    def validate_unique_days(self) -> Self:
        """Ensure no duplicate days."""
        report = ValidationReport('schedule')
        if len(self.days) != len(set(self.days)):
            report.add(
                ConfigurationError(
                    message='WeeklySchedule has duplicate days',
                    code=ErrorCode.CONFIG_INVALID_SCHEDULE,
                    notes=[f'days: {[d.value for d in self.days]}'],
                    help_text='each day should appear only once in the list',
                )
            )
        raise_collected(report)
        return self


class MonthlySchedule(BaseModel):
    """
    Run task on a specific day of the month at a specific time.

    Note: If day > days in month (e.g., day=31 in February),
    the schedule will be skipped for that month.

    Examples:
        - First day of month at midnight:
          MonthlySchedule(day=1, time=time(0, 0, 0))
        - 15th of each month at 3 PM:
          MonthlySchedule(day=15, time=time(15, 0, 0))
    """

    type: Literal['monthly'] = 'monthly'
    day: int = Field(ge=1, le=31, description='Day of month (1-31)')
    time: datetime_time = Field(description='Time of day to run (HH:MM:SS)')


SchedulePattern = Union[
    IntervalSchedule,
    HourlySchedule,
    DailySchedule,
    WeeklySchedule,
    MonthlySchedule,
]


class TaskSchedule(BaseModel):
    """
    Definition of a scheduled task.

    Fields:
        - name: Unique identifier for this schedule (used as DB primary key)
        - task_name: Name of the task to execute (must be registered via @app.task)
        - pattern: Schedule pattern defining when the task runs
        - args: Positional arguments to pass to the task
        - kwargs: Keyword arguments to pass to the task
        - queue_name: Target queue (None = default queue)
        - enabled: Whether this schedule is active
        - timezone: Timezone for schedule evaluation (default: UTC)
        - catch_up_missed: If scheduler was down, should missed runs be executed?
    """

    name: str = Field(description='Unique schedule identifier')
    task_name: str = Field(description='Task to execute (must be registered)')
    pattern: SchedulePattern = Field(description='Schedule pattern')
    args: tuple[Any, ...] = Field(default=(), description='Task positional arguments')
    kwargs: dict[str, Any] = Field(
        default_factory=dict, description='Task keyword arguments'
    )
    queue_name: Optional[str] = Field(default=None, description='Target queue name')
    enabled: bool = Field(default=True, description='Whether schedule is active')
    timezone: str = Field(default='UTC', description='Timezone for schedule evaluation')
    catch_up_missed: bool = Field(
        default=False, description='Execute missed runs if scheduler was down'
    )
    max_catch_up_runs: int = Field(
        default=100,
        ge=1,
        le=10000,
        description='Maximum runs to enqueue per scheduler tick when catch_up_missed=True',
    )


class ScheduleConfig(BaseModel):
    """
    Scheduler configuration for AppConfig.

    Fields:
        - enabled: Master switch for scheduler (default: True)
        - schedules: List of scheduled tasks
        - check_interval_seconds: How often to check for due schedules (1-60 seconds)
    """

    enabled: bool = Field(default=True, description='Master scheduler enable switch')
    schedules: list[TaskSchedule] = Field(
        default_factory=list, description='List of scheduled tasks'
    )
    check_interval_seconds: int = Field(
        default=1, ge=1, le=60, description='Scheduler check interval (1-60 seconds)'
    )

    @model_validator(mode='after')
    def validate_unique_schedule_names(self) -> Self:
        """Ensure all schedule names are unique."""
        report = ValidationReport('schedule')
        names: list[str] = [s.name for s in self.schedules]
        if len(names) != len(set(names)):
            report.add(
                ConfigurationError(
                    message='duplicate schedule names',
                    code=ErrorCode.CONFIG_INVALID_SCHEDULE,
                    notes=[f'schedule names: {names}'],
                    help_text='each schedule must have a unique name',
                )
            )
        raise_collected(report)
        return self
