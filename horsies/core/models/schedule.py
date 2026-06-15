# horsies/core/models/schedule.py
from __future__ import annotations
from datetime import time as datetime_time
from typing import Annotated, Generic, Literal, TypeVar, Union, Optional, Any
from pydantic import BaseModel, ConfigDict, Field, model_validator
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


class Month(str, Enum):
    """Enum for months of the year. Mirrors Weekday as a str-enum.

    String values keep `model_dump(mode='json')` output stable, which the
    scheduler's config-change hash depends on.
    """

    JANUARY = 'january'
    FEBRUARY = 'february'
    MARCH = 'march'
    APRIL = 'april'
    MAY = 'may'
    JUNE = 'june'
    JULY = 'july'
    AUGUST = 'august'
    SEPTEMBER = 'september'
    OCTOBER = 'october'
    NOVEMBER = 'november'
    DECEMBER = 'december'


# Canonical ordinals for range/step semantics over enum fields.
# Weekday is Monday=0..Sunday=6 (Python's date.weekday() convention).
# scheduler/calculator.py imports these to avoid a second source of truth.
MONTH_ORDINAL: dict[Month, int] = {
    Month.JANUARY: 1,
    Month.FEBRUARY: 2,
    Month.MARCH: 3,
    Month.APRIL: 4,
    Month.MAY: 5,
    Month.JUNE: 6,
    Month.JULY: 7,
    Month.AUGUST: 8,
    Month.SEPTEMBER: 9,
    Month.OCTOBER: 10,
    Month.NOVEMBER: 11,
    Month.DECEMBER: 12,
}

WEEKDAY_ORDINAL: dict[Weekday, int] = {
    Weekday.MONDAY: 0,
    Weekday.TUESDAY: 1,
    Weekday.WEDNESDAY: 2,
    Weekday.THURSDAY: 3,
    Weekday.FRIDAY: 4,
    Weekday.SATURDAY: 5,
    Weekday.SUNDAY: 6,
}

# Leap-aware maximum day for each month ordinal (February allows 29).
# Used by the construction-time satisfiability check.
MONTH_MAX_DAYS: dict[int, int] = {
    1: 31, 2: 29, 3: 31, 4: 30, 5: 31, 6: 30,
    7: 31, 8: 31, 9: 30, 10: 31, 11: 30, 12: 31,
}


# --- Cron term families -----------------------------------------------------
#
# Numeric terms carry ints (minute, hour, day-of-month); enum terms are
# generic over the enum type so `month` and `day-of-week` stay distinct types.
# Per-field domain bounds are enforced by CronSchedule's validator, which names
# the offending field — the term classes are domain-agnostic.

EnumT = TypeVar('EnumT', Month, Weekday)


class CronEvery(BaseModel):
    """`*` — every value in the field's domain. Shared by all field families."""

    kind: Literal['every'] = 'every'


class CronStep(BaseModel):
    """`*/n` — every nth value across the field's full domain.

    `step` must be <= the field's span (e.g. 30 for day-of-month, 59 for
    minute). A larger step would select only the domain's first value, so it is
    rejected by validation as a likely mistake — use `CronValues` for a single
    value. This is intentionally stricter than some cron implementations.
    """

    kind: Literal['step'] = 'step'
    step: int = Field(ge=1, description='Step size (>= 1, <= field span)')


class CronValues(BaseModel):
    """`a,b,c` — an explicit set of integer values (single value = one entry)."""

    kind: Literal['values'] = 'values'
    values: list[int] = Field(min_length=1, description='Explicit integer values')


class CronRange(BaseModel):
    """`a-b` or `a-b/n` — an inclusive integer range with an optional step."""

    kind: Literal['range'] = 'range'
    start: int = Field(description='Range start (inclusive)')
    end: int = Field(description='Range end (inclusive)')
    step: int = Field(default=1, ge=1, description='Step within the range (>= 1)')


class CronEnumValues(BaseModel, Generic[EnumT]):
    """`a,b,c` over an enum field (Month or Weekday)."""

    kind: Literal['enum_values'] = 'enum_values'
    values: list[EnumT] = Field(min_length=1, description='Explicit enum values')


class CronEnumRange(BaseModel, Generic[EnumT]):
    """`a-b` or `a-b/n` over an enum field, ordered by the canonical ordinal."""

    kind: Literal['enum_range'] = 'enum_range'
    start: EnumT = Field(description='Range start (inclusive, by canonical order)')
    end: EnumT = Field(description='Range end (inclusive, by canonical order)')
    step: int = Field(default=1, ge=1, description='Step within the range (>= 1)')


class CronEnumStep(BaseModel, Generic[EnumT]):
    """`*/n` over an enum field's full domain, by canonical ordinal.

    `step` must be <= the field's ordinal span (11 for month, 6 for
    day-of-week); a larger step selects only the first value and is rejected.
    Intentionally stricter than some cron implementations — use `CronEnumValues`.
    """

    kind: Literal['enum_step'] = 'enum_step'
    step: int = Field(ge=1, description='Step size (>= 1, <= field span)')


CronNumericTerm = Annotated[
    Union[CronEvery, CronStep, CronValues, CronRange],
    Field(discriminator='kind'),
]
CronMonthTerm = Annotated[
    Union[
        CronEvery,
        CronEnumValues[Month],
        CronEnumRange[Month],
        CronEnumStep[Month],
    ],
    Field(discriminator='kind'),
]
CronWeekdayTerm = Annotated[
    Union[
        CronEvery,
        CronEnumValues[Weekday],
        CronEnumRange[Weekday],
        CronEnumStep[Weekday],
    ],
    Field(discriminator='kind'),
]


# --- Day dimension ----------------------------------------------------------
#
# A single sum type forces an explicit choice when both day-of-month and
# day-of-week are restricted, eliminating cron's silent OR ambiguity.


class EveryDay(BaseModel):
    """Both day-of-month and day-of-week unrestricted."""

    kind: Literal['every_day'] = 'every_day'


class ByMonthDay(BaseModel):
    """Restrict by day-of-month only."""

    kind: Literal['by_month_day'] = 'by_month_day'
    day_of_month: list[CronNumericTerm] = Field(min_length=1)


class ByWeekday(BaseModel):
    """Restrict by day-of-week only."""

    kind: Literal['by_weekday'] = 'by_weekday'
    day_of_week: list[CronWeekdayTerm] = Field(min_length=1)


class EitherDay(BaseModel):
    """Fire when day-of-month OR day-of-week matches (cron's '13th or Friday')."""

    kind: Literal['either_day'] = 'either_day'
    day_of_month: list[CronNumericTerm] = Field(min_length=1)
    day_of_week: list[CronWeekdayTerm] = Field(min_length=1)


class BothDays(BaseModel):
    """Fire only when day-of-month AND day-of-week both match (intersection)."""

    kind: Literal['both_days'] = 'both_days'
    day_of_month: list[CronNumericTerm] = Field(min_length=1)
    day_of_week: list[CronWeekdayTerm] = Field(min_length=1)


DaySelector = Annotated[
    Union[EveryDay, ByMonthDay, ByWeekday, EitherDay, BothDays],
    Field(discriminator='kind'),
]


# --- Validation + expansion helpers -----------------------------------------


def _numeric_config_error(field_name: str, detail: str) -> ConfigurationError:
    """Build a CONFIG_INVALID_SCHEDULE error scoped to a named field."""
    return ConfigurationError(
        message=f"invalid cron '{field_name}' field: {detail}",
        code=ErrorCode.CONFIG_INVALID_SCHEDULE,
        notes=[f'field: {field_name}'],
        help_text=f'every {field_name} value must satisfy the field domain',
    )


def _validate_numeric_terms(
    terms: list[CronNumericTerm],
    field_name: str,
    lo: int,
    hi: int,
    report: ValidationReport,
) -> None:
    """Validate integer-domain terms against [lo, hi]."""
    span = hi - lo
    for term in terms:
        match term:
            case CronEvery():
                continue
            case CronStep(step=step):
                if step > span:
                    report.add(
                        _numeric_config_error(
                            field_name,
                            f'step {step} exceeds the {field_name} span {span}; '
                            'use explicit values instead',
                        )
                    )
            case CronValues(values=values):
                for value in values:
                    if not (lo <= value <= hi):
                        report.add(
                            _numeric_config_error(
                                field_name, f'value {value} out of range {lo}-{hi}'
                            )
                        )
            case CronRange(start=start, end=end, step=step):
                if not (lo <= start <= hi):
                    report.add(
                        _numeric_config_error(
                            field_name, f'range start {start} out of range {lo}-{hi}'
                        )
                    )
                if not (lo <= end <= hi):
                    report.add(
                        _numeric_config_error(
                            field_name, f'range end {end} out of range {lo}-{hi}'
                        )
                    )
                if start > end:
                    report.add(
                        _numeric_config_error(
                            field_name, f'range start {start} exceeds end {end}'
                        )
                    )
                if step > span:
                    report.add(
                        _numeric_config_error(
                            field_name, f'range step {step} exceeds span {span}'
                        )
                    )


def _enum_ordinal(value: Month | Weekday) -> int:
    """Map an enum field value to its canonical ordinal."""
    match value:
        case Month():
            return MONTH_ORDINAL[value]
        case Weekday():
            return WEEKDAY_ORDINAL[value]


def _validate_enum_terms(
    terms: list[CronMonthTerm] | list[CronWeekdayTerm],
    field_name: str,
    span: int,
    report: ValidationReport,
) -> None:
    """Validate enum-domain terms: range ordering and step span.

    Enum values are in-domain by construction (the type permits only members),
    so only ordering and step bounds need checking. `span` is the field's
    ordinal span (max - min): 11 for month, 6 for day-of-week.
    """
    for term in terms:
        match term:
            case CronEvery():
                continue
            case CronEnumStep(step=step):
                if step > span:
                    report.add(
                        _numeric_config_error(
                            field_name,
                            f'step {step} exceeds the {field_name} span {span}',
                        )
                    )
            case CronEnumValues():
                continue
            case CronEnumRange(start=start, end=end, step=step):
                if _enum_ordinal(start) > _enum_ordinal(end):
                    report.add(
                        _numeric_config_error(
                            field_name,
                            f'range start {start.value} comes after end {end.value}; '
                            'wrap-around ranges are not allowed, use explicit values',
                        )
                    )
                if step > span:
                    report.add(
                        _numeric_config_error(
                            field_name, f'range step {step} exceeds span {span}'
                        )
                    )


def _validate_day_selector(day: DaySelector, report: ValidationReport) -> None:
    """Validate the day dimension's contained day-of-month/day-of-week terms."""
    match day:
        case EveryDay():
            return
        case ByMonthDay(day_of_month=dom):
            _validate_numeric_terms(dom, 'day_of_month', 1, 31, report)
        case ByWeekday(day_of_week=dow):
            _validate_enum_terms(dow, 'day_of_week', 6, report)
        case EitherDay(day_of_month=dom, day_of_week=dow) | BothDays(
            day_of_month=dom, day_of_week=dow
        ):
            _validate_numeric_terms(dom, 'day_of_month', 1, 31, report)
            _validate_enum_terms(dow, 'day_of_week', 6, report)


def expand_numeric_field(terms: list[CronNumericTerm], lo: int, hi: int) -> set[int]:
    """Expand numeric terms into the concrete set of matched values in [lo, hi].

    Public: shared by validation (satisfiability) and the scheduler matcher so
    term expansion is never reimplemented.
    """
    result: set[int] = set()
    for term in terms:
        match term:
            case CronEvery():
                result |= set(range(lo, hi + 1))
            case CronStep(step=step):
                result |= set(range(lo, hi + 1, step))
            case CronValues(values=values):
                result |= {v for v in values if lo <= v <= hi}
            case CronRange(start=start, end=end, step=step):
                result |= {v for v in range(start, end + 1, step) if lo <= v <= hi}
    return result


def expand_enum_field(
    terms: list[CronMonthTerm] | list[CronWeekdayTerm], lo: int, hi: int
) -> set[int]:
    """Expand enum terms into the concrete set of ordinals in [lo, hi].

    Month ordinals are 1-12, weekday ordinals 0-6. Public: shared by validation
    and the scheduler matcher so month and weekday matching reuse one expansion.
    """
    result: set[int] = set()
    for term in terms:
        match term:
            case CronEvery():
                result |= set(range(lo, hi + 1))
            case CronEnumStep(step=step):
                result |= set(range(lo, hi + 1, step))
            case CronEnumValues(values=values):
                result |= {_enum_ordinal(v) for v in values}
            case CronEnumRange(start=start, end=end, step=step):
                result |= set(
                    range(_enum_ordinal(start), _enum_ordinal(end) + 1, step)
                )
    return result


def _validate_satisfiable(
    month: list[CronMonthTerm], day: DaySelector, report: ValidationReport
) -> None:
    """Reject schedules whose month x day-of-month combination can never occur.

    Only ByMonthDay and BothDays constrain day-of-month such that the schedule
    can be empty (e.g. February + day 30). EitherDay always fires via its
    weekday branch; ByWeekday/EveryDay are always satisfiable.
    """
    match day:
        case ByMonthDay(day_of_month=dom) | BothDays(day_of_month=dom):
            month_set = expand_enum_field(month, 1, 12)
            dom_set = expand_numeric_field(dom, 1, 31)
            feasible = any(
                d <= MONTH_MAX_DAYS[m] for m in month_set for d in dom_set
            )
            if not feasible:
                report.add(
                    ConfigurationError(
                        message='cron schedule can never fire',
                        code=ErrorCode.CONFIG_INVALID_SCHEDULE,
                        notes=[
                            f'months: {sorted(month_set)}',
                            f'days-of-month: {sorted(dom_set)}',
                        ],
                        help_text='no selected day-of-month exists in any selected month',
                    )
                )
        case EveryDay() | ByWeekday() | EitherDay():
            return


# --- Concise rendering (for config logging) ---------------------------------


def _render_numeric_term(term: CronNumericTerm) -> str:
    """Render one numeric term in cron-like shorthand."""
    match term:
        case CronEvery():
            return '*'
        case CronStep(step=step):
            return f'*/{step}'
        case CronValues(values=values):
            return ','.join(str(v) for v in sorted(values))
        case CronRange(start=start, end=end, step=step):
            base = f'{start}-{end}'
            return f'{base}/{step}' if step != 1 else base


def _render_enum_term(term: CronMonthTerm | CronWeekdayTerm) -> str:
    """Render one enum term in cron-like shorthand using enum values."""
    match term:
        case CronEvery():
            return '*'
        case CronEnumStep(step=step):
            return f'*/{step}'
        case CronEnumValues(values=values):
            return ','.join(v.value for v in values)
        case CronEnumRange(start=start, end=end, step=step):
            base = f'{start.value}-{end.value}'
            return f'{base}/{step}' if step != 1 else base


def _render_numeric_terms(terms: list[CronNumericTerm]) -> str:
    return '|'.join(_render_numeric_term(t) for t in terms)


def _render_enum_terms(terms: list[CronMonthTerm] | list[CronWeekdayTerm]) -> str:
    return '|'.join(_render_enum_term(t) for t in terms)


def _render_day(day: DaySelector) -> str:
    """Render the day selector, making OR/AND explicit."""
    match day:
        case EveryDay():
            return '*'
        case ByMonthDay(day_of_month=dom):
            return f'dom={_render_numeric_terms(dom)}'
        case ByWeekday(day_of_week=dow):
            return f'dow={_render_enum_terms(dow)}'
        case EitherDay(day_of_month=dom, day_of_week=dow):
            return f'(dom={_render_numeric_terms(dom)} OR dow={_render_enum_terms(dow)})'
        case BothDays(day_of_month=dom, day_of_week=dow):
            return f'(dom={_render_numeric_terms(dom)} AND dow={_render_enum_terms(dow)})'


class CronSchedule(BaseModel):
    """Typed 5-field cron-style schedule (with step extensions).

    Expresses what a `minute hour day-of-month month day-of-week` crontab line
    can, through typed fields instead of a cron-expression string. Fires at
    second :00 (minute granularity).

    The `day` field collapses day-of-month and day-of-week into an explicit
    DaySelector, so cron's silent OR-when-both-restricted ambiguity cannot be
    expressed by accident.
    """

    type: Literal['cron'] = 'cron'
    minute: list[CronNumericTerm] = Field(
        min_length=1, description='Minute field terms (0-59)'
    )
    hour: list[CronNumericTerm] = Field(
        min_length=1, description='Hour field terms (0-23)'
    )
    month: list[CronMonthTerm] = Field(
        min_length=1, description='Month field terms'
    )
    day: DaySelector = Field(description='Day-of-month / day-of-week selector')

    @model_validator(mode='after')
    def validate_fields(self) -> Self:
        """Validate per-field domains, range ordering, and satisfiability."""
        report = ValidationReport('schedule')
        _validate_numeric_terms(self.minute, 'minute', 0, 59, report)
        _validate_numeric_terms(self.hour, 'hour', 0, 23, report)
        _validate_enum_terms(self.month, 'month', 11, report)
        _validate_day_selector(self.day, report)
        # Satisfiability expansion is only meaningful once terms are in-domain.
        if not report.has_errors():
            _validate_satisfiable(self.month, self.day, report)
        raise_collected(report)
        return self

    def summary(self) -> str:
        """Concise one-line description for config logging."""
        return (
            f'cron min={_render_numeric_terms(self.minute)} '
            f'hour={_render_numeric_terms(self.hour)} '
            f'month={_render_enum_terms(self.month)} '
            f'day={_render_day(self.day)}'
        )


SchedulePattern = Union[
    IntervalSchedule,
    HourlySchedule,
    DailySchedule,
    WeeklySchedule,
    MonthlySchedule,
    CronSchedule,
]


class TaskSchedule(BaseModel):
    """
    Definition of a scheduled task.

    Fields:
        - name: Unique identifier for this schedule (used as DB primary key)
        - task_name: Name of the task to execute (must be registered via @app.task)
        - pattern: Schedule pattern defining when the task runs
        - kwargs: Keyword arguments to pass to the task (schedules are kwargs-only)
        - queue_name: Target queue (None = default queue)
        - enabled: Whether this schedule is active
        - timezone: Timezone for schedule evaluation (default: UTC)
        - catch_up_missed: If scheduler was down, should missed runs be executed?
    """

    model_config = ConfigDict(extra='forbid')

    name: str = Field(description='Unique schedule identifier')
    task_name: str = Field(description='Task to execute (must be registered)')
    pattern: SchedulePattern = Field(description='Schedule pattern')
    kwargs: dict[str, Any] = Field(
        default_factory=dict, description='Task keyword arguments (kwargs-only)'
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
