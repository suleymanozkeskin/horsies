"""The single-column rules of the history table, as data.

Each rule names one single-column CHECK of `horsies_task_history` and its
predicate over the column. Writers that copy values from outside the move
program (the cutover relocation) evaluate these predicates before they
write, so a violating row is refused with its id and rule instead of
failing on the constraint.

A rule fails only when its predicate is FALSE; NULL passes, as it does for
a CHECK constraint. The predicates match the constraints that the frozen
and gated history DDL install; a test compares each one with its
installed constraint.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Final

from horsies.core.lifecycle.operations import TerminalizationKind

from ..names import TASK_HISTORY_PARENT


@dataclass(frozen=True, slots=True)
class HistoryColumnRule:
    """One single-column CHECK of the history table."""

    constraint_name: str
    predicate: str


def _rule(column: str, predicate: str) -> HistoryColumnRule:
    return HistoryColumnRule(
        constraint_name=f'{TASK_HISTORY_PARENT}_{column}_check',
        predicate=predicate,
    )


def _length(column: str, low: int, high: int) -> str:
    return f'octet_length({column}) BETWEEN {low} AND {high}'


def _nullable(column: str, predicate: str) -> str:
    return f'{column} IS NULL OR {predicate}'


_KINDS: Final = ', '.join(f"'{kind.value}'" for kind in TerminalizationKind)
_DISPOSITIONS: Final = (
    "'INLINE', 'REFERENCE', 'DECLINED_BY_POLICY', 'OVER_BOUND', "
    "'NEVER_ELIGIBLE'"
)

HISTORY_COLUMN_RULES: Final[tuple[HistoryColumnRule, ...]] = (
    _rule('attempt_archive_version', 'attempt_archive_version > 0'),
    _rule('attempt_snapshot_codec', _length('attempt_snapshot_codec', 1, 64)),
    _rule(
        'attempt_snapshot_content_type',
        _length('attempt_snapshot_content_type', 1, 255),
    ),
    _rule(
        'attempt_snapshot_digest',
        'octet_length(attempt_snapshot_digest) = 32',
    ),
    _rule('command_fingerprint', 'octet_length(command_fingerprint) = 32'),
    _rule('command_fingerprint_version', 'command_fingerprint_version > 0'),
    _rule('history_schema_version', 'history_schema_version > 0'),
    _rule(
        'input_digest',
        _nullable('input_digest', 'octet_length(input_digest) = 32'),
    ),
    _rule('max_retries', 'max_retries >= 0'),
    _rule('priority', 'priority BETWEEN 1 AND 100'),
    _rule(
        'rerun_input_codec',
        _nullable('rerun_input_codec', _length('rerun_input_codec', 1, 64)),
    ),
    _rule(
        'rerun_input_content_type',
        _nullable(
            'rerun_input_content_type',
            _length('rerun_input_content_type', 1, 255),
        ),
    ),
    _rule(
        'rerun_input_digest',
        _nullable('rerun_input_digest', 'octet_length(rerun_input_digest) = 32'),
    ),
    _rule('rerun_input_disposition', f'rerun_input_disposition IN ({_DISPOSITIONS})'),
    _rule(
        'rerun_input_inline',
        _nullable('rerun_input_inline', 'octet_length(rerun_input_inline) <= 65536'),
    ),
    _rule(
        'rerun_input_reference',
        _nullable(
            'rerun_input_reference', _length('rerun_input_reference', 1, 2048),
        ),
    ),
    _rule(
        'rerun_input_version',
        _nullable('rerun_input_version', 'rerun_input_version > 0'),
    ),
    _rule('result_codec', _length('result_codec', 1, 64)),
    _rule('result_content_type', _length('result_content_type', 1, 255)),
    _rule(
        'result_digest',
        _nullable('result_digest', 'octet_length(result_digest) = 32'),
    ),
    _rule('result_envelope_version', 'result_envelope_version > 0'),
    _rule('retry_count', 'retry_count >= 0'),
    _rule('status', "status IN ('COMPLETED', 'FAILED', 'CANCELLED', 'EXPIRED')"),
    _rule('terminalization_kind', f'terminalization_kind IN ({_KINDS})'),
)


def first_rule_violation_sql(projection_sql: str) -> str:
    """The first (task id, rule) whose predicate is FALSE over a projection.

    `projection_sql` is a SELECT whose output columns carry the history
    column names, including `task_id`. Returns no row when every rule holds.
    """
    rules = ',\n        '.join(
        f"('{rule.constraint_name}', ({rule.predicate}))"
        for rule in HISTORY_COLUMN_RULES
    )
    return f"""SELECT projected.task_id, rule.constraint_name
FROM ({projection_sql}) AS projected
CROSS JOIN LATERAL (
    VALUES
        {rules}
) AS rule(constraint_name, holds)
WHERE rule.holds IS FALSE
ORDER BY projected.task_id, rule.constraint_name
LIMIT 1"""
