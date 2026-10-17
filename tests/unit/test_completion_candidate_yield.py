"""Completion candidate yield of the global workflow audit."""

from __future__ import annotations

import pytest

from horsies.core.workflows.recovery import (
    AuditPage,
    NoAuditPage,
    RecoveryPassFailure,
    RecoveryReport,
    completion_candidate_yield,
)

pytestmark = [pytest.mark.unit]


@pytest.mark.parametrize(
    ('audit', 'expected'),
    [
        (NoAuditPage(), None),
        (AuditPage(scanned=0, completion_candidates=0), None),
        (AuditPage(scanned=200, completion_candidates=0), 0.0),
        (AuditPage(scanned=200, completion_candidates=50), 0.25),
    ],
    ids=['no-page', 'empty-page', 'no-candidates', 'candidates'],
)
def test_yield_per_audit_result(
    audit: AuditPage | NoAuditPage,
    expected: float | None,
) -> None:
    assert completion_candidate_yield(audit) == expected


def test_report_without_an_audit_page_has_no_yield() -> None:
    assert RecoveryReport().health()['completion_candidate_yield'] is None


def test_partial_failure_keeps_the_audited_yield() -> None:
    report = RecoveryReport(
        audit=AuditPage(scanned=10, completion_candidates=2),
    )
    failure = RecoveryPassFailure(report, RuntimeError('candidate failed'))
    assert failure.health()['completion_candidate_yield'] == 0.2
