"""The shared decoder, checked on what it accepts and what it refuses.

Both drivers decode through this one function, so its refusals are the only
thing standing between a contract mismatch and two adapters quietly
interpreting the same row differently. The refusal cases carry as much weight
here as the round trips.

No database: a returned row is a mapping, and every case is built as one.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

import pytest

from horsies.core.lifecycle.operations import TerminalizationKind
from horsies.core.lifecycle.outcomes import (
    Applied,
    AlreadyApplied,
    LostClaim,
    ObservedClaim,
    ObservedDeadline,
    ObservedForeignTerminalization,
    ObservedStaleness,
    ObservedWorkflowLink,
    ObservedWorkflowState,
    OutcomeDecodeError,
    SourceStateConflict,
    TaskAbsent,
    decode_outcome_row,
)
from horsies.core.types.status import TaskStatus

pytestmark = [pytest.mark.unit]

_NOW = datetime(2026, 8, 4, 12, 0, tzinfo=timezone.utc)
_CLAIMED = datetime(2026, 8, 4, 11, 59, tzinfo=timezone.utc)


def _row(**overrides: Any) -> dict[str, Any]:
    """A complete row, so every test states only what it is about."""
    row: dict[str, Any] = {
        'task_id': 't1',
        'ordinality': None,
        'outcome': 'APPLIED',
        'terminal_at': _NOW,
        'terminalization_kind': 'COMPLETE_FUSED',
        'observed_status': 'RUNNING',
        'observed_worker_id': 'w1',
        'observed_claimed_at': _CLAIMED,
        'guard_kind': None,
        'observed_guard': None,
    }
    row.update(overrides)
    return row


class TestAppliedAndAlreadyApplied:
    def test_applied_carries_what_the_database_assigned(self) -> None:
        outcome = decode_outcome_row(_row())
        assert isinstance(outcome, Applied)
        assert outcome.terminal_at == _NOW
        assert outcome.kind is TerminalizationKind.COMPLETE_FUSED

    def test_applied_observed_is_the_matched_pre_image(self) -> None:
        """Not decoration: it is what the guarded update matched."""
        outcome = decode_outcome_row(_row())
        assert isinstance(outcome, Applied)
        assert outcome.observed.status is TaskStatus.RUNNING
        assert outcome.observed.worker_id == 'w1'
        assert outcome.observed.claimed_at == _CLAIMED

    def test_already_applied_carries_the_committed_kind(self) -> None:
        outcome = decode_outcome_row(
            _row(
                outcome='ALREADY_APPLIED',
                observed_status='COMPLETED',
                terminalization_kind='COMPLETE_LOCKED',
            )
        )
        assert isinstance(outcome, AlreadyApplied)
        assert outcome.kind is TerminalizationKind.COMPLETE_LOCKED

    def test_applied_without_a_kind_is_refused(self) -> None:
        """Every function writes its own; a NULL means another writer did."""
        with pytest.raises(OutcomeDecodeError, match='terminalization kind'):
            decode_outcome_row(_row(terminalization_kind=None))

    def test_applied_without_terminal_at_is_refused(self) -> None:
        with pytest.raises(OutcomeDecodeError, match='terminal_at'):
            decode_outcome_row(_row(terminal_at=None))

    def test_unknown_kind_is_refused_rather_than_ignored(self) -> None:
        """An unplaceable kind cannot be judged for equivalence."""
        with pytest.raises(OutcomeDecodeError, match='unknown terminalization kind'):
            decode_outcome_row(_row(terminalization_kind='SOMETHING_NEWER'))


class TestRefusalOutcomes:
    def test_lost_claim_reports_who_holds_it_now(self) -> None:
        outcome = decode_outcome_row(
            _row(
                outcome='LOST_CLAIM',
                terminal_at=None,
                terminalization_kind=None,
                observed_status='CLAIMED',
                observed_worker_id='w2',
            )
        )
        assert isinstance(outcome, LostClaim)
        assert outcome.observed.worker_id == 'w2'

    def test_requeued_row_reports_cleared_claim_fields(self) -> None:
        """PENDING with no claim is the requeue case, still a lost claim."""
        outcome = decode_outcome_row(
            _row(
                outcome='LOST_CLAIM',
                terminal_at=None,
                terminalization_kind=None,
                observed_status='PENDING',
                observed_worker_id=None,
                observed_claimed_at=None,
            )
        )
        assert isinstance(outcome, LostClaim)
        assert outcome.observed.claimed_at is None

    def test_conflict_without_a_guard_is_claim_shaped(self) -> None:
        outcome = decode_outcome_row(
            _row(
                outcome='SOURCE_STATE_CONFLICT',
                terminal_at=None,
                terminalization_kind=None,
                observed_status='CLAIMED',
            )
        )
        assert isinstance(outcome, SourceStateConflict)
        assert outcome.evidence == ObservedClaim(
            worker_id='w1', claimed_at=_CLAIMED,
        )

    def test_task_absent_carries_no_observations(self) -> None:
        outcome = decode_outcome_row(
            _row(
                outcome='TASK_ABSENT',
                terminal_at=None,
                terminalization_kind=None,
                observed_status=None,
                observed_worker_id=None,
                observed_claimed_at=None,
            )
        )
        assert isinstance(outcome, TaskAbsent)

    def test_task_absent_with_observations_is_refused(self) -> None:
        """A row that does not exist cannot have been seen in a state."""
        with pytest.raises(OutcomeDecodeError, match='does not exist'):
            decode_outcome_row(
                _row(
                    outcome='TASK_ABSENT',
                    terminal_at=None,
                    terminalization_kind=None,
                    observed_worker_id=None,
                    observed_claimed_at=None,
                )
            )

    def test_unknown_outcome_is_refused(self) -> None:
        """Guessing which known outcome it resembles is the failure mode."""
        with pytest.raises(OutcomeDecodeError, match='unknown outcome'):
            decode_outcome_row(_row(outcome='PROBABLY_FINE'))


class TestGuardEvidence:
    def test_deadline_evidence(self) -> None:
        outcome = decode_outcome_row(
            _row(
                outcome='SOURCE_STATE_CONFLICT',
                terminal_at=None,
                terminalization_kind=None,
                guard_kind='DEADLINE',
                observed_guard={
                    'good_until': '2026-08-04T13:00:00+00:00',
                    'evaluated_at': '2026-08-04T12:00:00+00:00',
                },
            )
        )
        assert isinstance(outcome, SourceStateConflict)
        assert outcome.evidence == ObservedDeadline(
            good_until=datetime(2026, 8, 4, 13, 0, tzinfo=timezone.utc),
            evaluated_at=_NOW,
        )

    def test_staleness_evidence(self) -> None:
        outcome = decode_outcome_row(
            _row(
                outcome='SOURCE_STATE_CONFLICT',
                terminal_at=None,
                terminalization_kind=None,
                guard_kind='STALENESS',
                observed_guard={
                    'last_heartbeat_at': None,
                    'started_at': '2026-08-04T11:00:00+00:00',
                    'stale_after_seconds': 60,
                    'finalizing_stale_after_seconds': 120,
                },
            )
        )
        assert isinstance(outcome, SourceStateConflict)
        assert outcome.evidence == ObservedStaleness(
            last_heartbeat_at=None,
            started_at=datetime(2026, 8, 4, 11, 0, tzinfo=timezone.utc),
            stale_after_seconds=60,
            finalizing_stale_after_seconds=120,
        )

    def test_workflow_state_evidence(self) -> None:
        outcome = decode_outcome_row(
            _row(
                outcome='SOURCE_STATE_CONFLICT',
                terminal_at=None,
                terminalization_kind=None,
                guard_kind='WORKFLOW_STATUS',
                observed_guard={
                    'workflow_id': 'wf1', 'workflow_status': 'RUNNING',
                },
            )
        )
        assert isinstance(outcome, SourceStateConflict)
        assert outcome.evidence == ObservedWorkflowState(
            workflow_id='wf1', workflow_status='RUNNING',
        )

    def test_absent_link_is_distinguishable_from_a_link_in_a_state(self) -> None:
        absent = decode_outcome_row(
            _row(
                outcome='SOURCE_STATE_CONFLICT',
                terminal_at=None,
                terminalization_kind=None,
                guard_kind='WORKFLOW_LINK_ABSENT',
            )
        )
        present = decode_outcome_row(
            _row(
                outcome='SOURCE_STATE_CONFLICT',
                terminal_at=None,
                terminalization_kind=None,
                guard_kind='WORKFLOW_LINK_STATE',
                observed_guard={'node_status': 'SKIPPED'},
            )
        )
        assert isinstance(absent, SourceStateConflict)
        assert isinstance(present, SourceStateConflict)
        assert absent.evidence == ObservedWorkflowLink(node_status=None)
        assert present.evidence == ObservedWorkflowLink(node_status='SKIPPED')

    def test_foreign_terminalization_names_who_won(self) -> None:
        """Claim evidence is all-NULL on a terminal row, which is useless here."""
        outcome = decode_outcome_row(
            _row(
                outcome='SOURCE_STATE_CONFLICT',
                observed_status='CANCELLED',
                observed_worker_id=None,
                observed_claimed_at=None,
                terminalization_kind='CANCEL_ADMIN',
                guard_kind='FOREIGN_TERMINALIZATION',
            )
        )
        assert isinstance(outcome, SourceStateConflict)
        assert outcome.evidence == ObservedForeignTerminalization(
            observed_status=TaskStatus.CANCELLED,
            committed_kind=TerminalizationKind.CANCEL_ADMIN,
            terminal_at=_NOW,
        )

    def test_foreign_terminalization_of_a_legacy_row_reports_no_kind(self) -> None:
        """Rows written before the column exist; their provenance is unknown."""
        outcome = decode_outcome_row(
            _row(
                outcome='SOURCE_STATE_CONFLICT',
                observed_status='CANCELLED',
                observed_worker_id=None,
                observed_claimed_at=None,
                terminalization_kind=None,
                guard_kind='FOREIGN_TERMINALIZATION',
            )
        )
        assert isinstance(outcome, SourceStateConflict)
        assert outcome.evidence == ObservedForeignTerminalization(
            observed_status=TaskStatus.CANCELLED,
            committed_kind=None,
            terminal_at=_NOW,
        )


class TestDecodingFailsClosed:
    def test_unknown_guard_kind_is_refused(self) -> None:
        with pytest.raises(OutcomeDecodeError, match='unknown guard_kind'):
            decode_outcome_row(
                _row(
                    outcome='SOURCE_STATE_CONFLICT',
                    terminal_at=None,
                    terminalization_kind=None,
                    guard_kind='SOMETHING_ELSE',
                    observed_guard={'anything': 1},
                )
            )

    def test_missing_payload_key_is_refused(self) -> None:
        with pytest.raises(OutcomeDecodeError, match='missing'):
            decode_outcome_row(
                _row(
                    outcome='SOURCE_STATE_CONFLICT',
                    terminal_at=None,
                    terminalization_kind=None,
                    guard_kind='WORKFLOW_STATUS',
                    observed_guard={'workflow_id': 'wf1'},
                )
            )

    def test_unknown_payload_key_is_refused(self) -> None:
        """Evidence this decoder would drop means the two sides disagree."""
        with pytest.raises(OutcomeDecodeError, match='unexpected'):
            decode_outcome_row(
                _row(
                    outcome='SOURCE_STATE_CONFLICT',
                    terminal_at=None,
                    terminalization_kind=None,
                    guard_kind='WORKFLOW_STATUS',
                    observed_guard={
                        'workflow_id': 'wf1',
                        'workflow_status': 'RUNNING',
                        'workflow_paused_at': '2026-08-04T12:00:00+00:00',
                    },
                )
            )

    def test_payload_without_a_discriminant_is_refused(self) -> None:
        with pytest.raises(OutcomeDecodeError, match='no guard_kind'):
            decode_outcome_row(
                _row(
                    outcome='SOURCE_STATE_CONFLICT',
                    terminal_at=None,
                    terminalization_kind=None,
                    guard_kind=None,
                    observed_guard={'node_status': 'SKIPPED'},
                )
            )

    def test_payload_on_a_uniform_column_guard_is_refused(self) -> None:
        """Duplicating the uniform columns into jsonb creates two truths."""
        with pytest.raises(OutcomeDecodeError, match='must not send a payload'):
            decode_outcome_row(
                _row(
                    outcome='SOURCE_STATE_CONFLICT',
                    observed_status='CANCELLED',
                    guard_kind='FOREIGN_TERMINALIZATION',
                    observed_guard={'committed_kind': 'CANCEL_ADMIN'},
                )
            )

    def test_missing_column_is_refused(self) -> None:
        row = _row()
        del row['guard_kind']
        with pytest.raises(OutcomeDecodeError, match='missing'):
            decode_outcome_row(row)

    def test_unexpected_column_is_refused(self) -> None:
        """A column this decoder ignores is a contract it does not implement."""
        with pytest.raises(OutcomeDecodeError, match='unexpected'):
            decode_outcome_row(_row(observed_retry_count=3))

    def test_status_outside_the_vocabulary_is_refused(self) -> None:
        with pytest.raises(OutcomeDecodeError, match='not a task status'):
            decode_outcome_row(_row(observed_status='SLEEPING'))

    def test_unparseable_timestamp_is_refused(self) -> None:
        with pytest.raises(OutcomeDecodeError, match='ISO-8601'):
            decode_outcome_row(
                _row(
                    outcome='SOURCE_STATE_CONFLICT',
                    terminal_at=None,
                    terminalization_kind=None,
                    guard_kind='DEADLINE',
                    observed_guard={
                        'good_until': 'whenever', 'evaluated_at': _NOW,
                    },
                )
            )


class TestBatchOrdinality:
    def test_ordinality_travels_with_the_outcome(self) -> None:
        """Adapters reconstruct by ordinal rather than trusting row order."""
        outcomes = [
            decode_outcome_row(_row(task_id=f't{i}', ordinality=i))
            for i in (2, 1, 3)
        ]
        assert [o.ordinality for o in outcomes] == [2, 1, 3]

    def test_single_row_operations_carry_no_ordinal(self) -> None:
        assert decode_outcome_row(_row()).ordinality is None

    def test_non_integer_ordinality_is_refused(self) -> None:
        with pytest.raises(OutcomeDecodeError, match='ordinality'):
            decode_outcome_row(_row(ordinality='1'))
