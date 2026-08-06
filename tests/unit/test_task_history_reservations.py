"""Reservation operations: the registry seam, outcome decode, and DDL shape.

The property worth pinning structurally is the seam itself: no reservation
statement may reference live or history storage — that isolation is what
makes the registry contract buildable and reviewable before the live table
carries its cutover columns. The decode tests enumerate the outcome
contract from both sides, and the DDL tests pin the ratified reservation
semantics (no live expiry, expired-terminal reuse, bounded batches).
"""

from __future__ import annotations

import re

import pytest

from horsies.core.history.ddl.fragments import frozen_fragments
from horsies.core.history.errors import HistoryContractError
from horsies.core.history.identity.reservations import (
    KEY_RESERVATION_CLAIM_FUNCTION_DDL,
    KEY_RESERVATION_CLEANUP_FUNCTION_DDL,
    KEY_RESERVATION_TERMINALIZE_FUNCTION_DDL,
    ReservationApplied,
    ReservationConflict,
    ReservationReplay,
    decode_reservation_row,
    reservation_function_fragments,
)

pytestmark = [pytest.mark.unit]


TASK_ID = '0198c0de-0000-7000-8000-000000000001'
ALL_FUNCTION_DDL = '\n'.join(reservation_function_fragments())


class TestRegistrySeam:
    def test_no_statement_references_live_or_history_storage(self) -> None:
        assert 'horsies_tasks' not in ALL_FUNCTION_DDL
        assert re.search(r'horsies_task_history\b', ALL_FUNCTION_DDL) is None

    def test_only_the_registry_relation_is_touched(self) -> None:
        relations = set(
            re.findall(r'(?:FROM|INTO|UPDATE|DELETE FROM)\s+(horsies_\w+)',
                       ALL_FUNCTION_DDL)
        )
        assert relations == {'horsies_key_reservations'}

    def test_fragments_install_after_the_registry_table(self) -> None:
        fragments = frozen_fragments()

        def position(marker: str) -> int:
            for index, fragment in enumerate(fragments):
                if marker in fragment:
                    return index
            raise AssertionError(f'no fragment contains {marker!r}')

        table = position('CREATE TABLE horsies_key_reservations')
        outcome_type = position('CREATE TYPE horsies_key_reservation_outcome')
        claim = position('CREATE FUNCTION horsies_key_reservation_claim')
        assert table < outcome_type < claim


class TestClaimFunctionDdl:
    def test_active_reservation_predicate_matches_the_ratified_window(
        self,
    ) -> None:
        assert (
            "disposition = 'LIVE' OR expires_at > statement_timestamp()"
            in KEY_RESERVATION_CLAIM_FUNCTION_DDL
        )

    def test_new_live_reservation_carries_no_expiry(self) -> None:
        assert re.search(
            r"'LIVE', p_reservation_window, NULL",
            KEY_RESERVATION_CLAIM_FUNCTION_DDL,
        )

    def test_expired_terminal_rows_are_reused_by_deletion(self) -> None:
        assert (
            "disposition = 'TERMINAL'\n      AND expires_at <= "
            'statement_timestamp()'
        ) in KEY_RESERVATION_CLAIM_FUNCTION_DDL

    def test_window_bounds_are_validated_before_any_mutation(self) -> None:
        validation = KEY_RESERVATION_CLAIM_FUNCTION_DDL.index(
            'reservation window must be positive'
        )
        first_write = KEY_RESERVATION_CLAIM_FUNCTION_DDL.index('DELETE FROM')
        assert validation < first_write
        assert "p_reservation_window > interval '30 days'" in (
            KEY_RESERVATION_CLAIM_FUNCTION_DDL
        )

    def test_active_row_is_locked_before_classification(self) -> None:
        assert 'FOR UPDATE' in KEY_RESERVATION_CLAIM_FUNCTION_DDL


class TestTerminalizeFunctionDdl:
    def test_addresses_by_digest_and_verifies_task_ownership(self) -> None:
        assert 'WHERE idempotency_key_digest = p_key_digest' in (
            KEY_RESERVATION_TERMINALIZE_FUNCTION_DDL
        )
        assert 'AND task_id = p_task_id' in (
            KEY_RESERVATION_TERMINALIZE_FUNCTION_DDL
        )

    def test_window_starts_at_terminal_time(self) -> None:
        assert 'expires_at = p_terminal_at + reservation_window' in (
            KEY_RESERVATION_TERMINALIZE_FUNCTION_DDL
        )

    def test_only_live_reservations_terminalize(self) -> None:
        assert "AND disposition = 'LIVE'" in (
            KEY_RESERVATION_TERMINALIZE_FUNCTION_DDL
        )


class TestCleanupFunctionDdl:
    def test_deletes_only_expired_terminal_rows(self) -> None:
        assert "disposition = 'TERMINAL'" in KEY_RESERVATION_CLEANUP_FUNCTION_DDL
        assert 'expires_at <= statement_timestamp()' in (
            KEY_RESERVATION_CLEANUP_FUNCTION_DDL
        )
        assert "'LIVE'" not in KEY_RESERVATION_CLEANUP_FUNCTION_DDL

    def test_batches_are_bounded_and_skip_locked(self) -> None:
        assert 'LIMIT p_batch_size' in KEY_RESERVATION_CLEANUP_FUNCTION_DDL
        assert 'FOR UPDATE SKIP LOCKED' in KEY_RESERVATION_CLEANUP_FUNCTION_DDL


class TestOutcomeDecode:
    def test_applied(self) -> None:
        assert decode_reservation_row(
            outcome='APPLIED',
            row_task_id=TASK_ID,
            observed_fingerprint_version=None,
        ) == ReservationApplied(task_id=TASK_ID)

    def test_replay(self) -> None:
        assert decode_reservation_row(
            outcome='REPLAY',
            row_task_id=TASK_ID,
            observed_fingerprint_version=1,
        ) == ReservationReplay(task_id=TASK_ID)

    def test_conflict_carries_the_observed_version(self) -> None:
        assert decode_reservation_row(
            outcome='CONFLICT',
            row_task_id=TASK_ID,
            observed_fingerprint_version=1,
        ) == ReservationConflict(task_id=TASK_ID, observed_fingerprint_version=1)

    def test_applied_with_observed_version_raises(self) -> None:
        with pytest.raises(HistoryContractError, match='applied reservation'):
            decode_reservation_row(
                outcome='APPLIED',
                row_task_id=TASK_ID,
                observed_fingerprint_version=1,
            )

    def test_conflict_without_observed_version_raises(self) -> None:
        with pytest.raises(HistoryContractError, match='observed fingerprint'):
            decode_reservation_row(
                outcome='CONFLICT',
                row_task_id=TASK_ID,
                observed_fingerprint_version=None,
            )

    def test_boolean_observed_version_raises(self) -> None:
        with pytest.raises(HistoryContractError, match='observed fingerprint'):
            decode_reservation_row(
                outcome='CONFLICT',
                row_task_id=TASK_ID,
                observed_fingerprint_version=True,
            )

    def test_unknown_outcome_raises(self) -> None:
        with pytest.raises(HistoryContractError, match='unknown reservation'):
            decode_reservation_row(
                outcome='EXPIRED',
                row_task_id=TASK_ID,
                observed_fingerprint_version=None,
            )

    def test_missing_task_id_raises(self) -> None:
        with pytest.raises(HistoryContractError, match='did not decode'):
            decode_reservation_row(
                outcome='APPLIED',
                row_task_id=None,
                observed_fingerprint_version=None,
            )
