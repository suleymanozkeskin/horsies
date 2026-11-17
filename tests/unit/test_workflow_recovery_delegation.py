"""Unit tests of the workflow recovery pass on the 0005 recovery functions.

The pass sends one statement per discovery and one transaction or one
statement per candidate; these tests drive it with a session factory that
records the parameters and answers with planned rows.
"""

from __future__ import annotations

import uuid
from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest

from horsies.core.lifecycle.outcomes import OutcomeDecodeError
from horsies.core.workflows import recovery
from horsies.core.workflows.recovery_limit import (
    RaiseAlreadyCounted,
    RaiseCountedUnderLimit,
    RaiseLimitRefused,
    RaiseLimitWriteFailed,
    RaiseLimitWritten,
    RaiseWorkflowAbsent,
    RecoveryLimitOutcome,
    decode_recorded_raise,
)
from horsies.core.workflows.steps import NoParentAdvance, ParentAdvanceOwed


def _rows_result(rows: list[Any]) -> MagicMock:
    result = MagicMock()
    result.fetchall.return_value = rows
    return result


def _session_factory(execute: AsyncMock) -> MagicMock:
    session = AsyncMock()
    session.__aenter__ = AsyncMock(return_value=session)
    session.__aexit__ = AsyncMock(return_value=None)
    session.execute = execute
    return MagicMock(return_value=session)


def _capture_discovery(captured: list[Any]) -> AsyncMock:
    async def _execute(_statement: Any, params: dict[str, Any], *_a: Any, **_k: Any) -> MagicMock:
        captured.append(params.get('max_rows', 'MISSING'))
        return _rows_result([])

    return AsyncMock(side_effect=_execute)


@pytest.mark.unit
class TestRecoveryScanRowCap:
    """Global passes bind GLOBAL_SCAN_ROW_CAP; scoped passes are uncapped.

    One global reaper pass must not read an unbounded backlog, while a
    resume-scoped pass must recover its tree completely.
    """

    @pytest.mark.asyncio
    async def test_global_discovery_binds_row_cap(self) -> None:
        captured: list[Any] = []
        pass_ = recovery._Pass(  # pyright: ignore[reportPrivateUsage]
            session_factory=_session_factory(_capture_discovery(captured)),
            broker=MagicMock(),
            scope=recovery._GlobalScope(  # pyright: ignore[reportPrivateUsage]
                max_rows=recovery.GLOBAL_SCAN_ROW_CAP,
            ),
            failure_policy=MagicMock(),
        )
        report = recovery.RecoveryReport()

        await recovery._recover_stuck_nodes(pass_, report)  # pyright: ignore[reportPrivateUsage]

        assert report.recovered == 0
        assert captured == [recovery.GLOBAL_SCAN_ROW_CAP] * 4

    @pytest.mark.asyncio
    async def test_scoped_discovery_is_uncapped(self) -> None:
        captured: list[Any] = []
        pass_ = recovery._Pass(  # pyright: ignore[reportPrivateUsage]
            session_factory=_session_factory(_capture_discovery(captured)),
            broker=MagicMock(),
            scope=recovery._TreeScope(  # pyright: ignore[reportPrivateUsage]
                workflow_ids=(str(uuid.uuid4()),),
            ),
            failure_policy=MagicMock(),
        )
        report = recovery.RecoveryReport()

        await recovery._recover_stuck_nodes(pass_, report)  # pyright: ignore[reportPrivateUsage]
        await recovery._recover_tree_case2_3(pass_, report)  # pyright: ignore[reportPrivateUsage]
        await recovery._recover_tree_case4(pass_, report)  # pyright: ignore[reportPrivateUsage]

        assert report.recovered == 0
        assert captured == [None] * 6


@pytest.mark.unit
class TestRecoveryQueryScopes:
    '''Global and workflow-tree queries have different fixed SQL shapes.'''

    @pytest.mark.parametrize(
        ('global_query', 'tree_query'),
        [
            (
                recovery.GLOBAL_GET_PENDING_WITH_TERMINAL_DEPS_SQL,
                recovery.GET_PENDING_WITH_TERMINAL_DEPS_SQL,
            ),
            (
                recovery.GLOBAL_GET_READY_NOT_ENQUEUED_SQL,
                recovery.GET_READY_NOT_ENQUEUED_SQL,
            ),
            (
                recovery.GLOBAL_GET_READY_SUBWORKFLOWS_NOT_STARTED_SQL,
                recovery.GET_READY_SUBWORKFLOWS_NOT_STARTED_SQL,
            ),
            (
                recovery.GLOBAL_GET_COMPLETED_CHILDREN_NOT_UPDATED_SQL,
                recovery.GET_COMPLETED_CHILDREN_NOT_UPDATED_SQL,
            ),
        ],
    )
    def test_global_query_has_no_nullable_scope_arm(
        self,
        global_query: Any,
        tree_query: Any,
    ) -> None:
        assert ':scope_ids' not in global_query.text
        assert ' IS NULL OR ' not in global_query.text
        assert '= ANY(CAST(:scope_ids AS uuid[]))' in tree_query.text


@pytest.mark.unit
@pytest.mark.asyncio
async def test_global_discovery_failure_keeps_partial_metrics() -> None:
    execute = AsyncMock(side_effect=RuntimeError('query failed'))

    with pytest.raises(recovery.RecoveryPassFailure) as raised:
        await recovery.recover_stuck_workflows_global(_session_factory(execute), MagicMock())

    health = raised.value.health()
    assert health['state'] == 'error'
    assert health['errors'] == 1
    cases = health['cases']
    assert isinstance(cases, dict)
    assert cases['case_0']['errors'] == 1


def _audit_row(**columns: Any) -> SimpleNamespace:
    row: dict[str, Any] = {
        'outcome': 'PAGE',
        'scanned_count': 0,
        'completion_ids': [],
        'orphan_ids': [],
        'orphan_names': [],
        'claim_token': None,
    }
    row.update(columns)
    return SimpleNamespace(**row)


@pytest.mark.unit
class TestAuditPageRows:
    """The row of ``horsies_workflow_recovery_audit_page``."""

    def test_a_page_with_candidates_holds_the_claim_and_pairs_each_orphan_with_its_name(
        self,
    ) -> None:
        completion, orphan, token = uuid.uuid4(), uuid.uuid4(), uuid.uuid4()

        page = recovery.decode_audit_page([
            _audit_row(
                scanned_count=3,
                completion_ids=[completion],
                orphan_ids=[orphan],
                orphan_names=['orphan.name'],
                claim_token=token,
            ),
        ])

        assert page == recovery._AuditPageRead(  # pyright: ignore[reportPrivateUsage]
            scanned_count=3,
            completion_ids=(str(completion),),
            orphans=((str(orphan), 'orphan.name'),),
            claim=recovery._AuditClaim(token=str(token)),  # pyright: ignore[reportPrivateUsage]
        )

    def test_a_page_without_candidates_carries_no_claim(self) -> None:
        page = recovery.decode_audit_page([_audit_row(scanned_count=2)])

        assert isinstance(page, recovery._AuditPageRead)  # pyright: ignore[reportPrivateUsage]
        assert page.claim == recovery._NoAuditClaim()  # pyright: ignore[reportPrivateUsage]

    def test_a_held_cursor_is_its_own_outcome(self) -> None:
        page = recovery.decode_audit_page([SimpleNamespace(outcome='CURSOR_HELD')])

        assert page == recovery._AuditCursorHeld()  # pyright: ignore[reportPrivateUsage]

    @pytest.mark.parametrize(
        'rows',
        [
            [],
            [_audit_row(outcome='OTHER')],
            [_audit_row(orphan_ids=[uuid.uuid4()], orphan_names=[])],
            [_audit_row(scanned_count=True)],
        ],
    )
    def test_a_row_outside_the_contract_is_refused(self, rows: list[Any]) -> None:
        with pytest.raises(OutcomeDecodeError):
            _ = recovery.decode_audit_page(rows)


@pytest.mark.unit
class TestAuditActionRows:
    """The row of ``horsies_workflow_recover_completion`` and ``horsies_workflow_fail_orphaned``."""

    def test_a_written_child_owes_its_parent_advance(self) -> None:
        child = uuid.uuid4()

        action = recovery.decode_audit_action(
            [SimpleNamespace(outcome='COMPLETED', advance_child_workflow_id=child)],
            'horsies_workflow_recover_completion',
        )

        assert action == recovery._AuditWritten(  # pyright: ignore[reportPrivateUsage]
            parent_advance=ParentAdvanceOwed(child_workflow_id=str(child)),
        )

    def test_a_written_top_level_workflow_owes_no_advance(self) -> None:
        action = recovery.decode_audit_action(
            [SimpleNamespace(outcome='FAILED', advance_child_workflow_id=None)],
            'horsies_workflow_fail_orphaned',
        )

        assert action == recovery._AuditWritten(  # pyright: ignore[reportPrivateUsage]
            parent_advance=NoParentAdvance(),
        )

    @pytest.mark.parametrize(
        ('outcome', 'expected'),
        [
            ('UNCHANGED', recovery._AuditUnchanged()),  # pyright: ignore[reportPrivateUsage]
            ('AUDIT_CLAIM_LOST', recovery._AuditClaimLost()),  # pyright: ignore[reportPrivateUsage]
        ],
    )
    def test_each_other_outcome_has_its_own_record(self, outcome: str, expected: Any) -> None:
        action = recovery.decode_audit_action(
            [SimpleNamespace(outcome=outcome, advance_child_workflow_id=None)],
            'horsies_workflow_recover_completion',
        )

        assert action == expected

    def test_an_unknown_outcome_is_refused(self) -> None:
        with pytest.raises(OutcomeDecodeError):
            _ = recovery.decode_audit_action(
                [SimpleNamespace(outcome='DONE', advance_child_workflow_id=None)],
                'horsies_workflow_recover_completion',
            )


def _raise_row(outcome: str, failed_passes: Any = None, cause: Any = None) -> SimpleNamespace:
    return SimpleNamespace(outcome=outcome, failed_passes=failed_passes, limit_write_error=cause)


@pytest.mark.unit
class TestRecordedRaiseRows:
    """The row of ``horsies_workflow_recovery_record_raise``."""

    @pytest.mark.parametrize(
        ('row', 'expected'),
        [
            (_raise_row('WORKFLOW_ABSENT'), RaiseWorkflowAbsent()),
            (_raise_row('ALREADY_COUNTED'), RaiseAlreadyCounted()),
            (_raise_row('COUNTED', 1), RaiseCountedUnderLimit(failed_passes=1)),
            (_raise_row('REFUSED', 3), RaiseLimitRefused(failed_passes=3)),
            (
                _raise_row('LIMIT_REACHED_FAILED', 3),
                RaiseLimitWritten(failed_passes=3, outcome=RecoveryLimitOutcome.WORKFLOW_FAILED),
            ),
            (
                _raise_row('LIMIT_REACHED_LIVE_NODES', 3),
                RaiseLimitWritten(
                    failed_passes=3, outcome=RecoveryLimitOutcome.LIVE_NODES_FINISH,
                ),
            ),
            (
                _raise_row('LIMIT_WRITE_FAILED', 3, '23514: refused'),
                RaiseLimitWriteFailed(failed_passes=3, cause='23514: refused'),
            ),
        ],
    )
    def test_each_outcome_has_its_own_record(self, row: Any, expected: Any) -> None:
        assert decode_recorded_raise([row]) == expected

    @pytest.mark.parametrize(
        'rows',
        [
            [],
            [_raise_row('COUNTED', 1), _raise_row('COUNTED', 1)],
            [_raise_row('COUNTED', None)],
            [_raise_row('COUNTED', True)],
            [_raise_row('LIMIT_WRITE_FAILED', 3, None)],
            [_raise_row('OTHER', 1)],
        ],
    )
    def test_a_row_outside_the_contract_is_refused(self, rows: list[Any]) -> None:
        with pytest.raises(OutcomeDecodeError):
            _ = decode_recorded_raise(rows)


@pytest.mark.unit
def test_the_case_4_error_is_the_catalog_code_with_the_workflow_name() -> None:
    from horsies.core.codec.json_io import loads_json
    from horsies.core.types.result import Ok

    workflow_id = str(uuid.uuid4())

    stored = recovery.orphaned_workflow_error(workflow_id, 'orphan.name')

    match loads_json(stored):
        case Ok(dict() as decoded):
            pass
        case other:
            raise AssertionError(f'the case 4 error is not an object: {other!r}')
    assert decoded['error_code'] == 'WORKFLOW_START_INCOMPLETE'
    assert decoded['message'] == (
        "Orphaned workflow 'orphan.name': no workflow_tasks found. "
        'Workflow task insertion did not complete.'
    )
    assert decoded['data'] == {'workflow_id': workflow_id, 'recovery': 'case_4'}
