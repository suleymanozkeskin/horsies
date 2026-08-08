"""A cell's cache posture must be read back, not asserted by the harness."""

from __future__ import annotations

from typing import Any

import pytest

from tests.task_history_prototypes.buffer_cold import (
    BUFFER_COLD_DEFINITION,
    EVICTION_COMMAND_BLOCK,
    EVICTION_PROCEDURE,
    NOT_PERFORMED,
    BufferCensus,
    CachePosture,
    CachePostureError,
    EvictionDeclaration,
    EvictionStep,
    MeasuredReads,
    PostureEvidence,
    ServerRestart,
    assert_shared_buffers_cold,
    assert_shared_buffers_warm,
    buffer_census_from_row,
    measured_reads_from_plan,
)

OPERATOR = 'operator'

# The shapes the real server produced: a fully resident relation that hits and
# does not read, and an evicted one that reads and does not hit.
WARM_CENSUS = BufferCensus(relation='t', buffers=6364, relation_blocks=6912)
WARM_READS = MeasuredReads(shared_hit=25268, shared_read=2380, io_time_ms=2.288)
COLD_CENSUS = BufferCensus(relation='t', buffers=0, relation_blocks=6912)
COLD_READS = MeasuredReads(shared_hit=0, shared_read=27648, io_time_ms=2.784)


def _restart() -> ServerRestart:
    return ServerRestart(
        start_time_before='2026-08-08 09:00:00+00',
        start_time_after='2026-08-08 09:04:00+00',
        in_recovery_after=False,
    )


def _declaration(**overrides: Any) -> EvictionDeclaration:
    performed = {step: OPERATOR for step in EVICTION_PROCEDURE}
    performed[EvictionStep.NO_WARMING_QUERY] = 'harness'
    performed.update(overrides.pop('performed_by', {}))
    return EvictionDeclaration(
        performed_by=performed,
        restart=overrides.pop('restart', _restart()),
        stopped_resident_containers=overrides.pop(
            'stopped_resident_containers', ('horsies-throughput-pg18',)
        ),
    )


def test_a_resident_relation_is_not_cold() -> None:
    """The failure this module exists for: warm measured, cold reported."""
    with pytest.raises(CachePostureError, match='is resident'):
        assert_shared_buffers_cold(WARM_CENSUS, WARM_READS)


def test_an_evicted_relation_is_cold() -> None:
    assert_shared_buffers_cold(COLD_CENSUS, COLD_READS)


def test_an_evicted_relation_is_not_warm() -> None:
    with pytest.raises(CachePostureError, match='nothing resident'):
        assert_shared_buffers_warm(COLD_CENSUS, COLD_READS)


def test_a_resident_relation_is_warm() -> None:
    assert_shared_buffers_warm(WARM_CENSUS, WARM_READS)


def test_a_query_that_read_nothing_is_not_a_cold_measurement() -> None:
    """An empty pool proves nothing if the measured query touched nothing.

    Both signals are required because either alone is satisfiable by a run
    that never went near the relation.
    """
    with pytest.raises(CachePostureError, match='a cold measurement reads'):
        assert_shared_buffers_cold(
            COLD_CENSUS, MeasuredReads(shared_hit=0, shared_read=0, io_time_ms=0.0)
        )


def test_a_query_that_mostly_hit_is_not_a_cold_measurement() -> None:
    with pytest.raises(CachePostureError, match='pool it was supposed'):
        assert_shared_buffers_cold(
            COLD_CENSUS,
            MeasuredReads(shared_hit=9000, shared_read=10, io_time_ms=0.1),
        )


def test_a_warm_cell_that_ran_cold_is_refused() -> None:
    """Warm is a claim too, and its limit assumes resident blocks."""
    with pytest.raises(CachePostureError, match='substantially cold'):
        assert_shared_buffers_warm(
            BufferCensus(relation='t', buffers=1, relation_blocks=6912),
            MeasuredReads(shared_hit=10, shared_read=6000, io_time_ms=40.0),
        )


def test_a_cold_cell_needs_an_eviction_declaration() -> None:
    with pytest.raises(CachePostureError, match='no eviction declaration'):
        PostureEvidence(
            posture=CachePosture.BUFFER_COLD,
            census=COLD_CENSUS,
            reads=COLD_READS,
            declaration=None,
        )


def test_a_warm_cell_carrying_an_eviction_declaration_is_refused() -> None:
    """The run it describes cannot have been warm."""
    with pytest.raises(CachePostureError, match='cannot have been warm'):
        PostureEvidence(
            posture=CachePosture.PREPARED_WARM,
            census=WARM_CENSUS,
            reads=WARM_READS,
            declaration=_declaration(),
        )


def test_an_unprepared_cell_carrying_an_eviction_declaration_is_refused() -> None:
    with pytest.raises(CachePostureError, match='under another name'):
        PostureEvidence(
            posture=CachePosture.UNPREPARED,
            census=COLD_CENSUS,
            reads=COLD_READS,
            declaration=_declaration(),
        )


def test_a_complete_cold_cell_is_accepted() -> None:
    evidence = PostureEvidence(
        posture=CachePosture.BUFFER_COLD,
        census=COLD_CENSUS,
        reads=COLD_READS,
        declaration=_declaration(),
    )
    conditions = evidence.as_conditions()
    assert conditions['eviction']['definition'] == BUFFER_COLD_DEFINITION
    assert conditions['eviction']['commands'] == EVICTION_COMMAND_BLOCK
    assert conditions['eviction']['stopped_resident_containers'] == [
        'horsies-throughput-pg18'
    ]
    assert 'not observable from SQL' in conditions['host_page_cache']


def test_a_cold_cell_without_io_timing_is_refused() -> None:
    """Without it the cell says nothing at all about the host page cache."""
    with pytest.raises(CachePostureError, match='no I/O read timing'):
        PostureEvidence(
            posture=CachePosture.BUFFER_COLD,
            census=COLD_CENSUS,
            reads=MeasuredReads(shared_hit=0, shared_read=27648, io_time_ms=None),
            declaration=_declaration(),
        )


def test_a_declaration_missing_a_step_is_refused() -> None:
    performed = {step: OPERATOR for step in EVICTION_PROCEDURE}
    del performed[EvictionStep.DROP_HOST_PAGE_CACHE]
    with pytest.raises(CachePostureError, match='drop-host-page-cache'):
        EvictionDeclaration(
            performed_by=performed,
            restart=_restart(),
            stopped_resident_containers=(),
        )


def test_a_step_recorded_as_not_performed_is_refused() -> None:
    """Recording an omission and proceeding would make this paperwork."""
    with pytest.raises(CachePostureError, match='not performed'):
        _declaration(
            performed_by={EvictionStep.DROP_HOST_PAGE_CACHE: NOT_PERFORMED}
        )


@pytest.mark.parametrize(
    'step',
    [
        EvictionStep.STOP_SERVER,
        EvictionStep.DROP_HOST_PAGE_CACHE,
        EvictionStep.START_SERVER,
    ],
)
def test_a_privileged_step_claimed_by_the_harness_is_refused(
    step: EvictionStep,
) -> None:
    """The harness cannot run these, so a claim that it did is false."""
    with pytest.raises(CachePostureError, match='privileges this harness'):
        _declaration(performed_by={step: 'harness'})


def test_a_server_that_never_restarted_is_refused() -> None:
    """The stop and the start are evidenced, not attested."""
    with pytest.raises(CachePostureError, match='did not move'):
        ServerRestart(
            start_time_before='2026-08-08 09:00:00+00',
            start_time_after='2026-08-08 09:00:00+00',
            in_recovery_after=False,
        )


def test_a_server_still_in_recovery_is_refused() -> None:
    with pytest.raises(CachePostureError, match='still in recovery'):
        ServerRestart(
            start_time_before='2026-08-08 09:00:00+00',
            start_time_after='2026-08-08 09:04:00+00',
            in_recovery_after=True,
        )


def test_a_restart_claim_needs_both_times() -> None:
    with pytest.raises(CachePostureError, match='nothing to compare'):
        ServerRestart(
            start_time_before='',
            start_time_after='2026-08-08 09:04:00+00',
            in_recovery_after=False,
        )


def test_the_command_block_covers_every_privileged_step() -> None:
    """One paste, and it is the one the checks were written against."""
    assert 'launchctl stop com.edb.launchd.postgresql-18' in EVICTION_COMMAND_BLOCK
    assert '/usr/sbin/purge' in EVICTION_COMMAND_BLOCK
    assert 'launchctl start com.edb.launchd.postgresql-18' in EVICTION_COMMAND_BLOCK
    assert 'docker stop horsies-throughput-pg18' in EVICTION_COMMAND_BLOCK


def test_the_census_is_read_from_the_row_the_query_returns() -> None:
    census = buffer_census_from_row('t', [6912, 6912])
    assert census.buffers == 6912
    assert census.relation_blocks == 6912


def test_a_census_row_of_the_wrong_shape_is_refused() -> None:
    with pytest.raises(CachePostureError, match='expected 2'):
        buffer_census_from_row('t', [1])


def test_buffer_counters_are_summed_over_the_whole_plan() -> None:
    """A cell reading only the root can miss the scan that did the reading."""
    plan = {
        'Shared Hit Blocks': 1,
        'Shared Read Blocks': 0,
        'Plans': [
            {'Shared Hit Blocks': 10, 'Shared Read Blocks': 200},
            {
                'Shared Hit Blocks': 5,
                'Shared Read Blocks': 50,
                'Plans': [{'Shared Hit Blocks': 0, 'Shared Read Blocks': 7}],
            },
        ],
    }
    reads = measured_reads_from_plan(plan)
    assert reads.shared_hit == 16
    assert reads.shared_read == 257


def test_the_modern_io_timing_key_is_read() -> None:
    """PostgreSQL 17 split the counter; the older name finds nothing now."""
    reads = measured_reads_from_plan(
        {'Shared Read Blocks': 10, 'Shared I/O Read Time': 2.784}
    )
    assert reads.io_time_ms == pytest.approx(2.784)


def test_the_older_io_timing_key_is_still_read() -> None:
    reads = measured_reads_from_plan(
        {'Shared Read Blocks': 10, 'I/O Read Time': 1.5}
    )
    assert reads.io_time_ms == pytest.approx(1.5)


def test_absent_io_timing_is_reported_as_absent_not_as_zero() -> None:
    """Zero would read as a measurement; absent says nothing was measured."""
    reads = measured_reads_from_plan({'Shared Read Blocks': 10})
    assert reads.io_time_ms is None
