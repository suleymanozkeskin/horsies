"""The reader of the core's metric catalog snapshot (``tests/metric_catalog.py``)."""

from __future__ import annotations

import pytest

from tests.metric_catalog import (
    CatalogLabel,
    ClosedValues,
    MetricKind,
    OpenValues,
    Recorder,
    parse_catalog,
    read_catalog,
)

_TWO_METRICS = """horsies.task.attempts
  prometheus: horsies_task_attempts_total
  kind: counter
  unit: 1
  recorder: worker
  help: Attempts.
  label task (horsies.task.name): declared task names, fallback "(unregistered)"
  label outcome (horsies.task.outcome): "COMPLETED" "FAILED" "WORKER_FAILURE"
  label error_code (error.type): "" "TASK_TIMEOUT" then declared user error codes, fallback "undeclared"
horsies.task.run.duration
  prometheus: horsies_task_run_duration_seconds
  kind: histogram
  unit: s
  recorder: worker
  help: Run time.
  buckets: 0.005 1
  label version (horsies.version): the version of the process
"""


def _failure(text: str) -> str:
    with pytest.raises(pytest.fail.Exception) as failed:
        parse_catalog(text, 'snapshot')
    return str(failed.value)


def test_a_block_gives_its_names_kind_recorder_and_labels_in_order() -> None:
    attempts, _ = parse_catalog(_TWO_METRICS, 'snapshot')

    assert attempts.name == 'horsies.task.attempts'
    assert attempts.prometheus_name == 'horsies_task_attempts_total'
    assert attempts.kind is MetricKind.COUNTER
    assert attempts.recorder is Recorder.WORKER
    assert [label.name for label in attempts.labels] == [
        'task',
        'outcome',
        'error_code',
    ]
    assert attempts.labels[1].attribute_key == 'horsies.task.outcome'


def test_a_quoted_list_is_a_closed_set() -> None:
    attempts, _ = parse_catalog(_TWO_METRICS, 'snapshot')

    assert attempts.labels[1] == CatalogLabel(
        name='outcome',
        attribute_key='horsies.task.outcome',
        values=ClosedValues(
            values=frozenset({'COMPLETED', 'FAILED', 'WORKER_FAILURE'})
        ),
    )


def test_declared_names_are_open() -> None:
    attempts, _ = parse_catalog(_TWO_METRICS, 'snapshot')

    assert attempts.labels[0].values == OpenValues(fixed=frozenset())


def test_a_quoted_list_then_declared_names_is_open_with_its_fixed_values() -> None:
    attempts, _ = parse_catalog(_TWO_METRICS, 'snapshot')

    assert attempts.labels[2].values == OpenValues(
        fixed=frozenset({'', 'TASK_TIMEOUT'})
    )


def test_a_fact_of_the_process_is_open() -> None:
    _, duration = parse_catalog(_TWO_METRICS, 'snapshot')

    assert duration.labels[0].values == OpenValues(fixed=frozenset())


def test_a_counter_has_one_series_name() -> None:
    attempts, _ = parse_catalog(_TWO_METRICS, 'snapshot')

    assert attempts.series_names() == ('horsies_task_attempts_total',)


def test_a_histogram_has_its_bucket_sum_and_count_series() -> None:
    _, duration = parse_catalog(_TWO_METRICS, 'snapshot')

    assert duration.series_names() == (
        'horsies_task_run_duration_seconds_bucket',
        'horsies_task_run_duration_seconds_sum',
        'horsies_task_run_duration_seconds_count',
    )


def test_an_empty_line_fails_with_its_line_number() -> None:
    assert 'snapshot:2: an empty line' in _failure('horsies.schema.version\n\n')


def test_a_field_before_the_first_name_fails() -> None:
    assert 'a field before the first metric name' in _failure('  kind: gauge\n')


def test_a_line_that_is_not_a_field_fails() -> None:
    assert 'not a field or a label line' in _failure(
        'horsies.schema.version\n  colour: blue\n'
    )


def test_a_field_given_twice_fails() -> None:
    text = 'horsies.schema.version\n  kind: gauge\n  kind: counter\n'

    assert 'kind given twice for horsies.schema.version' in _failure(text)


def test_a_value_list_that_is_not_quoted_fails() -> None:
    text = 'horsies.workflows\n  label status (horsies.status): RUNNING PAUSED\n'

    assert 'not a list of quoted values' in _failure(text)


def test_a_block_without_a_required_field_fails() -> None:
    text = 'horsies.schema.version\n  prometheus: horsies_schema_version\n'

    assert 'horsies.schema.version has no kind, recorder' in _failure(text)


def test_an_unknown_kind_fails() -> None:
    text = (
        'horsies.schema.version\n  prometheus: horsies_schema_version\n'
        '  kind: summary\n  recorder: worker\n'
    )

    assert "'summary' is not a valid MetricKind" in _failure(text)


def test_a_prometheus_name_given_twice_fails() -> None:
    block = '  prometheus: horsies_schema_version\n  kind: gauge\n  recorder: fleet collector\n'
    text = f'horsies.schema.version\n{block}horsies.schema.version.copy\n{block}'

    assert "Prometheus names given twice: ['horsies_schema_version']" in _failure(text)


def test_the_committed_snapshot_parses_and_names_every_metric_once() -> None:
    metrics = read_catalog()

    assert metrics
    assert all(metric.prometheus_name.startswith('horsies_') for metric in metrics)
    assert len({metric.name for metric in metrics}) == len(metrics)
