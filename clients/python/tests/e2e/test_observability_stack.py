"""The files of ``deploy/`` against a worker endpoint and the fleet
collector:

- the OpenTelemetry Collector config ``deploy/otel-collector/collector.yaml``
  exports their series through OTLP;
- Prometheus evaluates the alert rules ``deploy/prometheus/horsies-alerts.yaml``
  without error, and runs each query of the dashboard;
- Grafana loads the dashboard ``deploy/grafana/horsies-dashboard.json``.

The stack is ``tests/fixtures/observability/compose.yaml``. The Collector
runs the shipped config with ``collector-test.yaml`` merged over it (a 2 s
scrape interval, and a file exporter in place of the OTLP exporter).
Prometheus scrapes the same two targets every 2 s. The module fixture starts
one worker with ``--metrics-listen`` and one ``horsies metrics`` on the host,
at the static targets of the shipped config.

Runs only with ``HORSIES_OBSERVABILITY_STACK_TEST=1`` and the stack up: the
``observability-stack`` job of ``tests-full.yml``. ``HORSIES_OTLP_OUTPUT_DIR``
is the host folder of the file exporter.
"""

from __future__ import annotations

import json
import os
import signal
import subprocess
import tempfile
import time
import urllib.error
import urllib.parse
import urllib.request
from collections.abc import Generator
from contextlib import contextmanager
from dataclasses import dataclass
from enum import Enum
from pathlib import Path
from typing import Final, cast

import pytest

from horsies.core.types.result import is_err

from tests.e2e.helpers.worker import REPO_ROOT, run_worker
from tests.e2e.tasks import basic as basic_tasks
from tests.metric_catalog import CatalogMetric, MetricKind, read_catalog
from tests.observability_deploy import (
    alert_rules,
    dashboard,
    dashboard_queries,
    mapping,
    panels,
    sequence,
    text,
    variable_queries,
)

STACK_TEST_ENV: Final = 'HORSIES_OBSERVABILITY_STACK_TEST'
OTLP_OUTPUT_DIR_ENV: Final = 'HORSIES_OTLP_OUTPUT_DIR'
OTLP_OUTPUT_FILE: Final = 'metrics.jsonl'
"""The file of the file exporter in ``collector-test.yaml``."""

if os.environ.get(STACK_TEST_ENV) != '1':
    pytest.skip(
        f'the observability stack test needs {STACK_TEST_ENV}=1 and the stack '
        'of tests/fixtures/observability/compose.yaml',
        allow_module_level=True,
    )

pytestmark = [pytest.mark.e2e]

DEFAULT_INSTANCE: Final = 'tests.e2e.tasks.instance:app'

WORKER_METRICS_ADDRESS: Final = '127.0.0.1:9464'
"""The static target of the ``horsies-worker`` job of the shipped config."""
FLEET_METRICS_ADDRESS: Final = '127.0.0.1:9465'
"""The static target of the ``horsies-fleet`` job of the shipped config."""
WORKER_JOB: Final = 'horsies-worker'
FLEET_JOB: Final = 'horsies-fleet'

WORKER_SERIES: Final = (
    'horsies_task_attempts_total',
    'horsies_task_run_duration_seconds',
    'horsies_worker_children_started_total',
    'horsies_worker_claim_passes_total',
)
"""Worker series that exist once the worker has run one task."""
FLEET_SERIES: Final = (
    'horsies_fleet_workers',
    'horsies_fleet_seats',
    'horsies_workflows',
    'horsies_schema_version',
    'horsies_exporter_collect_failed',
    'horsies_exporter_collect_duration_seconds',
)
"""Fleet series that exist after the first read of the fleet collector."""
ATTEMPT_ATTRIBUTE_KEYS: Final = frozenset({'task', 'queue', 'execution', 'outcome'})
"""The attributes of every point of ``horsies_task_attempts_total``. An empty
``error_code`` (a ``COMPLETED`` attempt) is no Prometheus label, so it is no
attribute either."""

SERVICE_NAME_ATTRIBUTE: Final = 'service.name'
"""The resource attribute that the receiver sets from the scrape job."""
CUMULATIVE_TEMPORALITY: Final = 2
"""``AGGREGATION_TEMPORALITY_CUMULATIVE``: OTLP JSON writes enums as integers."""

WORKER_READY_TIMEOUT_S: Final = 30.0
"""Seconds for the worker to run the first healthcheck task."""
ENDPOINT_READY_TIMEOUT_S: Final = 60.0
ENDPOINT_REQUEST_TIMEOUT_S: Final = 2.0
OTLP_ARRIVAL_TIMEOUT_S: Final = 90.0
"""Many scrape intervals of 2 s, and the first read of the fleet collector."""
POLL_INTERVAL_S: Final = 1.0
PROCESS_STOP_TIMEOUT_S: Final = 10.0
OTLP_OUTPUT_MAX_BYTES: Final = 256 * 1024 * 1024
"""The largest OTLP file that the test reads. A 2 s scrape of both targets
writes a few kB per export."""
LOG_TAIL_BYTES: Final = 8_192

PROMETHEUS_URL: Final = 'http://127.0.0.1:9090'
GRAFANA_URL: Final = 'http://127.0.0.1:3000'
DASHBOARD_UID: Final = 'horsies-overview'
STACK_ANSWER_TIMEOUT_S: Final = 90.0
"""Seconds for Prometheus to scrape and evaluate, and for Grafana to load the
dashboard."""
HTTP_BODY_MAX_BYTES: Final = 16 * 1024 * 1024
GRAFANA_VARIABLE_VALUES: Final = {
    '$__rate_interval': '1m',
    '$queue': '.*',
    '$task': '.*',
}
"""The value of each Grafana variable of the dashboard queries in a query to
Prometheus: every queue and task."""


class OtlpPoints(Enum):
    """The OTLP data type of a metric."""

    GAUGE = 'gauge'
    MONOTONIC_CUMULATIVE_SUM = 'monotonic cumulative sum'
    HISTOGRAM = 'histogram'


@dataclass(frozen=True)
class OtherPoints:
    """An OTLP data type that no catalog kind maps to."""

    description: str


@dataclass(frozen=True)
class ArrivedMetric:
    """One metric of one OTLP export."""

    job: str
    name: str
    points: OtlpPoints | OtherPoints
    attribute_keys: tuple[frozenset[str], ...]
    """The attribute keys of each data point."""


def _expected_points(metric: CatalogMetric) -> OtlpPoints:
    """The OTLP data type of a catalog metric that the receiver scraped."""
    match metric.kind:
        case MetricKind.COUNTER:
            return OtlpPoints.MONOTONIC_CUMULATIVE_SUM
        case MetricKind.GAUGE:
            return OtlpPoints.GAUGE
        case MetricKind.HISTOGRAM:
            return OtlpPoints.HISTOGRAM


def _objects(value: object, where: str) -> list[dict[str, object]]:
    """``value`` as a list of JSON objects; fails the test otherwise."""
    if not isinstance(value, list):
        pytest.fail(f'{where}: not a JSON list: {value!r}')
    objects: list[dict[str, object]] = []
    for item in cast(list[object], value):
        if not isinstance(item, dict):
            pytest.fail(f'{where}: not a JSON object: {item!r}')
        entries = cast(dict[object, object], item)
        objects.append({str(key): entry for key, entry in entries.items()})
    return objects


def _object(value: object, where: str) -> dict[str, object]:
    (single,) = _objects([value], where)
    return single


def _attribute_keys(point: dict[str, object], where: str) -> frozenset[str]:
    return frozenset(
        str(attribute.get('key'))
        for attribute in _objects(point.get('attributes', []), f'{where}.attributes')
    )


def _service_name(resource_metrics: dict[str, object], where: str) -> str:
    resource = _object(resource_metrics.get('resource', {}), f'{where}.resource')
    for attribute in _objects(
        resource.get('attributes', []), f'{where}.resource.attributes'
    ):
        if attribute.get('key') == SERVICE_NAME_ATTRIBUTE:
            value = _object(
                attribute.get('value'), f'{where}.resource.{SERVICE_NAME_ATTRIBUTE}'
            )
            return str(value.get('stringValue'))
    pytest.fail(f'{where}: no {SERVICE_NAME_ATTRIBUTE} resource attribute')


def _sum_points(total: dict[str, object]) -> OtlpPoints | OtherPoints:
    """The data type of an OTLP sum: a Prometheus counter is monotonic and
    cumulative."""
    monotonic = total.get('isMonotonic')
    temporality = total.get('aggregationTemporality')
    match monotonic is True and temporality == CUMULATIVE_TEMPORALITY:
        case True:
            return OtlpPoints.MONOTONIC_CUMULATIVE_SUM
        case False:
            return OtherPoints(
                f'sum, isMonotonic={monotonic}, aggregationTemporality={temporality}'
            )


def _points(
    metric: dict[str, object],
    where: str,
) -> tuple[OtlpPoints | OtherPoints, list[dict[str, object]]]:
    """The data type of an OTLP metric, and its data points."""
    match metric:
        case {'gauge': data}:
            gauge = _object(data, f'{where}.gauge')
            return OtlpPoints.GAUGE, _objects(gauge.get('dataPoints', []), where)
        case {'sum': data}:
            total = _object(data, f'{where}.sum')
            return _sum_points(total), _objects(total.get('dataPoints', []), where)
        case {'histogram': data}:
            histogram = _object(data, f'{where}.histogram')
            return OtlpPoints.HISTOGRAM, _objects(
                histogram.get('dataPoints', []), where
            )
        case _:
            return OtherPoints(f'keys {sorted(metric)}'), []


def _export_metrics(export: object, where: str) -> list[ArrivedMetric]:
    """The horsies metrics of one OTLP export (one line of the file)."""
    arrived: list[ArrivedMetric] = []
    for resource_metrics in _objects(
        _object(export, where).get('resourceMetrics', []), where
    ):
        job = _service_name(resource_metrics, where)
        for scope_metrics in _objects(resource_metrics.get('scopeMetrics', []), where):
            for metric in _objects(scope_metrics.get('metrics', []), where):
                name = str(metric.get('name'))
                if not name.startswith('horsies_'):
                    continue
                points, data_points = _points(metric, f'{where}:{name}')
                arrived.append(
                    ArrivedMetric(
                        job=job,
                        name=name,
                        points=points,
                        attribute_keys=tuple(
                            _attribute_keys(point, f'{where}:{name}')
                            for point in data_points
                        ),
                    )
                )
    return arrived


def _output_size(path: Path) -> int:
    """The bytes in the OTLP file now: 0 before the first export."""
    match path.exists():
        case True:
            return path.stat().st_size
        case False:
            return 0


def _read_arrived(path: Path, start: int) -> dict[tuple[str, str], ArrivedMetric]:
    """The last export of each (job, metric) in the lines that begin at or
    after byte ``start`` of the file, so the exports of an earlier run do not
    count. A line that the exporter is still writing (no line feed yet) is
    left for the next read."""
    if not path.exists():
        return {}
    with path.open('rb') as output:
        _ = output.seek(max(0, start - 1))
        data = output.read(OTLP_OUTPUT_MAX_BYTES + 1)
    if len(data) > OTLP_OUTPUT_MAX_BYTES:
        pytest.fail(
            f'{path}: more than {OTLP_OUTPUT_MAX_BYTES} bytes after byte {start}'
        )
    if start > 0:
        # Drop the end of the line that holds byte start - 1.
        _, _, data = data.partition(b'\n')
    complete_lines = data.split(b'\n')[:-1]
    arrived: dict[tuple[str, str], ArrivedMetric] = {}
    for number, line in enumerate(complete_lines, start=1):
        try:
            export: object = json.loads(line)
        except json.JSONDecodeError as cause:
            pytest.fail(f'{path}:{number}: not JSON: {cause}')
        for metric in _export_metrics(export, f'{path}:{number}'):
            arrived[(metric.job, metric.name)] = metric
    return arrived


def _wait_for_arrival(
    path: Path,
    start: int,
    expected: frozenset[tuple[str, str]],
) -> dict[tuple[str, str], ArrivedMetric]:
    """Read the file from byte ``start`` until every expected (job, metric)
    arrived. Fails the test after ``OTLP_ARRIVAL_TIMEOUT_S`` with the missing
    ones and the ones that arrived."""
    deadline = time.monotonic() + OTLP_ARRIVAL_TIMEOUT_S
    arrived = _read_arrived(path, start)
    while not expected <= arrived.keys():
        if time.monotonic() >= deadline:
            missing = sorted(expected - arrived.keys())
            pytest.fail(
                f'no OTLP export of {missing} within {OTLP_ARRIVAL_TIMEOUT_S} s; '
                f'arrived: {sorted(arrived)}'
            )
        time.sleep(POLL_INTERVAL_S)
        arrived = _read_arrived(path, start)
    return arrived


@dataclass(frozen=True)
class HttpAnswer:
    """The status and the body of an HTTP answer."""

    status: int
    body: bytes


@dataclass(frozen=True)
class HttpUnreachable:
    """No HTTP answer: the connection failed or timed out."""

    cause: str


def _http_get(url: str) -> HttpAnswer | HttpUnreachable:
    """GET ``url``; the body is read up to ``HTTP_BODY_MAX_BYTES``."""
    try:
        with urllib.request.urlopen(
            url, timeout=ENDPOINT_REQUEST_TIMEOUT_S
        ) as response:
            return HttpAnswer(
                status=response.status, body=response.read(HTTP_BODY_MAX_BYTES)
            )
    except urllib.error.HTTPError as refused:
        return HttpAnswer(status=refused.code, body=refused.read(HTTP_BODY_MAX_BYTES))
    except (urllib.error.URLError, ConnectionError, TimeoutError) as failure:
        return HttpUnreachable(cause=repr(failure))


def _wait_for_endpoint(
    url: str, process: subprocess.Popen[str] | subprocess.Popen[bytes]
) -> None:
    """Poll ``url`` until it answers 200. Fails the test when ``process``
    exits first or after ``ENDPOINT_READY_TIMEOUT_S``."""
    deadline = time.monotonic() + ENDPOINT_READY_TIMEOUT_S
    last: HttpAnswer | HttpUnreachable = HttpUnreachable(cause='no request sent')
    while time.monotonic() < deadline:
        if process.poll() is not None:
            pytest.fail(f'the process of {url} exited with {process.returncode}')
        last = _http_get(url)
        match last:
            case HttpAnswer(status=200):
                return
            case HttpAnswer() | HttpUnreachable():
                time.sleep(POLL_INTERVAL_S)
    pytest.fail(f'{url} did not answer 200 within {ENDPOINT_READY_TIMEOUT_S} s: {last}')


@dataclass(frozen=True)
class PrometheusData:
    """The ``data`` of a Prometheus API answer with status ``success``."""

    data: object


def _prometheus_data(
    path: str, query: dict[str, str]
) -> PrometheusData | HttpAnswer | HttpUnreachable:
    """The ``data`` of a Prometheus API answer with status ``success``, or
    the answer that is not one."""
    answer = _http_get(f'{PROMETHEUS_URL}{path}?{urllib.parse.urlencode(query)}')
    match answer:
        case HttpAnswer(status=200, body=body):
            document = mapping(json.loads(body), path)
            match document.get('status'):
                case 'success':
                    return PrometheusData(data=document.get('data'))
                case _:
                    return answer
        case HttpAnswer() | HttpUnreachable():
            return answer


def _prometheus_series_count(selector: str) -> int:
    """The number of series that an instant query of ``selector`` returns
    now; 0 while Prometheus does not answer."""
    match _prometheus_data('/api/v1/query', {'query': selector}):
        case HttpAnswer() | HttpUnreachable():
            return 0
        case PrometheusData(data=data):
            return len(sequence(mapping(data, selector).get('result'), selector))


def _for_prometheus(expression: str, where: str) -> str:
    """``expression`` with each Grafana variable replaced by its value in
    ``GRAFANA_VARIABLE_VALUES``. Fails the test on another variable."""
    for variable, value in GRAFANA_VARIABLE_VALUES.items():
        expression = expression.replace(variable, value)
    if '$' in expression:
        pytest.fail(f'{where}: a Grafana variable without a test value: {expression!r}')
    return expression


def _log_tail(path: str) -> str:
    with open(path, 'rb') as log:
        log.seek(0, os.SEEK_END)
        log.seek(max(0, log.tell() - LOG_TAIL_BYTES))
        return log.read().decode('utf-8', 'replace')


def _stop_process_group(process: subprocess.Popen[bytes]) -> None:
    """SIGTERM to the group, then SIGKILL after ``PROCESS_STOP_TIMEOUT_S``."""
    if process.poll() is not None:
        return
    os.killpg(process.pid, signal.SIGTERM)
    try:
        _ = process.wait(timeout=PROCESS_STOP_TIMEOUT_S)
    except subprocess.TimeoutExpired:
        os.killpg(process.pid, signal.SIGKILL)
        _ = process.wait(timeout=PROCESS_STOP_TIMEOUT_S)


@contextmanager
def _run_fleet_metrics(
    instance_path: str, listen: str
) -> Generator[subprocess.Popen[bytes], None, None]:
    """``horsies metrics`` of the instance on ``listen``, with its output in
    a file; stopped and its log tail printed on exit."""
    log_handle, log_path = tempfile.mkstemp(
        prefix='horsies-e2e-fleet-metrics-', suffix='.log'
    )
    process = subprocess.Popen(
        [
            'uv',
            'run',
            'horsies',
            'metrics',
            instance_path,
            f'--listen={listen}',
            '--loglevel=warning',
        ],
        stdout=log_handle,
        stderr=subprocess.STDOUT,
        start_new_session=True,
        env={**os.environ, 'PYTHONPATH': REPO_ROOT},
    )
    os.close(log_handle)
    try:
        yield process
    finally:
        try:
            _stop_process_group(process)
        finally:
            print(
                f'--- horsies metrics {process.pid} log tail ---\n{_log_tail(log_path)}'
            )
            os.unlink(log_path)


def _healthcheck_completes() -> bool:
    sent = basic_tasks.healthcheck.send()
    if is_err(sent):
        return False
    return sent.ok_value.get(timeout_ms=2000).is_ok()


def _otlp_output_path() -> Path:
    folder = os.environ.get(OTLP_OUTPUT_DIR_ENV)
    if not folder:
        pytest.fail(f'{OTLP_OUTPUT_DIR_ENV} names no folder')
    return Path(folder) / OTLP_OUTPUT_FILE


@dataclass(frozen=True)
class OtlpOutput:
    """The OTLP file, and its size before the processes of this module
    started."""

    path: Path
    start: int


@pytest.fixture(scope='module')
def scraped_processes() -> Generator[OtlpOutput, None, None]:
    """One worker with ``--metrics-listen`` and one ``horsies metrics``, both
    serving ``/metrics``, for every test of this module."""
    path = _otlp_output_path()
    output = OtlpOutput(path=path, start=_output_size(path))
    with run_worker(
        DEFAULT_INSTANCE,
        profile='isolated-1',
        timeout=WORKER_READY_TIMEOUT_S,
        ready_check=_healthcheck_completes,
        extra_arguments=(f'--metrics-listen={WORKER_METRICS_ADDRESS}',),
    ) as worker:
        _wait_for_endpoint(f'http://{WORKER_METRICS_ADDRESS}/metrics', worker)
        with _run_fleet_metrics(
            DEFAULT_INSTANCE, FLEET_METRICS_ADDRESS
        ) as fleet_metrics:
            _wait_for_endpoint(f'http://{FLEET_METRICS_ADDRESS}/metrics', fleet_metrics)
            yield output


def test_the_collector_exports_the_worker_and_fleet_series_through_otlp(
    scraped_processes: OtlpOutput,
) -> None:
    catalog = {metric.prometheus_name: metric for metric in read_catalog()}
    assert set(WORKER_SERIES + FLEET_SERIES) <= catalog.keys()
    expected = frozenset(
        [(WORKER_JOB, name) for name in WORKER_SERIES]
        + [(FLEET_JOB, name) for name in FLEET_SERIES]
    )

    arrived = _wait_for_arrival(
        scraped_processes.path, scraped_processes.start, expected
    )

    data_types = {key: arrived[key].points for key in sorted(expected)}
    assert data_types == {
        key: _expected_points(catalog[key[1]]) for key in sorted(expected)
    }
    attempts = arrived[(WORKER_JOB, 'horsies_task_attempts_total')]
    assert attempts.attribute_keys
    assert all(ATTEMPT_ATTRIBUTE_KEYS <= keys for keys in attempts.attribute_keys), (
        attempts.attribute_keys
    )


def test_prometheus_scrapes_the_worker_and_the_fleet_collector(
    scraped_processes: OtlpOutput,
) -> None:
    selectors = (
        f'horsies_task_attempts_total{{job="{WORKER_JOB}"}}',
        f'horsies_fleet_workers{{job="{FLEET_JOB}"}}',
    )
    deadline = time.monotonic() + STACK_ANSWER_TIMEOUT_S
    counts = {selector: _prometheus_series_count(selector) for selector in selectors}
    while 0 in counts.values() and time.monotonic() < deadline:
        time.sleep(POLL_INTERVAL_S)
        counts = {
            selector: _prometheus_series_count(selector) for selector in selectors
        }

    assert 0 not in counts.values(), counts


def _rule_health() -> dict[str, tuple[str, str]]:
    """The health and the last error of each alerting rule that Prometheus
    loaded; empty while Prometheus does not answer."""
    match _prometheus_data('/api/v1/rules', {'type': 'alert'}):
        case HttpAnswer() | HttpUnreachable():
            return {}
        case PrometheusData(data=data):
            pass
    health: dict[str, tuple[str, str]] = {}
    for group in sequence(mapping(data, 'rules').get('groups'), 'rules.groups'):
        for value in sequence(
            mapping(group, 'rules group').get('rules'), 'rules group rules'
        ):
            rule = mapping(value, 'rule')
            health[text(rule.get('name'), 'rule name')] = (
                text(rule.get('health'), 'rule health'),
                text(rule.get('lastError', ''), 'rule lastError'),
            )
    return health


def test_prometheus_evaluates_every_alert_rule_without_error(
    scraped_processes: OtlpOutput,
) -> None:
    names = {rule.name for rule in alert_rules()}
    deadline = time.monotonic() + STACK_ANSWER_TIMEOUT_S
    health = _rule_health()
    while (
        health.keys() != names
        or any(state == 'unknown' for state, _ in health.values())
    ) and time.monotonic() < deadline:
        time.sleep(POLL_INTERVAL_S)
        health = _rule_health()

    assert health == {name: ('ok', '') for name in names}


def test_prometheus_runs_every_dashboard_query(scraped_processes: OtlpOutput) -> None:
    expressions = [
        (query.where, _for_prometheus(query.expression, query.where))
        for query in dashboard_queries()
    ] + [
        (variable.where, _for_prometheus(variable.selector, variable.where))
        for variable in variable_queries()
    ]

    refused: dict[str, HttpAnswer | HttpUnreachable] = {}
    for where, expression in expressions:
        match _prometheus_data('/api/v1/query', {'query': expression}):
            case PrometheusData():
                pass
            case HttpAnswer() | HttpUnreachable() as answer:
                refused[where] = answer

    assert refused == {}


def test_grafana_loads_the_provisioned_dashboard() -> None:
    url = f'{GRAFANA_URL}/api/dashboards/uid/{DASHBOARD_UID}'
    deadline = time.monotonic() + STACK_ANSWER_TIMEOUT_S
    answer = _http_get(url)
    while not (isinstance(answer, HttpAnswer) and answer.status == 200):
        if time.monotonic() >= deadline:
            pytest.fail(
                f'{url} did not answer 200 within {STACK_ANSWER_TIMEOUT_S} s: {answer}'
            )
        time.sleep(POLL_INTERVAL_S)
        answer = _http_get(url)

    loaded = mapping(json.loads(answer.body), url)
    board = mapping(loaded.get('dashboard'), f'{url} dashboard')
    meta = mapping(loaded.get('meta'), f'{url} meta')
    shipped = dashboard()
    assert board.get('schemaVersion') == shipped.get('schemaVersion')
    assert board.get('title') == shipped.get('title')
    assert len(panels(board)) == len(panels(shipped))
    assert meta.get('provisioned') is True
