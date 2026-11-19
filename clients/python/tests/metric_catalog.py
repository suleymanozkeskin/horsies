"""The metric catalog of the core, read from its committed snapshot.

``core/src/telemetry/catalog_snapshot.txt`` is the text of the catalog: the
core test ``the_catalog_is_its_snapshot`` fails when the two differ. One
block per metric:

    horsies.task.attempts
      prometheus: horsies_task_attempts_total
      kind: counter
      unit: 1
      recorder: worker
      help: ...
      buckets: 0.005 0.01 ...                      (histograms only)
      label task (horsies.task.name): <values>     (zero or more)

A label's values are a quoted list (a closed set), ``declared ...`` (names
of the app), ``the ... of the process`` (one value per process), or a quoted
list followed by ``then declared ...``.

A snapshot that does not parse fails the test that reads it, with the line
and the cause.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from enum import Enum
from pathlib import Path
from typing import Final

import pytest

CATALOG_SNAPSHOT: Final = (
    Path(__file__).resolve().parents[3]
    / 'core'
    / 'src'
    / 'telemetry'
    / 'catalog_snapshot.txt'
)
"""The committed catalog text of the core."""

CATALOG_SNAPSHOT_MAX_BYTES: Final = 1_000_000
"""The largest snapshot that ``read_catalog`` reads. The snapshot of 41
metrics is about 20 kB."""

HISTOGRAM_BUCKET_LABEL: Final = 'le'
"""The label of the upper bound on each ``_bucket`` series of a histogram."""

_QUOTED_VALUE: Final = re.compile(r'"((?:[^"\\]|\\.)*)"')
_LABEL_LINE: Final = re.compile(r'^  label (\S+) \((\S+)\): (.*)$')
_FIELD_LINE: Final = re.compile(
    r'^  (prometheus|kind|unit|recorder|help|buckets): (.*)$'
)
_DECLARED_TAIL: Final = ' then declared '


class MetricKind(Enum):
    """The Prometheus type of a metric."""

    COUNTER = 'counter'
    GAUGE = 'gauge'
    HISTOGRAM = 'histogram'


class Recorder(Enum):
    """The process that records a metric."""

    WORKER = 'worker'
    PRODUCER = 'producer'
    FLEET_COLLECTOR = 'fleet collector'


@dataclass(frozen=True)
class ClosedValues:
    """The label takes only these values."""

    values: frozenset[str]


@dataclass(frozen=True)
class OpenValues:
    """The label also takes names of the app or a fact of the process.
    ``fixed`` holds the values that every app has."""

    fixed: frozenset[str]


type LabelValues = ClosedValues | OpenValues


@dataclass(frozen=True)
class CatalogLabel:
    """One label of a metric."""

    name: str
    """The Prometheus label name."""
    attribute_key: str
    """The OpenTelemetry attribute key."""
    values: LabelValues


@dataclass(frozen=True)
class CatalogMetric:
    """One metric of the catalog."""

    name: str
    """The OpenTelemetry name."""
    prometheus_name: str
    kind: MetricKind
    recorder: Recorder
    labels: tuple[CatalogLabel, ...]

    def series_names(self) -> tuple[str, ...]:
        """The names of the series of the metric in the Prometheus text."""
        match self.kind:
            case MetricKind.COUNTER | MetricKind.GAUGE:
                return (self.prometheus_name,)
            case MetricKind.HISTOGRAM:
                return (
                    f'{self.prometheus_name}_bucket',
                    f'{self.prometheus_name}_sum',
                    f'{self.prometheus_name}_count',
                )

    def label_names(self) -> frozenset[str]:
        """The Prometheus label names of the metric."""
        return frozenset(label.name for label in self.labels)


@dataclass
class _Block:
    """The fields of one metric block, while its lines are read."""

    name: str
    fields: dict[str, str]
    labels: list[CatalogLabel]


def read_catalog(path: Path = CATALOG_SNAPSHOT) -> tuple[CatalogMetric, ...]:
    """The metrics of the snapshot at ``path``, in snapshot order. Fails the
    test when the file is larger than ``CATALOG_SNAPSHOT_MAX_BYTES`` or does
    not parse."""
    with path.open('rb') as snapshot:
        data = snapshot.read(CATALOG_SNAPSHOT_MAX_BYTES + 1)
    if len(data) > CATALOG_SNAPSHOT_MAX_BYTES:
        pytest.fail(f'{path}: larger than {CATALOG_SNAPSHOT_MAX_BYTES} bytes')
    return parse_catalog(data.decode('utf-8'), str(path))


def parse_catalog(text: str, source: str) -> tuple[CatalogMetric, ...]:
    """The metrics of a snapshot text. ``source`` names the text in a
    failure. Fails the test on a line that is not part of the format, on a
    block without a required field, and on a name given twice."""
    blocks: list[_Block] = []
    for number, line in enumerate(text.splitlines(), start=1):
        where = f'{source}:{number}'
        match line:
            case '':
                pytest.fail(f'{where}: an empty line')
            case _ if not line.startswith(' '):
                blocks.append(_Block(name=line, fields={}, labels=[]))
            case _ if not blocks:
                pytest.fail(f'{where}: a field before the first metric name')
            case _:
                _read_field(blocks[-1], line, where)
    metrics = tuple(_metric(block, source) for block in blocks)
    names = [metric.prometheus_name for metric in metrics]
    repeated = sorted({name for name in names if names.count(name) > 1})
    if repeated:
        pytest.fail(f'{source}: Prometheus names given twice: {repeated}')
    return metrics


def _read_field(block: _Block, line: str, where: str) -> None:
    """Add one indented line to ``block``."""
    match (_LABEL_LINE.match(line), _FIELD_LINE.match(line)):
        case (re.Match() as label, _):
            block.labels.append(
                CatalogLabel(
                    name=label.group(1),
                    attribute_key=label.group(2),
                    values=_label_values(label.group(3), where),
                )
            )
        case (None, re.Match() as field):
            if field.group(1) in block.fields:
                pytest.fail(f'{where}: {field.group(1)} given twice for {block.name}')
            block.fields[field.group(1)] = field.group(2)
        case _:
            pytest.fail(f'{where}: not a field or a label line: {line!r}')


def _label_values(text: str, where: str) -> LabelValues:
    """The values of a label line, after its ``:``."""
    match text:
        case _ if text.startswith('declared '):
            return OpenValues(fixed=frozenset())
        case _ if text.startswith('the ') and text.endswith(' of the process'):
            return OpenValues(fixed=frozenset())
        case _ if _DECLARED_TAIL in text:
            fixed, _ = text.split(_DECLARED_TAIL, 1)
            return OpenValues(fixed=_quoted_values(fixed, where))
        case _:
            return ClosedValues(values=_quoted_values(text, where))


def _quoted_values(text: str, where: str) -> frozenset[str]:
    """The values of a list of quoted texts separated by one space."""
    values = _QUOTED_VALUE.findall(text)
    if ' '.join(f'"{value}"' for value in values) != text:
        pytest.fail(f'{where}: not a list of quoted values: {text!r}')
    return frozenset(values)


def _metric(block: _Block, source: str) -> CatalogMetric:
    """The metric of a block whose lines are all read."""
    missing = sorted({'prometheus', 'kind', 'recorder'} - block.fields.keys())
    if missing:
        pytest.fail(f'{source}: {block.name} has no {", ".join(missing)}')
    try:
        kind = MetricKind(block.fields['kind'])
        recorder = Recorder(block.fields['recorder'])
    except ValueError as unknown:
        pytest.fail(f'{source}: {block.name}: {unknown}')
    return CatalogMetric(
        name=block.name,
        prometheus_name=block.fields['prometheus'],
        kind=kind,
        recorder=recorder,
        labels=tuple(block.labels),
    )
