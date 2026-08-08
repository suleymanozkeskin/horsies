"""Seeding both sides of a paired cell from one body, and proving they match.

Three things that a paired cell cannot be measured without, kept in one module
because separating them is the seam they drift through: the configuration each
side runs, the seed that fills each side's database, and the facts that say the
two sides were fed the same work.

**The seed is a source string, not an import.** A baseline that imports a
helper from this checkout is the shadowing hazard in
:mod:`paired_sides` — the import succeeds, the numbers look reasonable, and the
comparison is the candidate against itself. The body below is therefore text,
executed by each side's own interpreter, exactly as ``SIDE_IDENTITY_SNIPPET``
is text for the same reason.

**One body, not two.** Two hand-maintained seed paths drift, and the drift
lives inside the equivalence the cell is supposed to be proving. The two builds
expose the same app class, the same ``@app.task`` decorator, the same
``.send()`` and the same ``Result`` surface, which is what makes one body
valid for both.

**The facts describe what was sent, not what is stored.** The two sides run
different schema versions, so their stored rows are expected to differ — that
difference is the product difference. Every fact here is accumulated by the
seed loop as it sends, so it describes the work fed in, which is the boundary
where equivalence is required.

**Payloads are incompressible by construction.** A blob of one repeated
character is stored by PostgreSQL at a fraction of its nominal size, so a cell
claiming a 1 MiB payload would measure compression rather than payload. The
bytes are deterministic base64 of a seeded PRNG: same bytes on both sides,
reproducible from the spec alone, and not compressible into something smaller
than what was declared.
"""

from __future__ import annotations

import json
import re
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Final

from .paired_sides import (
    PairedSide,
    SideIdentity,
    assert_side_identity,
    measurement_environment,
    run_side,
    side_identity_from_output,
)
from .paired_cell import EquivalenceFacts

SIDE_CONFIG_MARKER: Final = '__horsies_side_config__'
SIDE_FACTS_MARKER: Final = '__horsies_side_facts__'

# Excluded from the cross-side configuration comparison because they are the
# one thing the two sides are required to differ in. Everything else that
# differs is a finding.
DSN_CONFIG_KEYS: Final = frozenset(
    {'broker.database_url', 'broker.session_database_url'}
)

# What a defaults map says about a field with no shipped default. A field that
# must be supplied has no default to be "at", so an extra key carrying this
# cannot be cleared and the cell is refused.
REQUIRED_FIELD_SENTINEL: Final = '__required__'

# What a redacted secret dumps as. Recorded so a reader can see that the
# comparison could not have compared DSNs even if they had been left in.
SECRET_SENTINEL: Final = '__secret__'


class SeedError(Exception):
    """A side could not be seeded, or did not report what it seeded."""


class ConfigEquivalenceError(Exception):
    """The two sides ran configurations that differ in more than the database."""


@dataclass(frozen=True, slots=True)
class SeedBucket:
    """One payload size class, and how many rows of it to send.

    ``status`` is the status the send is declared to produce. It is validated
    against each side's own ``TaskStatus`` inside that side's interpreter, so a
    status that exists on one build and not the other fails on the build that
    lacks it rather than being silently recorded as text.
    """

    payload_bytes: int
    count: int
    status: str = 'PENDING'

    def __post_init__(self) -> None:
        if self.payload_bytes < 1:
            raise SeedError(
                f'bucket payload_bytes must be at least 1, got '
                f'{self.payload_bytes}; a zero-length payload has no size '
                'class and would collapse the histogram it is meant to shape'
            )
        if self.count < 1:
            raise SeedError(
                f'bucket count must be at least 1, got {self.count}'
            )

    def as_payload(self) -> dict[str, Any]:
        return {
            'payload_bytes': self.payload_bytes,
            'count': self.count,
            'status': self.status,
        }


@dataclass(frozen=True, slots=True)
class SeedSpec:
    """The work both sides are fed, in full.

    Two sides given this same spec send byte-identical payloads in the same
    order, because ``payload_seed`` fixes the generator and the bucket order
    fixes the sequence.
    """

    task_name: str
    buckets: tuple[SeedBucket, ...]
    payload_seed: int

    def __post_init__(self) -> None:
        if not self.buckets:
            raise SeedError('a seed spec needs at least one bucket')

    def as_payload(self) -> dict[str, Any]:
        return {
            'task_name': self.task_name,
            'buckets': [bucket.as_payload() for bucket in self.buckets],
            'payload_seed': self.payload_seed,
        }

    def expected_rows(self) -> int:
        return sum(bucket.count for bucket in self.buckets)


@dataclass(frozen=True, slots=True)
class SeedConfigSpec:
    """The knobs pinned identically on both sides.

    Deliberately small. Every value pinned here is a value that cannot differ
    by accident; every value not pinned here is compared after the fact through
    each side's effective dump, so a default that moved between builds is
    caught rather than assumed stable.
    """

    pool_size: int = 10
    max_overflow: int = 0
    prefetch_buffer: int = 0
    claim_lease_ms: int | None = None

    def as_payload(self) -> dict[str, Any]:
        return {
            'pool_size': self.pool_size,
            'max_overflow': self.max_overflow,
            'prefetch_buffer': self.prefetch_buffer,
            'claim_lease_ms': self.claim_lease_ms,
        }


@dataclass(frozen=True, slots=True)
class SideConfig:
    """One side's configuration as that side's own interpreter reports it.

    ``effective`` is what the build actually ran; ``defaults`` is what that
    build ships. Both are flattened to dotted paths so a difference names the
    field it is in.
    """

    side: PairedSide
    effective: Mapping[str, Any]
    defaults: Mapping[str, Any]

    def as_conditions(self) -> dict[str, Any]:
        return {
            'effective': dict(self.effective),
            'defaults': dict(self.defaults),
        }


@dataclass(frozen=True, slots=True)
class SeedOutcome:
    """Everything one seeded side reported about itself.

    The three parts must agree on which side they describe. They are produced
    together from one run, so disagreement means they were assembled by hand
    from different runs — and a bundle whose identity says one build while its
    facts describe the other would pass every downstream check that reads only
    one of them.
    """

    identity: SideIdentity
    config: SideConfig
    facts: EquivalenceFacts

    def __post_init__(self) -> None:
        sides = {
            'identity': self.identity.side,
            'configuration': self.config.side,
            'facts': self.facts.side,
        }
        if len(set(sides.values())) != 1:
            raise SeedError(
                f'one side outcome describes more than one side: {sides}. Its '
                'parts came from different runs, so no downstream check can '
                'say which build it is evidence about'
            )

    @property
    def side(self) -> PairedSide:
        return self.identity.side


# Executed by each side's own interpreter. Written as text, and injected into
# only through JSON literals, so nothing in this checkout is importable from
# it. The leading underscores keep its names clear of anything the product
# exposes.
CONFIG_SOURCE_SNIPPET: Final = '''
import base64 as _b64, json as _json, random as _random, time as _time
from pydantic import SecretStr as _SecretStr
from horsies import TaskError as _TaskError, TaskResult as _TaskResult
from horsies import TaskStatus as _TaskStatus
from horsies.core.app import Horsies as _Horsies
from horsies.core.models.app import AppConfig as _AppConfig
from horsies.core.models.broker import PostgresConfig as _PostgresConfig
from horsies.core.models.queues import QueueMode as _QueueMode

_config_spec = _json.loads(__CONFIG_JSON__)
_dsn = _json.loads(__DSN_JSON__)

_app_config = _AppConfig(
    queue_mode=_QueueMode.DEFAULT,
    cluster_wide_cap=None,
    prefetch_buffer=_config_spec['prefetch_buffer'],
    claim_lease_ms=_config_spec['claim_lease_ms'],
    broker=_PostgresConfig(
        database_url=_SecretStr(_dsn),
        pool_size=_config_spec['pool_size'],
        max_overflow=_config_spec['max_overflow'],
    ),
)


def _leaf(_value):
    if isinstance(_value, _SecretStr):
        return __SECRET_SENTINEL__
    if isinstance(_value, (str, int, float, bool)) or _value is None:
        return _value
    if isinstance(_value, (list, tuple)):
        return [_leaf(_item) for _item in _value]
    if hasattr(_value, 'value'):
        return _leaf(_value.value)
    return repr(_value)


def _flatten(_prefix, _value, _out):
    if isinstance(_value, dict) and _value:
        for _key, _child in _value.items():
            _flatten(_prefix + (str(_key),), _child, _out)
        return
    _out['.'.join(_prefix)] = _leaf(_value)


def _walk_defaults(_model, _prefix, _out):
    for _name, _field in type(_model).model_fields.items():
        _value = getattr(_model, _name)
        _path = _prefix + (_name,)
        if hasattr(type(_value), 'model_fields'):
            _walk_defaults(_value, _path, _out)
            continue
        if _field.is_required():
            _out['.'.join(_path)] = __REQUIRED_SENTINEL__
            continue
        if _field.default_factory is not None:
            # A field built by a factory reports `default` as pydantic's
            # undefined marker, so reading `.default` alone records the marker
            # as though it were the shipped value and every such field looks
            # non-default. The factory IS the default; call it.
            try:
                _out['.'.join(_path)] = _leaf(_field.default_factory())
            except TypeError:
                # A factory taking validated data cannot be evaluated here.
                # Leaving the key absent makes the comparison refuse rather
                # than compare against a value this harness invented.
                pass
            continue
        _out['.'.join(_path)] = _leaf(_field.default)


_effective = {}
_flatten((), _app_config.model_dump(mode='json'), _effective)
_defaults = {}
_walk_defaults(_app_config, (), _defaults)
print(
    __CONFIG_MARKER__ + ' '
    + _json.dumps({'effective': _effective, 'defaults': _defaults}),
    flush=True,
)
'''

# Everything after the shared configuration. Seeding and measuring are two
# bodies over ONE configuration snippet: a measurement whose configuration was
# written separately from the seed's could differ from the run that filled the
# database it measures, and the difference would be attributed to the build.
_SEED_TAIL_TEMPLATE: Final = '''
_spec = _json.loads(__SPEC_JSON__)

_known_statuses = {_status.value for _status in _TaskStatus}
for _bucket in _spec['buckets']:
    if _bucket['status'] not in _known_statuses:
        raise RuntimeError(
            'this build has no task status named ' + repr(_bucket['status'])
            + '; the seed spec declares a status the build does not define, '
            'so the two sides would record different status vocabularies'
        )

_app = _Horsies(config=_app_config, run_schema_migrations=True)


@_app.task(task_name=_spec['task_name'])
def _seeded(*, blob: str, size_class: int) -> _TaskResult[int, _TaskError]:
    return _TaskResult(ok=size_class)


_rng = _random.Random(_spec['payload_seed'])
_rows = 0
_payload_bytes_total = 0
_histogram = {}
_status_mix = {}

for _bucket in _spec['buckets']:
    _size = _bucket['payload_bytes']
    for _index in range(_bucket['count']):
        _blob = _b64.b64encode(_rng.randbytes(_size)).decode('ascii')[:_size]
        if len(_blob) != _size:
            raise RuntimeError(
                'payload generator produced ' + str(len(_blob))
                + ' bytes for a declared size of ' + str(_size)
            )
        _sent = _seeded.send(blob=_blob, size_class=_size)
        if not _sent.is_ok():
            raise RuntimeError('seed send failed: ' + repr(_sent.err()))
        _rows += 1
        _payload_bytes_total += len(_blob)
        _histogram[_size] = _histogram.get(_size, 0) + 1
        _status_mix[_bucket['status']] = (
            _status_mix.get(_bucket['status'], 0) + 1
        )

print(
    __FACTS_MARKER__ + ' '
    + _json.dumps(
        {
            'rows': _rows,
            'payload_bytes_total': _payload_bytes_total,
            'payload_size_histogram': sorted(_histogram.items()),
            'status_mix': sorted(_status_mix.items()),
        }
    ),
    flush=True,
)
'''


# Every substitutable position in the template. Found rather than listed, so a
# token added to the body but forgotten here cannot slip through as a
# placeholder that runs.
_TOKEN_PATTERN: Final = re.compile(r'__[A-Z][A-Z0-9_]*__')


def substitute_seed_tokens(
    template: str, substitutions: Mapping[str, str]
) -> str:
    """Fill every token in a seed template, and refuse a template with any left.

    The tokens are read out of the template, not declared alongside it. A
    declared list is a second place to be right: add a token to the body,
    forget the list, and the side runs with the placeholder text in place of
    its own database — the run succeeds at the wrong thing.
    """
    present = set(_TOKEN_PATTERN.findall(template))
    missing = sorted(present - set(substitutions))
    if missing:
        raise SeedError(
            f'the seed template carries tokens with no value: {missing}. The '
            'side would run with placeholder text where its own values '
            'belong, and would measure something nobody declared'
        )
    body = template
    for token, literal in substitutions.items():
        body = body.replace(token, literal)
    remaining = sorted(_TOKEN_PATTERN.findall(body))
    if remaining:
        raise SeedError(
            f'substitution left tokens in the seed body: {remaining}'
        )
    return body


def seed_source(
    spec: SeedSpec, *, config_spec: SeedConfigSpec, database_url: str
) -> str:
    """The one seed body, with this side's values substituted as JSON literals.

    Substitution is by JSON literal rather than by interpolated expression:
    the values reach the body already quoted and escaped, so a database name
    or task name cannot become code.
    """
    return substitute_seed_tokens(
        CONFIG_SOURCE_SNIPPET + _SEED_TAIL_TEMPLATE,
        {
            '__SPEC_JSON__': json.dumps(json.dumps(spec.as_payload())),
            '__CONFIG_JSON__': json.dumps(json.dumps(config_spec.as_payload())),
            '__DSN_JSON__': json.dumps(json.dumps(database_url)),
            '__CONFIG_MARKER__': json.dumps(SIDE_CONFIG_MARKER),
            '__FACTS_MARKER__': json.dumps(SIDE_FACTS_MARKER),
            '__SECRET_SENTINEL__': json.dumps(SECRET_SENTINEL),
            '__REQUIRED_SENTINEL__': json.dumps(REQUIRED_FIELD_SENTINEL),
        },
    )


def _marked_payload(output: str, marker: str, *, side: PairedSide) -> Any:
    for line in output.splitlines():
        if not line.startswith(marker):
            continue
        try:
            return json.loads(line[len(marker) :].strip())
        except ValueError as error:
            raise SeedError(
                f'{side} emitted an unparseable {marker} line: {line[:200]!r}'
            ) from error
    raise SeedError(
        f'{side} produced no {marker} line; the side did not report what it '
        'ran, and a measurement that cannot describe its own inputs is not '
        'usable as one'
    )


def config_from_output(output: str, *, side: PairedSide) -> SideConfig:
    """Read the configuration the seeding process reported about itself."""
    payload = _marked_payload(output, SIDE_CONFIG_MARKER, side=side)
    return SideConfig(
        side=side,
        effective=dict(payload['effective']),
        defaults=dict(payload['defaults']),
    )


def facts_from_output(output: str, *, side: PairedSide) -> EquivalenceFacts:
    """Read what the seed loop reported sending."""
    payload = _marked_payload(output, SIDE_FACTS_MARKER, side=side)
    return EquivalenceFacts(
        side=side,
        rows=int(payload['rows']),
        payload_bytes_total=int(payload['payload_bytes_total']),
        payload_size_histogram=tuple(
            (int(size), int(count))
            for size, count in payload['payload_size_histogram']
        ),
        status_mix=tuple(
            (str(status), int(count)) for status, count in payload['status_mix']
        ),
    )


def assert_facts_match_spec(
    facts: EquivalenceFacts, spec: SeedSpec
) -> None:
    """The side sent the work the spec asked for.

    Cross-side equality alone cannot catch a spec that was under-sent on both
    sides identically. This checks each side against the declaration instead,
    so a truncated seed is a refusal rather than a smaller cell that still
    compares equal.
    """
    expected_rows = spec.expected_rows()
    if facts.rows != expected_rows:
        raise SeedError(
            f'{facts.side} sent {facts.rows} rows, the spec declares '
            f'{expected_rows}; the cell would measure a workload nobody '
            'declared'
        )
    expected_histogram = tuple(
        sorted(
            (
                bucket.payload_bytes,
                sum(
                    other.count
                    for other in spec.buckets
                    if other.payload_bytes == bucket.payload_bytes
                ),
            )
            for bucket in {
                bucket.payload_bytes: bucket for bucket in spec.buckets
            }.values()
        )
    )
    if facts.payload_size_histogram != expected_histogram:
        raise SeedError(
            f'{facts.side} sent size distribution '
            f'{facts.payload_size_histogram}, the spec declares '
            f'{expected_histogram}'
        )


def assert_databases_differ(
    baseline_database_url: str, candidate_database_url: str
) -> None:
    """The two sides cannot share a database.

    The baseline migrates to schema 26 and the candidate to schema 30 through
    each build own migration chain. Pointed at one database they would migrate
    over each other, and whichever ran second would decide what both measured.
    """
    if baseline_database_url == candidate_database_url:
        raise ConfigEquivalenceError(
            'both sides were given the same database URL; the two builds run '
            'different schema versions and would migrate over each other, so '
            'neither side would be measuring the schema it declares'
        )


def assert_config_equivalence(
    baseline: SideConfig, candidate: SideConfig
) -> None:
    """The two sides differ in their database and in nothing else that is set.

    Shared fields must hold equal effective values — a default that moved
    between builds is a configuration difference that would be attributed to
    the build.

    A field present on only one build is a product change, not a harness
    choice, so it cannot be forbidden. It is instead required to be at that
    build shipped default: at its default the field is the prior behaviour and
    the cell measures the build, while at any other value the cell measures a
    configuration, and no item in this batch treats such a field as an axis. A
    recorded deviation is still a deviation.
    """
    if baseline.side is not PairedSide.BASELINE:
        raise ConfigEquivalenceError(
            f'the baseline configuration reports side {baseline.side}'
        )
    if candidate.side is not PairedSide.CANDIDATE:
        raise ConfigEquivalenceError(
            f'the candidate configuration reports side {candidate.side}'
        )
    shared = (set(baseline.effective) & set(candidate.effective)) - (
        DSN_CONFIG_KEYS
    )
    divergent = sorted(
        key
        for key in shared
        if baseline.effective[key] != candidate.effective[key]
    )
    if divergent:
        detail = ', '.join(
            f'{key}: baseline {baseline.effective[key]!r} vs candidate '
            f'{candidate.effective[key]!r}'
            for key in divergent
        )
        raise ConfigEquivalenceError(
            f'the two sides ran different configurations ({detail}). Only the '
            'database may differ; any other difference is measured as though '
            'it were the build'
        )
    for side_config, other in (
        (baseline, candidate),
        (candidate, baseline),
    ):
        for key in sorted(set(side_config.effective) - set(other.effective)):
            if key in DSN_CONFIG_KEYS:
                continue
            if key not in side_config.defaults:
                raise ConfigEquivalenceError(
                    f'{side_config.side} carries the extra field {key}, which '
                    'reports no shipped default; it cannot be shown to be at '
                    'one, so the cell cannot claim to measure the build alone'
                )
            default = side_config.defaults[key]
            if default == REQUIRED_FIELD_SENTINEL:
                raise ConfigEquivalenceError(
                    f'{side_config.side} carries the extra field {key}, which '
                    'has no default because it must be supplied; a value the '
                    'harness chose is a configuration the cell would measure'
                )
            if side_config.effective[key] != default:
                raise ConfigEquivalenceError(
                    f'{side_config.side} runs the extra field {key} at '
                    f'{side_config.effective[key]!r}, not its shipped default '
                    f'{default!r}. Only one build has this field, so at a '
                    'non-default value the cell measures a configuration '
                    'rather than the build'
                )


def config_conditions(
    baseline: SideConfig, candidate: SideConfig
) -> dict[str, Any]:
    """What the artifact records about the two configurations.

    The fields present on only one side are named explicitly with their
    effective values, so the reader checks the build difference rather than
    trusting that it was checked.
    """
    baseline_only = sorted(set(baseline.effective) - set(candidate.effective))
    candidate_only = sorted(set(candidate.effective) - set(baseline.effective))
    return {
        'baseline': baseline.as_conditions(),
        'candidate': candidate.as_conditions(),
        'build_only_fields': {
            'baseline': {
                key: baseline.effective[key] for key in baseline_only
            },
            'candidate': {
                key: candidate.effective[key] for key in candidate_only
            },
        },
    }


def run_seed_side(
    side: PairedSide,
    *,
    interpreter: Path,
    expected_root: Path,
    expected_schema_version: int,
    cwd: Path,
    database_url: str,
    spec: SeedSpec,
    config_spec: SeedConfigSpec,
    environment: Mapping[str, str] | None = None,
) -> SeedOutcome:
    """Seed one side, and refuse anything it cannot account for.

    The identity is checked here rather than at cell construction, so a side
    that imported the wrong build fails before it has written rows into a
    database that a later cell would then measure.
    """
    completed = run_side(
        interpreter,
        seed_source(spec, config_spec=config_spec, database_url=database_url),
        environment=(
            measurement_environment() if environment is None else environment
        ),
        cwd=cwd,
    )
    if completed.returncode != 0:
        raise SeedError(
            f'{side} seeding exited {completed.returncode}: '
            f'{completed.stderr[-2000:]}'
        )
    identity = side_identity_from_output(
        completed.stdout,
        side=side,
        interpreter=interpreter,
        expected_root=expected_root,
        expected_schema_version=expected_schema_version,
    )
    assert_side_identity(identity)
    facts = facts_from_output(completed.stdout, side=side)
    assert_facts_match_spec(facts, spec)
    return SeedOutcome(
        identity=identity,
        config=config_from_output(completed.stdout, side=side),
        facts=facts,
    )


@dataclass(frozen=True, slots=True)
class SeededPair:
    """Two seeded sides that have passed every cross-side check.

    A function that validates and hands back a plain tuple is discipline
    wearing a type's clothes: a caller already holding both outcomes can simply
    not call it, and nothing downstream re-checks — ``PairedCell`` verifies
    identities and equivalence facts, not configurations. So the checks live in
    ``__post_init__`` and the pair is the only thing a cell is built from. The
    database URLs are required fields because they are what
    ``assert_databases_differ`` needs and what the conditions must record;
    keeping them here stops a second copy from drifting out of step.
    """

    baseline: SeedOutcome
    candidate: SeedOutcome
    baseline_database_url: str
    candidate_database_url: str

    def __post_init__(self) -> None:
        if self.baseline.side is not PairedSide.BASELINE:
            raise ConfigEquivalenceError(
                f'the baseline slot holds a {self.baseline.side} outcome; the '
                'pair would compare a build against itself while every '
                'per-side check passed'
            )
        if self.candidate.side is not PairedSide.CANDIDATE:
            raise ConfigEquivalenceError(
                f'the candidate slot holds a {self.candidate.side} outcome; '
                'the pair would compare a build against itself while every '
                'per-side check passed'
            )
        assert_databases_differ(
            self.baseline_database_url, self.candidate_database_url
        )
        assert_config_equivalence(self.baseline.config, self.candidate.config)

    def conditions(
        self, *, spec: SeedSpec, config_spec: SeedConfigSpec
    ) -> dict[str, Any]:
        """The full declaration of how the two sides were filled."""
        return {
            'spec': spec.as_payload(),
            'config_spec': config_spec.as_payload(),
            'databases': {
                'baseline': _database_name(self.baseline_database_url),
                'candidate': _database_name(self.candidate_database_url),
            },
            'configuration': config_conditions(
                self.baseline.config, self.candidate.config
            ),
        }


def _database_name(database_url: str) -> str:
    """The database a URL points at, without its credentials.

    Conditions name the database so a reader can tell the two sides apart. The
    rest of the URL carries a password and never reaches an artifact.
    """
    return database_url.rsplit('/', 1)[-1].split('?')[0]
