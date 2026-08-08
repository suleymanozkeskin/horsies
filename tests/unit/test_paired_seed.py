"""One seed body, two builds, and the checks that say they were fed the same work."""

from __future__ import annotations

import json

import pytest

from tests.task_history_prototypes.paired_cell import EquivalenceFacts
from tests.task_history_prototypes.paired_seed import (
    DSN_CONFIG_KEYS,
    REQUIRED_FIELD_SENTINEL,
    SIDE_CONFIG_MARKER,
    SIDE_FACTS_MARKER,
    ConfigEquivalenceError,
    SeedBucket,
    SeedConfigSpec,
    SeedError,
    SeedSpec,
    SideConfig,
    SeedOutcome,
    SeededPair,
    assert_config_equivalence,
    assert_databases_differ,
    assert_facts_match_spec,
    config_conditions,
    config_from_output,
    facts_from_output,
    seed_source,
    substitute_seed_tokens,
)
from tests.task_history_prototypes.paired_sides import (
    BASELINE_SCHEMA_VERSION,
    CANDIDATE_SCHEMA_VERSION,
    PairedSide,
    SideIdentity,
)

SPEC = SeedSpec(
    task_name='paired_seed_probe',
    buckets=(
        SeedBucket(payload_bytes=200, count=4),
        SeedBucket(payload_bytes=65536, count=2),
    ),
    payload_seed=17,
)

# The shape observed from the two builds themselves: a large shared body, and
# exactly one field the candidate has and the baseline does not.
SHARED_EFFECTIVE: dict[str, object] = {
    'broker.pool_size': 10,
    'broker.max_overflow': 0,
    'prefetch_buffer': 0,
    'queue_mode': 'default',
}
CANDIDATE_ONLY_KEY = 'broker.retain_rerun_input_default'


def _config(
    side: PairedSide,
    *,
    effective: dict[str, object] | None = None,
    defaults: dict[str, object] | None = None,
) -> SideConfig:
    base = dict(SHARED_EFFECTIVE)
    base['broker.database_url'] = f'**{side.value}**'
    if side is PairedSide.CANDIDATE:
        base[CANDIDATE_ONLY_KEY] = False
    if effective is not None:
        base.update(effective)
    shipped: dict[str, object] = {CANDIDATE_ONLY_KEY: False}
    if defaults is not None:
        shipped.update(defaults)
    return SideConfig(side=side, effective=base, defaults=shipped)


def _facts(side: PairedSide) -> EquivalenceFacts:
    return EquivalenceFacts(
        side=side,
        rows=6,
        payload_bytes_total=200 * 4 + 65536 * 2,
        payload_size_histogram=((200, 4), (65536, 2)),
        status_mix=(('PENDING', 6),),
    )


def test_the_seed_body_is_text_and_imports_nothing_from_this_checkout() -> None:
    """The baseline must not be able to reach this repository.

    A baseline that imports a helper from here resolves the checkout, which is
    the shadowing failure the identity guard exists for — defeated while
    appearing to be satisfied.
    """
    body = seed_source(
        SPEC, config_spec=SeedConfigSpec(), database_url='postgresql://x/y'
    )
    assert 'tests.task_history_prototypes' not in body
    assert 'tests/' not in body
    for line in body.splitlines():
        stripped = line.strip()
        if stripped.startswith(('import ', 'from ')):
            assert stripped.split()[1].split('.')[0] in {
                'base64',
                'json',
                'random',
                'pydantic',
                'horsies',
            }


def test_values_reach_the_body_as_data_not_as_code() -> None:
    """A task name is a value, never an expression."""
    hostile = SeedSpec(
        task_name="x'); import os; os.system('true",
        buckets=(SeedBucket(payload_bytes=8, count=1),),
        payload_seed=1,
    )
    body = seed_source(
        hostile, config_spec=SeedConfigSpec(), database_url='postgresql://x/y'
    )
    assert "os.system" not in body.replace(
        json.dumps(json.dumps(hostile.as_payload())), ''
    )


def test_the_real_body_comes_back_fully_substituted() -> None:
    body = seed_source(
        SPEC, config_spec=SeedConfigSpec(), database_url='postgresql://x/y'
    )
    for token in (
        '__SPEC_JSON__',
        '__CONFIG_JSON__',
        '__DSN_JSON__',
        '__CONFIG_MARKER__',
        '__FACTS_MARKER__',
    ):
        assert token not in body


def test_a_template_token_with_no_value_is_refused() -> None:
    """A token added to the body but not given a value would run as text.

    The side would then measure something nobody declared — a placeholder
    database name, or a marker the harness never looks for — while exiting
    zero.
    """
    with pytest.raises(SeedError, match='tokens with no value'):
        substitute_seed_tokens('_dsn = __DSN_JSON__\n', {})


def test_a_substitution_that_reintroduces_a_token_is_refused() -> None:
    """The body is checked after substitution, not only before."""
    with pytest.raises(SeedError, match='left tokens'):
        substitute_seed_tokens('x = __A__\n', {'__A__': '__B__'})


def test_tokens_are_found_in_the_template_not_declared_beside_it() -> None:
    """A declared list is a second place to be right, and it drifts."""
    body = substitute_seed_tokens('a = __ONE__; b = __TWO__', {'__ONE__': '1', '__TWO__': '2'})
    assert body == 'a = 1; b = 2'


def test_matching_configurations_are_accepted() -> None:
    assert_config_equivalence(
        _config(PairedSide.BASELINE), _config(PairedSide.CANDIDATE)
    )


def test_a_shared_field_that_differs_is_refused() -> None:
    """A default that moved between builds is a configuration difference."""
    with pytest.raises(ConfigEquivalenceError, match='broker.pool_size'):
        assert_config_equivalence(
            _config(PairedSide.BASELINE),
            _config(PairedSide.CANDIDATE, effective={'broker.pool_size': 25}),
        )


def test_the_database_is_the_one_permitted_difference() -> None:
    """The excluded keys are excluded, and nothing else is."""
    assert 'broker.database_url' in DSN_CONFIG_KEYS
    assert_config_equivalence(
        _config(PairedSide.BASELINE),
        _config(
            PairedSide.CANDIDATE,
            effective={'broker.database_url': '**something-else**'},
        ),
    )


def test_a_build_only_field_off_its_default_is_refused() -> None:
    """At a non-default value the cell measures a configuration, not a build.

    No item in this batch treats such a field as an axis, so a recorded
    deviation would be a deviation nobody asked for.
    """
    with pytest.raises(ConfigEquivalenceError, match='shipped default'):
        assert_config_equivalence(
            _config(PairedSide.BASELINE),
            _config(PairedSide.CANDIDATE, effective={CANDIDATE_ONLY_KEY: True}),
        )


def test_a_build_only_field_at_its_default_rides() -> None:
    assert_config_equivalence(
        _config(PairedSide.BASELINE),
        _config(PairedSide.CANDIDATE, effective={CANDIDATE_ONLY_KEY: False}),
    )


def test_a_build_only_field_with_no_recorded_default_is_refused() -> None:
    with pytest.raises(ConfigEquivalenceError, match='no shipped default'):
        assert_config_equivalence(
            _config(PairedSide.BASELINE),
            _config(PairedSide.CANDIDATE, effective={'broker.invented': 7}),
        )


def test_a_build_only_field_that_must_be_supplied_is_refused() -> None:
    """A required field has no default to be at, so it cannot be cleared."""
    with pytest.raises(ConfigEquivalenceError, match='must be supplied'):
        assert_config_equivalence(
            _config(PairedSide.BASELINE),
            _config(
                PairedSide.CANDIDATE,
                effective={'broker.invented': 7},
                defaults={'broker.invented': REQUIRED_FIELD_SENTINEL},
            ),
        )


def test_a_baseline_only_field_is_held_to_the_same_rule() -> None:
    """The rule is symmetric; the baseline is not the trusted side."""
    with pytest.raises(ConfigEquivalenceError, match='no shipped default'):
        assert_config_equivalence(
            _config(PairedSide.BASELINE, effective={'broker.retired': 3}),
            _config(PairedSide.CANDIDATE),
        )


def test_swapped_configuration_sides_are_refused() -> None:
    with pytest.raises(ConfigEquivalenceError, match='reports side'):
        assert_config_equivalence(
            _config(PairedSide.CANDIDATE), _config(PairedSide.CANDIDATE)
        )


def test_conditions_name_the_build_only_fields_with_their_values() -> None:
    """The reader checks the build difference instead of trusting it was checked."""
    conditions = config_conditions(
        _config(PairedSide.BASELINE), _config(PairedSide.CANDIDATE)
    )
    assert conditions['build_only_fields']['candidate'] == {
        CANDIDATE_ONLY_KEY: False
    }
    assert conditions['build_only_fields']['baseline'] == {}


def test_one_database_for_both_sides_is_refused() -> None:
    """Schema 26 and schema 30 would migrate over each other."""
    with pytest.raises(ConfigEquivalenceError, match='same database'):
        assert_databases_differ('postgresql://h/one', 'postgresql://h/one')


def test_facts_are_read_from_what_the_seed_reported_sending() -> None:
    output = (
        'noise\n'
        + SIDE_FACTS_MARKER
        + ' '
        + json.dumps(
            {
                'rows': 6,
                'payload_bytes_total': 131872,
                'payload_size_histogram': [[200, 4], [65536, 2]],
                'status_mix': [['PENDING', 6]],
            }
        )
        + '\nmore noise\n'
    )
    facts = facts_from_output(output, side=PairedSide.BASELINE)
    assert facts == _facts(PairedSide.BASELINE)


def test_output_without_a_facts_line_is_refused() -> None:
    with pytest.raises(SeedError, match='no __horsies_side_facts__'):
        facts_from_output('nothing here\n', side=PairedSide.CANDIDATE)


def test_output_without_a_config_line_is_refused() -> None:
    with pytest.raises(SeedError, match='no __horsies_side_config__'):
        config_from_output('nothing here\n', side=PairedSide.CANDIDATE)


def test_an_unparseable_marked_line_is_refused() -> None:
    with pytest.raises(SeedError, match='unparseable'):
        config_from_output(f'{SIDE_CONFIG_MARKER} not-json\n', side=PairedSide.BASELINE)


def test_a_side_that_under_sent_is_refused() -> None:
    """Cross-side equality cannot catch a seed truncated identically on both sides."""
    truncated = EquivalenceFacts(
        side=PairedSide.BASELINE,
        rows=5,
        payload_bytes_total=200 * 4 + 65536,
        payload_size_histogram=((200, 4), (65536, 1)),
        status_mix=(('PENDING', 5),),
    )
    with pytest.raises(SeedError, match='the spec declares 6'):
        assert_facts_match_spec(truncated, SPEC)


def test_a_side_matching_its_spec_is_accepted() -> None:
    assert_facts_match_spec(_facts(PairedSide.BASELINE), SPEC)


def test_a_reshaped_seed_with_the_right_row_count_is_refused() -> None:
    """Equal totals hide different shapes, so the distribution is checked too."""
    reshaped = EquivalenceFacts(
        side=PairedSide.CANDIDATE,
        rows=6,
        payload_bytes_total=200 * 4 + 65536 * 2,
        payload_size_histogram=((200, 3), (65536, 3)),
        status_mix=(('PENDING', 6),),
    )
    with pytest.raises(SeedError, match='size distribution'):
        assert_facts_match_spec(reshaped, SPEC)


@pytest.mark.parametrize('payload_bytes,count', [(0, 1), (-1, 1), (8, 0)])
def test_a_bucket_that_cannot_be_sent_is_refused(
    payload_bytes: int, count: int
) -> None:
    with pytest.raises(SeedError):
        SeedBucket(payload_bytes=payload_bytes, count=count)


def test_a_spec_with_no_buckets_is_refused() -> None:
    with pytest.raises(SeedError, match='at least one bucket'):
        SeedSpec(task_name='x', buckets=(), payload_seed=1)


def _outcome(side: PairedSide) -> SeedOutcome:
    return SeedOutcome(
        identity=SideIdentity(
            side=side,
            interpreter=f'/{side.value}/bin/python',
            module_path=f'/{side.value}/horsies/__init__.py',
            schema_version=(
                BASELINE_SCHEMA_VERSION
                if side is PairedSide.BASELINE
                else CANDIDATE_SCHEMA_VERSION
            ),
            expected_root=f'/{side.value}',
            expected_schema_version=(
                BASELINE_SCHEMA_VERSION
                if side is PairedSide.BASELINE
                else CANDIDATE_SCHEMA_VERSION
            ),
        ),
        config=_config(side),
        facts=_facts(side),
    )


def test_a_seeded_pair_runs_its_checks_on_construction() -> None:
    """The guarantee is the type's, not the caller's.

    A validating function that returns a plain tuple can be skipped by a
    caller who already holds both outcomes, and nothing downstream re-checks
    configurations.
    """
    pair = SeededPair(
        baseline=_outcome(PairedSide.BASELINE),
        candidate=_outcome(PairedSide.CANDIDATE),
        baseline_database_url='postgresql://h/base',
        candidate_database_url='postgresql://h/cand',
    )
    assert pair.baseline.identity.schema_version == BASELINE_SCHEMA_VERSION


def test_a_pair_sharing_one_database_cannot_be_constructed() -> None:
    with pytest.raises(ConfigEquivalenceError, match='same database'):
        SeededPair(
            baseline=_outcome(PairedSide.BASELINE),
            candidate=_outcome(PairedSide.CANDIDATE),
            baseline_database_url='postgresql://h/one',
            candidate_database_url='postgresql://h/one',
        )


def test_a_pair_with_divergent_configurations_cannot_be_constructed() -> None:
    baseline = _outcome(PairedSide.BASELINE)
    candidate = SeedOutcome(
        identity=_outcome(PairedSide.CANDIDATE).identity,
        config=_config(PairedSide.CANDIDATE, effective={'broker.pool_size': 25}),
        facts=_facts(PairedSide.CANDIDATE),
    )
    with pytest.raises(ConfigEquivalenceError, match='broker.pool_size'):
        SeededPair(
            baseline=baseline,
            candidate=candidate,
            baseline_database_url='postgresql://h/base',
            candidate_database_url='postgresql://h/cand',
        )


def test_a_candidate_outcome_in_the_baseline_slot_is_refused() -> None:
    """Every per-side check passes; only the positional check catches it."""
    with pytest.raises(ConfigEquivalenceError, match='baseline slot holds'):
        SeededPair(
            baseline=_outcome(PairedSide.CANDIDATE),
            candidate=_outcome(PairedSide.CANDIDATE),
            baseline_database_url='postgresql://h/base',
            candidate_database_url='postgresql://h/cand',
        )


def test_a_baseline_outcome_in_the_candidate_slot_is_refused() -> None:
    with pytest.raises(ConfigEquivalenceError, match='candidate slot holds'):
        SeededPair(
            baseline=_outcome(PairedSide.BASELINE),
            candidate=_outcome(PairedSide.BASELINE),
            baseline_database_url='postgresql://h/base',
            candidate_database_url='postgresql://h/cand',
        )


def test_an_outcome_assembled_from_two_runs_is_refused() -> None:
    """Identity, configuration and facts must agree on which build they describe."""
    with pytest.raises(SeedError, match='more than one side'):
        SeedOutcome(
            identity=_outcome(PairedSide.BASELINE).identity,
            config=_config(PairedSide.CANDIDATE),
            facts=_facts(PairedSide.BASELINE),
        )


def test_conditions_name_the_databases_without_their_credentials() -> None:
    pair = SeededPair(
        baseline=_outcome(PairedSide.BASELINE),
        candidate=_outcome(PairedSide.CANDIDATE),
        baseline_database_url='postgresql+psycopg://user:secret@h:5432/base_db',
        candidate_database_url='postgresql+psycopg://user:secret@h:5432/cand_db',
    )
    conditions = pair.conditions(spec=SPEC, config_spec=SeedConfigSpec())
    assert conditions['databases'] == {
        'baseline': 'base_db',
        'candidate': 'cand_db',
    }
    assert 'secret' not in json.dumps(conditions)
