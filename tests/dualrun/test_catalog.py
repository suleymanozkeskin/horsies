"""Unit tests of the rule catalog, the coverage check and the value parsers. No database."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta, timezone
from typing import Final

import pytest

from horsies.core.types.result import Err, Ok
from tests.dualrun.catalog import BASELINE_CATALOG
from tests.dualrun.coverage import (
    DatabaseType,
    DetachedLeaf,
    DetachedLeafShapeMismatch,
    ReferencedTypeMismatch,
    RuleTypeMismatch,
    RuleWithoutColumn,
    SchemaColumn,
    UnclassifiedColumn,
    UnknownType,
    UnsupportedColumnType,
    check_coverage,
    detached_leaf,
    parse_column_type,
    rule_fits_type,
    split_detached_leaves,
)
from tests.dualrun.rules import (
    AnchoredJsonTime,
    AnchorNull,
    AnchorReference,
    CatalogDefect,
    ColumnRule,
    ColumnSpec,
    DigestOf,
    DuplicateColumn,
    DuplicateTable,
    EachItem,
    EmptyIgnoreReason,
    EmptyJsonPath,
    EmptyKey,
    Exact,
    FormatVersion,
    GeneratedName,
    IdFamily,
    Ignored,
    JsonPath,
    JsonStorage,
    JsonTime,
    Key,
    KeyColumnNotComparable,
    KeyColumnUnknown,
    KeyRole,
    NegativeBound,
    NormalisedId,
    ReferencedColumnUnknown,
    RunWindowReference,
    SnapshotCatalog,
    StoredJson,
    TableRules,
    TimeEncoding,
    TimeOrderOnly,
    TimeWithin,
    Unversioned,
    catalog_defects,
)
from tests.dualrun.values import (
    FiniteTime,
    InfiniteFuture,
    InfinitePast,
    LiteralTime,
    RunWindow,
    RunWindowFault,
    RunWindowInvalid,
    TimeRank,
    make_run_window,
    parse_time_text,
    sort_token,
)

pytestmark = [pytest.mark.unit]

ONE_SECOND: Final = timedelta(seconds=1)
ANCHORED: Final = TimeWithin(
    reference=AnchorReference(column='made_at', when_anchor_null=AnchorNull.REFUSED),
    bound=ONE_SECOND,
)
RUN_CLOCK: Final = TimeWithin(reference=RunWindowReference(), bound=ONE_SECOND)


def _table(
    *columns: ColumnSpec,
    order_by: tuple[str, ...] = ('id',),
    identity: tuple[str, ...] = ('id',),
) -> TableRules:
    return TableRules(
        name='things', columns=columns, order_by=order_by, identity=identity
    )


ID: Final = ColumnSpec(name='id', rule=NormalisedId(family=IdFamily.TASK))
MADE_AT: Final = ColumnSpec(name='made_at', rule=RUN_CLOCK)


# ---- BASELINE_CATALOG ----


def test_baseline_catalog_has_no_shape_defects() -> None:
    assert catalog_defects(BASELINE_CATALOG) == ()


def test_baseline_catalog_has_a_rule_for_each_of_258_columns_in_20_tables() -> None:
    # 256 table columns of migration 1000, plus tableoid of the two partitioned parents.
    assert len(BASELINE_CATALOG.tables) == 20
    assert sum(len(table.columns) for table in BASELINE_CATALOG.tables) == 258


def test_baseline_catalog_ignores_exactly_these_columns_with_a_reason() -> None:
    ignored = {
        (table.name, spec.name): spec.rule.reason
        for table in BASELINE_CATALOG.tables
        for spec in table.columns
        if isinstance(spec.rule, Ignored)
    }

    assert set(ignored) == {
        ('horsies_migrations', 'execution_time'),
        ('horsies_worker_states', 'memory_usage_mb'),
        ('horsies_worker_states', 'memory_percent'),
        ('horsies_worker_states', 'cpu_percent'),
        ('horsies_worker_states', 'children_memory_mb'),
        ('horsies_workflow_phase2_pending', 'result_digest'),
    }
    assert all(len(reason) > 20 for reason in ignored.values())


def test_partitioned_parents_capture_the_leaf_of_each_row() -> None:
    leaf_rules = {
        table.name: spec.rule
        for table in BASELINE_CATALOG.tables
        for spec in table.columns
        if spec.name == 'tableoid'
    }

    assert leaf_rules == {
        'horsies_task_history': GeneratedName(),
        'horsies_heartbeats': GeneratedName(),
    }


def test_baseline_catalog_owners_come_before_their_references() -> None:
    names = [table.name for table in BASELINE_CATALOG.tables]

    assert (
        names.index('horsies_workflows')
        < names.index('horsies_workflow_tasks')
        < names.index('horsies_tasks')
    )
    assert names.index('horsies_tasks') < names.index('horsies_task_attempts')
    assert names.index('horsies_task_history_leaf_catalog') < names.index(
        'horsies_task_history'
    )


# ---- Catalog shape defects ----


@pytest.mark.parametrize(
    ('catalog', 'expected'),
    [
        pytest.param(
            SnapshotCatalog(tables=(_table(ID), _table(ID))),
            (DuplicateTable(table='things'),),
            id='duplicate-table',
        ),
        pytest.param(
            SnapshotCatalog(tables=(_table(ID, ID),)),
            (DuplicateColumn(table='things', column='id'),),
            id='duplicate-column',
        ),
        pytest.param(
            SnapshotCatalog(tables=(_table(ID, order_by=()),)),
            (EmptyKey(table='things', role=KeyRole.ORDER_BY),),
            id='empty-order-by',
        ),
        pytest.param(
            SnapshotCatalog(tables=(_table(ID, identity=('missing',)),)),
            (
                KeyColumnUnknown(
                    table='things', role=KeyRole.IDENTITY, column='missing'
                ),
            ),
            id='identity-column-unknown',
        ),
        pytest.param(
            SnapshotCatalog(
                tables=(
                    _table(
                        ID,
                        ColumnSpec(name='note', rule=Ignored(reason='free text')),
                        order_by=('note',),
                    ),
                ),
            ),
            (
                KeyColumnNotComparable(
                    table='things', role=KeyRole.ORDER_BY, column='note'
                ),
            ),
            id='ignored-column-in-order-by',
        ),
        pytest.param(
            SnapshotCatalog(
                tables=(
                    _table(
                        ID,
                        MADE_AT,
                        ColumnSpec(name='ends_at', rule=ANCHORED),
                        identity=('ends_at',),
                    ),
                ),
            ),
            (
                KeyColumnNotComparable(
                    table='things', role=KeyRole.IDENTITY, column='ends_at'
                ),
            ),
            id='anchored-column-in-identity',
        ),
        pytest.param(
            SnapshotCatalog(
                tables=(_table(ID, ColumnSpec(name='ends_at', rule=ANCHORED)),)
            ),
            (
                ReferencedColumnUnknown(
                    table='things', column='ends_at', referenced='made_at'
                ),
            ),
            id='anchor-unknown',
        ),
        pytest.param(
            SnapshotCatalog(
                tables=(_table(ID, ColumnSpec(name='note', rule=Ignored(reason='  '))),)
            ),
            (EmptyIgnoreReason(table='things', column='note'),),
            id='empty-ignore-reason',
        ),
        pytest.param(
            SnapshotCatalog(
                tables=(
                    _table(
                        ID,
                        ColumnSpec(
                            name='made_at',
                            rule=TimeWithin(
                                reference=RunWindowReference(), bound=-ONE_SECOND
                            ),
                        ),
                    ),
                ),
            ),
            (NegativeBound(table='things', column='made_at'),),
            id='negative-bound',
        ),
    ],
)
def test_catalog_defects_names_each_defect(
    catalog: SnapshotCatalog, expected: tuple[CatalogDefect, ...]
) -> None:
    assert catalog_defects(catalog) == expected


def test_run_window_time_and_time_order_are_valid_key_columns() -> None:
    catalog = SnapshotCatalog(
        tables=(
            _table(
                ID,
                MADE_AT,
                ColumnSpec(name='seen_at', rule=TimeOrderOnly()),
                ColumnSpec(name='name', rule=Exact()),
                order_by=('made_at', 'seen_at', 'name', 'id'),
            ),
        ),
    )

    assert catalog_defects(catalog) == ()


# ---- Coverage against a live schema ----

COVERED: Final = SnapshotCatalog(
    tables=(
        _table(
            ID,
            MADE_AT,
            ColumnSpec(name='ends_at', rule=ANCHORED),
            ColumnSpec(name='note', rule=Exact()),
        ),
    ),
)


def _schema(**types: DatabaseType | UnknownType) -> tuple[SchemaColumn, ...]:
    return tuple(
        SchemaColumn(table='things', column=name, column_type=column_type)
        for name, column_type in types.items()
    )


FULL_SCHEMA: Final = {
    'id': DatabaseType.UUID,
    'made_at': DatabaseType.TIMESTAMPTZ,
    'ends_at': DatabaseType.TIMESTAMPTZ,
    'note': DatabaseType.TEXT,
}


def test_full_coverage_has_no_gap() -> None:
    assert check_coverage(_schema(**FULL_SCHEMA), COVERED) == ()


def test_column_without_rule_is_unclassified() -> None:
    schema = _schema(**FULL_SCHEMA, added=DatabaseType.INT4)

    assert check_coverage(schema, COVERED) == (
        UnclassifiedColumn(table='things', column='added'),
    )


def test_table_without_rules_is_unclassified_per_column() -> None:
    schema = (
        *_schema(**FULL_SCHEMA),
        SchemaColumn(table='horsies_new', column='a', column_type=DatabaseType.TEXT),
        SchemaColumn(table='horsies_new', column='b', column_type=DatabaseType.INT4),
    )

    assert check_coverage(schema, COVERED) == (
        UnclassifiedColumn(table='horsies_new', column='a'),
        UnclassifiedColumn(table='horsies_new', column='b'),
    )


def test_rule_without_column_is_reported() -> None:
    schema = _schema(
        **{name: kind for name, kind in FULL_SCHEMA.items() if name != 'note'}
    )

    assert check_coverage(schema, COVERED) == (
        RuleWithoutColumn(table='things', column='note'),
    )


def test_column_of_unknown_type_is_unsupported() -> None:
    schema = _schema(**(FULL_SCHEMA | {'note': UnknownType(name='numeric')}))

    assert check_coverage(schema, COVERED) == (
        UnsupportedColumnType(table='things', column='note', type_name='numeric'),
    )


@pytest.mark.parametrize(
    ('column', 'column_type'),
    [
        pytest.param('made_at', DatabaseType.TEXT, id='time-rule-on-text'),
        pytest.param('id', DatabaseType.INT4, id='id-rule-on-integer'),
    ],
)
def test_rule_that_cannot_read_the_column_type_is_a_mismatch(
    column: str, column_type: DatabaseType
) -> None:
    schema = _schema(**(FULL_SCHEMA | {column: column_type}))
    rule = {spec.name: spec.rule for spec in COVERED.tables[0].columns}[column]

    gaps = check_coverage(schema, COVERED)

    assert (
        RuleTypeMismatch(
            table='things', column=column, rule=rule, column_type=column_type
        )
        in gaps
    )


def test_anchor_column_that_is_not_a_clock_is_a_mismatch() -> None:
    catalog = SnapshotCatalog(
        tables=(
            _table(
                ID,
                ColumnSpec(name='made_at', rule=Exact()),
                ColumnSpec(name='ends_at', rule=ANCHORED),
            ),
        ),
    )
    schema = _schema(
        id=DatabaseType.UUID,
        made_at=DatabaseType.TEXT,
        ends_at=DatabaseType.TIMESTAMPTZ,
    )

    assert check_coverage(schema, catalog) == (
        ReferencedTypeMismatch(
            table='things',
            column='ends_at',
            referenced='made_at',
            referenced_type=DatabaseType.TEXT,
        ),
    )


def test_column_type_names_parse_to_known_and_unknown_types() -> None:
    assert parse_column_type('timestamptz') is DatabaseType.TIMESTAMPTZ
    assert parse_column_type('_varchar') is DatabaseType.VARCHAR_ARRAY
    assert parse_column_type('numeric') == UnknownType(name='numeric')


# ---- Clock text and the run window ----


@pytest.mark.parametrize(
    ('text', 'expected'),
    [
        pytest.param('infinity', InfiniteFuture(), id='infinity'),
        pytest.param('-infinity', InfinitePast(), id='minus-infinity'),
        pytest.param(
            '2026-11-16 14:07:42.132222+00',
            FiniteTime(at=datetime(2026, 11, 16, 14, 7, 42, 132222, tzinfo=UTC)),
            id='utc-with-microseconds',
        ),
        pytest.param(
            '2026-11-16 15:07:42+01',
            FiniteTime(
                at=datetime(
                    2026, 11, 16, 15, 7, 42, tzinfo=timezone(timedelta(hours=1))
                )
            ),
            id='offset-hours',
        ),
    ],
)
def test_time_text_parses(
    text: str, expected: FiniteTime | InfinitePast | InfiniteFuture
) -> None:
    assert parse_time_text(text) == Ok(expected)


@pytest.mark.parametrize(
    'text',
    [
        pytest.param('2026-11-16 14:07:42', id='no-offset'),
        pytest.param('0044-03-15 12:00:00+00 BC', id='before-christ'),
        pytest.param('yesterday', id='not-a-time'),
    ],
)
def test_time_text_without_a_full_timestamptz_form_is_rejected(text: str) -> None:
    match parse_time_text(text):
        case Err(rejected):
            assert rejected.text == text
        case Ok(value):
            raise AssertionError(f'{text!r} parsed to {value}')


def test_time_sort_order_is_minus_infinity_then_time_then_infinity() -> None:
    early = LiteralTime(time=FiniteTime(at=datetime(2026, 1, 1, tzinfo=UTC)))
    late = LiteralTime(time=FiniteTime(at=datetime(2026, 1, 1, 0, 0, 0, 1, tzinfo=UTC)))
    values = [
        LiteralTime(time=InfiniteFuture()),
        late,
        early,
        LiteralTime(time=InfinitePast()),
    ]

    assert sorted(values, key=sort_token) == [
        LiteralTime(time=InfinitePast()),
        early,
        late,
        LiteralTime(time=InfiniteFuture()),
    ]


def test_rank_sort_order_is_numeric() -> None:
    assert sorted([TimeRank(rank=10), TimeRank(rank=2)], key=sort_token) == [
        TimeRank(rank=2),
        TimeRank(rank=10),
    ]


def test_run_window_needs_aware_readings_in_order() -> None:
    start = datetime(2026, 1, 1, tzinfo=UTC)
    end = start + ONE_SECOND
    naive = datetime(2026, 1, 1)

    assert make_run_window(start, end) == Ok(RunWindow(start=start, end=end))
    assert make_run_window(start, start) == Ok(RunWindow(start=start, end=start))
    assert make_run_window(end, start) == Err(
        RunWindowInvalid(start=end, end=start, fault=RunWindowFault.END_BEFORE_START)
    )
    assert make_run_window(naive, end) == Err(
        RunWindowInvalid(start=naive, end=end, fault=RunWindowFault.NAIVE_TIME)
    )


# ---- Rules that read stored values or other columns ----

_JSON_TEXT: Final = StoredJson(storage=JsonStorage.TEXT, format=Unversioned(), paths=())


@pytest.mark.parametrize(
    ('spec', 'expected'),
    [
        pytest.param(
            ColumnSpec(
                name='doc',
                rule=StoredJson(
                    storage=JsonStorage.UTF8_BYTES,
                    format=FormatVersion(column='version', version=1),
                    paths=(),
                ),
            ),
            (
                ReferencedColumnUnknown(
                    table='things', column='doc', referenced='version'
                ),
            ),
            id='format-version-column-unknown',
        ),
        pytest.param(
            ColumnSpec(name='doc_digest', rule=DigestOf(column='doc')),
            (
                ReferencedColumnUnknown(
                    table='things', column='doc_digest', referenced='doc'
                ),
            ),
            id='digest-source-unknown',
        ),
        pytest.param(
            ColumnSpec(
                name='doc',
                rule=StoredJson(
                    storage=JsonStorage.TEXT,
                    format=Unversioned(),
                    paths=(
                        JsonPath(steps=(), rule=NormalisedId(family=IdFamily.TASK)),
                    ),
                ),
            ),
            (EmptyJsonPath(table='things', column='doc'),),
            id='empty-json-path',
        ),
        pytest.param(
            ColumnSpec(
                name='doc',
                rule=StoredJson(
                    storage=JsonStorage.TEXT,
                    format=Unversioned(),
                    paths=(
                        JsonPath(
                            steps=(Key(name='at'),),
                            rule=JsonTime(
                                encoding=TimeEncoding.ISO_TEXT, rule=ANCHORED
                            ),
                        ),
                    ),
                ),
            ),
            (AnchoredJsonTime(table='things', column='doc'),),
            id='anchored-json-time',
        ),
        pytest.param(
            ColumnSpec(
                name='doc',
                rule=StoredJson(
                    storage=JsonStorage.TEXT,
                    format=Unversioned(),
                    paths=(JsonPath(steps=(EachItem(),), rule=Ignored(reason='')),),
                ),
            ),
            (EmptyIgnoreReason(table='things', column='doc'),),
            id='json-path-ignored-without-reason',
        ),
    ],
)
def test_catalog_defects_of_stored_value_rules(
    spec: ColumnSpec, expected: tuple[CatalogDefect, ...]
) -> None:
    assert catalog_defects(SnapshotCatalog(tables=(_table(ID, spec),))) == expected


def test_stored_json_digest_and_generated_name_are_valid_key_columns() -> None:
    catalog = SnapshotCatalog(
        tables=(
            _table(
                ID,
                ColumnSpec(name='doc', rule=_JSON_TEXT),
                ColumnSpec(name='leaf', rule=GeneratedName()),
                ColumnSpec(name='doc_digest', rule=DigestOf(column='doc')),
                order_by=('doc', 'leaf', 'doc_digest', 'id'),
            ),
        ),
    )

    assert catalog_defects(catalog) == ()


@pytest.mark.parametrize(
    ('rule', 'fits', 'misfits'),
    [
        pytest.param(
            _JSON_TEXT, DatabaseType.JSONB, DatabaseType.BYTEA, id='json-text'
        ),
        pytest.param(
            StoredJson(storage=JsonStorage.UTF8_BYTES, format=Unversioned(), paths=()),
            DatabaseType.BYTEA,
            DatabaseType.TEXT,
            id='json-bytes',
        ),
        pytest.param(
            GeneratedName(), DatabaseType.OID, DatabaseType.INT4, id='generated-name'
        ),
        pytest.param(
            DigestOf(column='doc'), DatabaseType.BYTEA, DatabaseType.TEXT, id='digest'
        ),
    ],
)
def test_stored_value_rules_read_only_their_column_types(
    rule: ColumnRule,
    fits: DatabaseType,
    misfits: DatabaseType,
) -> None:
    assert rule_fits_type(rule, fits)
    assert not rule_fits_type(rule, misfits)


@pytest.mark.parametrize(
    ('spec', 'referenced', 'referenced_type'),
    [
        pytest.param(
            ColumnSpec(
                name='doc',
                rule=StoredJson(
                    storage=JsonStorage.TEXT,
                    format=FormatVersion(column='note', version=1),
                    paths=(),
                ),
            ),
            'note',
            DatabaseType.TEXT,
            id='format-version-not-integer',
        ),
        pytest.param(
            ColumnSpec(name='doc', rule=DigestOf(column='note')),
            'note',
            DatabaseType.TEXT,
            id='digest-source-not-bytea',
        ),
    ],
)
def test_referenced_column_of_the_wrong_type_is_a_gap(
    spec: ColumnSpec, referenced: str, referenced_type: DatabaseType
) -> None:
    catalog = SnapshotCatalog(
        tables=(_table(ID, ColumnSpec(name='note', rule=Exact()), spec),)
    )
    schema = _schema(id=DatabaseType.UUID, note=referenced_type, doc=DatabaseType.BYTEA)

    gaps = check_coverage(schema, catalog)

    assert (
        ReferencedTypeMismatch(
            table='things',
            column='doc',
            referenced=referenced,
            referenced_type=referenced_type,
        )
        in gaps
    )


# ---- Detached leaves ----


def _columns_of(table: str, **types: DatabaseType) -> tuple[SchemaColumn, ...]:
    return tuple(
        SchemaColumn(table=table, column=name, column_type=kind)
        for name, kind in types.items()
    )


PARENT_SCHEMA: Final = _columns_of(
    'horsies_task_history',
    tableoid=DatabaseType.OID,
    task_id=DatabaseType.UUID,
    terminal_at=DatabaseType.TIMESTAMPTZ,
)


def test_detached_leaf_with_the_parent_columns_leaves_the_schema() -> None:
    leaf_columns = _columns_of(
        'horsies_task_history_standard_30d_2026_01_01',
        task_id=DatabaseType.UUID,
        terminal_at=DatabaseType.TIMESTAMPTZ,
    )
    leaf = detached_leaf('horsies_task_history_standard_30d_2026_01_01', 'standard_30d')

    remaining, leaves, gaps = split_detached_leaves(
        PARENT_SCHEMA + leaf_columns, (leaf,)
    )

    assert remaining == PARENT_SCHEMA
    assert leaves == (
        DetachedLeaf(
            table='horsies_task_history_standard_30d_2026_01_01',
            parent='horsies_task_history',
        ),
    )
    assert gaps == ()


def test_detached_leaf_with_other_columns_is_a_gap() -> None:
    leaf_columns = _columns_of(
        'horsies_task_history_standard_30d_2026_01_01',
        task_id=DatabaseType.UUID,
        terminal_at=DatabaseType.TEXT,
    )
    leaf = detached_leaf('horsies_task_history_standard_30d_2026_01_01', 'standard_30d')

    _, _, gaps = split_detached_leaves(PARENT_SCHEMA + leaf_columns, (leaf,))

    assert gaps == (
        DetachedLeafShapeMismatch(table=leaf.table, parent='horsies_task_history'),
    )


def test_detached_leaf_whose_table_is_gone_is_skipped() -> None:
    leaf = detached_leaf('horsies_heartbeats_2026_01_01_10', 'heartbeats')

    remaining, leaves, gaps = split_detached_leaves(PARENT_SCHEMA, (leaf,))

    assert (remaining, leaves, gaps) == (PARENT_SCHEMA, (), ())
    assert leaf.parent == 'horsies_heartbeats'
