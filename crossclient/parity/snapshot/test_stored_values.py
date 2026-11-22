"""Unit tests of the stored-value rules: JSON documents, the attempt archive, digests, generated names."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Final

import pytest

from horsies.core.types.result import Err, Ok, Result
from crossclient.parity.snapshot.catalog import (
    ARGS_FROM_MARKER,
    ATTEMPT_ARCHIVE,
    LEAF_NAME,
    NODE_RESULT,
    RUN_CLOCK,
    SUB_WORKFLOW_SUMMARY,
    TASK_ID,
    TASK_INPUT,
    TASK_OPTIONS,
    WORKER_ID,
    WORKFLOW_ERROR,
    WORKFLOW_ID,
    WORKFLOW_RESULT,
)
from crossclient.parity.snapshot.cells import (
    NormalizeError,
    PendingId,
    PendingText,
    RawRow,
    RawSnapshot,
    RawTable,
    UnreadableBytes,
    UnreadableJson,
    UnsupportedStoredFormat,
    generated_name,
    text_with_ids,
)
from crossclient.parity.snapshot.compare import ColumnDifference, compare_snapshots
from crossclient.parity.snapshot.normalize import NormalizedSnapshot, normalize_snapshot
from crossclient.parity.snapshot.rules import (
    ColumnSpec,
    DigestOf,
    EachMarkedMember,
    EachMember,
    Exact,
    IdFamily,
    JsonPath,
    JsonStorage,
    JsonTime,
    Key,
    NormalisedId,
    SnapshotCatalog,
    StoredJson,
    TableRules,
    TimeEncoding,
    TimeOrderOnly,
    Unversioned,
)
from crossclient.parity.snapshot.values import (
    CalendarUnit,
    DatedName,
    DigestMatches,
    DigestMismatch,
    DuringRun,
    ExactValue,
    IdLabel,
    JsonDocument,
    LabelledText,
    LiteralTime,
    NormalizedValue,
    NullValue,
    RunWindow,
    TimeRank,
    UnparsedDocument,
    json_leaves,
    render_value,
)

pytestmark = [pytest.mark.unit]

RUN_A: Final = datetime(2026, 1, 1, 10, 30, tzinfo=UTC)
RUN_B: Final = datetime(2026, 3, 5, 17, 45, tzinfo=UTC)

DOCUMENT: Final = StoredJson(
    storage=JsonStorage.TEXT,
    format=Unversioned(),
    paths=(
        JsonPath(steps=(Key(name='owner'),), rule=NormalisedId(family=IdFamily.TASK)),
        JsonPath(
            steps=(Key(name='due'),),
            rule=JsonTime(encoding=TimeEncoding.ISO_TEXT, rule=TimeOrderOnly()),
        ),
        JsonPath(
            steps=(Key(name='seen'),),
            rule=JsonTime(encoding=TimeEncoding.ISO_TEXT, rule=RUN_CLOCK),
        ),
    ),
)

CATALOG: Final = SnapshotCatalog(
    tables=(
        TableRules(
            name='runs',
            columns=(
                ColumnSpec(name='id', rule=NormalisedId(family=IdFamily.TASK)),
                ColumnSpec(name='worker', rule=WORKER_ID),
                ColumnSpec(name='doc', rule=DOCUMENT),
                ColumnSpec(name='options', rule=TASK_OPTIONS),
                ColumnSpec(name='attempt_archive_version', rule=Exact()),
                ColumnSpec(name='archive', rule=ATTEMPT_ARCHIVE),
                ColumnSpec(name='archive_digest', rule=DigestOf(column='archive')),
                ColumnSpec(name='leaf', rule=LEAF_NAME),
            ),
            order_by=('doc', 'options', 'id'),
            identity=('id',),
        ),
    ),
)


def _hex(data: bytes) -> str:
    return '\\x' + data.hex()


def _epoch_us(at: datetime) -> int:
    return round(at.timestamp() * 1_000_000)


def _archive(worker: str, run: datetime) -> bytes:
    """An attempt archive of version 2: one attempt that the worker recorded
    with its phase record, and the open next attempt, claimed once."""
    attempt = [
        1,
        'FAILED',
        True,
        _epoch_us(run),
        _epoch_us(run + timedelta(seconds=2)),
        'E1',
        'message',
        None,
        worker,
        'host',
        7,
        'proc',
        1,
        _epoch_us(run),
        _epoch_us(run + timedelta(milliseconds=200)),
        1,
        _epoch_us(run + timedelta(milliseconds=200)),
        _epoch_us(run + timedelta(milliseconds=300)),
        None,
        'WORKER',
        None,
    ]
    open_attempt = [1, _epoch_us(run + timedelta(seconds=3)), None]
    return json.dumps([[attempt], open_attempt], separators=(',', ':')).encode()


def _row(
    raw_id: str,
    run: datetime,
    *,
    doc: str | None = None,
    options: str | None = '{"good_until":null,"retry_policy":null}',
    version: str = '2',
    digest: str | None = None,
    leaf: str | None = None,
) -> RawRow:
    worker = f'{raw_id}-worker'
    archive = _archive(worker, run)
    document = (
        doc
        if doc is not None
        else json.dumps({'owner': raw_id, 'due': None, 'seen': run.isoformat()})
    )
    return (
        raw_id,
        worker,
        document,
        options,
        version,
        _hex(archive),
        digest if digest is not None else _hex(hashlib.sha256(archive).digest()),
        leaf
        if leaf is not None
        else f'horsies_task_history_standard_30d_{run:%Y_%m_%d}',
    )


def _raw(rows: Sequence[RawRow]) -> RawSnapshot:
    return RawSnapshot(tables=(RawTable(name='runs', rows=tuple(rows)),))


def _window(run: datetime) -> RunWindow:
    return RunWindow(start=run, end=run + timedelta(minutes=1))


def _normalized(
    rows: Sequence[RawRow], run: datetime
) -> Result[NormalizedSnapshot, NormalizeError]:
    return normalize_snapshot(_raw(rows), CATALOG, _window(run))


def _snapshot(rows: Sequence[RawRow], run: datetime) -> NormalizedSnapshot:
    match _normalized(rows, run):
        case Ok(snapshot):
            return snapshot
        case Err(error):
            raise AssertionError(f'normalize failed: {error}')


def _cell(snapshot: NormalizedSnapshot, column: str) -> NormalizedValue:
    table = snapshot.table('runs')
    (row,) = table.rows
    return row[table.rules.position(column)]


def _leaves(value: NormalizedValue) -> list[NormalizedValue]:
    match value:
        case JsonDocument(root=root):
            return json_leaves(root)
        case _:
            raise AssertionError(f'{value} is not a JSON document')


# ---- JSON documents ----


def test_same_documents_with_other_ids_times_key_order_and_spacing_compare_equal() -> (
    None
):
    reference = _snapshot(
        [
            _row(
                'a1',
                RUN_A,
                doc='{"owner": "a1", "due": null, "seen": "'
                + RUN_A.isoformat()
                + '", "n": [1, 2]}',
            )
        ],
        RUN_A,
    )
    candidate = _snapshot(
        [
            _row(
                'b1',
                RUN_B,
                doc='{"n":[1,2],"seen":"'
                + RUN_B.isoformat()
                + '","due":null,"owner":"b1"}',
            )
        ],
        RUN_B,
    )

    assert compare_snapshots(reference, candidate).entries == ()


def test_an_id_inside_a_document_shares_the_label_of_the_same_id_in_a_column() -> None:
    snapshot = _snapshot([_row('a1', RUN_A)], RUN_A)

    assert _leaves(_cell(snapshot, 'doc'))[
        1
    ] == IdLabel(  # keys sorted: due, owner, seen
        family=IdFamily.TASK, ordinal=1
    )
    assert _cell(snapshot, 'id') == IdLabel(family=IdFamily.TASK, ordinal=1)


def test_a_changed_value_outside_the_paths_is_a_difference() -> None:
    reference = _snapshot([_row('a1', RUN_A, doc='{"owner": "a1", "n": 1}')], RUN_A)
    candidate = _snapshot([_row('b1', RUN_B, doc='{"owner": "b1", "n": 2}')], RUN_B)

    assert [
        entry.column
        for entry in compare_snapshots(reference, candidate).entries
        if isinstance(entry, ColumnDifference)
    ] == ['doc']


def test_an_integer_and_a_float_of_equal_value_differ() -> None:
    reference = _snapshot([_row('a1', RUN_A, doc='{"n": 1}')], RUN_A)
    candidate = _snapshot([_row('b1', RUN_B, doc='{"n": 1.0}')], RUN_B)

    assert len(compare_snapshots(reference, candidate).entries) == 1


def test_document_clock_paths_rank_and_window_their_values() -> None:
    rows = [
        _row(
            'a1',
            RUN_A,
            doc=json.dumps(
                {
                    'owner': 'a1',
                    'due': '2030-01-02T00:00:00+00:00',
                    'seen': '2001-01-01T00:00:00+00:00',
                }
            ),
        ),
        _row(
            'a2',
            RUN_A,
            doc=json.dumps(
                {
                    'owner': 'a2',
                    'due': '2030-01-01T00:00:00+00:00',
                    'seen': RUN_A.isoformat(),
                }
            ),
        ),
    ]

    table = _snapshot(rows, RUN_A).table('runs')

    leaves = sorted(
        (_leaves(row[table.rules.position('doc')]) for row in table.rows),
        key=lambda leaf: str(leaf[0]),
    )
    assert [leaf[0] for leaf in leaves] == [TimeRank(rank=1), TimeRank(rank=2)]
    assert leaves[0][2] == DuringRun()
    assert isinstance(leaves[1][2], LiteralTime)


def test_task_options_rank_their_good_until() -> None:
    late = '{"good_until":"2030-01-02T00:00:00+00:00"}'
    early = '{"good_until":"2030-01-01T00:00:00+00:00"}'
    table = _snapshot(
        [_row('a1', RUN_A, options=late), _row('a2', RUN_A, options=early)], RUN_A
    ).table('runs')

    ranks = sorted(
        str(_leaves(row[table.rules.position('options')])) for row in table.rows
    )
    assert ranks == [str([TimeRank(rank=1)]), str([TimeRank(rank=2)])]


@pytest.mark.parametrize(
    'doc',
    [
        pytest.param('{"owner": ', id='truncated'),
        pytest.param('{"n": NaN}', id='nan'),
        pytest.param('', id='empty'),
        pytest.param('not json', id='a-word'),
    ],
)
def test_a_stored_text_that_is_not_json_compares_as_that_text(doc: str) -> None:
    reference = _snapshot([_row('a1', RUN_A, doc=doc)], RUN_A)
    candidate = _snapshot([_row('b1', RUN_B, doc=doc)], RUN_B)
    other = _snapshot([_row('b1', RUN_B, doc=f'{doc}x')], RUN_B)
    parsed = _snapshot([_row('b1', RUN_B, doc='{"owner": "b1"}')], RUN_B)

    assert _cell(reference, 'doc') == UnparsedDocument(text=doc)
    assert compare_snapshots(reference, candidate).entries == ()
    for differing in (other, parsed):
        assert [
            entry.column
            for entry in compare_snapshots(reference, differing).entries
            if isinstance(entry, ColumnDifference)
        ] == ['doc']


def test_stored_bytes_that_are_utf8_text_but_not_json_compare_as_that_text() -> None:
    text = b'{"attempt": '
    row = _row('a1', RUN_A)

    snapshot = _snapshot(
        [(*row[:5], _hex(text), _hex(hashlib.sha256(text).digest()), row[7])], RUN_A
    )

    assert _cell(snapshot, 'archive') == UnparsedDocument(text=text.decode('utf-8'))
    assert _cell(snapshot, 'archive_digest') == DigestMatches()


@pytest.mark.parametrize(
    ('doc', 'cause_part'),
    [
        pytest.param('{"owner": 12}', 'not text', id='id-path-holds-a-number'),
        pytest.param(
            '{"due": "tomorrow"}', 'clock path', id='clock-path-holds-other-text'
        ),
        pytest.param('[' * 70 + ']' * 70, 'nesting deeper than 64', id='too-deep'),
    ],
)
def test_an_unreadable_document_is_an_error(doc: str, cause_part: str) -> None:
    match _normalized([_row('a1', RUN_A, doc=doc)], RUN_A):
        case Err(UnreadableJson(table='runs', column='doc', cause=cause)):
            assert cause_part in cause
        case other:
            raise AssertionError(f'expected UnreadableJson, got {other}')


def test_a_null_document_is_null() -> None:
    row = _row('a1', RUN_A)
    snapshot = _snapshot([(*row[:2], None, *row[3:])], RUN_A)

    assert _cell(snapshot, 'doc') == NullValue()


# ---- Ids inside a text ----

WORKFLOWS: Final = SnapshotCatalog(
    tables=(
        TableRules(
            name='workflows',
            columns=(
                ColumnSpec(name='id', rule=WORKFLOW_ID),
                ColumnSpec(name='name', rule=Exact()),
                ColumnSpec(name='error', rule=WORKFLOW_ERROR),
                ColumnSpec(name='result', rule=NODE_RESULT),
            ),
            order_by=('name',),
            identity=('id',),
        ),
    ),
)
"""A parent that a failed child paused, and the child: the error of the
parent and the result of its node name the child."""

CHILD_A: Final = '0190b2c4-1a2b-7c3d-8e4f-5a6b7c8d9e01'
PARENT_A: Final = '0190b2c4-1a2b-7c3d-8e4f-5a6b7c8d9e02'
CHILD_B: Final = '0191aaaa-bbbb-7ccc-8ddd-eeeeffff0001'
PARENT_B: Final = '0191aaaa-bbbb-7ccc-8ddd-eeeeffff0002'


def _parent_and_child(parent: str, child: str, named: str) -> list[RawRow]:
    error = json.dumps(
        {
            'error_code': 'SUBWORKFLOW_FAILED',
            'message': f'Subworkflow {named} failed',
            'sub_workflow_id': child,
        }
    )
    result = json.dumps(
        {
            '__h_task_result__': True,
            'ok': None,
            'err': {
                'error_code': 'SUBWORKFLOW_FAILED',
                'message': f'Subworkflow {named} failed with status FAILED',
                'sub_workflow_id': child,
            },
        }
    )
    return [(parent, 'parent', error, result), (child, 'child', None, None)]


def _workflows(rows: Sequence[RawRow], run: datetime) -> NormalizedSnapshot:
    raw = RawSnapshot(tables=(RawTable(name='workflows', rows=tuple(rows)),))
    match normalize_snapshot(raw, WORKFLOWS, _window(run)):
        case Ok(snapshot):
            return snapshot
        case Err(error):
            raise AssertionError(f'normalize failed: {error}')


def test_a_child_id_in_the_error_and_the_node_result_compares_by_its_label() -> None:
    reference = _workflows(_parent_and_child(PARENT_A, CHILD_A, CHILD_A), RUN_A)
    candidate = _workflows(_parent_and_child(PARENT_B, CHILD_B, CHILD_B), RUN_B)

    assert compare_snapshots(reference, candidate).entries == ()
    table = reference.table('workflows')
    child_row, parent_row = table.rows  # ordered by name: child, parent
    child = IdLabel(family=IdFamily.WORKFLOW, ordinal=1)
    assert child_row[table.rules.position('id')] == child
    error = _leaves(parent_row[table.rules.position('error')])
    result = _leaves(parent_row[table.rules.position('result')])
    # keys sorted: message, then sub_workflow_id
    assert error == [
        LabelledText(parts=('Subworkflow ', child, ' failed')),
        child,
    ]
    assert result == [
        LabelledText(parts=('Subworkflow ', child, ' failed with status FAILED')),
        child,
    ]


def test_a_message_that_names_another_workflow_is_a_difference() -> None:
    reference = _workflows(_parent_and_child(PARENT_A, CHILD_A, CHILD_A), RUN_A)
    candidate = _workflows(_parent_and_child(PARENT_B, CHILD_B, PARENT_B), RUN_B)

    assert [
        entry.column
        for entry in compare_snapshots(reference, candidate).entries
        if isinstance(entry, ColumnDifference)
    ] == ['error', 'result']


def test_a_text_path_that_holds_no_text_is_an_error() -> None:
    raw = RawSnapshot(
        tables=(
            RawTable(
                name='workflows',
                rows=((PARENT_A, 'parent', '{"message": 12}', None),),
            ),
        ),
    )

    match normalize_snapshot(raw, WORKFLOWS, _window(RUN_A)):
        case Err(UnreadableJson(table='workflows', column='error', cause=cause)):
            assert 'text path' in cause
        case other:
            raise AssertionError(f'expected UnreadableJson, got {other}')


def _id(raw: str) -> PendingId:
    return PendingId(raw=raw, family=IdFamily.WORKFLOW)


@pytest.mark.parametrize(
    ('text', 'parts'),
    [
        pytest.param('no id here', ('no id here',), id='no-id'),
        pytest.param('', (), id='empty'),
        pytest.param(CHILD_A, (_id(CHILD_A),), id='only-an-id'),
        pytest.param(
            f'{CHILD_A} and {PARENT_A}.',
            (_id(CHILD_A), ' and ', _id(PARENT_A), '.'),
            id='two-ids',
        ),
        pytest.param(CHILD_A.upper(), (CHILD_A.upper(),), id='upper-case'),
        pytest.param(f'x{CHILD_A}', (f'x{CHILD_A}',), id='letter-before'),
        pytest.param(f'{CHILD_A}-1', (f'{CHILD_A}-1',), id='hyphen-after'),
        pytest.param(f'{CHILD_A}0', (f'{CHILD_A}0',), id='digit-after'),
    ],
)
def test_text_with_ids_takes_only_canonical_lower_case_uuids(
    text: str, parts: tuple[str | PendingId, ...]
) -> None:
    assert text_with_ids(text, IdFamily.WORKFLOW) == PendingText(parts=parts)


# ---- Ids inside stored errors, results, summaries and the workflow context ----

TASK_A: Final = '0190b2c4-1a2b-7c3d-8e4f-5a6b7c8d9e03'
TASK_B: Final = '0191aaaa-bbbb-7ccc-8ddd-eeeeffff0003'


@dataclass(frozen=True, slots=True)
class Run:
    """The library-made ids and the start of one run."""

    workflow: str
    task: str
    worker: str
    start: datetime


RUN_IDS_A: Final = Run(
    workflow=CHILD_A, task=TASK_A, worker='worker-a', start=RUN_A
)
RUN_IDS_B: Final = Run(
    workflow=CHILD_B, task=TASK_B, worker='worker-b', start=RUN_B
)


def _envelope(error: dict[str, object] | None) -> dict[str, object]:
    return {'__h_task_result__': True, 'ok': None, 'err': error}


def _crash_error(run: Run) -> dict[str, object]:
    """The error that the stale recovery writes, with the task of a worker failure."""
    return {
        'error_code': 'WORKER_CRASHED',
        'message': 'the worker stopped',
        'data': {
            'task_id': run.task,
            'worker_id': run.worker,
            'started_at': (run.start + timedelta(seconds=1)).isoformat(),
            'detected_at': (run.start + timedelta(seconds=30)).isoformat(),
        },
    }


def _skipped_error(named: str) -> dict[str, object]:
    return {
        'error_code': 'WORKFLOW_OUTPUT_SKIPPED',
        'message': f'Workflow {named} is COMPLETED and has no output',
        'data': {'workflow_id': named, 'output_task_index': 1},
    }


def _child_failed_error(named: str) -> dict[str, object]:
    return {
        'error_code': 'SUBWORKFLOW_FAILED',
        'message': f'Subworkflow {named} failed with status FAILED',
        'sub_workflow_id': named,
        'sub_workflow_summary': {
            'status': 'FAILED',
            'error_summary': f'Subworkflow {named} failed with status FAILED',
        },
    }


def _unparsed_child_error(named: str) -> dict[str, object]:
    return {
        'error_code': 'RESULT_DESERIALIZATION_ERROR',
        'message': (
            f'Child workflow {named} is COMPLETED and its stored result does not '
            'parse: the result is NULL'
        ),
        'data': {'workflow_id': named},
    }


def _args_from_entry(result: dict[str, object]) -> dict[str, object]:
    """The entry that the engine writes for one ``args_from`` argument."""
    return {
        ARGS_FROM_MARKER: True,
        'source_task_name': 'child',
        'source_definition_key': 'tests.child.v1',
        'inner': result,
    }


def _task_input(run: Run, named: str) -> dict[str, object]:
    return {
        'value': 1,
        'upstream': _args_from_entry(_envelope(_skipped_error(named))),
        'unparsed': _args_from_entry(_envelope(_unparsed_child_error(named))),
        '__h_workflow_meta__': {'workflow_id': run.workflow, 'task_index': 2},
        '__h_workflow_ctx__': {
            'workflow_id': named,
            'task_index': 2,
            'task_name': 'consume',
            'results_by_id': {
                'source:0': _envelope(_skipped_error(named)),
                'child:1': _envelope(_child_failed_error(named)),
                'other:3': None,
            },
            'summaries_by_id': {
                'child:1': {'status': 'COMPLETED', 'error_summary': f'Workflow {named} ended'},
            },
            'task_name_by_id': {'source:0': 'produce'},
        },
    }


def _outputless_result(named: str) -> dict[str, object]:
    return {
        '__h_task_result__': True,
        '__h_outputless_terminals__': True,
        'ok': {'results_by_id': {'source:0': _envelope(_skipped_error(named))}},
        'err': None,
    }


def _documents(run: Run, named: str) -> dict[str, object]:
    """One stored document of each column rule, naming the workflow ``named``."""
    return {
        'input': _task_input(run, named),
        'workflow_error': _crash_error(run),
        'node_result': _envelope(_crash_error(run)),
        'child_node_result': _envelope(_child_failed_error(named)),
        'workflow_result': _envelope(_skipped_error(named)),
        'outputless_result': _outputless_result(named),
        'summary': {'status': 'COMPLETED', 'error_summary': f'Workflow {named} ended'},
    }


DOCUMENTS: Final = SnapshotCatalog(
    tables=(
        TableRules(
            name='documents',
            columns=(
                ColumnSpec(name='workflow', rule=WORKFLOW_ID),
                ColumnSpec(name='task', rule=TASK_ID),
                ColumnSpec(name='worker', rule=WORKER_ID),
                ColumnSpec(name='input', rule=TASK_INPUT),
                ColumnSpec(name='workflow_error', rule=WORKFLOW_ERROR),
                ColumnSpec(name='node_result', rule=NODE_RESULT),
                ColumnSpec(name='child_node_result', rule=NODE_RESULT),
                ColumnSpec(name='workflow_result', rule=WORKFLOW_RESULT),
                ColumnSpec(name='outputless_result', rule=WORKFLOW_RESULT),
                ColumnSpec(name='summary', rule=SUB_WORKFLOW_SUMMARY),
            ),
            order_by=('workflow',),
            identity=('workflow',),
        ),
    ),
)
"""A workflow, a task and a worker, then one document of each rule that holds them."""

DOCUMENT_COLUMNS: Final = tuple(spec.name for spec in DOCUMENTS.tables[0].columns[3:])


def _documents_snapshot(run: Run, named: str) -> NormalizedSnapshot:
    documents = _documents(run, named)
    row = (
        run.workflow,
        run.task,
        run.worker,
        *(json.dumps(documents[column]) for column in DOCUMENT_COLUMNS),
    )
    raw = RawSnapshot(tables=(RawTable(name='documents', rows=(row,)),))
    match normalize_snapshot(raw, DOCUMENTS, _window(run.start)):
        case Ok(snapshot):
            return snapshot
        case Err(error):
            raise AssertionError(f'normalize failed: {error}')


def test_library_ids_in_stored_documents_compare_by_their_labels() -> None:
    reference = _documents_snapshot(RUN_IDS_A, RUN_IDS_A.workflow)
    candidate = _documents_snapshot(RUN_IDS_B, RUN_IDS_B.workflow)

    assert compare_snapshots(reference, candidate).entries == ()


@pytest.mark.parametrize('column', DOCUMENT_COLUMNS)
def test_no_raw_library_id_is_left_in_a_stored_document(column: str) -> None:
    snapshot = _documents_snapshot(RUN_IDS_A, RUN_IDS_A.workflow)
    table = snapshot.table('documents')
    (row,) = table.rows

    rendered = render_value(row[table.rules.position(column)])

    for raw in (RUN_IDS_A.workflow, RUN_IDS_A.task, RUN_IDS_A.worker):
        assert raw not in rendered, f'{column}: {rendered}'


def test_a_document_that_names_another_workflow_is_a_difference() -> None:
    reference = _documents_snapshot(RUN_IDS_A, RUN_IDS_A.workflow)
    candidate = _documents_snapshot(RUN_IDS_B, PARENT_B)

    assert [
        entry.column
        for entry in compare_snapshots(reference, candidate).entries
        if isinstance(entry, ColumnDifference)
    ] == [
        'input',
        'child_node_result',
        'workflow_result',
        'outputless_result',
        'summary',
    ]


def test_crash_data_times_in_a_stored_error_are_run_clock_values() -> None:
    snapshot = _documents_snapshot(RUN_IDS_A, RUN_IDS_A.workflow)
    table = snapshot.table('documents')
    (row,) = table.rows

    # data keys sorted: detected_at, started_at, task_id, worker_id
    assert _leaves(row[table.rules.position('workflow_error')])[:4] == [
        DuringRun(),
        DuringRun(),
        IdLabel(family=IdFamily.TASK, ordinal=1),
        IdLabel(family=IdFamily.WORKER, ordinal=1),
    ]


def test_each_member_steps_into_every_member_of_an_object_and_no_array_item() -> None:
    rule = StoredJson(
        storage=JsonStorage.TEXT,
        format=Unversioned(),
        paths=(JsonPath(steps=(EachMember(), Key(name='id')), rule=WORKFLOW_ID),),
    )
    catalog = SnapshotCatalog(
        tables=(
            TableRules(
                name='maps',
                columns=(
                    ColumnSpec(name='name', rule=Exact()),
                    ColumnSpec(name='doc', rule=rule),
                ),
                order_by=('name',),
                identity=('name',),
            ),
        ),
    )
    rows = (
        ('object', json.dumps({'x': {'id': CHILD_A}, 'y': {'id': PARENT_A}})),
        ('array', json.dumps([{'id': CHILD_A}])),
    )

    match normalize_snapshot(
        RawSnapshot(tables=(RawTable(name='maps', rows=rows),)), catalog, _window(RUN_A)
    ):
        case Ok(snapshot):
            pass
        case Err(error):
            raise AssertionError(f'normalize failed: {error}')
    array_row, object_row = snapshot.table('maps').rows  # ordered by name

    assert _leaves(object_row[1]) == [
        IdLabel(family=IdFamily.WORKFLOW, ordinal=1),
        IdLabel(family=IdFamily.WORKFLOW, ordinal=2),
    ]
    assert _leaves(array_row[1]) == []


TASK_INPUTS: Final = SnapshotCatalog(
    tables=(
        TableRules(
            name='tasks',
            columns=(
                ColumnSpec(name='workflow', rule=WORKFLOW_ID),
                ColumnSpec(name='input', rule=TASK_INPUT),
            ),
            order_by=('workflow',),
            identity=('workflow',),
        ),
    ),
)
"""A workflow and the input of one of its tasks."""


def _tasks_snapshot(task_input: str, run: datetime, workflow: str) -> NormalizedSnapshot:
    raw = RawSnapshot(tables=(RawTable(name='tasks', rows=((workflow, task_input),)),))
    match normalize_snapshot(raw, TASK_INPUTS, _window(run)):
        case Ok(snapshot):
            return snapshot
        case Err(error):
            raise AssertionError(f'normalize failed: {error}')


def _task_cell(snapshot: NormalizedSnapshot, column: str) -> NormalizedValue:
    table = snapshot.table('tasks')
    (row,) = table.rows
    return row[table.rules.position(column)]


def test_an_input_member_without_the_args_from_marker_keeps_its_ids_raw() -> None:
    """A user argument that looks like an ``args_from`` entry, without the
    marker or with another value at it, is user data: it compares exactly."""

    def member(named: str, marker: object) -> str:
        entry = _args_from_entry(_envelope(_skipped_error(named)))
        match marker:
            case None:
                del entry[ARGS_FROM_MARKER]
            case _:
                entry[ARGS_FROM_MARKER] = marker
        return json.dumps({'lookalike': entry})

    for marker in (None, False, 'true', 1):
        reference = _tasks_snapshot(member(CHILD_A, marker), RUN_A, CHILD_A)
        candidate = _tasks_snapshot(member(CHILD_B, marker), RUN_B, CHILD_B)

        assert [
            entry.column
            for entry in compare_snapshots(reference, candidate).entries
            if isinstance(entry, ColumnDifference)
        ] == ['input'], marker
        rendered = render_value(_task_cell(reference, 'input'))
        assert CHILD_A in rendered, marker


def test_an_args_from_entry_that_names_another_workflow_is_a_difference() -> None:
    reference = _tasks_snapshot(
        json.dumps({'upstream': _args_from_entry(_envelope(_skipped_error(CHILD_A)))}),
        RUN_A,
        workflow=CHILD_A,
    )
    candidate = _tasks_snapshot(
        json.dumps({'upstream': _args_from_entry(_envelope(_skipped_error(PARENT_B)))}),
        RUN_B,
        workflow=CHILD_B,
    )
    same = _tasks_snapshot(
        json.dumps({'upstream': _args_from_entry(_envelope(_skipped_error(CHILD_B)))}),
        RUN_B,
        workflow=CHILD_B,
    )

    assert compare_snapshots(reference, same).entries == ()
    assert [
        entry.column
        for entry in compare_snapshots(reference, candidate).entries
        if isinstance(entry, ColumnDifference)
    ] == ['input']


def test_each_marked_member_steps_into_marked_object_members_only() -> None:
    rule = StoredJson(
        storage=JsonStorage.TEXT,
        format=Unversioned(),
        paths=(
            JsonPath(
                steps=(EachMarkedMember(marker='mark'), Key(name='id')),
                rule=WORKFLOW_ID,
            ),
        ),
    )
    catalog = SnapshotCatalog(
        tables=(
            TableRules(
                name='maps',
                columns=(
                    ColumnSpec(name='name', rule=Exact()),
                    ColumnSpec(name='doc', rule=rule),
                ),
                order_by=('name',),
                identity=('name',),
            ),
        ),
    )
    rows = (
        (
            'object',
            json.dumps(
                {
                    'x': {'mark': True, 'id': CHILD_A},
                    'y': {'mark': False, 'id': PARENT_A},
                    'z': {'id': PARENT_A},
                },
            ),
        ),
        ('array', json.dumps([{'mark': True, 'id': CHILD_A}])),
    )

    match normalize_snapshot(
        RawSnapshot(tables=(RawTable(name='maps', rows=rows),)), catalog, _window(RUN_A)
    ):
        case Ok(snapshot):
            pass
        case Err(error):
            raise AssertionError(f'normalize failed: {error}')
    array_row, object_row = snapshot.table('maps').rows  # ordered by name

    assert _leaves(object_row[1]) == [IdLabel(family=IdFamily.WORKFLOW, ordinal=1)]
    assert PARENT_A in render_value(object_row[1])
    assert _leaves(array_row[1]) == []


# ---- Attempt archive ----


def test_archive_times_and_worker_ids_are_normalized_field_by_field() -> None:
    snapshot = _snapshot([_row('a1', RUN_A)], RUN_A)

    leaves = _leaves(_cell(snapshot, 'archive'))
    assert leaves == [
        # started_at, finished_at, worker_id
        DuringRun(),
        DuringRun(),
        IdLabel(family=IdFamily.WORKER, ordinal=1),
        # enqueued_at, claimed_at, first_claimed_at, placed_at, finalizing_at
        # (not marked), last_seen_at (a worker's record)
        DuringRun(),
        DuringRun(),
        DuringRun(),
        DuringRun(),
        NullValue(),
        NullValue(),
        # the open attempt: first_claimed_at, finalizing_at
        DuringRun(),
        NullValue(),
    ]
    assert _cell(snapshot, 'worker') == IdLabel(family=IdFamily.WORKER, ordinal=1)


def test_archives_of_two_runs_compare_equal_and_a_changed_outcome_differs() -> None:
    reference = _snapshot([_row('a1', RUN_A)], RUN_A)
    candidate = _snapshot([_row('b1', RUN_B)], RUN_B)
    changed_archive = _archive('b1-worker', RUN_B).replace(
        b'"FAILED"', b'"WORKER_FAILURE"'
    )
    changed_row = _row('b1', RUN_B)
    changed = _snapshot(
        [
            (
                *changed_row[:5],
                _hex(changed_archive),
                _hex(hashlib.sha256(changed_archive).digest()),
                changed_row[7],
            )
        ],
        RUN_B,
    )

    assert compare_snapshots(reference, candidate).entries == ()
    assert [
        entry.column
        for entry in compare_snapshots(reference, changed).entries
        if isinstance(entry, ColumnDifference)
    ] == ['archive']


def test_an_archive_of_another_format_version_is_an_error() -> None:
    """Every move of the chain writes version 2, so a version-1 archive in a
    snapshot is an error, not a document read with the wrong paths."""
    outcome = _normalized([_row('a1', RUN_A, version='1')], RUN_A)

    assert outcome == Err(
        UnsupportedStoredFormat(
            table='runs',
            column='archive',
            version_column='attempt_archive_version',
            found='1',
        ),
    )


def test_archive_bytes_that_are_not_utf8_are_an_error() -> None:
    row = _row('a1', RUN_A)
    outcome = _normalized([(*row[:5], '\\xff00', *row[6:])], RUN_A)

    match outcome:
        case Err(UnreadableJson(column='archive')):
            pass
        case _:
            raise AssertionError(f'expected UnreadableJson for archive, got {outcome}')


# ---- Digests ----


def test_a_digest_of_its_source_matches() -> None:
    assert (
        _cell(_snapshot([_row('a1', RUN_A)], RUN_A), 'archive_digest')
        == DigestMatches()
    )


def test_a_digest_of_other_bytes_does_not_match() -> None:
    other = _hex(hashlib.sha256(b'other').digest())

    assert (
        _cell(_snapshot([_row('a1', RUN_A, digest=other)], RUN_A), 'archive_digest')
        == DigestMismatch()
    )


def test_a_digest_that_is_not_bytea_text_is_an_error() -> None:
    outcome = _normalized([_row('a1', RUN_A, digest='not hex')], RUN_A)

    assert outcome == Err(UnreadableBytes(table='runs', column='archive_digest'))


# ---- Generated names ----


@pytest.mark.parametrize(
    ('name', 'expected'),
    [
        pytest.param(
            'horsies_task_history_standard_30d_2026_01_02',
            DatedName(
                prefix='horsies_task_history_standard_30d',
                unit=CalendarUnit.DAY,
                offset=1,
            ),
            id='daily-next-day',
        ),
        pytest.param(
            'horsies_task_history_standard_30d_open_2026_01_01',
            DatedName(
                prefix='horsies_task_history_standard_30d_open',
                unit=CalendarUnit.DAY,
                offset=0,
            ),
            id='open-ended',
        ),
        pytest.param(
            'horsies_heartbeats_2026_01_01_09',
            DatedName(prefix='horsies_heartbeats', unit=CalendarUnit.HOUR, offset=-1),
            id='hourly-previous-hour',
        ),
        pytest.param(
            'horsies_task_history_forever_open',
            ExactValue(text='horsies_task_history_forever_open'),
            id='undated',
        ),
        pytest.param(
            'horsies_heartbeats_2026_02_30',
            ExactValue(text='horsies_heartbeats_2026_02_30'),
            id='invalid-date',
        ),
    ],
)
def test_generated_names_become_offsets_from_the_run_start(
    name: str, expected: NormalizedValue
) -> None:
    assert generated_name(name, _window(RUN_A)) == expected


def test_leaves_at_the_same_offset_from_two_runs_compare_equal_and_one_day_later_differs() -> (
    None
):
    reference = _snapshot([_row('a1', RUN_A)], RUN_A)
    candidate = _snapshot([_row('b1', RUN_B)], RUN_B)
    later = _snapshot(
        [
            _row(
                'b1',
                RUN_B,
                leaf=f'horsies_task_history_standard_30d_{RUN_B + timedelta(days=1):%Y_%m_%d}',
            )
        ],
        RUN_B,
    )

    assert compare_snapshots(reference, candidate).entries == ()
    assert [
        entry.column
        for entry in compare_snapshots(reference, later).entries
        if isinstance(entry, ColumnDifference)
    ] == ['leaf']
