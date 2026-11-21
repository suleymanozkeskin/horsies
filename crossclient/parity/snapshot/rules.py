"""Column rules of the snapshot catalog, and the checks of a catalog's own shape.

Every captured column has exactly one rule. The rule decides how a stored
value becomes a normalized value, and how two normalized values compare.
"""

from __future__ import annotations

import re
from collections import Counter
from dataclasses import dataclass
from datetime import timedelta
from enum import Enum
from typing import Final


class IdFamily(Enum):
    """Name space of a label. A label reads ``<family>#<ordinal>``."""

    TASK = 'task'
    WORKFLOW = 'workflow'
    WORKFLOW_NODE = 'workflow_node'
    WORKER = 'worker'
    COMMAND_FINGERPRINT = 'command_fingerprint'
    RECOVERY_PASS = 'recovery_pass'
    RECOVERY_CLAIM = 'recovery_claim'
    SCAN_POSITION = 'scan_position'


@dataclass(frozen=True, slots=True)
class Exact:
    """Compared as stored: the text form of the value under the capture session settings."""


@dataclass(frozen=True, slots=True)
class NormalisedId:
    """An opaque value replaced by a label.

    One raw value keeps one label in the whole snapshot, in every column
    that holds it. A new raw value gets the next ordinal of ``family``;
    order of first appearance is the catalog's table order, then the
    table's ``order_by`` order, then column order.
    """

    family: IdFamily


@dataclass(frozen=True, slots=True)
class TimeOrderOnly:
    """A clock value replaced by its dense rank among the non-null values of its column in its table."""


@dataclass(frozen=True, slots=True)
class TimeOrderBeforeRun:
    """A clock value that the run can write, in a snapshot whose rows before
    the run start were written in an order the scenario sets.

    A finite value at or after the run start becomes the token ``DuringRun``:
    the order of two values that the run writes follows run events (which
    child ended first), which no code sets. Any other value is replaced by
    its dense rank among the values of its column in its table that are not
    ``DuringRun``.
    """


@dataclass(frozen=True, slots=True)
class RunWindowReference:
    """Clock values compared against the run window.

    A value inside [run start - bound, run end + bound] becomes the token
    ``DuringRun``. A value outside that range stays literal and compares
    exactly, so a fixed test time still compares exactly.
    """


class AnchorNull(Enum):
    """What an anchored clock value becomes when its anchor is NULL."""

    REFUSED = 'the capture fails with UnanchoredTime'
    """No row of the column sets the value without its anchor."""

    RANKED = 'the value is ranked among the values of its column whose anchor is NULL'
    """A row can hold the value without its anchor: a test seeds a lease
    (``claim_expires_at``) on a row it never claimed (``claimed_at`` NULL).
    The row does not carry the time the value came from, so only its order
    among such values compares, as under ``TimeOrderOnly``."""


@dataclass(frozen=True, slots=True)
class AnchorReference:
    """Clock values compared as an offset from another clock column of the same row.

    Two offsets are equal when they differ by at most the bound. A value
    whose anchor is NULL follows ``when_anchor_null``; a ranked value never
    equals an offset.
    """

    column: str
    when_anchor_null: AnchorNull


TimeReference = RunWindowReference | AnchorReference


@dataclass(frozen=True, slots=True)
class TimeWithin:
    """A clock value compared within ``bound`` of its reference."""

    reference: TimeReference
    bound: timedelta


@dataclass(frozen=True, slots=True)
class Ignored:
    """Not compared. ``reason`` states why the column carries no behaviour to compare."""

    reason: str


# ---- Stored JSON ----


@dataclass(frozen=True, slots=True)
class Key:
    """A path step into an object member."""

    name: str


@dataclass(frozen=True, slots=True)
class Index:
    """A path step into one array element, from 0."""

    position: int


@dataclass(frozen=True, slots=True)
class EachItem:
    """A path step into every element of an array."""


@dataclass(frozen=True, slots=True)
class EachMember:
    """A path step into every member of an object, whatever its key: a map
    whose keys are data (node ids), not names of the format."""


@dataclass(frozen=True, slots=True)
class EachMarkedMember:
    """A path step into every member of an object whose value is an object
    that holds ``marker`` with the value ``true``, whatever its key.

    The keys are names that the user gives, and the marker names the members
    that the library writes among them: an ``args_from`` entry of a task
    input (``__h_taskresult_envelope__``). A member without the marker, or
    with another value at the marker, does not match: it is user data and
    compares exactly.
    """

    marker: str


PathStep = Key | Index | EachItem | EachMember | EachMarkedMember


class TimeEncoding(Enum):
    """How a JSON document stores a clock value."""

    ISO_TEXT = 'ISO 8601 text with a UTC offset'
    EPOCH_MICROSECONDS = 'integer microseconds since 1970-01-01 UTC'


@dataclass(frozen=True, slots=True)
class JsonTime:
    """A clock value inside a JSON document.

    ``rule`` applies as it does to a column. ``TimeOrderOnly`` ranks the
    values of one path in one column. An anchor is not available inside a
    document.
    """

    encoding: TimeEncoding
    rule: TimeOrderOnly | TimeWithin


CANONICAL_UUID: Final = re.compile(
    r'(?<![0-9A-Za-z-])'
    r'[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}'
    r'(?![0-9A-Za-z-])',
)
"""An id inside a text: the text form of a uuid column. ``IdsInText`` finds
the ids of a text with it, and ``catalog.notification_row`` the task id of a
retry wake."""


@dataclass(frozen=True, slots=True)
class IdsInText:
    """A text that names ids of ``family`` in their canonical UUID form.

    Each lower-case canonical UUID inside the text (``8-4-4-4-12`` hex
    digits, with no letter, digit or hyphen next to it) is an id of
    ``family``: it takes its label from the label book of the snapshot, as
    an id in a column does. The text around the ids compares exactly.
    """

    family: IdFamily


@dataclass(frozen=True, slots=True)
class RunMeasure:
    """A number inside a stored text that the run measures or that the
    operating system assigns (a duration, a process id), not a decision of
    the code.

    ``pattern`` is a regular expression with exactly one group, which matches
    the decimal digits of the number; the text around the group names the
    message that holds it. Each match's group becomes the token ``<name>``.
    """

    name: str
    pattern: str


@dataclass(frozen=True, slots=True)
class RunMeasuresInText:
    """A text in which each match of ``measures`` becomes its token; the text
    around the tokens compares exactly. A text with no match compares
    exactly."""

    measures: tuple[RunMeasure, ...]


JsonLeafRule = NormalisedId | IdsInText | JsonTime | Ignored | RunMeasuresInText


@dataclass(frozen=True, slots=True)
class JsonPath:
    """The rule of the values at one path of a JSON document."""

    steps: tuple[PathStep, ...]
    rule: JsonLeafRule


class JsonStorage(Enum):
    """How a column stores a JSON document."""

    TEXT = 'text'
    """A text, varchar or jsonb column."""

    UTF8_BYTES = 'utf-8 bytes'
    """A bytea column that holds UTF-8 JSON text."""


@dataclass(frozen=True, slots=True)
class FormatVersion:
    """The document is read only when ``column`` of the same row holds ``version``."""

    column: str
    version: int


@dataclass(frozen=True, slots=True)
class Unversioned:
    """The column has no format version column."""


@dataclass(frozen=True, slots=True)
class StoredJson:
    """A stored JSON document, parsed and compared as JSON.

    Key order and white space do not count. The values at ``paths`` get
    their own rule; every other value compares exactly.
    """

    storage: JsonStorage
    format: FormatVersion | Unversioned
    paths: tuple[JsonPath, ...]


@dataclass(frozen=True, slots=True)
class GeneratedName:
    """A relation name that can end in ``_YYYY_MM_DD`` or ``_YYYY_MM_DD_HH`` (UTC).

    The date or hour becomes an offset from the day or hour of the run
    start. A name without such an ending compares exactly.
    """


@dataclass(frozen=True, slots=True)
class DigestOf:
    """A SHA-256 digest of the bytea column ``column`` of the same row.

    Normalized to whether it matches, because the source column compares
    by its own rule.
    """

    column: str


ColumnRule = (
    Exact
    | NormalisedId
    | TimeOrderOnly
    | TimeOrderBeforeRun
    | TimeWithin
    | Ignored
    | StoredJson
    | GeneratedName
    | DigestOf
)


@dataclass(frozen=True, slots=True)
class ColumnSpec:
    """One captured column and its rule."""

    name: str
    rule: ColumnRule


@dataclass(frozen=True, slots=True)
class TableRules:
    """The rules of one table.

    ``order_by``: columns whose normalized values order the rows before
    labels are assigned (the natural key). ``identity``: columns whose
    normalized values pair a reference row with a candidate row.
    """

    name: str
    columns: tuple[ColumnSpec, ...]
    order_by: tuple[str, ...]
    identity: tuple[str, ...]

    def position(self, column: str) -> int:
        """Index of ``column`` in ``columns``. The column must exist."""
        for index, spec in enumerate(self.columns):
            if spec.name == column:
                return index
        raise AssertionError(f'table {self.name} has no column {column!r} in its rules')


@dataclass(frozen=True, slots=True)
class SnapshotCatalog:
    """The rules of every captured table, in labelling order."""

    tables: tuple[TableRules, ...]


# ---- Catalog shape defects ----


class KeyRole(Enum):
    """Which key of a table a defect concerns."""

    ORDER_BY = 'order_by'
    IDENTITY = 'identity'


@dataclass(frozen=True, slots=True)
class DuplicateTable:
    """Two entries of the catalog name the same table."""

    table: str


@dataclass(frozen=True, slots=True)
class DuplicateColumn:
    """Two rules of one table name the same column."""

    table: str
    column: str


@dataclass(frozen=True, slots=True)
class EmptyKey:
    """A table has no column in a key."""

    table: str
    role: KeyRole


@dataclass(frozen=True, slots=True)
class KeyColumnUnknown:
    """A key names a column that has no rule in its table."""

    table: str
    role: KeyRole
    column: str


@dataclass(frozen=True, slots=True)
class KeyColumnNotComparable:
    """A key names a column whose rule gives no value to order or pair by (ignored or anchored)."""

    table: str
    role: KeyRole
    column: str


@dataclass(frozen=True, slots=True)
class ReferencedColumnUnknown:
    """A rule names another column (anchor, format version, digest source) that has no rule in its table."""

    table: str
    column: str
    referenced: str


@dataclass(frozen=True, slots=True)
class EmptyJsonPath:
    """A JSON path rule has no step."""

    table: str
    column: str


@dataclass(frozen=True, slots=True)
class AnchoredJsonTime:
    """A JSON clock rule names an anchor, which a document does not have."""

    table: str
    column: str


@dataclass(frozen=True, slots=True)
class EmptyIgnoreReason:
    """An ignored column has no written reason."""

    table: str
    column: str


@dataclass(frozen=True, slots=True)
class NegativeBound:
    """A time rule has a bound below zero."""

    table: str
    column: str


@dataclass(frozen=True, slots=True)
class InvalidMeasurePattern:
    """A run measure has no name, or a pattern that does not compile with exactly one group."""

    table: str
    column: str
    measure: str


CatalogDefect = (
    DuplicateTable
    | DuplicateColumn
    | EmptyKey
    | KeyColumnUnknown
    | KeyColumnNotComparable
    | ReferencedColumnUnknown
    | EmptyIgnoreReason
    | NegativeBound
    | EmptyJsonPath
    | AnchoredJsonTime
    | InvalidMeasurePattern
)


def is_key_comparable(rule: ColumnRule) -> bool:
    """Whether a rule gives values that can order and pair rows by plain equality."""
    match rule:
        case (
            Exact()
            | NormalisedId()
            | TimeOrderOnly()
            | TimeOrderBeforeRun()
            | StoredJson()
            | GeneratedName()
            | DigestOf()
        ):
            return True
        case TimeWithin(reference=reference):
            match reference:
                case RunWindowReference():
                    return True
                case AnchorReference():
                    return False
        case Ignored():
            return False


def _key_defects(
    table: TableRules, role: KeyRole, key: tuple[str, ...]
) -> list[CatalogDefect]:
    if not key:
        return [EmptyKey(table=table.name, role=role)]
    rules = {spec.name: spec.rule for spec in table.columns}
    defects: list[CatalogDefect] = []
    for column in key:
        match rules.get(column):
            case None:
                defects.append(
                    KeyColumnUnknown(table=table.name, role=role, column=column)
                )
            case rule if not is_key_comparable(rule):
                defects.append(
                    KeyColumnNotComparable(table=table.name, role=role, column=column)
                )
            case _:
                pass
    return defects


def _time_defects(
    table: str, column: str, rule: TimeWithin, names: set[str]
) -> list[CatalogDefect]:
    defects: list[CatalogDefect] = []
    if rule.bound < timedelta(0):
        defects.append(NegativeBound(table=table, column=column))
    match rule.reference:
        case AnchorReference(column=anchor) if anchor not in names:
            defects.append(
                ReferencedColumnUnknown(table=table, column=column, referenced=anchor)
            )
        case AnchorReference() | RunWindowReference():
            pass
    return defects


def _measure_fits(measure: RunMeasure) -> bool:
    """Whether ``measure`` has a name and a pattern that compiles with exactly one group."""
    if not measure.name.strip():
        return False
    try:
        return re.compile(measure.pattern).groups == 1
    except re.error:
        return False


def _path_defects(table: str, column: str, path: JsonPath) -> list[CatalogDefect]:
    defects: list[CatalogDefect] = []
    if not path.steps:
        defects.append(EmptyJsonPath(table=table, column=column))
    match path.rule:
        case RunMeasuresInText(measures=measures):
            defects.extend(
                InvalidMeasurePattern(table=table, column=column, measure=measure.name)
                for measure in measures
                if not _measure_fits(measure)
            )
        case Ignored(reason=reason) if not reason.strip():
            defects.append(EmptyIgnoreReason(table=table, column=column))
        case JsonTime(rule=TimeWithin() as rule):
            match rule.reference:
                case AnchorReference():
                    defects.append(AnchoredJsonTime(table=table, column=column))
                case RunWindowReference():
                    pass
            if rule.bound < timedelta(0):
                defects.append(NegativeBound(table=table, column=column))
        case Ignored() | JsonTime() | NormalisedId() | IdsInText():
            pass
    return defects


def _json_defects(
    table: str, column: str, rule: StoredJson, names: set[str]
) -> list[CatalogDefect]:
    defects: list[CatalogDefect] = []
    match rule.format:
        case FormatVersion(column=version_column) if version_column not in names:
            defects.append(
                ReferencedColumnUnknown(
                    table=table, column=column, referenced=version_column
                ),
            )
        case FormatVersion() | Unversioned():
            pass
    for path in rule.paths:
        defects.extend(_path_defects(table, column, path))
    return defects


def _column_defects(
    table: TableRules, spec: ColumnSpec, names: set[str]
) -> list[CatalogDefect]:
    match spec.rule:
        case Ignored(reason=reason):
            if reason.strip():
                return []
            return [EmptyIgnoreReason(table=table.name, column=spec.name)]
        case TimeWithin() as rule:
            return _time_defects(table.name, spec.name, rule, names)
        case StoredJson() as rule:
            return _json_defects(table.name, spec.name, rule, names)
        case DigestOf(column=source) if source not in names:
            return [
                ReferencedColumnUnknown(
                    table=table.name, column=spec.name, referenced=source
                )
            ]
        case (
            Exact()
            | NormalisedId()
            | TimeOrderOnly()
            | TimeOrderBeforeRun()
            | GeneratedName()
            | DigestOf()
        ):
            return []


def catalog_defects(catalog: SnapshotCatalog) -> tuple[CatalogDefect, ...]:
    """Every shape defect of a catalog. Pure; an empty result means the catalog is usable."""
    defects: list[CatalogDefect] = []
    table_counts = Counter(table.name for table in catalog.tables)
    defects.extend(
        DuplicateTable(table=name) for name, count in table_counts.items() if count > 1
    )
    for table in catalog.tables:
        column_counts = Counter(spec.name for spec in table.columns)
        defects.extend(
            DuplicateColumn(table=table.name, column=name)
            for name, count in column_counts.items()
            if count > 1
        )
        names = set(column_counts)
        for spec in table.columns:
            defects.extend(_column_defects(table, spec, names))
        defects.extend(_key_defects(table, KeyRole.ORDER_BY, table.order_by))
        defects.extend(_key_defects(table, KeyRole.IDENTITY, table.identity))
    return tuple(defects)
