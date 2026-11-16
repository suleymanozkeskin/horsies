"""Classification of a database and the install decision for each state."""

from __future__ import annotations

import pytest

from horsies.core.baseline.install import (
    AppliedMigration,
    BaselineCurrent,
    ClientFamily,
    CompleteInstall,
    DatabaseState,
    EmptyDatabase,
    FamilyNotRecorded,
    FamilyRecorded,
    ForeignMigrations,
    InstallBaseline,
    MigrationEdited,
    MigrationIncomplete,
    ReaderPresence,
    ReadersMissing,
    ReadersPublished,
    RefuseInstall,
    SchemaNewer,
    SchemaReady,
    UnknownHorsiesObjects,
    ZeroXPythonDatabase,
    classify_database,
    decide_install,
    describe_state,
)

OWN = b'\x01' * 48
OTHER = b'\x02' * 48


def _classify(
    *,
    migrations_table: bool = True,
    schema_version_table: bool = True,
    tables: tuple[str, ...] = ('horsies_migrations', 'horsies_tasks'),
    applied: tuple[AppliedMigration, ...] = (AppliedMigration(1000, True, OWN),),
    family: FamilyRecorded | FamilyNotRecorded = FamilyRecorded(ClientFamily.PYTHON),
    readers: ReaderPresence = ReadersPublished(),
) -> DatabaseState:
    return classify_database(
        migrations_table=migrations_table,
        schema_version_table=schema_version_table,
        tables=tables,
        applied=applied,
        family=family,
        readers=readers,
        own_checksum=OWN,
    )


PYTHON = FamilyRecorded(ClientFamily.PYTHON)
RUST = FamilyRecorded(ClientFamily.RUST)


class TestClassifyDatabase:
    def test_no_horsies_table_is_empty(self) -> None:
        state = _classify(migrations_table=False, schema_version_table=False, tables=(), applied=())
        assert state == EmptyDatabase()

    def test_version_table_without_migrations_is_zero_x_python(self) -> None:
        tables = ('horsies_schema_version', 'horsies_tasks')
        state = _classify(migrations_table=False, tables=tables, applied=())
        assert state == ZeroXPythonDatabase(tables=tables)

    def test_tables_without_bookkeeping_are_unknown(self) -> None:
        state = _classify(
            migrations_table=False, schema_version_table=False, tables=('horsies_tasks',), applied=()
        )
        assert state == UnknownHorsiesObjects(tables=('horsies_tasks',))

    def test_version_above_the_baseline_is_newer(self) -> None:
        applied = (AppliedMigration(1001, True, OTHER), AppliedMigration(1000, True, OWN))
        assert _classify(applied=applied) == SchemaNewer(stored=1001, own=1000)

    @pytest.mark.parametrize(
        'versions',
        [(), (65,), (1000, 65)],
    )
    def test_bookkeeping_without_exactly_the_baseline_is_foreign(
        self,
        versions: tuple[int, ...],
    ) -> None:
        applied = tuple(AppliedMigration(version, True, OWN) for version in versions)
        assert _classify(applied=applied) == ForeignMigrations(versions=versions)

    def test_failed_baseline_row_is_incomplete(self) -> None:
        applied = (AppliedMigration(1000, False, OWN),)
        assert _classify(applied=applied) == MigrationIncomplete(version=1000)

    def test_other_checksum_is_edited(self) -> None:
        applied = (AppliedMigration(1000, True, OTHER),)
        assert _classify(applied=applied) == MigrationEdited(
            version=1000, stored_checksum=OTHER, own_checksum=OWN
        )

    def test_matching_baseline_is_current_with_its_family_and_readers(self) -> None:
        assert _classify(family=FamilyNotRecorded()) == BaselineCurrent(
            family=FamilyNotRecorded(), readers=ReadersPublished()
        )
        assert _classify() == BaselineCurrent(family=PYTHON, readers=ReadersPublished())
        assert _classify(readers=ReadersMissing()) == BaselineCurrent(
            family=PYTHON, readers=ReadersMissing()
        )


class TestDecideInstall:
    def test_empty_database_is_installed(self) -> None:
        assert decide_install(EmptyDatabase()) == InstallBaseline()

    def test_own_family_with_its_readers_is_ready(self) -> None:
        state = BaselineCurrent(family=PYTHON, readers=ReadersPublished())
        assert decide_install(state) == SchemaReady()

    @pytest.mark.parametrize(
        ('family', 'readers'),
        [
            (FamilyNotRecorded(), ReadersPublished()),
            (FamilyNotRecorded(), ReadersMissing()),
            (PYTHON, ReadersMissing()),
        ],
    )
    def test_missing_family_or_readers_complete_the_install(
        self,
        family: FamilyRecorded | FamilyNotRecorded,
        readers: ReaderPresence,
    ) -> None:
        state = BaselineCurrent(family=family, readers=readers)
        assert decide_install(state) == CompleteInstall()

    @pytest.mark.parametrize('readers', [ReadersPublished(), ReadersMissing()])
    def test_other_family_is_refused(self, readers: ReaderPresence) -> None:
        state = BaselineCurrent(family=RUST, readers=readers)
        match decide_install(state):
            case RefuseInstall(state=refused, cause=cause):
                assert refused == state
                assert 'installed by the RUST client' in cause
                assert 'this is the PYTHON client' in cause
            case other:
                pytest.fail(f'expected a refusal, got {other!r}')

    @pytest.mark.parametrize(
        ('state', 'expected'),
        [
            (ZeroXPythonDatabase(tables=('horsies_schema_version',)), 'horsies 0.x schema'),
            (UnknownHorsiesObjects(tables=('horsies_tasks',)), 'no schema bookkeeping'),
            (SchemaNewer(stored=1001, own=1000), 'version 1001 is newer than this build supports (1000)'),
            (ForeignMigrations(versions=(65,)), 'found: 65; this build ships 1000'),
            (ForeignMigrations(versions=()), 'found: none'),
            (MigrationIncomplete(version=1000), 'migration 1000 is recorded as failed'),
            (
                MigrationEdited(version=1000, stored_checksum=OTHER, own_checksum=OWN),
                f'stored checksum {OTHER.hex()}, this build {OWN.hex()}',
            ),
        ],
    )
    def test_foreign_states_are_refused_with_their_description(
        self,
        state: DatabaseState,
        expected: str,
    ) -> None:
        match decide_install(state):
            case RefuseInstall(state=refused, cause=cause):
                assert refused == state
                assert cause == describe_state(state)
                assert expected in cause
            case other:
                pytest.fail(f'expected a refusal, got {other!r}')
