"""Each command's database identity, checked against the statements it replaces.

Three properties, none visible by reading:

- every command has a function name and a kind, and no kind is stranded
  outside an equivalence class, so already-applied is decidable for every
  operation that can write a row;
- the kind vocabulary is frozen — these values are persisted, so the test
  pins the exact spellings rather than deriving them from code that can be
  renamed;
- a command carries a payload field exactly where its statement takes one
  from the caller, read off the statement text rather than declared here.

The last one is what stops a caller from supplying a result the database was
never going to read.
"""

from __future__ import annotations

import dataclasses
import re
from typing import get_args

import pytest

from horsies.core.lifecycle.commands import TerminalizationCommand
from horsies.core.lifecycle.operations import (
    EQUIVALENCE_CLASSES,
    TerminalizationKind,
    equivalence_class_of,
    function_name_of,
    is_already_applied,
    kind_of,
)
from tests.lifecycle_matrix import MATRIX
from tests.unit.test_lifecycle_commands import ONE_OF_EACH
from tests.unit.test_lifecycle_matrix import _STATEMENT_TEXT
from tests.unit.test_terminal_writer_inventory import _update_clauses

pytestmark = [pytest.mark.unit]

# Which command each writer becomes. Two failure writers share one command;
# every other writer maps alone. This is the only place the correspondence is
# written down, and the tests below read it rather than restating it.
COMMAND_BY_WRITER: dict[str, str] = {
    'T01': 'CancelLockedTask',
    'T02': 'AbandonOwnedNodes',
    'T03': 'CancelOwnedNodes',
    'T04': 'FailLockedTask',
    'T05': 'FailLockedTask',
    'T06': 'CompleteLockedTask',
    'T07': 'CompleteTaskFused',
    'T08': 'CancelOwnedOrphan',
    'T09': 'AbandonNodesOfPausedWorkflows',
    'T10': 'AbandonOwnedNode',
    'T11': 'CancelOwnedNode',
    'T12': 'ExpireOwnedClaim',
    'T13': 'FailStaleTask',
    'T14': 'ExpirePendingTasks',
    'T15': 'CancelOrphanedTasks',
    'T16': 'CancelNodesOfCancelledWorkflow',
}

# The persisted vocabulary. A row written today still has to mean this in a
# year, so the values are pinned here, not generated.
EXPECTED_KINDS: dict[str, TerminalizationKind] = {
    'CompleteLockedTask': TerminalizationKind.COMPLETE_LOCKED,
    'CompleteTaskFused': TerminalizationKind.COMPLETE_FUSED,
    'FailLockedTask': TerminalizationKind.FAIL_RUNNING,
    'FailStaleTask': TerminalizationKind.FAIL_STALE,
    'ExpireOwnedClaim': TerminalizationKind.EXPIRE_CLAIMED,
    'ExpirePendingTasks': TerminalizationKind.EXPIRE_PENDING,
    'CancelLockedTask': TerminalizationKind.CANCEL_ADMIN,
    'CancelOwnedOrphan': TerminalizationKind.CANCEL_ORPHAN,
    'CancelOrphanedTasks': TerminalizationKind.CANCEL_ORPHAN_SWEEP,
    'AbandonOwnedNode': TerminalizationKind.PAUSE_ABANDON_CLAIM,
    'AbandonOwnedNodes': TerminalizationKind.PAUSE_ABANDON_CLAIM_BATCH,
    'AbandonNodesOfPausedWorkflows': TerminalizationKind.PAUSE_ABANDON_WORKFLOW,
    'CancelOwnedNode': TerminalizationKind.WORKFLOW_CANCEL_CLAIM,
    'CancelOwnedNodes': TerminalizationKind.WORKFLOW_CANCEL_CLAIM_BATCH,
    'CancelNodesOfCancelledWorkflow': TerminalizationKind.WORKFLOW_CANCEL_WORKFLOW,
}

# Fields that carry a value the caller supplies into the row's payload
# columns. Anything else on a command is fence, target, or a bound.
_PAYLOAD_FIELDS: frozenset[str] = frozenset({
    'result_json', 'error_code', 'failed_reason',
})

_PAYLOAD_COLUMN_OF_FIELD: dict[str, str] = {
    'result_json': 'result',
    'error_code': 'error_code',
    'failed_reason': 'failed_reason',
}


def _command_by_name() -> dict[str, TerminalizationCommand]:
    return {type(command).__name__: command for command in ONE_OF_EACH}


def _caller_supplied_payload_columns(writer_id: str) -> set[str]:
    """Payload columns this statement takes from its caller.

    A literal in the SET clause is the statement's own; only a bound parameter
    is something the caller chose and a command therefore has to carry.
    """
    assignments = ' '.join(
        ' '.join(window.split()) for window, _ in _update_clauses(_STATEMENT_TEXT[writer_id])
    )
    supplied: set[str] = set()
    for column in ('result', 'error_code', 'failed_reason'):
        match = re.search(rf'(?<![a-z_]){column}\s*=\s*([^,]+?)(?:,|$)', assignments)
        if match is None:
            continue
        value = match.group(1).strip()
        if value.startswith(':') or value.startswith('%s') or value == '%s':
            supplied.add(column)
    return supplied


class TestEveryCommandHasADatabaseIdentity:
    def test_function_name_is_derived_from_the_variant(self) -> None:
        """A variant without a function cannot ship unnoticed."""
        expected = {
            'CompleteLockedTask': 'horsies_complete_locked_task',
            'CompleteTaskFused': 'horsies_complete_task_fused',
            'FailLockedTask': 'horsies_fail_locked_task',
            'FailStaleTask': 'horsies_fail_stale_task',
            'ExpireOwnedClaim': 'horsies_expire_owned_claim',
            'ExpirePendingTasks': 'horsies_expire_pending_tasks',
            'CancelLockedTask': 'horsies_cancel_locked_task',
            'CancelOwnedOrphan': 'horsies_cancel_owned_orphan',
            'CancelOrphanedTasks': 'horsies_cancel_orphaned_tasks',
            'AbandonOwnedNode': 'horsies_abandon_owned_node',
            'AbandonOwnedNodes': 'horsies_abandon_owned_nodes',
            'AbandonNodesOfPausedWorkflows': (
                'horsies_abandon_nodes_of_paused_workflows'
            ),
            'CancelOwnedNode': 'horsies_cancel_owned_node',
            'CancelOwnedNodes': 'horsies_cancel_owned_nodes',
            'CancelNodesOfCancelledWorkflow': (
                'horsies_cancel_nodes_of_cancelled_workflow'
            ),
        }
        actual = {
            name: function_name_of(command)
            for name, command in _command_by_name().items()
        }
        assert actual == expected

    def test_function_names_are_distinct(self) -> None:
        names = [function_name_of(command) for command in ONE_OF_EACH]
        assert len(set(names)) == len(names)

    def test_function_names_fit_postgres_identifier_length(self) -> None:
        """Over 63 bytes and PostgreSQL truncates, silently colliding names."""
        for command in ONE_OF_EACH:
            name = function_name_of(command)
            assert len(name.encode('utf-8')) <= 63, name

    def test_kind_of_matches_the_frozen_vocabulary(self) -> None:
        actual = {
            name: kind_of(command)
            for name, command in _command_by_name().items()
        }
        assert actual == EXPECTED_KINDS

    def test_every_variant_has_a_distinct_kind(self) -> None:
        """Cardinality is provenance too: a batch pause is not a single pause.

        Equivalence is expressed by the class table, never by two commands
        committing the same value — which would erase which writer ran.
        """
        kinds = [kind_of(command) for command in ONE_OF_EACH]
        assert len(set(kinds)) == len(kinds)

    def test_kind_vocabulary_is_exactly_what_the_commands_write(self) -> None:
        variants = get_args(TerminalizationCommand.__value__)
        assert len(set(TerminalizationKind)) == len(variants)
        assert {kind_of(c) for c in ONE_OF_EACH} == set(TerminalizationKind)


class TestEquivalenceClasses:
    def test_classes_partition_the_vocabulary(self) -> None:
        """Every kind in exactly one class — no overlap, nothing stranded."""
        members = [kind for members in EQUIVALENCE_CLASSES for kind in members]
        assert sorted(m.value for m in members) == sorted(
            k.value for k in TerminalizationKind
        )
        assert len(members) == len(set(members))

    def test_every_kind_resolves_to_its_class(self) -> None:
        for kind in TerminalizationKind:
            assert kind in equivalence_class_of(kind)

    def test_cancellation_families_do_not_conflate(self) -> None:
        """Five operations write CANCELLED; only same-family ones replay.

        An orphan sweep finding a workflow cancellation's row must be told the
        state conflicts — its coupled node write did not happen.
        """
        assert not is_already_applied(
            requested=TerminalizationKind.CANCEL_ORPHAN,
            committed=TerminalizationKind.WORKFLOW_CANCEL_CLAIM,
        )
        assert not is_already_applied(
            requested=TerminalizationKind.PAUSE_ABANDON_CLAIM,
            committed=TerminalizationKind.WORKFLOW_CANCEL_CLAIM,
        )
        assert not is_already_applied(
            requested=TerminalizationKind.CANCEL_ADMIN,
            committed=TerminalizationKind.CANCEL_ORPHAN,
        )

    def test_same_family_across_cardinality_is_already_applied(self) -> None:
        assert is_already_applied(
            requested=TerminalizationKind.PAUSE_ABANDON_CLAIM,
            committed=TerminalizationKind.PAUSE_ABANDON_WORKFLOW,
        )
        assert is_already_applied(
            requested=TerminalizationKind.COMPLETE_LOCKED,
            committed=TerminalizationKind.COMPLETE_FUSED,
        )
        assert is_already_applied(
            requested=TerminalizationKind.EXPIRE_CLAIMED,
            committed=TerminalizationKind.EXPIRE_PENDING,
        )

    def test_failure_and_stale_recovery_do_not_conflate(self) -> None:
        """Different events, so a replay is told the state conflicts."""
        assert not is_already_applied(
            requested=TerminalizationKind.FAIL_RUNNING,
            committed=TerminalizationKind.FAIL_STALE,
        )

    def test_unknown_provenance_is_never_already_applied(self) -> None:
        """A row from before the kind column proves nothing about who won."""
        for kind in TerminalizationKind:
            assert not is_already_applied(requested=kind, committed=None)


class TestPayloadFieldsMatchTheStatements:
    """A command carries payload exactly where its statement takes one."""

    def test_every_writer_maps_to_a_command(self) -> None:
        assert set(COMMAND_BY_WRITER) == {row.writer_id for row in MATRIX}
        assert set(COMMAND_BY_WRITER.values()) == set(_command_by_name())

    @pytest.mark.parametrize('writer_id', sorted(COMMAND_BY_WRITER))
    def test_command_carries_the_payload_its_statement_takes(
        self,
        writer_id: str,
    ) -> None:
        """Read off the SQL, so changing the SQL changes the requirement.

        A field the statement does not bind is data the database would ignore;
        a bound parameter with no field is data the command cannot supply.
        """
        command = _command_by_name()[COMMAND_BY_WRITER[writer_id]]
        fields = {
            field.name
            for field in dataclasses.fields(command)
            if field.name in _PAYLOAD_FIELDS
        }
        declared_columns = {_PAYLOAD_COLUMN_OF_FIELD[name] for name in fields}
        supplied = _caller_supplied_payload_columns(writer_id)
        assert supplied <= declared_columns, (
            f'{writer_id} binds {sorted(supplied - declared_columns)} that '
            f'{type(command).__name__} cannot supply'
        )

    def test_no_command_carries_payload_no_statement_binds(self) -> None:
        """The union of a command's writers must justify each payload field."""
        supplied_by_command: dict[str, set[str]] = {}
        for writer_id, command_name in COMMAND_BY_WRITER.items():
            supplied_by_command.setdefault(command_name, set()).update(
                _caller_supplied_payload_columns(writer_id)
            )
        for command_name, command in _command_by_name().items():
            declared = {
                _PAYLOAD_COLUMN_OF_FIELD[field.name]
                for field in dataclasses.fields(command)
                if field.name in _PAYLOAD_FIELDS
            }
            assert declared == supplied_by_command[command_name], (
                f'{command_name} declares {sorted(declared)} but its writers '
                f'bind {sorted(supplied_by_command[command_name])}'
            )

    def test_workflow_cancellation_commands_carry_no_payload(self) -> None:
        """Their statements write no error code or reason at all.

        Which is why provenance for that family lives in the committed kind
        and nowhere else — there is no marker on the row to infer it from.
        """
        by_name = _command_by_name()
        for command_name in (
            'CancelOwnedNode',
            'CancelOwnedNodes',
            'CancelNodesOfCancelledWorkflow',
        ):
            fields = {f.name for f in dataclasses.fields(by_name[command_name])}
            assert not (fields & _PAYLOAD_FIELDS), command_name
