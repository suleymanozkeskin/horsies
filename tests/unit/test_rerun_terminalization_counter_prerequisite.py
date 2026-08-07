"""A bench without statement counters must say so, not fail four times over.

`CREATE EXTENSION pg_stat_statements` succeeds on a server that never loaded
the library, and the failure then surfaces later as an error from the view.
Running the driver against such a bench produced four unexplained failures in
tests about a measurement, when the one true fact was that the environment
could not count statements at all.

The classifier is a pure function so both unusable states can be pinned without
standing up a second server in each configuration.
"""

from __future__ import annotations

import pytest

from tests.task_history_prototypes.rerun_terminalization_evidence import (
    CounterAvailability,
    classify_statement_counters,
)


class TestClassifierNamesTheMissingPrerequisite:
    def test_preloaded_and_created_is_available(self) -> None:
        assert (
            classify_statement_counters(
                shared_preload_libraries='pg_stat_statements',
                extension_present=True,
            )
            is CounterAvailability.AVAILABLE
        )

    def test_empty_preload_is_named_not_preloaded(self) -> None:
        # The shape the shared bench reports.
        assert (
            classify_statement_counters(
                shared_preload_libraries='',
                extension_present=False,
            )
            is CounterAvailability.NOT_PRELOADED
        )

    def test_preload_is_checked_before_the_extension(self) -> None:
        # An extension created on a server that never loaded the library looks
        # equipped and is not. Reporting it as "extension absent" would send a
        # reader to create something that already exists.
        assert (
            classify_statement_counters(
                shared_preload_libraries='',
                extension_present=True,
            )
            is CounterAvailability.NOT_PRELOADED
        )

    def test_preloaded_without_the_extension_is_named_separately(self) -> None:
        assert (
            classify_statement_counters(
                shared_preload_libraries='pg_stat_statements',
                extension_present=False,
            )
            is CounterAvailability.EXTENSION_ABSENT
        )

    @pytest.mark.parametrize(
        'setting',
        (
            'pg_stat_statements',
            'pg_stat_statements,auto_explain',
            'auto_explain, pg_stat_statements',
            ' pg_stat_statements , pgaudit ',
        ),
    )
    def test_library_is_found_among_others_in_any_spacing(
        self,
        setting: str,
    ) -> None:
        assert (
            classify_statement_counters(
                shared_preload_libraries=setting,
                extension_present=True,
            )
            is CounterAvailability.AVAILABLE
        )

    @pytest.mark.parametrize(
        'setting',
        ('', 'auto_explain', 'pgaudit,auto_explain', '   '),
    )
    def test_absent_library_is_never_read_as_present(
        self,
        setting: str,
    ) -> None:
        assert (
            classify_statement_counters(
                shared_preload_libraries=setting,
                extension_present=True,
            )
            is CounterAvailability.NOT_PRELOADED
        )

    def test_a_similarly_named_library_does_not_satisfy_the_requirement(
        self,
    ) -> None:
        # Substring matching would accept this and then fail at the first read.
        assert (
            classify_statement_counters(
                shared_preload_libraries='pg_stat_statements_extra',
                extension_present=True,
            )
            is CounterAvailability.NOT_PRELOADED
        )


class TestUnusableStatesCarryAReasonName:
    @pytest.mark.parametrize(
        'availability',
        (
            CounterAvailability.NOT_PRELOADED,
            CounterAvailability.EXTENSION_ABSENT,
        ),
    )
    def test_reason_name_says_it_is_the_environment(
        self,
        availability: CounterAvailability,
    ) -> None:
        assert availability.value.startswith('environment_lacks_')

    def test_available_is_not_an_environment_complaint(self) -> None:
        assert not CounterAvailability.AVAILABLE.value.startswith(
            'environment_lacks_'
        )
