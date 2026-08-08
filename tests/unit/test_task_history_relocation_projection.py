"""The relocation projection never routes a row to a null partition.

The history table is LIST-partitioned on retention_class_key, so that
column is the one projection entry where an absent source value is not
a degraded record but an aborted batch. This pins the coalesce, its
target, and the reason the target is forever rather than the finite
default: the class drives deletion, and a deployment that recorded no
policy must not acquire one by relocation.
"""

from __future__ import annotations

import pytest

from horsies.core.history.cutover.relocation import relocation_insert_sql
from horsies.core.history.ddl.classes import DEFAULT_RETENTION_CLASS_KEY
from horsies.core.history.ddl.tables import FOREVER_CLASS_KEY

pytestmark = [pytest.mark.unit]


class TestRetentionClassProjection:
    def test_absent_class_projects_as_forever(self) -> None:
        assert (
            f"COALESCE(t.retention_class_key, '{FOREVER_CLASS_KEY}')"
            in relocation_insert_sql()
        )

    def test_absent_class_never_projects_as_the_finite_default(self) -> None:
        # The finite default would put every legacy row past its
        # duration on the drop path at the first retention pass.
        assert (
            f"COALESCE(t.retention_class_key, '{DEFAULT_RETENTION_CLASS_KEY}')"
            not in relocation_insert_sql()
        )

    def test_the_column_is_never_projected_bare(self) -> None:
        # A bare projection is what routes NULL at the partition and
        # aborts the batch; the coalesce is the whole guard.
        sql = relocation_insert_sql()
        bare = 't.retention_class_key'
        guarded = f"COALESCE(t.retention_class_key, '{FOREVER_CLASS_KEY}')"
        assert sql.count(bare) == sql.count(guarded)
