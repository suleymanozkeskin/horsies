"""Names of the outcome row shape and the history-aware miss classifier.

One composite row shape every terminal operation returns, one decoder on
the client side, and one miss-classification order: absent, then
already-applied, then foreign terminalization, then lost claim, then
conflict. A terminal task is a history row, so the miss path classifies
through the staged provenance lookup; the live table cannot hold terminal
rows (CHECK ``horsies_tasks_status_live``). The type and the function are
SQL in the baseline migration.
"""

from __future__ import annotations

from typing import Final

OUTCOME_TYPE: Final = 'horsies_terminalization_outcome'
MISS_CLASSIFIER_FUNCTION: Final = 'horsies_terminalization_miss'
