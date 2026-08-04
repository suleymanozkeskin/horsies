# fused-completion-1mib-result

plain task completion at the result-size warning threshold

## Conditions

| | |
|---|---|
| measured | 2026-08-04T22:40:46+00:00 |
| server | PostgreSQL 16.14 (Debian 16.14-1.pgdg13+1) |
| compared | existing statement against database-owned operation |
| observations per side | 10000 |
| block size | 100 |
| pre-existing terminal rows | 100000 |
| result payload | 1048576 bytes |
| batch | not applicable |
| fsync | off |
| full_page_writes | off |
| synchronous_commit | off |
| autovacuum | off |
| bootstrap resamples | 1000 |
| bootstrap seed | 20260804 |
| demo units quiesced | yes |

## Latency

| percentile | baseline | candidate | delta | 95% interval | budget | verdict |
|---|---|---|---|---|---|---|
| p50 | 29.703 ms | 29.847 ms | +0.144 ms | +0.072 to +0.194 ms | 1.485 ms | PASS |
| p99 | 56.142 ms | 56.314 ms | +0.172 ms | -2.901 to +2.272 ms | 5.614 ms | PASS |

## Instrumented plan evidence

One eligible transition per side, executed with `EXPLAIN (ANALYZE, BUFFERS, WAL, TIMING OFF, FORMAT JSON)` and rolled back. These executions are excluded from the latency and server-count measurements.

| | baseline | candidate |
|---|---|---|
| node shape | `CTE Scan > LockRows > Index Scan(horsies_tasks) > ModifyTable(horsies_task_attempts) > CTE Scan > ModifyTable(horsies_tasks) > Nested Loop > CTE Scan > Index Scan(horsies_tasks)` | `Function Scan(horsies_complete_task_fused)` |
| shared hit blocks | 48 | 75 |
| shared read blocks | 0 | 0 |
| shared dirtied blocks | 1 | 2 |
| shared written blocks | 1 | 2 |
| WAL records | 25 | 30 |
| WAL full-page images | 0 | 0 |
| WAL bytes | 13919 | 14341 |

## Server counts

| | baseline | candidate |
|---|---|---|
| client statements | 10000 | 10000 |
| statements inside functions | 30000 | 60000 |
| write transactions | 10000 | 10000 |
| client rows | 10000 | 10000 |
| nested rows | 30000 | 60000 |
| terminal task rows | 10000 | 10000 |
| WAL records per terminal task row | 31.34 | 31.30 |
| WAL bytes per terminal task row | 14454 | 14473 |
| full-page images | 0 | 0 |

## Contract checks

Limits: client statements may not increase; write transactions and terminal task rows must match; WAL-record delta must be at most 0.100 per terminal task row; WAL-byte delta must be at most the greater of 10% or 128 bytes per terminal task row.

- PASS

Full-page writes are disabled in this disposable measurement environment, so checkpoint-dependent image bytes are absent from the WAL-byte comparison.

**Verdict: PASS**
