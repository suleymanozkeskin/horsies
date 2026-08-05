# fused-completion-small-result

plain task completion, one statement, small result

## Conditions

| | |
|---|---|
| measured | 2026-08-05T00:18:50+00:00 |
| server | PostgreSQL 14.23 (Debian 14.23-1.pgdg13+1) |
| compared | existing statement against database-owned operation |
| observations per side | 10000 |
| block size | 100 |
| pre-existing terminal rows | 100000 |
| result payload | 200 bytes |
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
| p50 | 2.412 ms | 2.573 ms | +0.161 ms | +0.158 to +0.165 ms | 0.200 ms | PASS |
| p99 | 3.285 ms | 3.585 ms | +0.299 ms | +0.035 to +0.712 ms | 1.000 ms | PASS |

## Instrumented plan evidence

One eligible transition per side, executed with `EXPLAIN (ANALYZE, BUFFERS, WAL, TIMING OFF, FORMAT JSON)` and rolled back. These executions are excluded from the latency and server-count measurements.

| | baseline | candidate |
|---|---|---|
| node shape | `CTE Scan > LockRows > Index Scan(horsies_tasks) > ModifyTable(horsies_task_attempts) > CTE Scan > ModifyTable(horsies_tasks) > Nested Loop > CTE Scan > Index Scan(horsies_tasks)` | `Function Scan(horsies_complete_task_fused)` |
| shared hit blocks | 31 | 66 |
| shared read blocks | 0 | 1 |
| shared dirtied blocks | 0 | 1 |
| shared written blocks | 0 | 0 |
| WAL records | 11 | 16 |
| WAL full-page images | 0 | 0 |
| WAL bytes | 1166 | 1588 |

## Server counts

| | baseline | candidate |
|---|---|---|
| client statements | 10000 | 10000 |
| statements inside functions | 30000 | 60000 |
| write transactions | 10000 | 10000 |
| client rows | 10000 | 10000 |
| nested rows | 30000 | 60000 |
| terminal task rows | 10000 | 10000 |
| WAL records per terminal task row | 16.75 | 16.77 |
| WAL bytes per terminal task row | 1639 | 1656 |
| full-page images | 0 | 0 |

## Contract checks

Limits: client statements may not increase; write transactions and terminal task rows must match; WAL-record delta must be at most 0.100 per terminal task row; WAL-byte delta must be at most the greater of 10% or 128 bytes per terminal task row.

- PASS

Full-page writes are disabled in this disposable measurement environment, so checkpoint-dependent image bytes are absent from the WAL-byte comparison.

**Verdict: PASS**
