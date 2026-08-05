# pending-expiry-batch

deadline expiry of unclaimed tasks, one bounded batch

## Conditions

| | |
|---|---|
| measured | 2026-08-05T02:29:59+00:00 |
| server | PostgreSQL 14.23 (Debian 14.23-1.pgdg13+1) |
| compared | existing statement against database-owned operation |
| observations per side | 200 |
| block size | 10 |
| pre-existing terminal rows | 100000 |
| result payload | 200 bytes |
| batch | 500 rows |
| fsync | off |
| full_page_writes | off |
| synchronous_commit | off |
| autovacuum | off |
| bootstrap resamples | 1000 |
| bootstrap seed | 20260804 |
| demo units quiesced | yes |

## Batch performance

| metric | baseline | candidate | comparison | 95% interval | budget | verdict |
|---|---|---|---|---|---|---|
| throughput | 4195 rows/s | 4038 rows/s | 0.962x | 0.950x to 0.974x | >= 0.90x | PASS |
| p95 statement-to-commit duration | 130.525 ms | 136.754 ms | +6.229 ms | +3.001 to +12.161 ms | 13.053 ms | PASS |

The duration envelope starts before the statement can acquire its first row lock and ends after commit releases every lock. It includes result decoding and outcome logging performed before commit.

## Instrumented plan evidence

One eligible transition per side, executed with `EXPLAIN (ANALYZE, BUFFERS, WAL, TIMING OFF, FORMAT JSON)` and rolled back. These executions are excluded from the latency and server-count measurements.

| | baseline | candidate |
|---|---|---|
| node shape | `ModifyTable(horsies_tasks) > Nested Loop > Subquery Scan > Limit > LockRows > Index Scan(horsies_tasks) > Index Scan(horsies_tasks)` | `Function Scan(horsies_expire_pending_tasks)` |
| shared hit blocks | 13928 | 15624 |
| shared read blocks | 202 | 259 |
| shared dirtied blocks | 50 | 33 |
| shared written blocks | 30 | 31 |
| WAL records | 6166 | 6218 |
| WAL full-page images | 0 | 0 |
| WAL bytes | 623858 | 631038 |

## Server counts

| | baseline | candidate |
|---|---|---|
| client statements | 200 | 200 |
| statements inside functions | 200000 | 200200 |
| write transactions | 200 | 200 |
| client rows | 100000 | 100000 |
| nested rows | 200000 | 300000 |
| terminal task rows | 100000 | 100000 |
| WAL records per terminal task row | 12.24 | 12.19 |
| WAL bytes per terminal task row | 1243 | 1253 |
| full-page images | 0 | 0 |

## Contract checks

Limits: client statements may not increase; write transactions and terminal task rows must match; WAL-record delta must be at most 0.100 per terminal task row; WAL-byte delta must be at most the greater of 10% or 128 bytes per terminal task row.

- PASS

Full-page writes are disabled in this disposable measurement environment, so checkpoint-dependent image bytes are absent from the WAL-byte comparison.

**Verdict: PASS**
