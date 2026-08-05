# owned-claim-expiry

deadline expiry of a worker-owned claim before user code starts

## Conditions

| | |
|---|---|
| measured | 2026-08-05T02:22:01+00:00 |
| server | PostgreSQL 16.14 (Debian 16.14-1.pgdg13+1) |
| compared | existing statement against database-owned operation |
| observations per side | 10000 |
| block size | 100 |
| pre-existing terminal rows | 100000 |
| result payload | 205 bytes |
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
| p50 | 1.872 ms | 2.074 ms | +0.201 ms | +0.199 to +0.205 ms | 0.500 ms | PASS |
| p99 | 2.554 ms | 2.942 ms | +0.388 ms | +0.226 to +0.533 ms | 1.500 ms | PASS |

## Instrumented plan evidence

One eligible transition per side, executed with `EXPLAIN (ANALYZE, BUFFERS, WAL, TIMING OFF, FORMAT JSON)` and rolled back. These executions are excluded from the latency and server-count measurements.

| | baseline | candidate |
|---|---|---|
| node shape | `ModifyTable(horsies_tasks) > Index Scan(horsies_tasks)` | `Function Scan(horsies_expire_owned_claim)` |
| shared hit blocks | 27 | 32 |
| shared read blocks | 0 | 0 |
| shared dirtied blocks | 0 | 0 |
| shared written blocks | 0 | 0 |
| WAL records | 12 | 12 |
| WAL full-page images | 0 | 0 |
| WAL bytes | 1264 | 1279 |

## Server counts

| | baseline | candidate |
|---|---|---|
| client statements | 10000 | 10000 |
| statements inside functions | 20000 | 40000 |
| write transactions | 10000 | 10000 |
| client rows | 10000 | 10000 |
| nested rows | 20000 | 40000 |
| terminal task rows | 10000 | 10000 |
| WAL records per terminal task row | 12.68 | 12.72 |
| WAL bytes per terminal task row | 1310 | 1330 |
| full-page images | 0 | 0 |

## Contract checks

Limits: client statements may not increase; write transactions and terminal task rows must match; WAL-record delta must be at most 0.100 per terminal task row; WAL-byte delta must be at most the greater of 10% or 128 bytes per terminal task row.

- PASS

Full-page writes are disabled in this disposable measurement environment, so checkpoint-dependent image bytes are absent from the WAL-byte comparison.

**Verdict: PASS**
