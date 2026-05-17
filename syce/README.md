# Syce

**Terminal-based monitoring dashboard for [Horsies](https://github.com/suleymanozkeskin/horsies) clusters.**

![Syce Dashboard](https://suleymanozkeskin.github.io/horsies/images/syce/dashboard.png)

Syce connects directly to your Horsies PostgreSQL database to provide real-time visibility into your workers, tasks, and workflows without requiring any extra infrastructure.

For PgBouncer transaction pooling, use the pooled URL for queries and a
direct/session-capable URL for `LISTEN`:

```bash
syce \
  --database-url "$DATABASE_URL_POOLED" \
  --session-database-url "$DATABASE_URL_DIRECT" \
  --pgbouncer-transaction-mode
```

[**Read the Full Documentation**](https://suleymanozkeskin.github.io/horsies/monitoring/syce-overview/)
