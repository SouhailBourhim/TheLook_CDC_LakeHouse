# Demo video script (under 3 minutes)

The deliverable from spec section 10: the generator creates an order, the
order is returned, and the change reaches the analytics tables a dashboard
reads. Recorded by Souhail; commands and timings by Claude (P9 wrap-up).

The dashboard is Grafana's (P9b, ADR 023): *business* for the revenue mart,
*pipeline health* for the change moving through. Athena's query editor
shows the raw change log in bronze, which no dashboard should.

## Before recording

```bash
make up PROFILES="core monitoring stream airflow bi"   # ~3 min to healthy
make connector-status                               # both RUNNING
# Airflow UI http://localhost:8088: transform unpaused, last run green
# Grafana http://localhost:3000: pipeline health, business (theLook folder)
# Athena console, workgroup thelook, a tab with the bronze query below
```

Open, side by side: a terminal (large font), Grafana, the Athena console. Pause transform's schedule during the take (the script triggers
it), so a scheduled run does not start in the middle.

Athena tab 1 (bronze, the change log):

```sql
SELECT op, after.status, source_ts, ingested_at
FROM thelook_bronze.shop_orders
WHERE after.id = '<order id>' ORDER BY source_ts
```

The dashboard: Grafana, *theLook: business (gold marts)*, panels "Daily
revenue" and "Provisional days".

## The take

| Time | Screen | Say |
|---|---|---|
| 0:00-0:15 | README banner and architecture picture | "theLook is a shop. Its orders live in PostgreSQL and its clickstream in MongoDB. This pipeline copies every change to a lakehouse on AWS within minutes, without reloading anything." |
| 0:15-0:35 | Terminal: `uv run drills/demo_order.py` prints step 1 | "The generator has just created this order. Debezium reads it from PostgreSQL's write-ahead log, not from the tables." |
| 0:35-1:00 | Step 2 line ("in bronze: N s"); Athena tab 1 shows one row, op `c` | "About a minute later it is in bronze, the append-only history in Iceberg on S3, queryable from Athena." |
| 1:00-1:25 | Step 3, then step 4; tab 1 now shows two rows, `c` then `u` with status Returned | "Now the customer returns it. Bronze does not overwrite anything: it appends the update. That history is what gives the user dimension its versions (SCD2) and what lets us repair any layer." |
| 1:25-1:45 | Grafana, pipeline health, Orchestration row: the triggered transform run, its task and duration | "Every 30 minutes Airflow merges the changes into silver and gold: incremental MERGEs on Iceberg, then the data contracts check the result." (cut the wait) |
| 1:45-2:15 | Step 5 line; Grafana, business: today's returns and net revenue moved, the day listed as provisional | "The revenue mart now counts the return. The day stays marked provisional until its return window closes." |
| 2:15-2:45 | README "Proven by failure drills" table | "Every failure in the spec was drilled: connector crash, broker restart, an hour of Debezium downtime, offsets replayed. Each one ended with zero lost and zero duplicated rows." |
| 2:45-2:55 | README stat tiles | "Built and measured end to end. The repository has the numbers and how each was obtained." |

## Notes

- The return is a real UPDATE in the source (one transaction, the order
  and its items), the change the generator's own status updates make.
- If a step waits longer than usual, it is the link to AWS:
  `python3 scripts/link_probe.py`. Record in daytime.
- Keep the measured seconds the script prints; they are the numbers to
  say, not the ones above.
