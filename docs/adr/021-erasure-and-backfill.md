# 021. GDPR erasure through every layer, and day-range backfill

- Status: Accepted
- Date: 2026-10-12 (P8; spec FR9, FR10, FR12, FR14, O5, O9; ADR 017)
- Deciders: Souhail Bourhim (approves), Claude Code (drafts)

## Context

FR9 asks that a deletion request remove the user from every layer and store,
time travel included, with an audit trail (O5: under 24 hours; Kafka bounded
by its retention). FR10 asks for a backfill of silver and gold over a date
range without duplicates. Both run as Airflow DAGs (FR12).

A user's data lives in many places (the erasure map, below), and the hardest
is bronze: append-only by design (FR2, with erasure as its one exception),
never rebuildable, and protected by ADR 017's rule that no key that can
delete bronze ever enters Airflow.

A DELETE alone is not erasure on Iceberg: older snapshots still reference
the files holding the rows (time travel), and on merge-on-read tables
(silver, gold) a DELETE only writes delete files, the data files still hold
the rows. Erasure = delete, rewrite the affected files, then expire every
snapshot older than the rewrite.

## Options considered (who purges bronze)

1. **The bronze stream**, bronze's only writer, with its own identity: the
   DAG sends the request through Kafka and waits for an acknowledgement.
2. A dedicated erasure user that can delete in every layer, its key in
   Airflow: simpler, but a leaked Airflow environment could wipe bronze.
3. A manual bronze step with the stream key: O5 would depend on a person,
   and FR12 wants the DAG to do it.

## Decision

Option 1 (Souhail, 2026-10-12). ADR 017 is amended: the stream also applies
erasure requests to its own tables.

**The erasure map.**

| Place | Cleaned by |
|---|---|
| PostgreSQL `shop.users`, `orders`, `order_items` (via order ids), legacy `shop.events` | role `eraser` (DELETE on those tables only); Debezium's delete events carry the key only (REPLICA IDENTITY DEFAULT) |
| MongoDB `web.events`, `web.reviews` | user `eraser` (remove on those collections only) |
| Kafka | not purged: bounded by its 3-day retention (O5) |
| Bronze (5 tables holding user data) | the stream: DELETE (copy-on-write), then expiry of every snapshot older than the purge |
| Silver (users, orders, order_items, events, reviews, dead_letters) | `jobs/erase.py`, batch user: DELETE, rewrite of the affected files applying the deletes, expiry |
| Gold (dim_user, fct_order_items, fct_orders, fct_sessions) | the same job; marts are aggregates and are kept (FR9) |
| Redis `user:{id}:*` | ACL user `eraser` (`DEL`, `SCAN` on `user:*` only) |
| Neo4j | nothing: products only (ADR 010) |
| Athena query results, logs | bounded by the results bucket's 7-day lifecycle; jobs log counts, not ids |
| A feature rebuild within Kafka's retention | would re-create Redis keys from old events: rerun the Redis step after one |

**Order.** Sources first, so a re-snapshot or a replay cannot bring the user
back; then bronze, Redis, silver and gold (the lake job also writes every
earlier step's counts to the audit: one writer), verification.

**Steps outside Spark** (`erasure/steps.py`, one command per DAG task, its
own virtualenv in the Airflow image from a hashed lock): the user id comes
in an environment variable, never on a logged command line, every task
masks it in its log, and each step prints counts only. The request is sent
only once **none of the user's keys is alive in bronze** (each key's latest
event, by Kafka offset, is a delete), read with Athena at the ledger's cut:
a request sent earlier could be overtaken by a change still in Kafka.
Counting delete events instead would be wrong: the review simulator deletes
reviews too, so older delete events can satisfy a count before ours arrive
(found while testing). The run's conf keeps the user id in Airflow's
database; the runbook covers removing old runs.

**The request.** `drills/request_erasure.py <user_id>` (the synthetic
deletion request of spec 4.3) triggers `gdpr_erasure` with
`{"user_id": ...}`; the run id is the request id.

**Bronze, in the stream.** A plain-JSON topic, `thelook.erasure-requests`,
joins the stream's subscription. In a batch that holds requests, after the
CDC appends and **before the ledger row**, the stream deletes the user's
rows from `shop_users` (id), `shop_orders` (user_id), `shop_order_items`
(the user's order ids, read from bronze `shop_orders` first), `web_events`
and `web_reviews` (`$.user_id` in the JSON image); expires every snapshot of
those tables older than the purge; and appends one row to
`thelook_bronze.erasure_log` (request id, sha256 of the user id, rows
deleted per table, time), stamped with the batch id like every commit, so a
replayed batch does not purge twice. The ledger row comes last, so it points
at post-purge snapshots and silver never needs an expired one. Rows are
deleted by key (primary key, MongoDB document id), the keys found through
the user id first: the delete events, which carry only the key, go too.
Before the expiry, an empty commit stamps the batch's marks on each purged
table: the expiry removes the append snapshot that held them, and without
them a replay of the batch would append its rows twice (step 3 finding).

**Silver and gold.** `jobs/erase.py <user_id> <request_id>` (batch user,
`lake` pool): deletes (order items through the user's order ids, read
before the orders are deleted, from silver and from `fct_orders`: silver
may already have deleted them when the source deletion reached it; dead
letters by `$.user_id` in the payload), rewrites every data file that has
delete files (`upkeep.apply_deletes`, `delete-file-threshold` 1: Iceberg's
default thresholds keep a large file with one deleted row), drops the
history (`upkeep.drop_history`: an empty commit, then expiry of every
other snapshot, so the snapshot that removed the files is expired too and
incremental cleanup deletes them), and appends the audit rows. Every table
of the map goes through the rewrite and the expiry even when the job finds
no row: rows silver's MERGE already deleted are still inside data files.

**Audit.** `lake.thelook_silver.erasure_audit`: one row per step (request
id, sha256 of the user id, step, rows affected, time). It proves what was
done without keeping the identifier.

**Verification.** A task (and `drills/verify_erasure.py`) checks every row of
the map: zero rows for the user in each source, layer and store; on every
affected table, no snapshot older than its purge.

**Backfill (FR10).** A `backfill` DAG with a daily timetable, kept paused
and run only with `airflow backfill create --from-date --to-date`
(`--reprocess-behavior completed` to rerun days). Each run recomputes one
order day's gold facts and marts from silver and MERGEs them on the grain
keys: rerunning a day replaces its rows, so duplicates cannot appear. The
drill injects a logic bug as an uncommitted working-tree patch, lets the
existing contract rule catch it, reverts it and backfills.

**Failure drills (FR14).** `drills/failure_drills.py <drill>`: Connect killed
mid-stream, Kafka restarted under load, Debezium stopped for an hour,
connector offsets reset, bronze stream killed mid-batch, Neo4j frozen
(`docker pause`). Each ends with the exact reconciliation (P7) and a bronze
duplicate check by Kafka coordinates.

**Learned from the first real erasure (P8 step 6, on a network that
dropped every 15 to 30 minutes).** Each item below was a defect found there,
now fixed and tested:

- Bronze deletes children before parents (items, orders, users): items are
  found through the orders, and an attempt that died between the two left
  115 item rows unreachable for every retry.
- Every bronze user table's history is dropped, whatever the attempt
  deleted: an interrupted attempt deleted rows, its replay deleted none, and
  the old snapshots still held them.
- Each purge step (lookup, DELETE, history drop, log row) is retried on its
  own; the waits in the DAG poll again after a network error.
- `erase_lake` has the highest weight for the one-slot `lake` pool: with
  the default weights it waited over an hour behind transform and graph.
- A purge that cannot find rows any more (their links already purged) is
  repaired by an operator: `jobs/repair_erasure.py`, the stream's identity,
  the stream stopped, by key; it keeps the newest batch marks (runbook).
- The erasure_log counts the rows the completed attempt deleted: after an
  interrupted attempt it under-reports (the first run logged 0 for three
  tables). The proof is `drills/verify_erasure.py`, not the counts.

## Consequences

- ✅ Erasure reaches every copy, time travel included, and proves it.
- ✅ ADR 017 holds: no key that can delete bronze enters Airflow.
- ✅ Backfill is idempotent by construction (MERGE on grain keys).
- ❌ The bronze stream gains a second duty and a non-CDC topic; a bug there
  touches the unrebuildable layer, so it is tested on a local catalog first.
- ❌ Expiring every pre-purge snapshot removes time travel on the affected
  tables for everyone, not just the erased user (the price of FR9).
- ❌ Kafka keeps the user's change events up to 3 days; a feature rebuild in
  that window needs the Redis step rerun.
- ❌ Erasure rewrites files in bronze, silver and gold: S3 transfer and
  minutes per request, fine at this scale, batched requests at production
  scale.
- ❌ Under load, one request kept a bronze batch busy for 58 minutes on the
  first run (18 minutes for web_events' lookup alone, network failures on
  top): bronze freshness is missed for that long.
- ❌ The bronze key lookups read whole tables (web_events' JSON above all):
  a batch holding a request took 867 s instead of ~50 s (step 3, a user
  with no rows; a real purge adds the rewrites). Bronze freshness (O1,
  5 minutes) is missed for that one batch per request. A later option: look
  the keys up in a background thread (reads are safe beside appends) and
  only DELETE in the batch.

## References

- Iceberg: row-level deletes, `rewrite_data_files`, snapshot expiry
- Airflow 3: `airflow backfill create`
- Cahier des charges v2.3: FR2, FR9, FR10, FR12, FR14, O5, O9; ADR 006,
  ADR 010, ADR 012, ADR 014, ADR 017
