# Lake inventory

Taken 2026-10-17 20:35 UTC from Iceberg's metadata (`scripts/lake_inventory.py`), at the end of P9: what the lake
held. Live rows = rows in data files minus position
deletes (merge-on-read). Sizes are the current snapshot's files.

## thelook_bronze

| Table | Live rows | Data files | Size (MB) |
|---|---|---|---|
| erasure_log | 4 | 4 | 0.0 |
| shop_dist_centers | 721 | 52 | 0.4 |
| shop_order_items | 3,577,198 | 522 | 249.8 |
| shop_orders | 2,466,668 | 522 | 159.7 |
| shop_products | 2,096,640 | 52 | 89.3 |
| shop_users | 332,810 | 521 | 38.2 |
| stream_batches | 2,302 | 6 | 0.1 |
| web_events | 12,191,713 | 522 | 1,339.1 |
| web_reviews | 158,516 | 517 | 31.2 |
| **total** | **20,826,572** | | **1,907.8** |

## thelook_silver

| Table | Live rows | Data files | Size (MB) |
|---|---|---|---|
| dead_letters | 1 | 1 | 0.0 |
| dist_centers | 10 | 1 | 0.0 |
| erasure_audit | 54 | 1 | 0.0 |
| events | 12,137,990 | 29 | 1,295.8 |
| order_items | 2,557,551 | 8 | 253.8 |
| orders | 1,763,847 | 8 | 168.3 |
| products | 29,120 | 1 | 2.4 |
| reviews | 33,020 | 8 | 5.5 |
| users | 176,963 | 9 | 19.2 |
| **total** | **16,698,556** | | **1,745.0** |

## thelook_gold

| Table | Live rows | Data files | Size (MB) |
|---|---|---|---|
| dim_date | 4,018 | 1 | 0.0 |
| dim_distribution_center | 10 | 1 | 0.0 |
| dim_product | 29,120 | 1 | 1.3 |
| dim_user | 312,431 | 4 | 20.2 |
| fct_order_items | 2,546,563 | 12 | 193.0 |
| fct_orders | 1,756,324 | 9 | 100.6 |
| fct_sessions | 2,934,051 | 8 | 138.7 |
| mart_daily_revenue | 112 | 7 | 0.0 |
| mart_product_ratings | 18,542 | 1 | 0.6 |
| mart_session_funnel | 24 | 9 | 0.0 |
| **total** | **7,601,195** | | **454.5** |

**Lake total:** 45,126,323 rows, 4.11 GB in the current snapshots.

**The bucket as a whole** (every object: current files, files kept for
time travel, Athena results), `aws s3 ls --summarize`:

- Total Objects: 22741
- Total Size: 6875916326
