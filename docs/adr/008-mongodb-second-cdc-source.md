# 008. MongoDB as a second CDC source; clickstream moves there

- Status: Accepted
- Date: 2026-10-03 (decision D2, spec v2.0)
- Deciders: Souhail Bourhim (approves), Claude Code (drafts)

## Context

The extension adds a NoSQL source captured with Debezium. theLook's
clickstream (`events`) and product reviews are document-shaped website
data. `events` already exists as a PostgreSQL table written by the
vendored generator, and it is about two thirds of all change volume.
Reviews do not exist in theLook. Cart events carry no product or price,
and products are picked uniformly at random, which leaves two planned
features without data (cart value in Redis, recommendations in Neo4j).

## Options considered

| Option | Assessment |
|---|---|
| A. Move `events` to MongoDB: copy existing rows once, patch the generator to write new events there, remove the table from the PostgreSQL publication | One source of truth per dataset; realistic split (transactions in an RDBMS, clickstream in a document store); erasure spans two databases |
| B. Keep `events` in PostgreSQL and also write it to MongoDB | Two sources of truth for the same events; double volume; which one does the lake trust? |
| C. MongoDB holds only synthetic data (reviews, a new clickstream) | Two different clickstreams in one project |

## Decision

Option A, plus synthetic additions labelled per spec 4.3:

- **MongoDB 8, single-node replica set** (`rs0`): Debezium reads change
  streams, which only exist on replica sets. Keyfile authentication
  (required once a replica set has users); least-privilege users for the
  writers and for Debezium.
- **Database `web`, one document per event** (`_id` = the event's UUID).
  A document per session with an embedded events array was rejected:
  every new event would be an update rewriting a growing document, and
  CDC would emit the whole session each time.
- **Generator patch** (one commit, marked in `NOTICE`): events go to
  MongoDB; cart events get the `product_id` viewed just before them and
  its `retail_price`; with a set probability, later items in an order come
  from the first item's category (basket affinity).
- **Review simulator**: a separate service, not another generator patch,
  so the vendored code changes as little as possible. It reviews delivered
  order items, edits some, adds helpful votes and deletes a few.
- **Debezium MongoDB connector**: topic prefix `thelook_mongo` (Debezium
  needs a distinct prefix per connector; the heartbeat topic name is
  derived from it), so topics are `thelook_mongo.web.events` and
  `thelook_mongo.web.reviews`, the same `<prefix>.<db>.<collection>`
  shape as `thelook.shop.<table>`. Full document on update. The first
  draft used `thelook-mongo`: Debezium also builds the Avro namespace from
  the prefix, and Avro names cannot contain hyphens, so Schema Registry
  rejected the schema (an underscore is valid in both).
- `shop.events` leaves the publication and the connector's table list;
  the table stays in PostgreSQL but is no longer written.

## Consequences

- ✅ NoSQL and schemaless-data handling become part of the pipeline
  (schema-on-read in silver).
- ✅ Erasure (FR9) covers two databases, a stronger story.
- ✅ Cart value and recommendations have real signal.
- ❌ The P1 results (throughput, slot drill) were measured with events in
  PostgreSQL; they stay valid as history, but WAL volume and Debezium
  load drop after the move.
- ❌ `verify_cdc.py` must also reconcile Kafka against MongoDB.
- ❌ Historical cart events (before the patch) have no product or price.
- ❌ One more stateful service and one more failure mode (the oplog
  window, FR13).

## References

- Debezium MongoDB connector documentation (replica sets, capture modes, topic names)
- MongoDB documentation: replica set oplog, change streams, keyfile authentication
- Cahier des charges v2.0: 4.1, 4.3, FR1, FR13
