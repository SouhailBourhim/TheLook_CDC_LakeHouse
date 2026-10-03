# 010. Serving layer: Redis features, Neo4j graph, FastAPI

- Status: Proposed
- Date: 2026-10-03 (decision D4, spec v2.0)
- Deciders: Souhail Bourhim (approves), Claude Code (drafts)

## Context

The lake answers analytical questions in seconds, at Athena prices per
query. A website needs millisecond answers to per-user and per-product
questions: what did this user just look at, what is in their cart, what is
usually bought with this product. The extension adds a serving layer for
these, using NoSQL stores (Redis, Neo4j) and an API.

## Options considered

| Option | Assessment |
|---|---|
| A. Redis for user features (from the stream), Neo4j for co-purchases (from gold), FastAPI in front | Each store fits its access pattern; two more services to run |
| B. Everything in Redis (co-purchase lists as sorted sets) | One store, but no graph practice and no traversal beyond one hop |
| C. Query Athena from the API | No new services, but seconds per request and a cost per call |

## Decision

Option A.

- **Redis**, fed by a separate query of the streaming job (ADR 007). Key
  design and TTLs are decided in P5; the principles are fixed now: writes
  are idempotent (sorted sets, not list pushes, because a failed
  micro-batch is replayed), every key has a TTL, ghost sessions are
  skipped, and Redis is a cache that can be rebuilt from the stream.
- **Neo4j Community**, rebuilt by a batch job from gold order items:
  `(:Product)-[:BOUGHT_WITH {weight}]->(:Product)`, weight = number of
  orders containing both products. The graph holds **products only**, so
  it holds no personal data and is outside the erasure workflow.
  Recommendations rely on the synthetic basket affinity (ADR 008),
  labelled as such.
- **FastAPI**: `GET /users/{id}/features` and
  `GET /products/{id}/recommendations`, typed responses, tested with
  TestClient (fakeredis for Redis).

## Consequences

- ✅ Covers key-value and graph NoSQL, and the stream-to-online-store
  pattern.
- ✅ TTLs bound how long personal data lives in Redis; erasure also
  deletes the user's keys (FR9).
- ❌ Two more services (~1.5 GB RAM with the API); run in their own
  Compose profile.
- ❌ Recommendations are only as meaningful as the synthetic affinity.
- ❌ Neo4j Community has no clustering or role-based access; acceptable
  for a local demo, not for production.

## References

- Redis documentation: sorted sets, key expiration
- Neo4j documentation: Cypher `MERGE`, `UNWIND` batching
- FastAPI documentation: testing with TestClient
- Cahier des charges v2.0: FR15, FR16, FR17
