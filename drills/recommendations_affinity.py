# /// script
# requires-python = ">=3.12"
# dependencies = [
#   "psycopg[binary]==3.3.6",
# ]
# ///
"""P6 acceptance (ADR 019): recommendations follow the generator's affinity.

For the 20 most popular products (the generator's ranking), the API's top
5 must hold at least 4 of the product's 5 companions. The generator picks a
later item of an order among the first item's companions with probability
0.6, so those pairs gain weight hour after hour while any other pair stays
near 1 or 2.

Companions and ranking are recomputed here from their rule (sha256, as in
onprem/generator/src/models.py), not imported: the drill needs only the
products' ids and categories from PostgreSQL. What it tests is the path
PostgreSQL -> Kafka -> bronze -> silver -> gold -> Spark pairs -> Neo4j ->
API, which shares no code with the generator.

Needs the core and serving profiles up and a graph built from enough hours
of companion orders (ADR 019: ~0.9 per hour per companion pair of the 20th
product). Usage:
  uv run drills/recommendations_affinity.py
"""

import hashlib
import json
import sys
import urllib.request
from pathlib import Path

import psycopg

TOP = 20  # most popular products checked
COMPANIONS = 5  # the generator's --companions
LIMIT = 5  # recommendations read per product
REQUIRED = 4  # companions needed among them
API = "http://localhost:8000"


def load_env(path: Path) -> dict:
    env = {}
    for line in path.read_text().splitlines():
        if line and not line.startswith("#") and "=" in line:
            k, v = line.split("=", 1)
            env[k] = v
    return env


def digest(text: str) -> bytes:
    return hashlib.sha256(text.encode()).digest()


def companions(pid: str, by_category: dict, category: dict) -> set:
    """The n products of its category with the smallest sha256("<id>:<c>")."""
    others = [c for c in by_category[category[pid]] if c != pid]
    return set(sorted(others, key=lambda c: digest(f"{pid}:{c}"))[:COMPANIONS])


def recommendations(pid: str) -> list[dict]:
    url = f"{API}/products/{pid}/recommendations?limit={LIMIT}"
    with urllib.request.urlopen(url, timeout=5) as response:
        return json.load(response)["recommendations"]


def main() -> int:
    env = load_env(Path(__file__).resolve().parent.parent / "onprem" / ".env")
    with psycopg.connect(
        host="localhost",
        dbname=env.get("POSTGRES_DB", "thelook"),
        user=env.get("POSTGRES_USER", "postgres"),
        password=env["POSTGRES_PASSWORD"],
    ) as pg:
        rows = pg.execute("SELECT id::text, category FROM shop.products").fetchall()
    category = dict(rows)
    by_category: dict = {}
    for pid, cat in rows:
        by_category.setdefault(cat, []).append(pid)
    # The generator's ranking: the whole catalogue by sha256("popularity:<id>").
    top = sorted(category, key=lambda p: digest(f"popularity:{p}"))[:TOP]

    print("rank  product  hits  companion weights   best other  category")
    failed = 0
    for rank, pid in enumerate(top, 1):
        expected = companions(pid, by_category, category)
        found = recommendations(pid)
        hits = [r for r in found if str(r["product_id"]) in expected]
        others = [r["weight"] for r in found if str(r["product_id"]) not in expected]
        ok = len(hits) >= REQUIRED
        failed += not ok
        print(
            f"{rank:4}  {pid:>7}  {len(hits)}/{LIMIT}  "
            f"{' '.join(str(r['weight']) for r in hits):18}  "
            f"{max(others, default='-')!s:>10}  {category[pid]}"
            f"{'' if ok else '  <- FAIL'}"
        )
    print(
        f"\n{TOP - failed}/{TOP} products have >= {REQUIRED} of their "
        f"{COMPANIONS} companions in their top {LIMIT}: "
        f"{'PASS' if not failed else 'FAIL'}"
    )
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
