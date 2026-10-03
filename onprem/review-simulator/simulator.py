"""Review simulator: synthetic product reviews in MongoDB web.reviews.

theLook has no reviews, so this service adds them (spec 4.3, ADR 008). It
reviews order items that were really delivered (read from PostgreSQL with
a read-only role), so every review points at a real user, product and order
item. Each tick it does one action, picked at random:

  create   a review for a delivered, not yet reviewed order item
  vote     +1 helpful vote on a random review           (update event)
  edit     change a random review's rating and text     (update event)
  delete   remove a random review                        (delete event)

The mix gives the reviews topic inserts, updates and deletes, which silver
must handle. Unlike the generator, it keeps retrying when a database is
unreachable instead of exiting.
"""

import argparse
import logging
import os
import random
import time
import uuid
from collections import Counter
from datetime import UTC, datetime
from urllib.parse import quote_plus

import psycopg
from faker import Faker
from pymongo import MongoClient
from pymongo.errors import DuplicateKeyError, PyMongoError

log = logging.getLogger("review-simulator")

# Share of each action per tick. Creates dominate so the collection grows;
# deletes are rare, as in a real shop.
ACTIONS = {"create": 0.60, "vote": 0.25, "edit": 0.10, "delete": 0.05}

# Ratings skew positive, like most review sites.
RATINGS = {5: 0.40, 4: 0.30, 3: 0.15, 2: 0.08, 1: 0.07}

TAGS = ["fit", "quality", "value", "comfort", "style", "shipping", "size"]

# TABLESAMPLE SYSTEM (1) reads about 1 % of the table's pages at random
# instead of scanning the whole table like ORDER BY random(). The join to
# orders uses its primary key. One query refills a batch of candidates.
CANDIDATES_SQL = """
    SELECT oi.id AS order_item_id, oi.product_id, o.user_id
    FROM shop.order_items AS oi TABLESAMPLE SYSTEM (1)
    JOIN shop.orders AS o ON o.id = oi.order_id
    WHERE oi.status = 'Delivered'
    LIMIT %s
"""


def choose(rng: random.Random, weights: dict):
    """One key of `weights`, drawn with the given probabilities."""
    return rng.choices(list(weights), weights=list(weights.values()))[0]


def new_review(candidate: dict, fake: Faker, rng: random.Random, now: datetime) -> dict:
    """A review document for one delivered order item. _id is a fresh UUID;
    order_item_id carries the unique index (one review per item)."""
    rating = choose(rng, RATINGS)
    return {
        "_id": str(uuid.uuid4()),
        "order_item_id": candidate["order_item_id"],
        "product_id": int(candidate["product_id"]),
        "user_id": candidate["user_id"],
        "rating": rating,
        "title": fake.sentence(nb_words=5).rstrip("."),
        "text": fake.paragraph(nb_sentences=3),
        # An array: documents are not flat rows, silver must handle it.
        "tags": rng.sample(TAGS, k=rng.randint(0, 3)),
        "helpful_votes": 0,
        "created_at": now,
        "updated_at": now,
    }


def edit_fields(review: dict, fake: Faker, rng: random.Random, now: datetime) -> dict:
    """The fields an edit changes: rating moves by one star (kept in 1..5),
    new text, and edited_at appears (a field older versions do not have)."""
    rating = min(5, max(1, review["rating"] + rng.choice([-1, 1])))
    return {
        "rating": rating,
        "text": fake.paragraph(nb_sentences=3),
        "edited_at": now,
        "updated_at": now,
    }


class ReviewSimulator:
    def __init__(self, args: argparse.Namespace):
        self.args = args
        self.fake = Faker()
        self.rng = random.Random()
        self.candidates: list[dict] = []
        self.stats: Counter = Counter()
        self.pg_dsn = (
            f"host={args.pg_host} dbname={args.pg_db} user={args.pg_user} "
            f"password={args.pg_password} connect_timeout=5"
        )
        # replicaSet: always write to the primary, waiting for one during an
        # election. authSource: the user is defined in the web database.
        self.mongo = MongoClient(
            f"mongodb://{quote_plus(args.mongo_user)}:{quote_plus(args.mongo_password)}"
            f"@{args.mongo_host}:27017/?replicaSet=rs0&authSource=web",
            serverSelectionTimeoutMS=5000,
        )
        self.reviews = self.mongo["web"]["reviews"]

    def refill_candidates(self):
        with psycopg.connect(self.pg_dsn) as conn:
            rows = conn.execute(CANDIDATES_SQL, (self.args.batch,)).fetchall()
        self.candidates = [
            {"order_item_id": r[0], "product_id": r[1], "user_id": r[2]} for r in rows
        ]
        self.rng.shuffle(self.candidates)

    def random_review(self) -> dict | None:
        # $sample picks documents at random on the server, without a scan.
        found = list(self.reviews.aggregate([{"$sample": {"size": 1}}]))
        return found[0] if found else None

    def step(self):
        action = choose(self.rng, ACTIONS)
        now = datetime.now(UTC)
        if action != "create":
            review = self.random_review()
            if review is None:
                action = "create"  # nothing to update yet
        if action == "create":
            if not self.candidates:
                self.refill_candidates()
            if not self.candidates:
                self.stats["no_candidate"] += 1
                return
            doc = new_review(self.candidates.pop(), self.fake, self.rng, now)
            try:
                self.reviews.insert_one(doc)
            except DuplicateKeyError:
                # This order item already has a review: the unique index
                # enforced the business rule.
                self.stats["duplicate_skipped"] += 1
                return
        elif action == "vote":
            self.reviews.update_one(
                {"_id": review["_id"]},
                {"$inc": {"helpful_votes": 1}, "$set": {"updated_at": now}},
            )
        elif action == "edit":
            self.reviews.update_one(
                {"_id": review["_id"]},
                {"$set": edit_fields(review, self.fake, self.rng, now)},
            )
        elif action == "delete":
            self.reviews.delete_one({"_id": review["_id"]})
        self.stats[action] += 1

    def run(self):
        last_report = time.monotonic()
        failures = 0
        while True:
            # Exponential gaps give a Poisson process, like the generator.
            time.sleep(self.rng.expovariate(self.args.rate))
            try:
                self.step()
                failures = 0
            except (psycopg.OperationalError, PyMongoError) as e:
                failures += 1
                delay = min(60, 2**failures)
                log.warning(
                    "database error (%s), retrying in %ss: %s", failures, delay, e
                )
                time.sleep(delay)
            if time.monotonic() - last_report >= 60:
                log.info("last minute: %s", dict(self.stats))
                self.stats.clear()
                last_report = time.monotonic()


def main():
    logging.basicConfig(
        level=logging.INFO, format="[%(asctime)s] %(levelname)s: %(message)s"
    )
    p = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    p.add_argument(
        "--rate", type=float, default=1.0, help="Average actions per second."
    )
    p.add_argument(
        "--batch", type=int, default=200, help="Delivered items fetched per refill."
    )
    p.add_argument("--pg-host", default="localhost")
    p.add_argument("--pg-db", default="thelook")
    p.add_argument("--pg-user", default="reviewer")
    p.add_argument("--pg-password", default=os.environ.get("PG_PASSWORD", ""))
    p.add_argument("--mongo-host", default="localhost")
    p.add_argument("--mongo-user", default="reviewer")
    p.add_argument("--mongo-password", default=os.environ.get("MONGO_PASSWORD", ""))
    args = p.parse_args()
    log.info({**vars(args), "pg_password": "***", "mongo_password": "***"})
    ReviewSimulator(args).run()


if __name__ == "__main__":
    main()
