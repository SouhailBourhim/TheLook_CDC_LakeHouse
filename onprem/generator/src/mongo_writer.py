# Added in thelook-cdc-lakehouse (see NOTICE): clickstream events are written
# to MongoDB (database "web", collection "events") instead of PostgreSQL.
import dataclasses
import logging
from urllib.parse import quote_plus

from pymongo import MongoClient, ReplaceOne

from src.models import Event


def event_to_document(event: Event) -> dict:
    """One event = one document. The event's UUID becomes _id (MongoDB's
    primary key, always indexed and unique); fields that are None are left
    out, so only cart events carry product_id and price (schemaless)."""
    doc = {k: v for k, v in dataclasses.asdict(event).items() if v is not None}
    doc["_id"] = doc.pop("id")
    return doc


class EventWriter:
    def __init__(self, user: str, password: str, host: str, db_name: str):
        # replicaSet: the driver discovers the set and always writes to the
        # primary, waiting for one (serverSelectionTimeoutMS) during an
        # election. authSource: the user is defined in the "web" database.
        uri = (
            f"mongodb://{quote_plus(user)}:{quote_plus(password)}@{host}:27017/"
            f"?replicaSet=rs0&authSource={db_name}"
        )
        # MongoClient is thread-safe and pools connections, so the threads
        # started with asyncio.to_thread can share it.
        self.client = MongoClient(uri, tz_aware=False)
        self.collection = self.client[db_name]["events"]

    def upsert(self, events: list[Event]):
        """Replace-or-insert by _id, like the PostgreSQL writer's
        INSERT ... ON CONFLICT: a retried batch does not fail on duplicate
        keys. A new _id is reported by change streams as an insert."""
        if not events:
            return
        ops = [
            ReplaceOne({"_id": d["_id"]}, d, upsert=True)
            for d in map(event_to_document, events)
        ]
        # ordered=False: the server applies all operations even if one fails.
        self.collection.bulk_write(ops, ordered=False)
        logging.info(f"[events] Upserted {len(ops)} documents into MongoDB.")

    def close(self):
        self.client.close()
