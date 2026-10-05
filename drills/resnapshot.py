"""Re-snapshot only the rows a verify run found missing or different
(runbook: "Repair silver after a loss").

Reads a drills/logs/silver-diff-*.json file written by verify_silver.py and
builds one Debezium execute-snapshot signal per connector: BLOCKING (reads
only, no write access to the sources), limited to those keys with
additional-conditions. The same "filter" field means different things:
PostgreSQL's blocking snapshot runs it as the whole SELECT statement (a
bare condition fails with a syntax error), MongoDB parses it as a query
document (Document.parse). The signal
key is the connector's topic.prefix, so each connector acts on its own.

Keys present in silver only ("extra") are listed, not re-sent: the source
has nothing to send for them.

Usage: python3 drills/resnapshot.py drills/logs/silver-diff-<time>.json [--send]
       [--only=postgres | --only=mongo]   (resend one connector's signal)
"""

import json
import subprocess
import sys
from pathlib import Path

POSTGRES = {"users", "orders", "order_items", "products", "dist_centers"}
# A signal is one Kafka message (1 MB limit); ~40 bytes per quoted UUID.
MAX_KEYS = 20_000
COMPOSE = Path(__file__).resolve().parent.parent / "onprem" / "compose.yaml"


def signal(prefix: str, schema: str, keys_by_table: dict, sql: bool) -> str:
    conditions = []
    for table, keys in keys_by_table.items():
        if sql:
            quoted = ", ".join("'" + k.replace("'", "''") + "'" for k in keys)
            condition = f"SELECT * FROM {schema}.{table} WHERE id IN ({quoted})"
        else:
            condition = json.dumps({"_id": {"$in": keys}})
        conditions.append({"data-collection": f"{schema}.{table}", "filter": condition})
    data = {
        "data-collections": [c["data-collection"] for c in conditions],
        "type": "BLOCKING",
        "additional-conditions": conditions,
    }
    return f"{prefix}|" + json.dumps({"type": "execute-snapshot", "data": data})


def main() -> int:
    send = "--send" in sys.argv[1:]
    only = {a.removeprefix("--only=") for a in sys.argv[1:] if a.startswith("--only=")}
    files = [a for a in sys.argv[1:] if not a.startswith("--")]
    if len(files) != 1:
        sys.exit(__doc__)
    diff = json.loads(Path(files[0]).read_text())

    postgres, mongo = {}, {}
    for table, keys in diff.items():
        if keys["extra"]:
            print(f"{table}: {len(keys['extra'])} keys in silver only: decide by hand")
        todo = sorted(set(keys["missing"]) | set(keys["differ"]))
        if not todo:
            continue
        if len(todo) > MAX_KEYS:
            sys.exit(f"{table}: {len(todo)} keys; snapshot the whole table instead")
        (postgres if table in POSTGRES else mongo)[table] = todo
        print(f"{table}: {len(todo)} keys to re-snapshot")

    signals = []
    if postgres and only <= {"postgres"}:
        signals.append(signal("thelook", "shop", postgres, sql=True))
    if mongo and only <= {"mongo"}:
        signals.append(signal("thelook_mongo", "web", mongo, sql=False))
    for s in signals:
        print(s[:240] + (" ..." if len(s) > 240 else ""))

    if send and signals:
        subprocess.run(
            [
                "docker",
                "compose",
                "-f",
                str(COMPOSE),
                "exec",
                "-T",
                "kafka",
                "/opt/kafka/bin/kafka-console-producer.sh",
                "--bootstrap-server",
                "kafka:29092",
                "--topic",
                "thelook.signals",
                "--property",
                "parse.key=true",
                "--property",
                "key.separator=|",
            ],
            input="\n".join(signals) + "\n",
            text=True,
            check=True,
        )
        print(
            "sent; progress: docker compose -f onprem/compose.yaml logs -f connect "
            "| grep -i snapshot"
        )
    return 0


if __name__ == "__main__":
    sys.exit(main())
