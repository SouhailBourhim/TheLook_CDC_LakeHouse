"""Structured Streaming progress as Prometheus metrics (P9, NFR
observability): a StreamingQueryListener in the driver keeps each query's
latest progress, and a small HTTP server (standard library) serves it in
Prometheus' text format for the monitoring profile to scrape.

Why not Spark's own PrometheusServlet: its streaming source has no time
since the last batch (what tells a stalled stream from a quiet one) and no
Kafka backlog, and its metric names contain the application id, which
changes at each restart. A Kafka consumer-group exporter would not help
either: Spark keeps its offsets in its checkpoint, never in a group.

Usage, in a streaming driver: serve(spark, port=9108)
"""

import json
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from pyspark.sql import SparkSession
from pyspark.sql.streaming import StreamingQueryListener

PREFIX = "thelook_stream"
HELP = {
    "start_timestamp_seconds": "When the query last started (Unix time); a change is a restart",
    "last_progress_timestamp_seconds": "When the query last finished a batch or found nothing to do (Unix time)",
    "batch_id": "Latest finished micro-batch",
    "batch_duration_seconds": "Duration of the latest micro-batch (trigger to commit)",
    "input_rows": "Records read by the latest micro-batch",
    "input_rows_per_second": "Records arriving per second, latest micro-batch",
    "processed_rows_per_second": "Records processed per second, latest micro-batch",
    "offsets_behind_latest": "Records left in Kafka when the latest batch was planned (non-zero only beyond maxOffsetsPerTrigger)",
}


def _offsets(text) -> dict | None:
    """A source offset as PySpark gives it: str() of the JVM value, so a
    JSON null arrives as the string "None" (seen on the first batch after a
    start). None unless it is Kafka's {topic: {partition: offset}}."""
    try:
        value = json.loads(text) if text else None
    except ValueError:
        return None
    return value if isinstance(value, dict) else None


def backlog(sources: list) -> int:
    """Records between each source's end offset and Kafka's latest offset
    at planning time, summed over topics and partitions. Spark reads up to
    the latest offset unless maxOffsetsPerTrigger caps the batch, so this
    is non-zero only for a backlog beyond the cap (e.g. after downtime)."""
    total = 0
    for source in sources:
        latest, end = _offsets(source.latestOffset), _offsets(source.endOffset)
        if latest is None or end is None:
            continue  # not reported for this batch, or not a Kafka source
        for topic, partitions in latest.items():
            for partition, offset in partitions.items():
                total += max(0, offset - end.get(topic, {}).get(partition, offset))
    return total


class ProgressMetrics(StreamingQueryListener):
    """Latest progress of every query of this driver, by query name."""

    def __init__(self, clock=time.time):
        self.clock = clock
        self.lock = threading.Lock()
        self.values: dict[str, dict[str, float]] = {}

    def _set(self, name: str, **values: float) -> None:
        with self.lock:
            self.values.setdefault(name, {}).update(values)

    def onQueryStarted(self, event) -> None:
        # Counts as progress: a restarted query is not stalled before its
        # first batch. So a driver crashing every minute never looks
        # stalled: the start time, changing at each restart, shows it.
        now = self.clock()
        self._set(
            event.name or str(event.id),
            start_timestamp_seconds=now,
            last_progress_timestamp_seconds=now,
        )

    def onQueryProgress(self, event) -> None:
        p = event.progress
        self._set(
            p.name or str(p.id),
            last_progress_timestamp_seconds=self.clock(),
            batch_id=p.batchId,
            batch_duration_seconds=p.durationMs.get("triggerExecution", 0) / 1000,
            input_rows=p.numInputRows,
            input_rows_per_second=p.inputRowsPerSecond,
            processed_rows_per_second=p.processedRowsPerSecond,
            offsets_behind_latest=backlog(p.sources),
        )

    def onQueryIdle(self, event) -> None:
        # Nothing new in Kafka at the trigger: quiet, not stalled.
        self._set(
            event.name or str(event.id), last_progress_timestamp_seconds=self.clock()
        )

    def onQueryTerminated(self, event) -> None:
        pass  # the container restarts; the stale timestamp tells the rest

    def render(self) -> str:
        with self.lock:
            snapshot = {q: dict(v) for q, v in self.values.items()}
        lines = []
        for metric, text in HELP.items():
            name = f"{PREFIX}_{metric}"
            lines += [f"# HELP {name} {text}", f"# TYPE {name} gauge"]
            for query, values in sorted(snapshot.items()):
                if metric in values:
                    lines.append(f'{name}{{query="{query}"}} {values[metric]}')
        return "\n".join(lines) + "\n"


def serve(spark: SparkSession, port: int = 9108) -> ProgressMetrics:
    """Register the listener and serve /metrics on `port` from a daemon
    thread (it never keeps the driver alive)."""
    metrics = ProgressMetrics()
    spark.streams.addListener(metrics)

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            body = metrics.render().encode()
            self.send_response(200 if self.path == "/metrics" else 404)
            self.send_header("Content-Type", "text/plain; version=0.0.4")
            self.end_headers()
            if self.path == "/metrics":
                self.wfile.write(body)

        def log_message(self, *args):
            pass  # one line per scrape would flood the driver's log

    server = ThreadingHTTPServer(("0.0.0.0", port), Handler)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    return metrics
