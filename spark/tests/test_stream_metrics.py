"""Tests for lakehouse.stream_metrics: streaming progress as Prometheus
metrics (P9)."""

import json
import time
import urllib.request
from types import SimpleNamespace

from lakehouse.stream_metrics import ProgressMetrics, backlog, serve


def source(latest, end):
    return SimpleNamespace(latestOffset=json.dumps(latest), endOffset=json.dumps(end))


def test_backlog_counts_records_left_beyond_the_batch():
    sources = [
        source({"t1": {"0": 100, "1": 50}}, {"t1": {"0": 60, "1": 50}}),
        source({"t2": {"0": 7}}, {"t2": {"0": 2}}),
    ]
    assert backlog(sources) == 45


def test_backlog_ignores_sources_without_kafka_offsets():
    # PySpark passes str() of the JVM value: a JSON null arrives as "None"
    # (bronze's first batch after a start, found live).
    assert (
        backlog([SimpleNamespace(latestOffset="None", endOffset='{"t": {"0": 5}}')])
        == 0
    )
    assert backlog([SimpleNamespace(latestOffset=None, endOffset="5")]) == 0
    assert backlog([SimpleNamespace(latestOffset="12", endOffset="10")]) == 0


def progress(name="bronze", batch=7, ms=42_000, rows=1200):
    return SimpleNamespace(
        progress=SimpleNamespace(
            name=name,
            id="q",
            batchId=batch,
            durationMs={"triggerExecution": ms},
            numInputRows=rows,
            inputRowsPerSecond=20.0,
            processedRowsPerSecond=28.5,
            sources=[source({"t": {"0": 10}}, {"t": {"0": 10}})],
        )
    )


def test_render_gives_each_query_its_latest_batch():
    metrics = ProgressMetrics(clock=lambda: 1000.0)
    metrics.onQueryProgress(progress(batch=7))
    metrics.onQueryProgress(progress(batch=8, ms=61_500))
    metrics.onQueryProgress(progress(name="features", batch=3, ms=900))

    text = metrics.render()

    assert 'thelook_stream_batch_id{query="bronze"} 8' in text
    assert 'thelook_stream_batch_duration_seconds{query="bronze"} 61.5' in text
    assert 'thelook_stream_batch_duration_seconds{query="features"} 0.9' in text
    assert (
        'thelook_stream_last_progress_timestamp_seconds{query="bronze"} 1000.0' in text
    )
    assert "# TYPE thelook_stream_input_rows gauge" in text


def test_an_idle_trigger_counts_as_progress():
    now = [1000.0]
    metrics = ProgressMetrics(clock=lambda: now[0])
    metrics.onQueryProgress(progress())
    now[0] = 1600.0
    metrics.onQueryIdle(SimpleNamespace(name="bronze", id="q"))
    assert 'last_progress_timestamp_seconds{query="bronze"} 1600.0' in metrics.render()


def test_a_real_query_is_served_over_http(spark):
    # Real PySpark progress objects: attribute names checked against Spark.
    metrics = serve(spark, port=19108)
    query = (
        spark.readStream.format("rate")
        .option("rowsPerSecond", 5)
        .load()
        .writeStream.format("noop")
        .queryName("probe")
        .trigger(processingTime="1 second")
        .start()
    )
    try:
        deadline = time.monotonic() + 60
        while 'batch_id{query="probe"} 2' not in metrics.render():
            assert time.monotonic() < deadline, metrics.render()
            time.sleep(0.5)
    finally:
        query.stop()
    body = urllib.request.urlopen("http://localhost:19108/metrics").read().decode()
    assert 'thelook_stream_batch_duration_seconds{query="probe"}' in body
    assert 'thelook_stream_offsets_behind_latest{query="probe"} 0' in body


def test_a_start_is_recorded_and_counts_as_progress():
    metrics = ProgressMetrics(clock=lambda: 500.0)
    metrics.onQueryStarted(SimpleNamespace(name="bronze", id="q"))
    text = metrics.render()
    assert 'thelook_stream_start_timestamp_seconds{query="bronze"} 500.0' in text
    assert (
        'thelook_stream_last_progress_timestamp_seconds{query="bronze"} 500.0' in text
    )
