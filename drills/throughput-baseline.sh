#!/usr/bin/env bash
# Baseline throughput run (P1 commit 15). Applies the method written in
# docs/results.md ("Baseline throughput run — method") before measuring.
#
# For each GENERATOR_QPS level: restart the generator at that rate, skip a
# 2-minute warm-up (re-seed burst), then measure a 4-minute window from
# monotonic counters. Then the wal_compression side experiment at 5/s.
#
# Prerequisites: core and monitoring profiles up, connector RUNNING.
# Usage (from the repo root): drills/throughput-baseline.sh [log file]
set -euo pipefail
cd "$(dirname "$0")/.."

LEVELS=${LEVELS:-"5 10 20 40 80"}
WARMUP=120
WINDOW=240
LOG=${1:-drills/logs/throughput-$(date -u +%Y%m%dT%H%M%SZ).log}
mkdir -p "$(dirname "$LOG")"
exec > >(tee -a "$LOG") 2>&1

dc() { (cd onprem && docker compose "$@"); }
sql() { dc exec -T postgres psql -U postgres -d thelook -X -A -t -c "$1"; }
prom() {  # instant query evaluated now; prints the first value or "nan"
  curl -s localhost:9090/api/v1/query --data-urlencode "query=$1" | python3 -c \
    'import json,sys; r=json.load(sys.stdin)["data"]["result"]; print(r[0]["value"][1] if r else "nan")'
}
retained() { sql "select pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn)::bigint from pg_replication_slots where slot_name = 'thelook_debezium'"; }
cpu() { docker stats --no-stream --format '{{.Name}}={{.CPUPerc}}' | sed 's/thelook-//; s/-1=/=/' | grep -E '^(generator|postgres|connect|kafka)=' | tr '\n' ' '; }
# Run a measurement window of $WINDOW seconds; print WAL MB/h and retained delta.
window() {
  local lsn0 lsn1 r0 r1
  lsn0=$(sql "select pg_current_wal_lsn()"); r0=$(retained)
  sleep "$WINDOW"
  lsn1=$(sql "select pg_current_wal_lsn()"); r1=$(retained)
  WAL_MBH=$(sql "select round(pg_wal_lsn_diff('$lsn1', '$lsn0') * 3600 / $WINDOW / 1e6)")
  RET_DELTA_MB=$(( (r1 - r0) / 1000000 ))
}
f() { python3 -c "import sys; v=sys.argv[1]; print('nan' if v=='nan' else f'{float(v):.$2f}')" "$1"; }

make --no-print-directory connector-status | grep -q "postgres-source RUNNING \['RUNNING'\]" \
  || { echo "connector not RUNNING"; exit 1; }
curl -sf localhost:9090/-/ready > /dev/null || { echo "Prometheus not up"; exit 1; }

W="${WINDOW}s"
echo "| target/s | achieved/s | events/s | lag p95 (s) | lag max (s) | WAL MB/h | retained Δ MB | sustained | CPU |"
echo "|---|---|---|---|---|---|---|---|---|"
for q in $LEVELS; do
  GENERATOR_QPS=$q dc --profile core up -d generator > /dev/null 2>&1
  sleep "$WARMUP"
  window
  achieved=$(prom "sum(increase(pg_stat_user_tables_n_tup_ins{relname=\"orders\"}[$W])) / $WINDOW")
  events=$(prom "sum(increase(kafka_connect_source_task_metrics_source_record_write[$W])) / $WINDOW")
  p95=$(prom "quantile_over_time(0.95, debezium_streaming_millisecondsbehindsource[$W]) / 1000")
  lmax=$(prom "max_over_time(debezium_streaming_millisecondsbehindsource[$W]) / 1000")
  ok=$(python3 -c "
a,t,m,d=float('$achieved'),$q,float('$lmax'),$RET_DELTA_MB
print('yes' if a>=0.9*t and m<30 and d<=16 else 'NO')")
  echo "| $q | $(f "$achieved" 1) | $(f "$events" 0) | $(f "$p95" 1) | $(f "$lmax" 1) | $WAL_MBH | $RET_DELTA_MB | $ok | $(cpu)|"
done

echo
echo "== wal_compression side experiment at 5/s"
GENERATOR_QPS=5 dc --profile core up -d generator > /dev/null 2>&1
sleep "$WARMUP"
WINDOW=360
window; off=$WAL_MBH
sql "alter system set wal_compression = 'lz4'" > /dev/null; sql "select pg_reload_conf()" > /dev/null
echo "wal_compression now: $(sql 'show wal_compression')"
window; lz4=$WAL_MBH
sql "alter system reset wal_compression" > /dev/null; sql "select pg_reload_conf()" > /dev/null
echo "| wal_compression | WAL MB/h |"
echo "|---|---|"
echo "| off | $off |"
echo "| lz4 | $lz4 |"
echo "wal_compression restored to: $(sql 'show wal_compression')"

# Back to the configured rate from onprem/.env.
dc --profile core up -d generator > /dev/null 2>&1
echo "== done, log: $LOG"
