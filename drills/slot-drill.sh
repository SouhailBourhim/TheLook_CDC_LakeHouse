#!/usr/bin/env bash
# Replication slot drill (spec 9, FR13; P1 acceptance part 2).
#
# Stop the Debezium consumer while the generator keeps writing, watch the
# slot retain WAL and the alert fire, restart, and prove nothing was lost.
#
#   1. Stop Kafka Connect for STOP_MINUTES (default 60); the generator runs.
#      Every minute, log the slot's retained WAL and the firing alerts.
#   2. Stop the load (generator, review simulator) and note the WAL
#      position: the last change to catch.
#      Run the verifier: it MUST report a mismatch (Kafka is behind), which
#      proves the check can fail.
#   3. Start Connect. Measure how long until the slot has confirmed that WAL
#      position (catch-up time).
#   4. Run the verifier: it must report every table identical. With the
#      stream profile up (P8 step 11), bronze must equal Kafka over the
#      drill too (drills/verify_bronze.py), ReplicationSlotInactive must
#      have fired, and Alertmanager must have sent notifications to SNS
#      (which emails them) without a failure.
#   5. Restart the load.
#
# Prerequisites: core and monitoring profiles up, connector RUNNING.
# Usage (from the repo root): drills/slot-drill.sh [log file]
set -euo pipefail
cd "$(dirname "$0")/.."

STOP_MINUTES=${STOP_MINUTES:-60}
LOG=${1:-drills/logs/slot-drill-$(date -u +%Y%m%dT%H%M%SZ).log}
mkdir -p "$(dirname "$LOG")"
exec > >(tee -a "$LOG") 2>&1

dc() { (cd onprem && docker compose "$@"); }
sql() { dc exec -T postgres psql -U postgres -d thelook -X -A -t -F' ' -c "$1"; }
now() { date -u +%H:%M:%S; }
# Notifications Alertmanager sent to SNS so far, and those that failed (every
# reason). The label is the integration type: "sns" here, "email" would be
# SMTP, which is not used (P8 step 11).
emails() {
  curl -s localhost:9093/metrics | awk '/^alertmanager_notifications_total\{.*integration="sns"/ {s+=$2} END {print s+0}'
}
email_failures() {
  curl -s localhost:9093/metrics | awk '/^alertmanager_notifications_failed_total\{.*integration="sns"/ {f+=$2} END {print f+0}'
}
firing() {
  curl -s localhost:9090/api/v1/alerts | python3 -c \
    'import json,sys; a=json.load(sys.stdin)["data"]["alerts"]; print(",".join(sorted(x["labels"]["alertname"]+":"+x["state"] for x in a)) or "-")'
}
slot() {
  sql "select active, pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn)), wal_status,
              pg_size_pretty(safe_wal_size) from pg_replication_slots where slot_name = 'thelook_debezium'"
}

echo "== $(now) preflight"
make --no-print-directory connector-status | grep -q "postgres-source RUNNING \['RUNNING'\]" \
  || { echo "connector not RUNNING"; exit 1; }
curl -sf localhost:9090/-/ready > /dev/null || { echo "Prometheus not up (monitoring profile)"; exit 1; }
dc --profile core up -d generator review-simulator > /dev/null 2>&1
echo "slot (active, retained, wal_status, safe_wal_size): $(slot)"
capture=""
if docker inspect thelook-bronze-stream-1 > /dev/null 2>&1; then
  capture=$(uv run -q drills/verify_bronze.py --capture)
  echo "bronze capture: $capture"
fi
emails_before=$(emails)
failures_before=$(email_failures)
slot_alert=no

echo "== $(now) step 1: stop Kafka Connect for ${STOP_MINUTES} min, generator running"
dc stop connect > /dev/null 2>&1
for m in $(seq 1 "$STOP_MINUTES"); do
  sleep 60
  alerts=$(firing)
  echo "$(now) min=$m slot=[$(slot)] alerts=[$alerts]"
  case "$alerts" in *ReplicationSlotInactive:firing*) slot_alert=yes ;; esac
done

echo "== $(now) step 2: stop the load, record the last WAL position"
dc stop generator review-simulator > /dev/null 2>&1
target_lsn=$(sql "select pg_current_wal_lsn()")
echo "target LSN: $target_lsn"
echo "verifier while Connect is down (must MISMATCH):"
if uv run -q drills/verify_cdc.py; then
  echo "UNEXPECTED: verifier passed while Kafka is behind; it cannot detect loss"; exit 1
fi

echo "== $(now) step 3: start Kafka Connect, measure catch-up"
start=$(date +%s)
dc --profile core up -d --wait connect > /dev/null 2>&1
until [ "$(sql "select confirmed_flush_lsn >= '$target_lsn' from pg_replication_slots where slot_name = 'thelook_debezium'")" = "t" ]; do
  sleep 5
done
echo "$(now) slot confirmed $target_lsn after $(( $(date +%s) - start )) s (includes worker start-up)"
echo "slot now: [$(slot)]"

echo "== $(now) step 4: verifier after catch-up (must be identical)"
sleep 30   # let the last transaction's records become visible to read_committed consumers
# (if/else, not "cmd; result=$?": under set -e a failure would exit here)
if uv run -q drills/verify_cdc.py; then result=0; else result=$?; fi
if [ -n "$capture" ]; then
  echo "== $(now) bronze = Kafka over the drill"
  if uv run -q drills/verify_bronze.py "$capture"; then :; else result=1; fi
fi
sent=$(( $(emails) - emails_before ))
failed=$(( $(email_failures) - failures_before ))
echo "ReplicationSlotInactive fired: $slot_alert; SNS notifications sent: $sent, failed: $failed"
[ "$slot_alert" = yes ] || { echo "ReplicationSlotInactive never fired"; result=1; }
[ "$sent" -gt 0 ] && [ "$failed" -eq 0 ] || { echo "alert not delivered to SNS"; result=1; }

echo "== $(now) step 5: restart the load"
dc --profile core up -d generator review-simulator > /dev/null 2>&1
sleep 60
echo "alerts one minute later: [$(firing)]"
echo "== $(now) done, verifier exit code $result, log: $LOG"
exit "$result"
