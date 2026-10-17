#!/usr/bin/env bash
# O7 (reproducibility), shown from scratch: the stack deployed and destroyed
# with one command each, from a fresh clone of the public repository.
#
#   1. clone from GitHub into an empty folder: only what is committed
#   2. copy the two git-ignored inputs .env.example and the tfvars example
#      document: onprem/.env (passwords, keys) and terraform.tfvars (email)
#   3. one command up: `make bootstrap` (core + monitoring, database roles,
#      topics, connectors) under another Compose project name, so every
#      volume starts empty (new databases, a new Kafka)
#   4. check it works from zero: connectors RUNNING, change events in the
#      new Kafka, Prometheus scraping
#   5. AWS: `make tf-init`, `terraform plan` must find no changes (the code
#      is AWS), `plan -destroy` lists what one `terraform destroy` removes.
#      The destroy itself is not run: the lake stays deployed (spec v1.9)
#   6. one command down, volumes included (`down -v`, this project only)
#
# Not started: the stream and airflow profiles. A fresh bronze stream would
# read the new Kafka from the start and append an unrelated dataset to the
# real lake's bronze tables. The AWS side is covered by step 5.
#
# Needs: the usual stack stopped (`make down`: same host ports), the
# `thelook` AWS profile, gh/git access to the repository.
# Usage: drills/fresh_clone.sh [work dir]   Log: drills/logs/fresh_clone-*.log
set -euo pipefail

ROOT=$(cd "$(dirname "$0")/.." && pwd)
WORK=${1:-$(mktemp -d)}
PROJECT=thelook-o7
REPO=https://github.com/SouhailBourhim/TheLook_CDC_LakeHouse.git
LOG="$ROOT/drills/logs/fresh_clone-$(date -u +%Y%m%dT%H%M%SZ).log"
mkdir -p "$(dirname "$LOG")"
exec > >(tee -a "$LOG") 2>&1

step() { echo; echo "[$(date -u +%H:%M:%S)] == $*"; }
export COMPOSE_PROJECT_NAME=$PROJECT AWS_PROFILE=thelook

if docker ps --format '{{.Names}}' | grep -q '^thelook-[a-z]'; then
  echo "the usual stack is running: stop it first (make down), it holds the ports"
  exit 1
fi

# Whatever happens, this project's containers and volumes are removed.
cleanup() {
  [ -f "$WORK/repo/onprem/compose.yaml" ] || return 0
  docker compose -f "$WORK/repo/onprem/compose.yaml" --profile '*' down -v > /dev/null 2>&1 || true
}
trap cleanup EXIT

step "1. clone $REPO into $WORK/repo"
git clone --quiet "$REPO" "$WORK/repo"
cd "$WORK/repo"
git log --oneline -1

step "2. copy the git-ignored inputs (documented in .env.example, terraform.tfvars.example)"
cp "$ROOT/onprem/.env" onprem/.env
cp "$ROOT/infra/terraform/lake/terraform.tfvars" infra/terraform/lake/terraform.tfvars
git status --short --ignored | grep -E 'onprem/.env|terraform.tfvars' || true

step "3. one command up (project $PROJECT, empty volumes): make bootstrap"
start=$(date +%s)
make bootstrap
echo "bootstrapped in $(( $(date +%s) - start )) s"

step "4. checks from zero"
make --no-print-directory connector-status
sleep 60  # the generator writes; the initial snapshots and the stream flow
docker compose -f onprem/compose.yaml exec -T kafka /opt/kafka/bin/kafka-get-offsets.sh \
  --bootstrap-server localhost:9092 --topic-partitions 'thelook.shop.orders:0,thelook_mongo.web.events:0'
curl -s localhost:9090/api/v1/targets | python3 -c '
import json, sys
for t in json.load(sys.stdin)["data"]["activeTargets"]:
    print("  prometheus target", t["labels"]["job"], t["health"])'

step "5. AWS: the code reproduces it, one command would destroy it"
make tf-init > /dev/null
set +e
terraform -chdir=infra/terraform/lake plan -lock=false -detailed-exitcode -no-color > "$WORK/plan.txt" 2>&1
code=$?
set -e
grep -E "No changes|Plan:" "$WORK/plan.txt" || tail -5 "$WORK/plan.txt"
echo "plan exit code $code (0 = no changes)"
terraform -chdir=infra/terraform/lake plan -destroy -lock=false -no-color 2>&1 | grep -E "^Plan:"

step "6. one command down, volumes included (project $PROJECT only)"
start=$(date +%s)
docker compose -f onprem/compose.yaml --profile '*' down -v
echo "down in $(( $(date +%s) - start )) s"
echo "left behind: containers $(docker ps -a --filter "label=com.docker.compose.project=$PROJECT" -q | wc -l)," \
  "volumes $(docker volume ls --filter "label=com.docker.compose.project=$PROJECT" -q | wc -l)"
[ "$code" -eq 0 ]
