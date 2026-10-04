# Entry points for common tasks. Run `make <target>` from the repository root.

CONNECT_URL ?= http://localhost:8083
CONNECTORS  := $(wildcard onprem/connect/connectors/*.json)

.PHONY: up down ps spark-run test-spark register-connectors connector-status tf-bootstrap tf-init tf-plan tf-apply cost-report

# --- On-prem stack (Docker Compose) ------------------------------------------
# Profiles group services so a laptop runs only what a task needs (RAM per
# profile in the README). Examples:
#   make up                              # core: sources, Kafka, Connect
#   make up PROFILES="core monitoring"
#   make down                            # stops every profile, keeps volumes
PROFILES ?= core
COMPOSE  := docker compose -f onprem/compose.yaml

# --wait returns once every started service is healthy (or, for one-shot
# setup containers, has exited successfully), so the next command can rely
# on the stack being ready.
up:
	$(COMPOSE) $(foreach p,$(PROFILES),--profile $(p)) up -d --build --wait

# --profile '*' selects every profile, so nothing is left running whatever
# was started. Volumes are kept (data survives); `down -v` would delete them.
down:
	$(COMPOSE) --profile '*' down

ps:
	@$(COMPOSE) --profile '*' ps --format 'table {{.Service}}\t{{.Status}}'

# Run a PySpark job from spark/ on the cluster (needs the stream profile up):
#   make spark-run JOB=jobs/smoke_test.py
JOB ?= jobs/smoke_test.py
spark-run:
	$(COMPOSE) --profile jobs run --rm spark-job /opt/lakehouse/$(JOB)

# PySpark unit tests on the image's versions (Java 17 must be installed).
test-spark:
	cd spark && uv run --no-project --python 3.10 --with-requirements requirements-test.txt python -m pytest -q

# Create or update every connector. PUT /connectors/<name>/config is
# idempotent: it creates the connector if missing, otherwise replaces its
# config. --fail-with-body makes curl exit non-zero on an HTTP error and
# still print Connect's error message. PUT returns once the config is stored,
# before the worker has (re)started the connector, so wait up to 30 s for its
# state to become RUNNING before printing the status.
register-connectors:
	@for f in $(CONNECTORS); do \
	  name=$$(basename $$f .json); \
	  echo "==> $$name"; \
	  curl -sS --fail-with-body -X PUT -H 'Content-Type: application/json' \
	    --data @$$f $(CONNECT_URL)/connectors/$$name/config > /dev/null || exit 1; \
	  for i in $$(seq 30); do \
	    curl -sf $(CONNECT_URL)/connectors/$$name/status | grep -q '"connector":{"state":"RUNNING"' && break; sleep 1; \
	  done; \
	done
	@$(MAKE) --no-print-directory connector-status

# Connector and task states (RUNNING, PAUSED, FAILED + error trace).
connector-status:
	@curl -sS --fail-with-body '$(CONNECT_URL)/connectors?expand=status' | python3 -c \
	  'import json,sys; d = json.load(sys.stdin); print("(no connectors)") if not d else [print(n, s["status"]["connector"]["state"], [t["state"] for t in s["status"]["tasks"]]) for n, s in d.items()]'

# --- Terraform (AWS) ---------------------------------------------------------
# All targets use the project's own AWS profile, never the default one.
AWS_PROFILE ?= thelook
export AWS_PROFILE
# Download each provider once (it is ~190 MB) and share it between
# bootstrap and lake instead of one copy per .terraform/ folder.
TF_PLUGIN_CACHE_DIR ?= $(HOME)/.terraform.d/plugin-cache
export TF_PLUGIN_CACHE_DIR
TF_LAKE := infra/terraform/lake

# One-off: create the state bucket (local state, see bootstrap/main.tf).
tf-bootstrap:
	@mkdir -p $(TF_PLUGIN_CACHE_DIR)
	terraform -chdir=infra/terraform/bootstrap init
	terraform -chdir=infra/terraform/bootstrap apply

# The backend bucket name contains the account ID: computed, not committed.
tf-init:
	terraform -chdir=$(TF_LAKE) init \
	  -backend-config="bucket=thelook-tfstate-$$(aws sts get-caller-identity --query Account --output text)"

tf-plan:
	terraform -chdir=$(TF_LAKE) plan -out=tfplan

# Applies exactly the plan that was reviewed (tf-plan), nothing newer.
tf-apply:
	terraform -chdir=$(TF_LAKE) apply tfplan

# Weekly: last 7 days of spend by project tag and service (scripts/cost-report.sh).
cost-report:
	@scripts/cost-report.sh
