# Entry points for common tasks. Run `make <target>` from the repository root.

CONNECT_URL ?= http://localhost:8083
CONNECTORS  := $(wildcard onprem/connect/connectors/*.json)

.PHONY: register-connectors connector-status

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
