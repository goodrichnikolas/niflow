# --- interpreter discovery ----------------------------------------------------
# Not every machine has a bare `python` on PATH (work's doesn't), and the
# project may live in a venv or be uv-managed. Take the first that exists:
#   1. an activated virtualenv ($VIRTUAL_ENV)
#   2. a project-local ./.venv
#   3. `uv run python`, when uv is installed AND this is a uv project (uv.lock)
#   4. python3, else plain python
# The user always wins:  make PY=/path/to/python <target>   (`make help` prints
# which interpreter was detected).
PY ?= $(shell \
	if [ -n "$$VIRTUAL_ENV" ] && [ -x "$$VIRTUAL_ENV/bin/python" ]; then echo "$$VIRTUAL_ENV/bin/python"; \
	elif [ -x .venv/bin/python ]; then echo .venv/bin/python; \
	elif command -v uv >/dev/null 2>&1 && [ -f uv.lock ]; then echo "uv run python"; \
	elif command -v python3 >/dev/null 2>&1; then echo python3; \
	else echo python; fi)
# uv owns its environment, so install through `uv pip`; otherwise use the
# detected interpreter's own pip, so `make install` lands where `make test`
# will look for it.
PIP ?= $(if $(filter uv,$(firstword $(PY))),uv pip,$(PY) -m pip)

.PHONY: help install nifi-up nifi-down nifi-logs nifi-wait nifi1-up nifi1-down nifi1-wait \
	test test-integration test-integration-v1 fuzz fuzz-v1 catalog catalog-v1 \
	import-defaults import-defaults-v1 version-map convert example clean \
	list pull push copy diff validate gui

help:
	@echo "NiFlow make targets:"
	@echo ""
	@echo "  Interpreter: $(PY)  (override: make PY=/path/to/python <target>)"
	@echo ""
	@echo "  Workflow (against the NiFi in NIFLOW_NIFI_HOST, default local Docker):"
	@echo "    list             Show the process-group tree with ids"
	@echo "    copy GROUP=name              Clone a group as a detached working copy"
	@echo "    pull GROUP=name OUT=flow.py  Pull a group into Python code"
	@echo "    validate FILE=flow.py        Statically check a flow before pushing"
	@echo "    diff FILE=flow.py            Diff local Python vs the live group"
	@echo "    push FILE=flow.py [START=1]  Replace the live group from Python"
	@echo "    gui / webgui                 Desktop helper / browser helper"
	@echo ""
	@echo "  More CLI (run 'niflow <cmd> -h'): plan, test, drift, diagram, tidy,"
	@echo "    explain, backup, rollback, commit, doctor, pull/push --all"
	@echo ""
	@echo "  Environment:"
	@echo "    install          Install niflow + dev deps (editable, via $(PIP))"
	@echo "    nifi-up          Start local NiFi 2.7.2  (https://localhost:8443/nifi)"
	@echo "    nifi1-up         Start local NiFi 1.24.0 (https://localhost:8444/nifi)"
	@echo "    nifi-wait        Block until NiFi 2.x is healthy   (nifi1-wait for 1.24)"
	@echo "    nifi-down        Stop NiFi 2.x                     (nifi1-down for 1.24)"
	@echo "    nifi-logs        Tail NiFi container logs"
	@echo ""
	@echo "  Development:"
	@echo "    test                 Run unit tests (no NiFi required)"
	@echo "    test-integration     Integration tests against NiFi 2.x (localhost:8443)"
	@echo "    test-integration-v1  Integration tests against NiFi 1.24 (localhost:8444)"
	@echo "    fuzz                 Bug-hunt: thousands of generated micro-flows"
	@echo "                         (TIER=1|2|3 COUNT=n SEED=n TYPES=re RESUME=1; exit 1 = found one)"
	@echo "    catalog              Regenerate processor/service catalogs from NiFi"
	@echo "    catalog-v1           Regenerate the 1.x property compat table (localhost:8444)"
	@echo "    import-defaults      Record what a flow IMPORT writes that create does not"
	@echo "                         (import-defaults-v1 for 1.24; one block per line)"
	@echo "    version-map          Rebuild the 1.x-vs-2.x property difference map (BOTH NiFis up)"
	@echo "    fixtures             Refresh real-server golden snapshots (tests/fixtures/real/)"
	@echo "    convert              make convert IN=flow.json OUT=flow.py [FLAGS=...]"
	@echo "    example              Deploy examples/simple_etl.py"
	@echo "    clean                Remove caches and NiFi data dirs"

install:
	$(PIP) install -e ".[dev]"

# --- live-NiFi workflow -------------------------------------------------------

list:
	$(PY) -m niflow list

copy:
	@if [ -z "$(GROUP)" ]; then echo "Usage: make copy GROUP=<name-or-id> [NAME='My Copy']"; exit 2; fi
	$(PY) -m niflow copy "$(GROUP)" $(if $(NAME),--name "$(NAME)")

pull:
	@if [ -z "$(GROUP)" ] || [ -z "$(OUT)" ]; then \
		echo "Usage: make pull GROUP=<name-or-id> OUT=flows/my_flow.py"; exit 2; \
	fi
	$(PY) -m niflow pull "$(GROUP)" -o "$(OUT)"

diff:
	@if [ -z "$(FILE)" ]; then echo "Usage: make diff FILE=flows/my_flow.py"; exit 2; fi
	$(PY) -m niflow diff "$(FILE)"

validate:
	@if [ -z "$(FILE)" ]; then echo "Usage: make validate FILE=flows/my_flow.py"; exit 2; fi
	$(PY) -m niflow validate "$(FILE)"

push:
	@if [ -z "$(FILE)" ]; then echo "Usage: make push FILE=flows/my_flow.py [START=1]"; exit 2; fi
	$(PY) -m niflow push "$(FILE)" $(if $(START),--start)

gui:
	$(PY) -m niflow.gui

webgui:
	$(PY) -m niflow.webgui --reload

# --- local NiFi containers ----------------------------------------------------

nifi-up:
	mkdir -p .nifi-data/in .nifi-data/out
	docker compose up -d nifi registry
	@echo ""
	@echo "NiFi 2.x starting:"
	@echo "  UI:       https://localhost:8443/nifi  (admin / adminpassword123)"
	@echo "  API:      https://localhost:8443/nifi-api"
	@echo "  Registry: http://localhost:18080/nifi-registry"
	@echo "Run 'make nifi-wait' to block until it's ready."

nifi1-up:
	mkdir -p .nifi-data/in .nifi-data/out
	docker compose --profile v1 up -d nifi1 registry1
	@echo ""
	@echo "NiFi 1.24.0 starting:"
	@echo "  UI:       https://localhost:8444/nifi  (admin / adminpassword123)"
	@echo "  API:      https://localhost:8444/nifi-api  (set NIFLOW_NIFI_HOST to this for the CLI)"
	@echo "  Registry: http://localhost:18081/nifi-registry"
	@echo "Run 'make nifi1-wait' to block until it's ready."

# A REAL two-node cluster (1.24, plain HTTP — see docker-compose.yml for why).
# The only way to exercise primary-node-only scheduling, load-balanced
# connections actually redistributing, and what niflow does when a node drops.
cluster-up:
	docker compose --profile cluster up -d cluster-zk cluster-n1 cluster-n2
	@echo ""
	@echo "NiFi 1.24.0 cluster starting (2 nodes, no auth):"
	@echo "  node 1: http://localhost:8180/nifi   API http://localhost:8180/nifi-api"
	@echo "  node 2: http://localhost:8181/nifi   API http://localhost:8181/nifi-api"
	@echo "Run 'make cluster-wait' to block until BOTH nodes have joined."

cluster-wait:
	@echo "Waiting for both cluster nodes to join..."
	@until [ "$$(docker inspect -f '{{.State.Health.Status}}' niflow-cluster-n1 2>/dev/null)" = "healthy" ] \
	    && [ "$$(docker inspect -f '{{.State.Health.Status}}' niflow-cluster-n2 2>/dev/null)" = "healthy" ]; do \
		printf "."; sleep 5; \
	done; echo " ready."
	@curl -s http://localhost:8180/nifi-api/flow/cluster/summary; echo ""

cluster-down:
	docker compose --profile cluster down

cluster-logs:
	docker compose --profile cluster logs -f cluster-n1 cluster-n2

nifi-wait:
	@echo "Waiting for NiFi 2.x to become healthy..."
	@until [ "$$(docker inspect -f '{{.State.Health.Status}}' niflow-nifi 2>/dev/null)" = "healthy" ]; do \
		printf "."; sleep 5; \
	done; echo " ready."
	@echo "NiFi 2.x is up at https://localhost:8443/nifi  (admin / adminpassword123)"

nifi1-wait:
	@echo "Waiting for NiFi 1.24 to become healthy..."
	@until [ "$$(docker inspect -f '{{.State.Health.Status}}' niflow-nifi1 2>/dev/null)" = "healthy" ]; do \
		printf "."; sleep 5; \
	done; echo " ready."
	@echo "NiFi 1.24.0 is up at https://localhost:8444/nifi  (admin / adminpassword123)"

nifi-mtls-up:
	./scripts/gen-mtls-certs.sh certs/mtls
	docker compose --profile mtls up -d nifi-mtls
	@echo ""
	@echo "mTLS NiFi 1.24.0 starting (client-certificate auth, NO password):"
	@echo "  API:  https://localhost:8445/nifi-api"
	@echo "  Test: NIFLOW_CONFIG=certs/mtls/niflow.env niflow doctor"
	@printf "NIFLOW_NIFI_HOST=https://localhost:8445/nifi-api\nNIFLOW_NIFI_CLIENT_CERT=certs/mtls/client.pem\nNIFLOW_NIFI_CLIENT_KEY=certs/mtls/client.key\nNIFLOW_NIFI_CA_BUNDLE=certs/mtls/ca.pem\nNIFLOW_NIFI_PASSWORD=\n" > certs/mtls/niflow.env
	@echo "Run 'make nifi-mtls-wait' to block until it's ready."

nifi-mtls-wait:
	@echo "Waiting for mTLS NiFi to become healthy..."
	@until [ "$$(docker inspect -f '{{.State.Health.Status}}' niflow-nifi-mtls 2>/dev/null)" = "healthy" ]; do \
		printf "."; sleep 5; \
	done; echo " ready."
	@echo "mTLS NiFi is up at https://localhost:8445/nifi-api (auth: certs/mtls/client.pem)"

nifi-mtls-down:
	docker compose --profile mtls down

nifi-down:
	docker compose down

nifi1-down:
	docker compose --profile v1 down

nifi-logs:
	docker compose logs -f nifi

# --- tests / codegen ----------------------------------------------------------

test:
	$(PY) -m pytest -m "not integration" -v

test-integration:
	$(PY) -m pytest -m integration -v

test-integration-v1:
	NIFLOW_NIFI_HOST=https://localhost:8444/nifi-api $(PY) -m pytest -m integration -v

# The cluster-only suite (T7h). Skips itself unless a clustered NiFi answers
# at NIFLOW_CLUSTER_HOST, so it is safe to leave in the default run.
test-cluster:
	NIFLOW_NIFI_HOST=$(CLUSTER_HOST) NIFLOW_NIFI_PASSWORD= $(PY) -m pytest -m integration tests/test_cluster_live.py -v

CLUSTER_HOST ?= http://localhost:8180/nifi-api

# Bug-hunting sweep: thousands of generated micro-flows through niflow's own
# pipeline. Tier 1 needs no NiFi at all and takes seconds; tiers 2/3 push
# sandboxes into $(NIFLOW_NIFI_HOST) and want a long run:
#   make fuzz                                    # whole catalog, offline
#   make fuzz TIER=2 COUNT=200                   # + NiFi's own validation
#   make fuzz TIER=3 COUNT=100 TYPES='standard\.'  # + live push/pull/plan
#   make fuzz RESUME=1                           # continue an interrupted sweep
# Findings land in .niflow-fuzz/ (results.jsonl + a standalone repro per bug).
fuzz:
	$(PY) -m niflow fuzz \
		$(if $(TIER),--tier $(TIER)) \
		$(if $(COUNT),--count $(COUNT)) \
		$(if $(SEED),--seed $(SEED)) \
		$(if $(KINDS),--kinds "$(KINDS)") \
		$(if $(TYPES),--types "$(TYPES)") \
		$(if $(OUT),-o "$(OUT)") \
		$(if $(RESUME),--resume) \
		$(if $(REPLAY),--replay "$(REPLAY)")

fuzz-v1:
	NIFLOW_NIFI_HOST=https://localhost:8444/nifi-api $(MAKE) fuzz TIER=$(or $(TIER),3)

catalog:
	$(PY) -m niflow.codegen

catalog-v1:
	NIFLOW_NIFI_HOST=https://localhost:8444/nifi-api $(PY) -m niflow.codegen --compat

# What a NiFi line writes onto a component when it IMPORTS a flow, over and
# above the property descriptors' own defaults (2.7.2 gives a JsonRecordSetWriter
# `Allow Scientific Notation = true` on import, `false` on create). Run once per
# line — each writes its own block into niflow/import_defaults.py and leaves the
# other alone. Both containers up:  make import-defaults import-defaults-v1
import-defaults:
	$(PY) -m niflow.codegen --import-defaults

import-defaults-v1:
	NIFLOW_NIFI_HOST=https://localhost:8444/nifi-api $(PY) -m niflow.codegen --import-defaults

# Regenerate the cross-version property difference map (niflow/version_map.py)
# and its human-readable report (docs/version-compat.md). Needs BOTH lines up:
#   make nifi-up nifi1-up && make nifi-wait nifi1-wait && make version-map
# Point NIFLOW_NIFI_HOST_NEW/_OLD elsewhere to map the pair you actually run
# (e.g. your 2.x sandbox against work's 1.28) instead of the local containers.
VERSION_MAP_NEW ?= https://localhost:8443/nifi-api
VERSION_MAP_OLD ?= https://localhost:8444/nifi-api
VERSION_MAP_DIR ?= .niflow-rulebooks

version-map:
	@mkdir -p $(VERSION_MAP_DIR)
	NIFLOW_NIFI_HOST=$(VERSION_MAP_NEW) $(PY) -m niflow.codegen \
		--dump-rulebook $(VERSION_MAP_DIR)/new.json
	NIFLOW_NIFI_HOST=$(VERSION_MAP_OLD) $(PY) -m niflow.codegen \
		--dump-rulebook $(VERSION_MAP_DIR)/old.json
	$(PY) -m niflow.versiondiff $(VERSION_MAP_DIR)/new.json $(VERSION_MAP_DIR)/old.json

# Refresh the real-server golden fixtures (tests/fixtures/real/) from the
# NiFi in NIFLOW_NIFI_HOST. Run once per NiFi line:
#   make fixtures
#   NIFLOW_NIFI_HOST=https://localhost:8444/nifi-api make fixtures
fixtures:
	$(PY) scripts/capture_fixtures.py

convert:
	@if [ -z "$(IN)" ] || [ -z "$(OUT)" ]; then \
		echo "Usage: make convert IN=<input> OUT=<output> [FLAGS='--from xml --to py']"; exit 2; \
	fi
	$(PY) -m niflow.convert $(IN) $(OUT) $(FLAGS)

example:
	$(PY) examples/simple_etl.py

clean:
	rm -rf .pytest_cache **/__pycache__ .nifi-data
