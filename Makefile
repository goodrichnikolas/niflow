.PHONY: help install nifi-up nifi-down nifi-logs nifi-wait nifi1-up nifi1-down nifi1-wait \
	test test-integration test-integration-v1 catalog catalog-v1 convert example clean \
	list pull push copy diff validate gui

help:
	@echo "NiFlow make targets:"
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
	@echo "    install          Install niflow + dev deps (editable)"
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
	@echo "    catalog              Regenerate processor/service catalogs from NiFi"
	@echo "    catalog-v1           Regenerate the 1.x property compat table (localhost:8444)"
	@echo "    fixtures             Refresh real-server golden snapshots (tests/fixtures/real/)"
	@echo "    convert              make convert IN=flow.json OUT=flow.py [FLAGS=...]"
	@echo "    example              Deploy examples/simple_etl.py"
	@echo "    clean                Remove caches and NiFi data dirs"

install:
	pip install -e ".[dev]"

# --- live-NiFi workflow -------------------------------------------------------

list:
	python -m niflow list

copy:
	@if [ -z "$(GROUP)" ]; then echo "Usage: make copy GROUP=<name-or-id> [NAME='My Copy']"; exit 2; fi
	python -m niflow copy "$(GROUP)" $(if $(NAME),--name "$(NAME)")

pull:
	@if [ -z "$(GROUP)" ] || [ -z "$(OUT)" ]; then \
		echo "Usage: make pull GROUP=<name-or-id> OUT=flows/my_flow.py"; exit 2; \
	fi
	python -m niflow pull "$(GROUP)" -o "$(OUT)"

diff:
	@if [ -z "$(FILE)" ]; then echo "Usage: make diff FILE=flows/my_flow.py"; exit 2; fi
	python -m niflow diff "$(FILE)"

validate:
	@if [ -z "$(FILE)" ]; then echo "Usage: make validate FILE=flows/my_flow.py"; exit 2; fi
	python -m niflow validate "$(FILE)"

push:
	@if [ -z "$(FILE)" ]; then echo "Usage: make push FILE=flows/my_flow.py [START=1]"; exit 2; fi
	python -m niflow push "$(FILE)" $(if $(START),--start)

gui:
	python -m niflow.gui

webgui:
	python -m niflow.webgui --reload

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
	pytest -m "not integration" -v

test-integration:
	pytest -m integration -v

test-integration-v1:
	NIFLOW_NIFI_HOST=https://localhost:8444/nifi-api pytest -m integration -v

catalog:
	python -m niflow.codegen

catalog-v1:
	NIFLOW_NIFI_HOST=https://localhost:8444/nifi-api python -m niflow.codegen --compat

# Refresh the real-server golden fixtures (tests/fixtures/real/) from the
# NiFi in NIFLOW_NIFI_HOST. Run once per NiFi line:
#   make fixtures
#   NIFLOW_NIFI_HOST=https://localhost:8444/nifi-api make fixtures
fixtures:
	python scripts/capture_fixtures.py

convert:
	@if [ -z "$(IN)" ] || [ -z "$(OUT)" ]; then \
		echo "Usage: make convert IN=<input> OUT=<output> [FLAGS='--from xml --to py']"; exit 2; \
	fi
	python -m niflow.convert $(IN) $(OUT) $(FLAGS)

example:
	python examples/simple_etl.py

clean:
	rm -rf .pytest_cache **/__pycache__ .nifi-data
