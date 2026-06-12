.PHONY: help install nifi-up nifi-down nifi-logs nifi-wait nifi1-up nifi1-down nifi1-wait \
	test test-integration test-integration-v1 catalog convert example clean \
	list pull push copy diff

help:
	@echo "NiFlow make targets:"
	@echo ""
	@echo "  Workflow (against the NiFi in NIFLOW_NIFI_HOST, default local Docker):"
	@echo "    list             Show the process-group tree with ids"
	@echo "    copy GROUP=name              Clone a group as a detached working copy"
	@echo "    pull GROUP=name OUT=flow.py  Pull a group into Python code"
	@echo "    diff FILE=flow.py            Diff local Python vs the live group"
	@echo "    push FILE=flow.py [START=1]  Replace the live group from Python"
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

push:
	@if [ -z "$(FILE)" ]; then echo "Usage: make push FILE=flows/my_flow.py [START=1]"; exit 2; fi
	python -m niflow push "$(FILE)" $(if $(START),--start)

# --- local NiFi containers ----------------------------------------------------

nifi-up:
	mkdir -p .nifi-data/in .nifi-data/out
	docker compose up -d nifi
	@echo ""
	@echo "NiFi 2.x starting:"
	@echo "  UI:  https://localhost:8443/nifi  (admin / adminpassword123)"
	@echo "  API: https://localhost:8443/nifi-api"
	@echo "Run 'make nifi-wait' to block until it's ready."

nifi1-up:
	mkdir -p .nifi-data/in .nifi-data/out
	docker compose --profile v1 up -d nifi1
	@echo ""
	@echo "NiFi 1.24.0 starting:"
	@echo "  UI:  https://localhost:8444/nifi  (admin / adminpassword123)"
	@echo "  API: https://localhost:8444/nifi-api  (set NIFLOW_NIFI_HOST to this for the CLI)"
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

convert:
	@if [ -z "$(IN)" ] || [ -z "$(OUT)" ]; then \
		echo "Usage: make convert IN=<input> OUT=<output> [FLAGS='--from xml --to py']"; exit 2; \
	fi
	python -m niflow.convert $(IN) $(OUT) $(FLAGS)

example:
	python examples/simple_etl.py

clean:
	rm -rf .pytest_cache **/__pycache__ .nifi-data
