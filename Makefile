MAKEFILE_PATH := $(abspath $(dir $(abspath $(lastword $(MAKEFILE_LIST)))))
COMPOSE ?= docker compose
SCYLLA_IMAGE ?= scylladb/scylla:2025.1
SCYLLA_HOST ?= localhost
SCYLLA_PORT ?= 9998
SCYLLA_READY_URL ?= http://$(SCYLLA_HOST):$(SCYLLA_PORT)/localnodes
DOCKER_CACHE_DIR := $(MAKEFILE_PATH)/.docker-cache
DOCKER_CACHE_FILE := $(DOCKER_CACHE_DIR)/scylla-image.tar
CERT_CACHE_DIR := $(MAKEFILE_PATH)/.cert-cache
CERT_DIR := $(MAKEFILE_PATH)/tests/scylla

.PHONY: install verify build compile compile-test compile-demo test-unit test-integration test-integration-only test-demo test-demo-only test-all test lint lint-fix clean wait-for-alternator scylla-start scylla-stop scylla-kill scylla-rm docker-pull docker-cache-save docker-cache-load cert-cache-save cert-cache-load help

# Default target
help:
	@echo "Available targets:"
	@echo "  install          - Install package in development mode"
	@echo "  verify           - Run lint, compile, unit tests, and package checks"
	@echo "  build            - Build package artifacts and validate metadata"
	@echo "  compile          - Compile package modules"
	@echo "  compile-test     - Compile test modules"
	@echo "  compile-demo     - Compile examples"
	@echo "  test-unit        - Run unit tests"
	@echo "  test-integration - Start Scylla, run integration tests, then stop Scylla"
	@echo "  test-demo        - Start Scylla, run examples, then stop Scylla"
	@echo "  test-all         - Run unit, integration, and demo tests"
	@echo "  test             - Run all tests"
	@echo "  lint             - Run linters (ruff + mypy)"
	@echo "  lint-fix         - Auto-fix linting issues with ruff"
	@echo "  clean            - Remove build artifacts"
	@echo "  scylla-start     - Start 3-node Scylla cluster via Docker"
	@echo "  scylla-stop      - Stop Scylla cluster"
	@echo "  scylla-kill      - Force kill Scylla cluster"
	@echo "  scylla-rm        - Remove Scylla containers and volumes"

# Install package in development mode with all dependencies
install:
	uv sync --all-extras

verify: lint compile compile-test compile-demo test-unit build

build:
	rm -rf build/ dist/ *.egg-info/
	uvx --from build pyproject-build
	uvx twine check dist/*

compile:
	uv run python -m compileall -q alternator/

compile-test:
	uv run python -m compileall -q tests/

compile-demo:
	uv run python -m compileall -q examples/

# Run unit tests only (no external dependencies)
test-unit:
	uv run pytest tests/unit/ -v --tb=short --timeout=60 \
		--cov=alternator --cov-report=xml --cov-report=term-missing --cov-fail-under=70

# Run integration tests and manage the local Scylla cluster lifecycle
test-integration:
	@set -e; \
	trap '$(MAKE) scylla-stop' EXIT; \
	$(MAKE) scylla-start; \
	$(MAKE) test-integration-only

# Run integration tests against an already-running Scylla cluster
test-integration-only:
	uv run pytest tests/integration/ -v --tb=short --timeout=120 \
		--cov=alternator --cov-report=xml --cov-report=term-missing

test-demo:
	@set -e; \
	trap '$(MAKE) scylla-stop' EXIT; \
	$(MAKE) scylla-start; \
	$(MAKE) test-demo-only

test-demo-only: compile-demo
	SCYLLA_HOST=$(SCYLLA_HOST) SCYLLA_PORT=$(SCYLLA_PORT) uv run python examples/sync_demo.py
	SCYLLA_HOST=$(SCYLLA_HOST) SCYLLA_PORT=$(SCYLLA_PORT) uv run python examples/async_demo.py
	uv run python examples/capability_configuration.py

test-all:
	$(MAKE) test-unit
	@set -e; \
	trap '$(MAKE) scylla-stop' EXIT; \
	$(MAKE) scylla-start; \
	$(MAKE) test-integration-only; \
	$(MAKE) test-demo-only

# Run all tests
test:
	uv run pytest tests/ -v --tb=short --timeout=60 \
		--cov=alternator --cov-report=xml --cov-report=term-missing

# Run linters
lint:
	uv run ruff check alternator/ tests/ examples/
	uv run ruff format --check alternator/ tests/ examples/
	$(MAKE) compile-demo
	uv run mypy alternator/ --strict
	@# Ensure every noqa/type: ignore has an explanation after it
	@if grep -rn --include='*.py' -P '#\s*(noqa|type:\s*ignore)(?::\s*\S+)?\s*$$' alternator/ tests/ examples/; then \
		echo "ERROR: Found noqa/type: ignore comments without explanations. Add ' -- reason' after each."; \
		exit 1; \
	fi

# Auto-fix linting issues
lint-fix:
	uv run ruff check --fix alternator/ tests/ examples/
	uv run ruff format alternator/ tests/ examples/

# Remove build artifacts
clean:
	rm -rf build/
	rm -rf dist/
	rm -rf *.egg-info/
	rm -rf .pytest_cache/
	rm -rf .mypy_cache/
	rm -rf .ruff_cache/
	rm -rf .coverage
	rm -rf htmlcov/
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
	find . -type f -name "*.pyc" -delete

# Generate self-signed certificate for Alternator HTTPS
.prepare-cert: tests/scylla/db.key tests/scylla/db.crt

tests/scylla/db.key tests/scylla/db.crt:
	openssl req -x509 -newkey rsa:4096 -keyout tests/scylla/db.key -out tests/scylla/db.crt \
		-days 365 -nodes -subj '/CN=localhost' \
		-addext 'subjectAltName=IP:172.43.0.2,IP:172.43.0.3,IP:172.43.0.4,DNS:localhost'

wait-for-alternator:
	@echo "Waiting for Scylla cluster to be ready..."
	@for i in $$(seq 1 60); do \
		if curl -sf "$(SCYLLA_READY_URL)" > /dev/null 2>&1; then \
			echo "Scylla cluster is ready."; \
			exit 0; \
		fi; \
		echo "Waiting for Scylla Alternator... ($$i)"; \
		sleep 5; \
	done; \
	echo "Timed out waiting for Scylla Alternator at $(SCYLLA_READY_URL)"; \
	exit 1

# Start Scylla cluster with Docker Compose
scylla-start: cert-cache-load docker-cache-load
	$(COMPOSE) up -d
	$(MAKE) wait-for-alternator

# Stop Scylla cluster
scylla-stop:
	$(COMPOSE) down -v

# Force kill Scylla cluster
scylla-kill:
	$(COMPOSE) kill

# Remove Scylla containers and volumes
scylla-rm:
	$(COMPOSE) rm -f -v

docker-pull:
	docker pull $(SCYLLA_IMAGE)

docker-cache-save: docker-pull
	mkdir -p $(DOCKER_CACHE_DIR)
	docker save $(SCYLLA_IMAGE) -o $(DOCKER_CACHE_FILE)

docker-cache-load:
	@if [ -f "$(DOCKER_CACHE_FILE)" ]; then \
		echo "Loading Docker image from cache..."; \
		docker load -i $(DOCKER_CACHE_FILE); \
	else \
		echo "Docker image cache not found, pulling $(SCYLLA_IMAGE)..."; \
		$(MAKE) docker-pull; \
	fi

cert-cache-save: .prepare-cert
	mkdir -p $(CERT_CACHE_DIR)
	cp $(CERT_DIR)/db.key $(CERT_DIR)/db.crt $(CERT_CACHE_DIR)/

cert-cache-load:
	@if [ -f "$(CERT_CACHE_DIR)/db.key" ] && [ -f "$(CERT_CACHE_DIR)/db.crt" ]; then \
		echo "Loading certificates from cache..."; \
		cp "$(CERT_CACHE_DIR)/db.key" "$(CERT_CACHE_DIR)/db.crt" "$(CERT_DIR)/"; \
		chmod 644 "$(CERT_DIR)/db.key"; \
	else \
		echo "Certificate cache not found, generating..."; \
		$(MAKE) .prepare-cert; \
	fi
