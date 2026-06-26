.PHONY: install test-unit test-integration test lint lint-fix clean scylla-start scylla-stop scylla-kill scylla-rm help

# Default target
help:
	@echo "Available targets:"
	@echo "  install          - Install package in development mode"
	@echo "  test-unit        - Run unit tests"
	@echo "  test-integration - Run integration tests (requires Scylla)"
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

# Run unit tests only (no external dependencies)
test-unit:
	uv run pytest tests/unit/ -v --tb=short --timeout=60 \
		--cov=alternator --cov-report=xml --cov-report=term-missing --cov-fail-under=70

# Run integration tests (requires running Scylla cluster)
test-integration:
	uv run pytest tests/integration/ -v --tb=short --timeout=120 \
		--cov=alternator --cov-report=xml --cov-report=term-missing

# Run all tests
test:
	uv run pytest tests/ -v --tb=short --timeout=60 \
		--cov=alternator --cov-report=xml --cov-report=term-missing

# Run linters
lint:
	uv run ruff check alternator/ tests/ examples/
	uv run ruff format --check alternator/ tests/ examples/
	uv run python -m py_compile examples/*.py
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

# Start Scylla cluster with Docker Compose
scylla-start: .prepare-cert
	docker compose up -d
	@echo "Waiting for Scylla cluster to be ready..."
	@for i in $$(seq 1 60); do \
		curl -sf http://localhost:9998/localnodes > /dev/null 2>&1 && break; \
		echo "Waiting for Scylla Alternator... ($$i)"; \
		sleep 5; \
	done
	@echo "Scylla cluster is ready."

# Stop Scylla cluster
scylla-stop:
	docker compose down -v

# Force kill Scylla cluster
scylla-kill:
	docker compose kill

# Remove Scylla containers and volumes
scylla-rm:
	docker compose rm -f -v
