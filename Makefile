.PHONY: install test-unit test-integration test lint lint-fix clean scylla-start scylla-stop help

# Default Python interpreter
PYTHON ?= python3

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

# Install package in development mode with all dependencies
install:
	$(PYTHON) -m pip install -e ".[dev,async]"

# Run unit tests only (no external dependencies)
test-unit:
	$(PYTHON) -m pytest tests/unit/ -v -m "not integration"

# Run integration tests (requires running Scylla cluster)
test-integration:
	$(PYTHON) -m pytest tests/integration/ -v -m "integration"

# Run all tests
test:
	$(PYTHON) -m pytest tests/ -v

# Run linters
lint:
	$(PYTHON) -m ruff check alternator/ tests/
	$(PYTHON) -m ruff format --check alternator/ tests/
	$(PYTHON) -m mypy alternator/

# Auto-fix linting issues
lint-fix:
	$(PYTHON) -m ruff check --fix alternator/ tests/
	$(PYTHON) -m ruff format alternator/ tests/

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

# Start Scylla cluster with Docker Compose
scylla-start:
	docker compose up -d
	@echo "Waiting for Scylla cluster to be ready..."
	@for i in $$(seq 1 60); do \
		curl -sf http://localhost:8000/localnodes > /dev/null 2>&1 && break; \
		echo "Waiting for Scylla Alternator... ($$i)"; \
		sleep 5; \
	done
	@echo "Scylla cluster is ready."

# Stop Scylla cluster
scylla-stop:
	docker compose down -v
