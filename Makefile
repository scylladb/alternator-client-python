MAKEFILE_PATH := $(abspath $(dir $(abspath $(lastword $(MAKEFILE_LIST)))))
SCYLLA_CCM_COMMIT := d15a2fab9d22fffad8a30c806a7c8e1632e58aae
SCYLLA_CCM_VENV := $(MAKEFILE_PATH)/bin/scylla-ccm-$(SCYLLA_CCM_COMMIT)
SCYLLA_CCM_PATH ?= $(SCYLLA_CCM_VENV)/bin/ccm
ifeq ($(strip $(SCYLLA_CCM_PATH)),)
override SCYLLA_CCM_PATH := $(SCYLLA_CCM_VENV)/bin/ccm
endif
SCYLLA_VERSION ?= release:2025.2.5
SCYLLA_CCM_DIAGNOSTICS_DIR ?= $(MAKEFILE_PATH)/test-results/ccm
export SCYLLA_CCM_PATH SCYLLA_VERSION SCYLLA_CCM_DIAGNOSTICS_DIR

.PHONY: install verify build compile compile-test compile-demo test-unit test-infrastructure test-integration test-integration-only test-demo test-demo-only test-all test lint lint-types typecheck lint-fix clean ccm-install help

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
	@echo "  test-infrastructure - Run CCM infrastructure unit tests"
	@echo "  test-integration - Run integration tests, including demos, with native CCM"
	@echo "  test-demo        - Run only examples against a CCM-provisioned cluster"
	@echo "  test-all         - Run unit, integration, and demo tests"
	@echo "  test             - Run all tests"
	@echo "  lint             - Run linters (ruff + mypy)"
	@echo "  lint-types       - Enforce type annotations with ruff"
	@echo "  typecheck        - Run mypy type checks"
	@echo "  lint-fix         - Auto-fix linting issues with ruff"
	@echo "  clean            - Remove build artifacts"
	@echo "  ccm-install      - Install and validate the pinned scylla-ccm revision"

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

test-infrastructure:
	uv run pytest tests/unit/test_ccm_*.py -v --tb=short --timeout=120

test-integration: ccm-install
	$(MAKE) test-integration-only

test-integration-only:
	mkdir -p "$(SCYLLA_CCM_DIAGNOSTICS_DIR)"
	INTEGRATION_TESTS=true uv run pytest \
		tests/integration/test_ccm_provisioning.py \
		-v --tb=short --timeout=1200 \
		--junitxml=test-results/ccm-provisioning.xml
	INTEGRATION_TESTS=true uv run pytest tests/integration/ \
		--ignore=tests/integration/test_ccm_provisioning.py \
		-v --tb=short --timeout=120 \
		--junitxml=test-results/integration.xml \
		--cov=alternator --cov-report=xml --cov-report=term-missing

test-demo: ccm-install
	$(MAKE) test-demo-only

test-demo-only: compile-demo
	INTEGRATION_TESTS=true uv run pytest tests/integration/test_demos.py \
		-v --tb=short --timeout=120

test-all:
	$(MAKE) test-unit
	$(MAKE) test-integration

# Run all tests
test: test-all

# Run linters
lint: lint-types typecheck
	uv run ruff check alternator/ tests/ examples/
	uv run ruff format --check alternator/ tests/ examples/
	$(MAKE) compile-demo
	@# Ensure every noqa/type: ignore has an explanation after it
	@if grep -rn --include='*.py' -P '#\s*(noqa|type:\s*ignore)(?::\s*\S+)?\s*$$' alternator/ tests/ examples/; then \
		echo "ERROR: Found noqa/type: ignore comments without explanations. Add ' -- reason' after each."; \
		exit 1; \
	fi

lint-types:
	uv run ruff check --select ANN alternator/ tests/ examples/

typecheck:
	uv run mypy alternator/ examples/ tests/unit/ \
		--exclude 'tests/unit/test_ccm_.*\.py' --strict
	uv run mypy --platform linux tests/testinfra/ \
		$(wildcard tests/unit/test_ccm_*.py) --strict

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
	rm -rf test-results/
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
	find . -type f -name "*.pyc" -delete

ccm-install:
	uv run python -m tests.testinfra.ccm_install
