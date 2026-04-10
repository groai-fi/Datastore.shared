# groai-fi-datastore-shared Makefile
# Usage: make <target>

.PHONY: help install install-dev build publish clean test lint

# ── Meta ─────────────────────────────────────────────────────────────────────
help:  ## Show this help message
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-18s\033[0m %s\n", $$1, $$2}'

# ── Setup ─────────────────────────────────────────────────────────────────────
install:  ## Install package (production deps)
	uv sync

install-dev:  ## Install package with dev/test extras
	uv sync --extra dev

# ── Testing ───────────────────────────────────────────────────────────────────
test:  ## Run tests
	uv run pytest tests/ -v --tb=short

test-cov:  ## Run tests with coverage report
	uv run pytest tests/ -v --cov=src/groai_fi_datastore_shared --cov-report=term-missing

# ── Code quality ─────────────────────────────────────────────────────────────
lint:  ## Run ruff linter
	uv run ruff check src/ tests/

format:  ## Format code with ruff
	uv run ruff format src/ tests/

# ── Build & Publish ───────────────────────────────────────────────────────────
build:  ## Build wheel and sdist into dist/
	uv build

publish: build  ## Publish to PyPI (requires UV_PUBLISH_TOKEN env var)
	uv publish

publish-test: build  ## Publish to TestPyPI
	uv publish --publish-url https://test.pypi.org/legacy/

# ── Cleanup ───────────────────────────────────────────────────────────────────
clean:  ## Remove build artifacts
	rm -rf dist/ *.egg-info/ .pytest_cache/ .ruff_cache/ htmlcov/ .coverage
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
