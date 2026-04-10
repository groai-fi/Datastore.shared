# Changelog

## v0.2.1
- Added S3 native CLI tools (download, merge, list, remove) powered by DuckDB.
- Added local filesystem parity scripts (`binance-list-symbols`, `binance-remove-symbol`).
- Removed file-based loggers from scripts to make them cleanly executable as a pip package.
- Overhauled test infrastructure with robust `pytest` unit and integration tests.
- Configured root `conftest.py` with `python-dotenv` for local testing against live S3 buckets.
- Fixed a `pandas` datetime parsing issue caused by variable Binance API responses.

## v0.1.2
- **Docs**: Updated `binance-auto-update` CLI flags in README.
- **Chore**: Configured OIDC-based Trusted Publishing for PyPI and forced Node 24 for publishing workflows.
- **Test**: Marked Binance integration tests to properly exclude them from CI workflows.

## v0.1.1
- **Refactor**: Removed manual `sys.path` hacks in favor of strict package-based imports.
- **Refactor**: Deprecated `get_project_root`, implemented lazy Binance client, and enforced absolute paths for storage.
- **Test**: Updated test assertions to explicitly use UTC.

## v0.1.0
- Initial package structure and commit.
