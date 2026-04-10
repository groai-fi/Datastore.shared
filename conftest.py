"""
Root pytest conftest — loaded automatically before any test session.

Loads .env from the project root so that integration tests can pick up
S3 credentials (S3_ENDPOINT_URL, S3_BUCKET_NAME, etc.) and Binance API
keys without needing them pre-exported in the shell.

Behaviour
---------
- If python-dotenv is installed, .env is loaded into os.environ.
- If python-dotenv is NOT installed, a warning is printed but tests continue
  (unit tests that don't need env vars will still run; integration tests
  that require them will be skipped by their own setUpClass guards).
- If .env does not exist, this is also a silent no-op.
"""
from pathlib import Path


def pytest_configure(config):
    """Load .env into os.environ before any test collection."""
    env_path = Path(__file__).parent / ".env"
    if not env_path.exists():
        return

    try:
        from dotenv import load_dotenv
        load_dotenv(dotenv_path=env_path, override=False)
    except ImportError:
        import warnings
        warnings.warn(
            "python-dotenv is not installed. "
            ".env credentials will NOT be loaded for integration tests. "
            "Install with: pip install python-dotenv",
            stacklevel=1,
        )
