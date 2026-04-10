# TODO: Path Resolution Refactoring

## Context

The `get_project_root()` function in `utils.py` currently resolves paths using
`Path(__file__).parent.parent.parent`, which returns the `src/` directory when
the package is installed via pip. CLI scripts that relied on this to build
relative paths like `appData/trainData_crypto/prices_v3.parquet` have been
**partially fixed** by requiring `--path` / `--price-root` as explicit CLI args.

However, there are remaining places in the codebase that still assume
a fixed project root. Below is the full list of items that need addressing.

---

## Issues to Fix

### 1. `get_project_root()` in `utils.py`
**File**: `src/groai_fi_datastore_shared/Binance/utils.py` — line 54

**Problem**: Returns `Path(__file__).parent.parent.parent` (= `src/` when installed).

**Options**:
- Option A: Remove entirely; require callers to pass paths explicitly (preferred)
- Option B: Replace with an env var: `os.getenv("GROAI_DATA_DIR", os.getcwd())`
- Option C: Keep for backward compat but add a deprecation warning

---

### 2. `BinanceMarketDataDownloader.catchup_price_binance`
**File**: `src/groai_fi_datastore_shared/Binance/BinanceMarketDataDownloader.py` — lines 307, 440

**Problem**: Still calls `utils.get_project_root()` internally to prepend to `price_root_dir`:
```python
dest_dir = f'{utils.get_project_root()}/{price_root_dir}/exchange={exchange}/symbol={symbol}'
```

**Fix**: `price_root_dir` should already be absolute; remove the `get_project_root()` prefix,
or add validation that raises if the path is relative.

---

### 3. `BinanceMarketDataDownloader.download_crypto_data`
**File**: `src/groai_fi_datastore_shared/Binance/BinanceMarketDataDownloader.py` — line 526

Same issue as above.

---

### 4. `BinanceMarketDataDownloader.is_price_table_exist`
**File**: `src/groai_fi_datastore_shared/Binance/BinanceMarketDataDownloader.py` — line 258

**Problem**: Detects relative paths like `./` or `appData/` and calls `get_project_root()` to make them absolute.

**Fix**: Remove heuristic; callers should always provide absolute paths.

---

### 5. `helper.setup_logger` (via `utils.py`)
**File**: `src/groai_fi_datastore_shared/Binance/utils.py` — line 135

**Problem**: Log file is written to `{project_root}/logs/{file_name}` using `get_project_root()`.

**Fix**: Accept an optional `log_dir` parameter. Default to `Path.cwd() / "logs"` or an env var.

---

### 6. `get_asset()` in `BinanceMarketDataDownloader.py`
**File**: line 52

**Problem**: Hardcodes output path `model_data/binance_asset.csv` — relative to CWD.

**Fix**: Make the output path a parameter, or use a configurable directory.

---

### 7. Module-level Binance client initialization
**File**: `src/groai_fi_datastore_shared/Binance/BinanceMarketDataDownloader.py` — line 24

**Problem**: `client = BinanceAPI(os.getenv(...), ...)` is executed at **import time**,
which means importing the module always attempts to create a Binance API client,
even in offline/test contexts.

**Fix**: Lazy-initialize the client inside functions, or use a module-level singleton
with explicit initialization (e.g., `BinanceMarketDataDownloader.init_client(key, secret)`).

---

## Recommended Approach

1. Add `GROAI_DATA_DIR` environment variable as a fallback for data paths
2. Make `get_project_root()` emit a `DeprecationWarning` and fall back to `GROAI_DATA_DIR`
3. Update all internal callers to use absolute paths
4. Move Binance client init to a lazy singleton pattern
5. Add a `validate_path(path)` helper that raises `ValueError` for relative paths

---

## Priority

| Issue | Impact | Priority |
|---|---|---|
| Module-level client init (#7) | Breaks tests and offline usage | 🔴 High |
| `catchup_price_binance` path (#2, #3) | Breaks installed CLI | 🔴 High |
| `get_project_root()` deprecation (#1) | Future-proofing | 🟡 Medium |
| Logger path (#5) | Minor inconvenience | 🟢 Low |
| `get_asset()` hardcoded path (#6) | Edge case | 🟢 Low |
