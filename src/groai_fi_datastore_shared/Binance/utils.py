"""Utility functions for Binance module"""
import traceback
import re
from decimal import Decimal, ROUND_DOWN, ROUND_UP
from pathlib import Path
from datetime import datetime as dt
from .config import tw_tz


def d(value):
    """Convert to Decimal"""
    if isinstance(value, Decimal):
        return value
    if value is None:
        return Decimal('0')
    return Decimal(str(value))


def d_round(value, precision, rounding=ROUND_DOWN):
    """Round Decimal to precision"""
    if precision == 0:
        return int(value)
    quantize_str = f"0.{'0' * precision}"
    return d(value).quantize(Decimal(quantize_str), rounding=rounding)


def d_round_fee(value, precision):
    """Round fee (always round up)"""
    return d_round(value, precision, rounding=ROUND_UP)


def d_abs(value):
    """Absolute value of Decimal"""
    return abs(d(value))


def d_negate(value):
    """Negate Decimal"""
    return -d(value)


def d_is_close(a, b, precision):
    """Check if two Decimals are close within precision"""
    threshold = Decimal(f"1e-{precision}")
    return abs(d(a) - d(b)) < threshold


def readable_error(e, file):
    """Format exception for logging"""
    tb = traceback.format_exc()
    return f"Error in {file}:\n{str(e)}\n{tb}"


def get_project_root():
    """Get project root directory.

    .. deprecated::
        This function resolves to the package's ``src/`` directory when the
        package is installed via pip, which is rarely what callers want.
        Pass absolute paths explicitly instead. This function will be removed
        in a future release.
    """
    import warnings
    warnings.warn(
        "get_project_root() is deprecated and will be removed in a future release. "
        "Pass absolute paths explicitly or set the GROAI_DATA_DIR environment variable.",
        DeprecationWarning,
        stacklevel=2,
    )
    return Path(__file__).parent.parent.parent


def normalize_fraction(value, precision):
    """Normalize fraction to precision"""
    return d_round(value, precision)


def least_significant_digit_power(value):
    """Get least significant digit power"""
    value_str = str(value)
    if '.' in value_str:
        return len(value_str.split('.')[1])
    return 0


def pretty_dict(d_dict):
    """Pretty print dictionary"""
    import json
    return json.dumps(d_dict, indent=2, default=str)


def convert_to_min(time_str):
    """Convert time string to minutes
    
    Supports formats like:
    - '1h' -> 60
    - '30m' -> 30
    - '1d' -> 1440
    """
    time_str = str(time_str).strip().lower()
    
    if 'd' in time_str:
        return int(time_str.replace('d', '')) * 1440
    elif 'h' in time_str:
        return int(time_str.replace('h', '')) * 60
    elif 'm' in time_str:
        return int(time_str.replace('m', ''))
    else:
        # Assume it's already in minutes
        return int(time_str)


def save_data(data, path):
    """Save data to file using pickle"""
    import pickle
    with open(path, 'wb') as f:
        pickle.dump(data, f)


def set_reset_trade_cash(value):
    """Set reset trade cash (placeholder for compatibility)"""
    pass


def get_reset_trade_cash_txt():
    """Get reset trade cash text (placeholder for compatibility)"""
    return ""


def return_not_matches(a, b):
    """Return items not matching (placeholder for compatibility)"""
    return []


import logging
import os


def setup_logger(file_name, symbol, log_level=logging.INFO, log_dir: Optional[str] = None):
    """Setup logger for scripts.

    :param file_name: Log filename (e.g. 'download.log').
    :param symbol:    Logger name / trading symbol label.
    :param log_level: Logging level (default: INFO).
    :param log_dir:   Absolute path to the log directory.  Resolution order:
                      1. ``log_dir`` argument (if provided)
                      2. ``GROAI_LOG_DIR`` environment variable
                      3. ``<cwd>/logs/``
    """
    # Resolve log directory
    if log_dir is None:
        log_dir = os.getenv("GROAI_LOG_DIR") or str(Path.cwd() / "logs")

    resolved_log_dir = Path(log_dir)
    resolved_log_dir.mkdir(parents=True, exist_ok=True)

    log_file = str(resolved_log_dir / file_name)

    logger = logging.getLogger(symbol)

    # Clear any existing handlers
    if logger.handlers:
        logger.handlers.clear()

    # file handler to save all levels
    file_handler = logging.FileHandler(log_file)
    file_handler.setLevel(log_level)

    # stream handler (console) to show only INFO level and above
    stream_handler = logging.StreamHandler()
    stream_handler.setLevel(log_level)

    # create formatter
    formatter = logging.Formatter(
        "%(asctime)s|%(levelname)s|%(name)s|%(filename)s:%(lineno)d|[tid:%(thread)d]|%(message)s",
        datefmt="%Y-%m-%d %H:%M:%S")

    # add formatter to handlers
    stream_handler.setFormatter(formatter)
    file_handler.setFormatter(formatter)

    # Add handlers to logger
    logger.addHandler(stream_handler)
    logger.addHandler(file_handler)

    logger.setLevel(log_level)
    logger.handlers[0].flush()

    return logger


def is_iso_format_str(date_text):
    """Check if string is in ISO format"""
    try:
        if not isinstance(date_text, str):
            return False
        from dateutil.parser import parse as dateparse
        dateparse(date_text)
        return True
    except (ValueError, ImportError):
        return False


def is_dir_exist(path):
    """Check if directory exists"""
    return os.path.exists(path) and os.path.isdir(path)


def date2tw(_d):
    """Convert date to Taiwan timezone datetime"""
    _d = date2datetime(_d)
    _d = _d.replace(tzinfo=tw_tz)
    return _d


def date2datetime(_d):
    """Convert date/datetime to naive datetime (date only)"""
    _d = dt(
        year=_d.year,
        month=_d.month,
        day=_d.day,
    )
    return _d


def calculate_days_to_download(s):
    """
    Calculate number of days to download based on timeframe string
    (e.g., '1m', '1h', '1d')
    """
    try:
        temp = re.compile("([0-9]+)([a-zA-Z]+)")
        res = temp.match(s).groups()
        days = int(1000 / (24 * 60 / int(res[0])) / 2)

        if days == 0:
            days = 1
        return days

    except Exception as e:
        print(readable_error(e, __file__), flush=True)
        return 1
