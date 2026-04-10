"""Configuration for Binance module"""
import pytz
from dataclasses import dataclass, field
from typing import Optional, Callable


@dataclass
class BinanceConfig:
    """Configuration for Binance API and trading"""
    
    # API Credentials
    api_key: str = ""
    api_secret: str = ""
    api_key_test: str = ""
    api_secret_test: str = ""
    
    # Network Settings
    recv_window: int = 60000
    testnet: bool = False
    
    # Email Settings (optional)
    send_mail_receiver: Optional[str] = None
    send_mail_callback: Optional[Callable] = None
    
    # Parquet Settings
    parquet_engine: str = "pyarrow"
    
    # Timezone Settings
    current_time_zone: str = "Asia/Taipei"
    us_tz: pytz.timezone = field(default_factory=lambda: pytz.timezone("America/New_York"))
    tw_tz: pytz.timezone = field(default_factory=lambda: pytz.timezone("Asia/Taipei"))
    
    # Trading Settings
    default_start_date: str = "2019/01/01"
    batch_size_hours: int = 8
    pause_between_batches: float = 0.5
    
    @classmethod
    def from_env(cls):
        """Create config from environment variables"""
        import os
        return cls(
            api_key=os.getenv("BINANCE_API_KEY", ""),
            api_secret=os.getenv("BINANCE_API_SECRET", ""),
            api_key_test=os.getenv("BINANCE_API_KEY_TEST", ""),
            api_secret_test=os.getenv("BINANCE_API_SECRET_TEST", ""),
            send_mail_receiver=os.getenv("SEND_MAIL_RECEIVER"),
        )


# Global default config instance
_default_config = None


def get_default_config() -> BinanceConfig:
    """Get or create the default configuration"""
    global _default_config
    if _default_config is None:
        _default_config = BinanceConfig.from_env()
    return _default_config


def set_default_config(config: BinanceConfig):
    """Set the default configuration"""
    global _default_config
    _default_config = config


# Backward compatibility: expose config values as module-level variables
parquet_engine = "pyarrow"
tw_tz = pytz.timezone("Asia/Taipei")
us_tz = pytz.timezone("America/New_York")
current_time_zone = "Asia/Taipei"
