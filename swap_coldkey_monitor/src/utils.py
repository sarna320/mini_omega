import bittensor as bt
import os
from datetime import datetime, timezone


def configure_logging() -> str:
    level_name = os.getenv("LOG_LEVEL", "TRACE").upper()
    level_map = {
        "TRACE": bt.logging.set_trace,
        "DEBUG": bt.logging.set_debug,
        "INFO": bt.logging.set_info,
        "WARNING": bt.logging.set_warning,
    }
    setter = level_map.get(level_name, bt.logging.set_debug)
    setter()
    bt.logging.debug(f"✅ Logging set to {level_name}")
    return level_name


def now_iso_utc() -> str:
    """
    Return current time as ISO-8601 UTC string with 'Z' suffix, e.g. '2025-08-31T12:34:56.789Z'.
    """
    return (
        datetime.now(timezone.utc)
        .isoformat(timespec="milliseconds")
        .replace("+00:00", "Z")
    )
