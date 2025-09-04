import os
from typing import List, Set
import pathlib
import bittensor as bt


def parse_ignore_netuids_from_env(env_var: str = "IGNORE_NETUIDS") -> Set[int]:
    """
    Parse a comma-separated list of netuids from an environment variable and return a set[int].

    Example:
        IGNORE_NETUIDS="1, 2, 42"  ->  {1, 2, 42}

    Behavior:
    - Trims whitespace around tokens.
    - Skips empty tokens.
    - Logs a warning for invalid (non-integer) or negative tokens.
    - Logs info with the final, sorted list when non-empty.

    Args:
        env_var: Environment variable name to read from. Defaults to "IGNORE_NETUIDS".

    Returns:
        A set of valid, non-negative netuids.
    """
    raw = os.getenv(env_var, "").strip()
    result: Set[int] = set()

    if not raw:
        return result

    for tok in raw.split(","):
        tok = tok.strip()
        if not tok:
            continue
        try:
            val = int(tok)
            if val < 0:
                bt.logging.warning(f"{env_var}: negative netuid '{tok}' skipped")
                continue
            result.add(val)
        except ValueError:
            bt.logging.warning(f"{env_var}: invalid netuid '{tok}' skipped")

    if result:
        bt.logging.info(f"Ignore list active for netuids: {sorted(result)}")

    return result


def parse_bootstrap(bootstrap: str) -> List[str]:
    """
    Split a comma-separated bootstrap string into a clean list.
    """
    return [item.strip() for item in bootstrap.split(",") if item.strip()]


def configure_logging() -> str:
    """
    Configure Bittensor logging based on LOG_LEVEL env and enable file logging.

    Rules:
    - Level is read from LOG_LEVEL: TRACE | DEBUG | INFO | WARNING (default TRACE).
    - File logging is controlled by BT_LOGGING_RECORD_LOG and BT_LOGGING_LOGGING_DIR.
    - Directory is created if it doesn't exist.
    """
    level_name = os.getenv("LOG_LEVEL", "TRACE").upper()

    record_log_env = os.getenv("BT_LOGGING_RECORD_LOG", "1")
    record_log = record_log_env.lower() in ("1", "true", "yes", "on")

    log_dir = os.getenv("BT_LOGGING_LOGGING_DIR", "./logs")
    log_dir_path = pathlib.Path(log_dir)
    log_dir_path.mkdir(parents=True, exist_ok=True)

    if level_name == "TRACE":
        bt.trace()
    elif level_name == "DEBUG":
        bt.debug()
    elif level_name in ("INFO", "WARNING", "WARN"):
        bt.debug(False)
        bt.trace(False)
    else:
        bt.debug()
        level_name = "DEBUG"

    bt.logging(record_log=record_log, logging_dir=str(log_dir_path))

    bt.logging.debug(
        f"✅ Logging configured: level={level_name}, record_log={record_log}, dir={log_dir_path}"
    )
    return level_name
