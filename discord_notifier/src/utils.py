import os
from typing import Any, Dict, Optional, Iterable, List
import bittensor as bt


def parse_bootstrap(bootstrap: str) -> List[str]:
    """
    Split a comma-separated bootstrap string into a clean list.
    """
    return [item.strip() for item in bootstrap.split(",") if item.strip()]


def _short_addr(addr: Optional[str]) -> str:
    """
    Shorten long SS58/hex-like addresses for display: ABCDEF…UVWXYZ.
    """
    if not addr or not isinstance(addr, str):
        return "n/a"
    if len(addr) <= 14:
        return addr
    return f"{addr[:6]}…{addr[-6:]}"


def _looks_hex(s: str) -> bool:
    """
    Heuristic: long-ish hex strings start with 0x and have meaningful length.
    """
    return isinstance(s, str) and s.startswith("0x") and len(s) > 18


def _short_hex(s: str) -> str:
    """
    Shorten long hex strings: 0x1234abcd…89efcdef
    """
    if not _looks_hex(s):
        return s
    return f"{s[:10]}…{s[-8:]}"


def _is_primitive(x: Any) -> bool:
    """
    Primitive values we allow in the compact key=value list.
    """
    return isinstance(x, (str, int, float, bool)) or x is None


def _kv_pairs(items: Iterable[tuple[str, Any]]) -> str:
    """
    Render key=value pairs joined by ', ' with light value formatting.
    - Shortens addresses/hex where appropriate.
    - Does NOT receive None values (they are filtered earlier).
    """
    rendered: list[str] = []
    for k, v in items:
        if isinstance(v, str):
            if k in {"coldkey", "new_coldkey", "hotkey", "address"}:
                v_str = _short_addr(v)
            elif _looks_hex(v):
                v_str = _short_hex(v)
            else:
                v_str = v
        else:
            v_str = str(v)  # int/float/bool
        rendered.append(f"{k}={v_str}")
    return ", ".join(rendered)


def default_formatter(message: Dict[str, Any]) -> Dict[str, Any]:
    """
    Format as: 'netuid=<val> | <event_type> | key1=val1, key2=val2, ...'

    Rules:
    - First segment: 'netuid=<val>' (or 'n/a' if missing/None).
    - Second segment: raw event type from 'type' (fallback 'event').
    - Third segment: only primitive fields (str/int/float/bool) with NON-None values.
      Complex values (dict/list) are skipped entirely (e.g., 'parsed', 'raw', 'signature', 'call').
    - Key order: block, coldkey, new_coldkey, observed_at, extrinsic_hash, address,
      hotkey, amount/amount_staked, limit_price, allow_partial, nonce, tip, then others (sorted).
    - Long addresses/hex are shortened.
    - Enforces Discord's 2000-char limit.
    """
    # Non-dict payloads: stringify safely
    if not isinstance(message, dict):
        text = str(message)
        if len(text) > 2000:
            text = text[:1990] + "\n…(truncated)"
        return {"content": text}

    # Segments 1-2
    netuid = message.get("netuid")
    netuid_seg = f"netuid={'n/a' if netuid is None else str(netuid)}"

    evt_type = message.get("type") or "event"
    event_seg = str(evt_type)

    # Fields to exclude explicitly (extendable via env)
    drop_keys_env = os.getenv("DISCORD_FORMAT_DROP_KEYS", "")
    drop_keys = {k.strip() for k in drop_keys_env.split(",") if k.strip()}
    # Always exclude already surfaced keys
    exclude = {"netuid", "type"} | drop_keys

    # Preferred order for readability
    preferred_order = [
        "block",
        "coldkey",
        "new_coldkey",
        "observed_at",
        "extrinsic_hash",
        "address",
        "hotkey",
        "amount",
        "amount_staked",
        "limit_price",
        "allow_partial",
        "nonce",
        "tip",
    ]
    remaining_keys = [
        k for k in message.keys() if k not in exclude and k not in preferred_order
    ]
    ordered_keys = preferred_order + sorted(remaining_keys)

    # Only include primitive values that are PRESENT and NOT None; skip dict/list entirely
    kv_items: list[tuple[str, Any]] = []
    for k in ordered_keys:
        if k in exclude:
            continue
        if k not in message:
            continue
        v = message[k]
        if v is None:
            continue  # <-- skip None so we don't render key=n/a
        if _is_primitive(v):
            kv_items.append((k, v))
        # else: skip complex types like dict/list

    values_seg = _kv_pairs(kv_items) if kv_items else ""

    # Join segments
    content = f"{netuid_seg} | {event_seg}" + (f" | {values_seg}" if values_seg else "")

    # Discord hard limit
    if len(content) > 2000:
        content = content[:1990] + "\n…(truncated)"

    return {"content": content}


def configure_logging() -> str:
    """
    Configure bittensor logger based on LOG_LEVEL env.
    """
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
