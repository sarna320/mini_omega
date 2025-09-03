import os
import datetime as dt
from typing import Any, Dict, Iterable, List, Optional
import pathlib
import bittensor as bt


# ---------- Logging ----------


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


# ---------- Time helpers ----------


def now_iso_utc() -> str:
    """Return timezone-aware ISO8601 timestamp in UTC."""
    return dt.datetime.now(dt.timezone.utc).isoformat()


def _ts_from(msg: Dict[str, Any]) -> str:
    """
    Pick a timestamp for Discord embeds: prefer msg['observed_at'] or ['timestamp'],
    otherwise current UTC in ISO8601 format.
    """
    ts = msg.get("observed_at") or msg.get("timestamp")
    if isinstance(ts, str) and len(ts) >= 10:
        return ts
    return now_iso_utc()


# ---------- Formatting / shortening helpers ----------


def parse_bootstrap(bootstrap: str) -> List[str]:
    """Split a comma-separated bootstrap string into a clean list."""
    return [item.strip() for item in bootstrap.split(",") if item.strip()]


def _short_addr(addr: Optional[str]) -> str:
    """Shorten long SS58/hex-like addresses for display: ABCDEF…UVWXYZ."""
    if not addr or not isinstance(addr, str):
        return "n/a"
    if len(addr) <= 14:
        return addr
    return f"{addr[:6]}…{addr[-6:]}"


def _looks_hex(s: str) -> bool:
    """Heuristic: long-ish hex strings start with 0x and have meaningful length."""
    return isinstance(s, str) and s.startswith("0x") and len(s) > 18


def _short_hex(s: str) -> str:
    """Shorten long hex strings: 0x1234abcd…89efcdef"""
    if not _looks_hex(s):
        return s
    return f"{s[:10]}…{s[-8:]}"


def _is_primitive(x: Any) -> bool:
    """Primitive values we allow in compact key=value lists."""
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


# ---------- Default (fallback) formatter ----------


def default_formatter(message: Dict[str, Any]) -> Dict[str, Any]:
    """
    Fallback text formatter for Discord.
    Format: 'netuid=<val> | <event_type> | key1=val1, key2=val2, ...'
    """
    # Non-dict payloads: stringify safely
    if not isinstance(message, dict):
        text = str(message)
        if len(text) > 2000:
            text = text[:1990] + "\n…(truncated)"
        return {"content": text}

    netuid = message.get("netuid")
    netuid_seg = f"netuid={'n/a' if netuid is None else str(netuid)}"
    evt_type = message.get("event") or message.get("type") or "event"

    # Fields to exclude explicitly (extendable via env)
    drop_keys_env = os.getenv("DISCORD_FORMAT_DROP_KEYS", "")
    drop_keys = {k.strip() for k in drop_keys_env.split(",") if k.strip()}
    exclude = {"netuid", "event", "type"} | drop_keys

    # Preferred order for readability
    preferred_order = [
        "block",
        "coldkey",
        "new_coldkey",
        "observed_at",
        "timestamp",
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

    kv_items: list[tuple[str, Any]] = []
    for k in ordered_keys:
        if k in exclude or k not in message:
            continue
        v = message[k]
        if v is None:
            continue
        if _is_primitive(v):
            kv_items.append((k, v))

    values_seg = _kv_pairs(kv_items) if kv_items else ""
    content = f"{netuid_seg} | {evt_type}" + (f" | {values_seg}" if values_seg else "")

    if len(content) > 2000:
        content = content[:1990] + "\n…(truncated)"
    return {"content": content}


# ---------- Rich Discord embeds per event ----------


def _fmt_kv_lines(d: Dict[str, Any]) -> str:
    """Render dict or diff-dict as markdown bullet lines."""
    if not isinstance(d, dict):
        return f"`{d}`"
    lines = []
    for k in sorted(d.keys()):
        v = d[k]
        if isinstance(v, dict) and "old" in v and "new" in v:
            lines.append(f"- **{k}**: `{v.get('old')}` → `{v.get('new')}`")
        else:
            lines.append(f"- **{k}**: `{v}`")
    return "\n".join(lines) if lines else "—"


def schedule_swap_coldkey_formatter(message: Dict[str, Any]) -> Dict[str, Any]:
    """
    Pretty embed for `schedule_swap_coldkey` event.
    """
    netuid = message.get("netuid", "n/a")
    coldkey = _short_addr(message.get("coldkey"))
    new_coldkey = _short_addr(message.get("new_coldkey"))
    amount = message.get("amount") or message.get("amount_staked")
    limit_price = message.get("limit_price")
    allow_partial = message.get("allow_partial")
    block = message.get("block")  # <-- keep block
    extr = _short_hex(message.get("extrinsic_hash"))
    nonce = message.get("nonce")
    tip = message.get("tip")

    embed = {
        "title": "🔁 Coldkey swap scheduled",
        "description": f"Netuid **{netuid}** • **{coldkey} → {new_coldkey}**",
        "color": 0x2ECC71,
        "timestamp": _ts_from(message),
        "fields": [],
        "footer": {"text": "schedule_swap_coldkey"},
    }
    if amount is not None:
        embed["fields"].append({"name": "Amount", "value": f"{amount}", "inline": True})
    if limit_price is not None:
        embed["fields"].append(
            {"name": "Limit price", "value": f"{limit_price}", "inline": True}
        )
    if allow_partial is not None:
        embed["fields"].append(
            {"name": "Allow partial", "value": f"{allow_partial}", "inline": True}
        )
    if block is not None:
        embed["fields"].append({"name": "Block", "value": str(block), "inline": True})
    if nonce is not None:
        embed["fields"].append({"name": "Nonce", "value": str(nonce), "inline": True})
    if tip is not None:
        embed["fields"].append({"name": "Tip", "value": str(tip), "inline": True})
    if extr and extr != message.get("extrinsic_hash"):
        embed["fields"].append({"name": "Extrinsic", "value": extr, "inline": False})
    if not embed["fields"]:
        embed["fields"] = [{"name": "Details", "value": "—", "inline": False}]

    mention = os.getenv("DISCORD_MENTION", "").strip()
    payload: Dict[str, Any] = {"embeds": [embed]}
    if mention:
        payload["content"] = mention
    return payload


def changed_subnet_formatter(message: Dict[str, Any]) -> Dict[str, Any]:
    netuid = message.get("netuid", "n/a")
    fields_diff = message.get("fields", {})
    block = message.get("block")
    changes_field = {
        "name": "Changes",
        "value": _fmt_kv_lines(fields_diff),
        "inline": False,
    }

    embed_fields = []
    if block is not None:
        embed_fields.append({"name": "Block", "value": str(block), "inline": True})
    embed_fields.append(changes_field)

    embed = {
        "title": "🛠️ Subnet identity updated",
        "description": f"Netuid **{netuid}**",
        "color": 0xF1C40F,
        "timestamp": _ts_from(message),
        "fields": embed_fields,
        "footer": {"text": "changed_subnet"},
    }
    return {"embeds": [embed]}


def added_subnet_formatter(message: Dict[str, Any]) -> Dict[str, Any]:
    netuid = message.get("netuid", "n/a")
    si = message.get("subnet_identity") or {}
    block = message.get("block")

    embed_fields = []
    if block is not None:
        embed_fields.append({"name": "Block", "value": str(block), "inline": True})
    embed_fields.append(
        {"name": "subnet_identity", "value": _fmt_kv_lines(si), "inline": False}
    )

    embed = {
        "title": "✨ Subnet added",
        "description": f"Netuid **{netuid}**",
        "color": 0x3498DB,
        "timestamp": _ts_from(message),
        "fields": embed_fields,
        "footer": {"text": "added_subnet"},
    }
    return {"embeds": [embed]}


def removed_subnet_formatter(message: Dict[str, Any]) -> Dict[str, Any]:
    netuid = message.get("netuid", "n/a")
    si = message.get("subnet_identity") or {}
    block = message.get("block")

    embed_fields = []
    if block is not None:
        embed_fields.append({"name": "Block", "value": str(block), "inline": True})
    embed_fields.append(
        {
            "name": "last known subnet_identity",
            "value": _fmt_kv_lines(si),
            "inline": False,
        }
    )

    embed = {
        "title": "🗑️ Subnet removed",
        "description": f"Netuid **{netuid}**",
        "color": 0xE74C3C,
        "timestamp": _ts_from(message),
        "fields": embed_fields,
        "footer": {"text": "removed_subnet"},
    }
    return {"embeds": [embed]}
