import os
import asyncio
from typing import Any, Dict, Optional, Tuple

import aiohttp
import bittensor as bt

# Reuse your time/format helpers if available; fall back gracefully
try:
    from utils import _ts_from  # type: ignore
except Exception:  # pragma: no cover
    import datetime as dt

    def _ts_from(message: Dict[str, Any]) -> str:
        return dt.datetime.now(dt.timezone.utc).isoformat()


class DiscordAlerter:
    """
    Minimal async Discord webhook alerter:
    - Non-blocking .notify(payload) -> enqueues to an internal queue
    - Background worker task posts to webhook with retry & 429 handling
    - Optional external aiohttp session; if not given, creates own
    """

    def __init__(
        self,
        webhook_url: str,
        *,
        max_retries: int = 5,
        request_timeout_s: float = 10.0,
        max_concurrent_posts: int = 2,
        session: Optional[aiohttp.ClientSession] = None,
        mention_env: str = "DISCORD_MENTION",
    ) -> None:
        self.webhook_url = webhook_url.rstrip("/")
        self.max_retries = max_retries
        self.request_timeout_s = request_timeout_s
        self._queue: "asyncio.Queue[Dict[str, Any]]" = asyncio.Queue()
        self._session = session
        self._own_session = session is None
        self._worker_task: Optional[asyncio.Task] = None
        self._stop = asyncio.Event()
        self._post_sema = asyncio.Semaphore(max_concurrent_posts)
        self._mention = os.getenv(mention_env, "").strip()

    def start(self) -> None:
        """Start background worker task if not already running."""
        if self._worker_task is not None and not self._worker_task.done():
            return
        if self._session is None:
            self._session = aiohttp.ClientSession(
                timeout=aiohttp.ClientTimeout(total=self.request_timeout_s)
            )
        self._stop.clear()
        self._worker_task = asyncio.create_task(self._worker(), name="discord-alerter")

    async def stop(self) -> None:
        """Stop worker and close session (if owned)."""
        self._stop.set()
        try:
            # Drain queue quickly to avoid dropping important alerts
            if self._worker_task is not None:
                await asyncio.wait_for(self._worker_task, timeout=5.0)
        except Exception:
            if self._worker_task:
                self._worker_task.cancel()
        finally:
            self._worker_task = None
            if self._own_session and self._session is not None:
                try:
                    await self._session.close()
                finally:
                    self._session = None

    def notify(self, payload: Dict[str, Any]) -> None:
        """
        Enqueue payload for background sending. Starts worker lazily.
        """
        if self._session is None or self._worker_task is None:
            # lazy start
            self.start()
        # Attach mention if not already present
        if self._mention and "content" not in payload:
            payload = dict(payload)
            payload["content"] = self._mention
        self._queue.put_nowait(payload)

    async def _post_webhook(
        self, payload: Dict[str, Any]
    ) -> Tuple[bool, Optional[str]]:
        """
        Post once with retry & rudimentary rate-limit handling.
        """
        assert self._session is not None
        url = f"{self.webhook_url}?wait=true"
        attempt = 0
        last_error: Optional[str] = None

        while attempt <= self.max_retries and not self._stop.is_set():
            try:
                async with self._post_sema:
                    async with self._session.post(url, json=payload) as resp:
                        if 200 <= resp.status < 300:
                            return True, None
                        if resp.status == 429:
                            retry_after = resp.headers.get(
                                "Retry-After"
                            ) or resp.headers.get("X-RateLimit-Reset-After")
                            sleep_s = (
                                float(retry_after)
                                if retry_after
                                else min(2**attempt, 10.0)
                            )
                            bt.logging.warning(
                                f"⌛ Discord rate-limited (429). Sleeping {sleep_s:.2f}s..."
                            )
                            await asyncio.sleep(sleep_s)
                            attempt += 1
                            continue
                        text = await resp.text()
                        last_error = f"HTTP {resp.status}: {text[:2000]}"
            except (aiohttp.ClientError, asyncio.TimeoutError) as e:
                last_error = f"{type(e).__name__}: {e}"

            backoff = min(0.5 * (2**attempt), 10.0)
            bt.logging.warning(
                f"⚠️ Discord webhook post failed (attempt {attempt + 1}/{self.max_retries + 1}): "
                f"{last_error or 'unknown error'}. Retrying in {backoff:.1f}s"
            )
            await asyncio.sleep(backoff)
            attempt += 1

        return False, last_error

    async def _worker(self) -> None:
        """Background worker loop."""
        try:
            while not self._stop.is_set():
                try:
                    payload = await asyncio.wait_for(self._queue.get(), timeout=0.5)
                except asyncio.TimeoutError:
                    continue
                try:
                    ok, err = await self._post_webhook(payload)
                    if not ok:
                        bt.logging.error(f"❌ Discord alert failed permanently: {err}")
                finally:
                    self._queue.task_done()
        except asyncio.CancelledError:
            pass


def build_skip_embed(
    *,
    source: str,
    event: Optional[str],
    reason: str,
    payload: Dict[str, Any],
    color: int = 0x95A5A6,  # concrete/gray
) -> Dict[str, Any]:
    """
    Build a standardized "skipped" embed (policy/filters in router or staker).
    """
    netuid = payload.get("netuid", "n/a")
    block = payload.get("block")
    fields = [
        {"name": "Source", "value": source, "inline": True},
        {"name": "Event", "value": str(event or "n/a"), "inline": True},
        {"name": "Reason", "value": reason, "inline": False},
    ]
    if block is not None:
        fields.insert(2, {"name": "Block", "value": str(block), "inline": True})

    embed = {
        "title": "⏭️ Signal skipped",
        "description": f"Netuid **{netuid}**",
        "color": color,
        "timestamp": _ts_from(payload if isinstance(payload, dict) else {}),
        "fields": fields,
        "footer": {"text": "autostaker/skip"},
    }
    return {"embeds": [embed]}


def build_stake_success_embed(
    *,
    netuid: int,
    amount_tao: float,
    wallet_coldkey: str,
    hotkey: str,
    new_balance_tao: float,
    payload: Dict[str, Any],
    color: int = 0x2ECC71,  # green
) -> Dict[str, Any]:
    """
    Build a standardized "stake success" embed.
    """
    embed = {
        "title": "✅ Stake executed",
        "description": f"Netuid **{netuid}**",
        "color": color,
        "timestamp": _ts_from(payload if isinstance(payload, dict) else {}),
        "fields": [
            {"name": "Amount (TAO)", "value": f"{amount_tao}", "inline": True},
            {
                "name": "New balance (TAO)",
                "value": f"{new_balance_tao}",
                "inline": True,
            },
            {"name": "Wallet Coldkey", "value": wallet_coldkey, "inline": False},
            {"name": "Validator Hotkey", "value": hotkey, "inline": False},
        ],
        "footer": {"text": "autostaker/stake"},
    }
    return {"embeds": [embed]}


def build_stake_failure_embed(
    *,
    netuid: Optional[int],
    amount_tao: float,
    wallet_coldkey: str,
    hotkey: str,
    error: str,
    payload: Dict[str, Any],
    color: int = 0xE74C3C,  # red
) -> Dict[str, Any]:
    """
    Build a standardized "stake failed" embed.
    """
    embed = {
        "title": "❌ Stake failed",
        "description": f"Netuid **{netuid if netuid is not None else 'n/a'}**",
        "color": color,
        "timestamp": _ts_from(payload if isinstance(payload, dict) else {}),
        "fields": [
            {"name": "Amount (TAO)", "value": f"{amount_tao}", "inline": True},
            {"name": "Coldkey", "value": wallet_coldkey, "inline": False},
            {"name": "Hotkey", "value": hotkey, "inline": False},
            {
                "name": "Error",
                "value": (error[:1024] if error else "unknown error"),
                "inline": False,
            },
        ],
        "footer": {"text": "autostaker/stake"},
    }
    # include block if present
    blk = payload.get("block")
    if blk is not None:
        embed["fields"].insert(1, {"name": "Block", "value": str(blk), "inline": True})
    return {"embeds": [embed]}
