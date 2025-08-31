import os
import json
import asyncio
import contextlib
from typing import Optional, Any, Dict, Callable, Tuple, List

import aiohttp
import bittensor as bt
from aiokafka import AIOKafkaConsumer
from aiokafka.errors import KafkaError
from aiokafka.admin import AIOKafkaAdminClient, NewTopic  # admin preflight
from utils import default_formatter, configure_logging, parse_bootstrap


class KafkaDiscordNotifier:
    """
    Consumes JSON messages from Kafka and forwards them to a Discord webhook.
    - Manual offset commit after successful delivery -> at-least-once semantics.
    - Exponential backoff for webhook failures and basic rate-limit handling.
    - Pluggable message formatter for Discord payloads.
    """

    def __init__(
        self,
        bootstrap_servers: str,
        topic: str,
        group_id: str,
        webhook_url: str,
        *,
        client_id: str = "discord-notifier",
        formatter: Callable[[Dict[str, Any]], Dict[str, Any]] = default_formatter,
        session: Optional[aiohttp.ClientSession] = None,
        max_webhook_retries: int = 5,
        webhook_timeout_s: float = 10.0,
        max_concurrent_posts: int = 5,
    ):
        self.bootstrap_servers = parse_bootstrap(bootstrap_servers)
        self.topic = topic
        self.group_id = group_id
        self.client_id = client_id
        self.webhook_url = webhook_url
        self.formatter = formatter
        self._session = session
        self._own_session = session is None
        self._consumer: Optional[AIOKafkaConsumer] = None
        self._stop_event = asyncio.Event()
        self.max_webhook_retries = max_webhook_retries
        self.webhook_timeout_s = webhook_timeout_s
        self._post_sema = asyncio.Semaphore(max_concurrent_posts)

    async def ensure_offsets_topic(
        self, bootstrap_servers: List[str], partitions: int = 50
    ) -> None:
        """
        Ensure __consumer_offsets exists (single-broker friendly).
        Creates it with replication factor = 1 and cleanup.policy=compact if missing.
        Safe to call on every startup.
        """
        admin = AIOKafkaAdminClient(
            bootstrap_servers=",".join(bootstrap_servers),
            client_id="discord-notifier-admin",
        )
        await admin.start()
        try:
            # Fast path: if it already exists, we're done
            topics = await admin.list_topics()
            if "__consumer_offsets" in topics:
                bt.logging.debug("ℹ️ __consumer_offsets already exists")
                return

            bt.logging.warning(
                "⛏️ __consumer_offsets not found, creating it (RF=1, compact)..."
            )
            topic = NewTopic(
                name="__consumer_offsets",
                num_partitions=partitions,
                replication_factor=1,
                topic_configs={"cleanup.policy": "compact"},
            )
            # Create is idempotent from our perspective; we ignore 'TopicAlreadyExistsError'
            await admin.create_topics([topic], timeout_ms=30)
            bt.logging.info("✅ __consumer_offsets created")
        except Exception as e:
            # If broker races us and creates it concurrently, we can ignore TopicAlreadyExistsError
            bt.logging.warning(f"ensure_offsets_topic: non-fatal error: {e}")
        finally:
            await admin.close()

    async def start(self):
        if self._session is None:
            self._session = aiohttp.ClientSession(
                timeout=aiohttp.ClientTimeout(total=self.webhook_timeout_s)
            )

        # Preflight: make sure internal offsets topic exists (single-node friendliness)
        await self.ensure_offsets_topic(self.bootstrap_servers, partitions=50)

        if self._consumer is None:
            self._consumer = AIOKafkaConsumer(
                self.topic,
                bootstrap_servers=self.bootstrap_servers,
                group_id=self.group_id,
                client_id=self.client_id,
                enable_auto_commit=False,  # commit only after successful delivery
                auto_offset_reset="latest",
                value_deserializer=lambda v: json.loads(v.decode("utf-8")),
                key_deserializer=lambda k: k.decode("utf-8") if k is not None else None,
            )
        await self._consumer.start()
        bt.logging.info(
            f"✅ Kafka consumer connected to {','.join(self.bootstrap_servers)} (topic={self.topic}, group_id={self.group_id})"
        )

    async def stop(self):
        self._stop_event.set()
        if self._consumer is not None:
            try:
                await self._consumer.stop()
            finally:
                self._consumer = None
                bt.logging.info("🛑 Kafka consumer stopped")
        if self._own_session and self._session is not None:
            try:
                await self._session.close()
            finally:
                self._session = None
                bt.logging.info("🔒 HTTP session closed")

    async def _post_webhook(
        self, payload: Dict[str, Any]
    ) -> Tuple[bool, Optional[str]]:
        """
        Post to Discord webhook with retry and rudimentary rate-limit handling.
        Returns (success, error_message).
        """
        assert self._session is not None
        url = f"{self.webhook_url}?wait=true"
        attempt = 0
        last_error: Optional[str] = None

        while attempt <= self.max_webhook_retries:
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
                                f"⌛ Discord rate-limited (429). Sleeping {sleep_s:.2f}s and retrying..."
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
                f"⚠️ Discord webhook post failed (attempt {attempt + 1}/{self.max_webhook_retries + 1}): "
                f"{last_error or 'unknown error'}. Retrying in {backoff:.1f}s"
            )
            await asyncio.sleep(backoff)
            attempt += 1

        return False, last_error

    async def _handle_message(self, record) -> bool:
        """
        Process a single Kafka record.
        - Build Discord payload with formatter.
        - Send to webhook (with retries).
        - Return True if delivered successfully, else False (so we don't commit).
        """
        key = record.key
        value = record.value
        partition = record.partition
        offset = record.offset

        bt.logging.debug(f"📥 Received message p={partition} o={offset} key={key!r}")

        payload = value if isinstance(value, dict) else {"message": value}

        try:
            discord_payload = self.formatter(payload)
            if not isinstance(discord_payload, dict):
                raise ValueError(
                    "Formatter must return a dict with a valid Discord payload"
                )
            # bt.logging.info(discord_payload)
            success, err = await self._post_webhook(discord_payload)
            if success:
                bt.logging.info(
                    f"✅ Delivered to Discord (partition={partition}, offset={offset}, key={key!r}) | {discord_payload}"
                )
                return True
            else:
                bt.logging.error(
                    f"❌ Delivery to Discord failed permanently for offset={offset}, key={key!r}: {err}| {discord_payload}"
                )
                return False
        except Exception as e:
            bt.logging.exception(
                f"❌ Exception while handling message offset={offset}, key={key!r}: {e}"
            )
            return False

    async def run(self):
        """
        Main consume loop. Stops when stop() is called or on CancelledError.
        """
        if self._consumer is None:
            await self.start()
        assert self._consumer is not None

        try:
            async for record in self._consumer:
                if self._stop_event.is_set():
                    bt.logging.info("🛑 Stop requested; exiting consume loop")
                    break

                delivered = await self._handle_message(record)
                if delivered:
                    try:
                        await self._consumer.commit()
                    except KafkaError as e:
                        bt.logging.warning(
                            f"⚠️ Commit failed at offset={record.offset}: {e}"
                        )
                else:
                    await asyncio.sleep(1.0)
        except asyncio.CancelledError:
            bt.logging.info("🧹 Consume loop cancelled")
            raise
        finally:
            await self.stop()


async def main():
    """
    Environment variables:
      - KAFKA_BOOTSTRAP (comma-separated). From host use: localhost:9093 ; from container: kafka:9092
      - KAFKA_TOPIC (default: "signal")
      - KAFKA_GROUP_ID (default: "discord")
      - DISCORD_WEBHOOK_URL (required)
    """
    configure_logging()
    bootstrap = os.getenv("KAFKA_BOOTSTRAP", "localhost:9093")
    topic = os.getenv("KAFKA_TOPIC", "signal")
    group_id = os.getenv("KAFKA_GROUP_ID", "discord")
    webhook = os.getenv(
        "DISCORD_WEBHOOK_URL",
        "https://discord.com/api/webhooks/1411814256646819944/xONggF8pZAP0SuVkn3Wv0vwdQ977tqOr10Hj_GBSBJ_4S7WVCeC6axcbtidEjb9nnCFc",
    )
    if not webhook:
        raise RuntimeError("DISCORD_WEBHOOK_URL is required")

    notifier = KafkaDiscordNotifier(
        bootstrap_servers=bootstrap,
        topic=topic,
        group_id=group_id,
        webhook_url=webhook,
    )

    loop = asyncio.get_running_loop()
    stop_event = asyncio.Event()
    for sig in ("SIGINT", "SIGTERM"):
        try:
            loop.add_signal_handler(getattr(__import__("signal"), sig), stop_event.set)
        except (NotImplementedError, AttributeError):
            pass
    runner = asyncio.create_task(notifier.run())
    try:
        await stop_event.wait()
    finally:
        await notifier.stop()
        runner.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await runner


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
