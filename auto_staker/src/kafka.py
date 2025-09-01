import json
import asyncio
from typing import Optional, Any, Dict, Callable, Awaitable, List
from aiokafka import AIOKafkaConsumer
from aiokafka.errors import KafkaError
from aiokafka.admin import AIOKafkaAdminClient, NewTopic
import bittensor as bt

from utils import parse_bootstrap


class KafkaSignalConsumer:
    """
    A minimal Kafka consumer service:
      - Owns Kafka connection, offset management and backoff on failures.
      - Delegates message handling to a provided async 'handler(payload) -> bool'.
      - Commits offsets only on successful handling.
    """

    def __init__(
        self,
        *,
        bootstrap_servers: str,
        topic: str,
        group_id: str,
        client_id: str = "auto-staker",
        value_deserializer: Optional[Callable[[bytes], Any]] = None,
        key_deserializer: Optional[Callable[[Optional[bytes]], Any]] = None,
        ensure_offsets_topic: bool = True,
        offsets_partitions: int = 50,
    ) -> None:
        self.bootstrap_servers_list: List[str] = parse_bootstrap(bootstrap_servers)
        self.topic = topic
        self.group_id = group_id
        self.client_id = client_id

        self._consumer: Optional[AIOKafkaConsumer] = None
        self._stop_event = asyncio.Event()
        self._ensure_offsets_topic = ensure_offsets_topic
        self._offsets_partitions = offsets_partitions

        self._value_deserializer = value_deserializer or (
            lambda v: json.loads(v.decode("utf-8"))
        )
        self._key_deserializer = key_deserializer or (
            lambda k: k.decode("utf-8") if k is not None else None
        )

    async def _ensure_internal_offsets_topic(self) -> None:
        """
        Best-effort creation for single-node setups.
        NOTE: In real Apache Kafka clusters '__consumer_offsets' is internal and auto-managed.
        """
        admin = AIOKafkaAdminClient(
            bootstrap_servers=",".join(self.bootstrap_servers_list),
            client_id=f"{self.client_id}-admin",
        )
        await admin.start()
        try:
            topics = await admin.list_topics()
            if "__consumer_offsets" in topics:
                bt.logging.debug("ℹ️ __consumer_offsets already exists")
                return

            bt.logging.warning(
                "⛏️ __consumer_offsets not found, creating it (RF=1, compact)..."
            )
            topic = NewTopic(
                name="__consumer_offsets",
                num_partitions=self._offsets_partitions,
                replication_factor=1,
                topic_configs={"cleanup.policy": "compact"},
            )
            await admin.create_topics([topic], timeout_ms=30)
            bt.logging.info("✅ __consumer_offsets created")
        except Exception as e:
            bt.logging.warning(f"ensure_offsets_topic: non-fatal error: {e}")
        finally:
            await admin.close()

    async def start(self) -> None:
        """Initialize consumer and ensure prerequisites."""
        if self._ensure_offsets_topic:
            await self._ensure_internal_offsets_topic()

        if self._consumer is None:
            self._consumer = AIOKafkaConsumer(
                self.topic,
                bootstrap_servers=self.bootstrap_servers_list,
                group_id=self.group_id,
                client_id=self.client_id,
                enable_auto_commit=False,  # manual commit on success
                auto_offset_reset="latest",
                value_deserializer=self._value_deserializer,
                key_deserializer=self._key_deserializer,
            )
        await self._consumer.start()
        bt.logging.info(
            f"✅ Kafka consumer connected to {','.join(self.bootstrap_servers_list)} "
            f"(topic={self.topic}, group_id={self.group_id})"
        )

    async def stop(self) -> None:
        """Signal stop and close consumer."""
        self._stop_event.set()
        if self._consumer is not None:
            try:
                await self._consumer.stop()
            finally:
                self._consumer = None
                bt.logging.info("🛑 Kafka consumer stopped")

    async def run(self, handler: Callable[[Dict[str, Any]], Awaitable[bool]]) -> None:
        """
        Main consume loop.
        'handler' must return True on success (to commit), False to keep the message uncommitted.
        """
        if self._consumer is None:
            await self.start()
        assert self._consumer is not None

        try:
            async for record in self._consumer:
                if self._stop_event.is_set():
                    bt.logging.info("🛑 Stop requested; exiting consume loop")
                    break

                key = record.key
                value = record.value
                partition = record.partition
                offset = record.offset
                bt.logging.debug(
                    f"📥 Received message p={partition} o={offset} key={key!r}"
                )

                # Normalize payload: we expect dict for staking signals
                payload = value if isinstance(value, dict) else {"message": value}

                try:
                    success = await handler(payload)
                except Exception as e:
                    bt.logging.exception(
                        f"❌ Exception while handling message offset={offset}, key={key!r}: {e}"
                    )
                    success = False

                if success:
                    try:
                        await self._consumer.commit()
                    except KafkaError as e:
                        bt.logging.warning(f"⚠️ Commit failed at offset={offset}: {e}")
                else:
                    # Backoff to avoid hot-looping on poison messages
                    await asyncio.sleep(1.0)

        except asyncio.CancelledError:
            bt.logging.info("🧹 Consume loop cancelled")
            raise
        finally:
            await self.stop()
