from aiokafka import AIOKafkaProducer
from aiokafka.errors import KafkaError
import json
import asyncio
import bittensor as bt
from typing import Optional, Any, Dict


class KafkaProducerManager:
    """
    Thin wrapper around AIOKafkaProducer with JSON sending helpers and retries.
    """

    def __init__(
        self,
        bootstrap_servers: str,
        topic: str,
        client_id: str = "subnet-monitor",
    ):
        self.bootstrap_servers = bootstrap_servers
        self.topic = topic
        self.client_id = client_id
        self._producer: Optional[AIOKafkaProducer] = None

    async def start(self):
        if self._producer is None:
            self._producer = AIOKafkaProducer(
                bootstrap_servers=self.bootstrap_servers,
                client_id=self.client_id,
                linger_ms=20,  # small batching without much latency
                acks="all",  # strongest durability for a single-node dev broker
                max_request_size=1024 * 1024,
                value_serializer=lambda v: json.dumps(
                    v, separators=(",", ":"), ensure_ascii=False
                ).encode("utf-8"),
            )
        await self._producer.start()
        bt.logging.info(
            f"✅ Kafka producer connected to {self.bootstrap_servers} (topic={self.topic})"
        )

    async def stop(self):
        if self._producer is not None:
            try:
                await self._producer.stop()
            finally:
                self._producer = None
                bt.logging.info("🛑 Kafka producer stopped")

    async def send_json(
        self, payload: Dict[str, Any], key: Optional[str] = None, retries: int = 3
    ):
        """
        Send a JSON-serializable payload to Kafka with basic retry.
        """
        if self._producer is None:
            raise RuntimeError("Producer not started")

        attempt = 0
        last_err: Optional[Exception] = None
        while attempt <= retries:
            try:
                await self._producer.send_and_wait(
                    self.topic,
                    value=payload,
                    key=(key.encode("utf-8") if key else None),
                )
                return
            except KafkaError as e:
                last_err = e
                backoff = min(0.5 * (2**attempt), 5.0)
                bt.logging.warning(
                    f"⚠️ Kafka send failed (attempt {attempt+1}/{retries+1}): {e}. Retrying in {backoff:.1f}s"
                )
                await asyncio.sleep(backoff)
                attempt += 1
        # If we get here, all retries failed
        bt.logging.error("❌ Kafka send permanently failed", exc_info=last_err)
