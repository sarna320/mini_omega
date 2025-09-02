import os
import json
import asyncio
from typing import Optional
from datetime import datetime, timezone

from aiokafka import AIOKafkaProducer
from aiokafka.admin import AIOKafkaAdminClient, NewTopic


async def ensure_topic(
    bootstrap: str, topic: str, partitions: int = 1, rf: int = 1
) -> None:
    """Best-effort create topic if it does not exist."""
    admin = AIOKafkaAdminClient(bootstrap_servers=bootstrap, client_id="producer-admin")
    await admin.start()
    try:
        topics = await admin.list_topics()
        if topic not in topics:
            await admin.create_topics(
                [NewTopic(name=topic, num_partitions=partitions, replication_factor=rf)]
            )
    finally:
        await admin.close()


def now_iso_utc() -> str:
    """
    Return current time as ISO-8601 UTC string with 'Z' suffix, e.g. '2025-08-31T12:34:56.789Z'.
    """
    return (
        datetime.now(timezone.utc)
        .isoformat(timespec="milliseconds")
        .replace("+00:00", "Z")
    )


async def main() -> None:
    """
    Env vars:
      - KAFKA_BOOTSTRAP (default: localhost:9093)
      - KAFKA_TOPIC (default: signal)
      - KAFKA_KEY (optional message key)
      - NETUID (default: 0)       # used if PAYLOAD is not provided
      - PAYLOAD (optional JSON)   # raw JSON to send; overrides NETUID
      - CREATE_TOPIC (default: true)
    """
    bootstrap = os.getenv("KAFKA_BOOTSTRAP", "localhost:9093")
    topic = os.getenv("KAFKA_TOPIC", "signal")
    key: Optional[str] = os.getenv("KAFKA_KEY")

    # Build payload from PAYLOAD or NETUID
    value = {
        "netuid": 0,
        "type": "schedule_swap_coldkey",
        "observed_at": now_iso_utc(),
        "block": 6351102 + 100000,
        "coldkey": "coldkeyxxxx",
        "new_coldkey": "new_coldkeyxxx",
        "parsed": "p",
        "raw": "data",  # keep raw for debugging/auditing
    }

    # Optionally ensure topic exists (for single-broker/dev setups)
    if os.getenv("CREATE_TOPIC", "true").lower() in ("1", "true", "yes"):
        await ensure_topic(bootstrap, topic)

    producer = AIOKafkaProducer(
        bootstrap_servers=bootstrap,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8") if k is not None else None,
    )
    await producer.start()
    try:
        # send_and_wait ensures the message is written before exiting
        md = await producer.send_and_wait(topic, value=value, key=key)
        print(
            f"✅ Sent to topic='{topic}', partition={md.partition}, offset={md.offset}, key={key!r}, value={value}"
        )
    finally:
        await producer.stop()


if __name__ == "__main__":
    asyncio.run(main())
