# simulate_subnet_events.py
import os
import json
import asyncio
from typing import Optional, List, Dict, Any
from datetime import datetime, timezone

from aiokafka import AIOKafkaProducer
from aiokafka.admin import AIOKafkaAdminClient, NewTopic


async def ensure_topic(
    bootstrap: str, topic: str, partitions: int = 1, rf: int = 1
) -> None:
    """Best-effort create topic if it does not exist (useful for single-broker dev)."""
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
    """Return current time as ISO-8601 UTC string with 'Z' suffix."""
    return (
        datetime.now(timezone.utc)
        .isoformat(timespec="milliseconds")
        .replace("+00:00", "Z")
    )


def build_messages(base_block: int = 1_000) -> List[Dict[str, Any]]:
    """
    Build a list of Kafka payloads that simulate:
      1) added_subnet (single)
      2) changed_subnet (single netuid, multiple fields)
      3) changed_subnet (two netuids, two fields each)
    """
    ts = now_iso_utc()

    # 1) added_subnet (example netuid=129)
    # added_129 = {
    #     "type": "added_subnet",
    #     "netuid": 129,
    #     "timestamp": ts,
    #     "block": base_block + 1,
    #     "subnet_identity": {
    #         "subnet_name": "ByteLeap",
    #         "github_repo": "https://github.com/byteleapai/byteleap-subnet",
    #         "subnet_contact": "lx8623001@gmail.com",
    #         "subnet_url": "",
    #         "logo_url": None,
    #         "discord": "",
    #         "description": "High-throughput inference subnet",
    #         "additional": "",
    #     },
    # }

    # # 2) changed_subnet (single netuid=1, multiple fields)
    # changed_1_multi = {
    #     "type": "changed_subnet",
    #     "netuid": 1,
    #     "timestamp": ts,
    #     "block": base_block + 2,
    #     "fields": {
    #         "subnet_name": {"old": "Apex", "new": "APEX_V2"},
    #         "logo_url": {
    #             "old": "https://www.macrocosmos.ai/images/mc_logo_black.png",
    #             "new": "https://cdn.example.com/mc_logo_new.png",
    #         },
    #         "description": {
    #             "old": "Building the world's fastest deep researchers",
    #             "new": "APEX V2 — faster deep research",
    #         },
    #     },
    # }

    # # 3) changed_subnet for two different netuids, with exactly two field diffs each
    # changed_2_two_fields = {
    #     "type": "changed_subnet",
    #     "netuid": 2,
    #     "timestamp": ts,
    #     "block": base_block + 3,
    #     "fields": {
    #         "subnet_name": {"old": "omron 🥩", "new": "omron v2"},
    #         "logo_url": {"old": None, "new": "https://cdn.example.com/omron_v2.svg"},
    #     },
    # }
    # changed_5_two_fields = {
    #     "type": "changed_subnet",
    #     "netuid": 5,
    #     "timestamp": ts,
    #     "block": base_block + 4,
    #     "fields": {
    #         "subnet_contact": {
    #             "old": "devs@manifold.inc",
    #             "new": "support@manifold.inc",
    #         },
    #         "subnet_url": {
    #             "old": "https://www.hone.training/",
    #             "new": "https://hone.training/",
    #         },
    #     },
    # }
    changed_0_two_fields = {
        "type": "changed_subnet",
        "netuid": 0,
        "timestamp": ts,
        "block": base_block + 4,
        "fields": {
            "subnet_contact": {
                "old": "devs@manifold.inc",
                "new": "support@manifold.inc",
            },
            "subnet_url": {
                "old": "https://www.hone.training/",
                "new": "https://hone.training/",
            },
        },
    }

    return [changed_0_two_fields]

    # return [added_129, changed_1_multi, changed_2_two_fields, changed_5_two_fields]


async def main() -> None:
    """
    Env:
      - KAFKA_BOOTSTRAP (default: localhost:9093)
      - KAFKA_TOPIC (default: signal)
      - KAFKA_KEY_PREFIX (optional; if set, keys will be e.g. '<prefix>:added:129')
      - CREATE_TOPIC (default: true)
    """
    bootstrap = os.getenv("KAFKA_BOOTSTRAP", "localhost:9093")
    topic = os.getenv("KAFKA_TOPIC", "signal")
    key_prefix: Optional[str] = os.getenv("KAFKA_KEY_PREFIX")

    # Optionally ensure topic exists (single-broker/dev)
    if os.getenv("CREATE_TOPIC", "true").strip().lower() in ("1", "true", "yes"):
        await ensure_topic(bootstrap, topic)

    producer = AIOKafkaProducer(
        bootstrap_servers=bootstrap,
        value_serializer=lambda v: json.dumps(
            v, separators=(",", ":"), ensure_ascii=False
        ).encode("utf-8"),
        key_serializer=lambda k: (k.encode("utf-8") if k is not None else None),
    )
    await producer.start()
    try:
        messages = build_messages(6365170)
        for msg in messages:
            # Build per-message key to mirror your monitor conventions
            evt = msg.get("type")
            uid = msg.get("netuid", "n/a")
            key = f"{evt}:{uid}"
            if key_prefix:
                key = "signal"

            md = await producer.send_and_wait(topic, value=msg, key=key)
            print(
                f"✅ Sent -> topic='{topic}', partition={md.partition}, offset={md.offset}, key={key!r}, value={msg}"
            )
    finally:
        await producer.stop()


if __name__ == "__main__":
    asyncio.run(main())
