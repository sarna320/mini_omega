from utils import configure_logging
import os
import websockets
import time
from bittensor.core.async_subtensor import get_async_subtensor
from bittensor.core.config import Config
import asyncio
import bittensor as bt


from monitor import SubnetMonitor
from kafka_producer import KafkaProducerManager
from utils import configure_logging


async def main():
    """
    Main entry point: configure logging, connect to subtensor, and start monitoring.
    """
    configure_logging()
    ENDPOINT = os.getenv(
        "SUBTENSOR_ENDPOINT",
        "ws://205.172.59.24:9944",
    )

    cfg = Config()
    cfg.fallback_endpoints = ["wss://entrypoint-finney.opentensor.ai:443"]
    cfg.websocket_shutdown_timer = 20.0

    subtensor = await get_async_subtensor(
        network=ENDPOINT,
        config=cfg,
        log_verbose=True,
    )
    KAFKA_BOOTSTRAP = os.getenv(
        "KAFKA_BOOTSTRAP_SERVERS",
        "localhost:9093",
    )
    KAFKA_TOPIC = os.getenv(
        "KAFKA_TOPIC",
        "signal",
    )
    producer = KafkaProducerManager(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        topic=KAFKA_TOPIC,
        client_id="subnet-monitor",
    )
    await producer.start()

    try:
        monitor = SubnetMonitor(subtensor, producer)
        async with websockets.connect(ENDPOINT) as ws:
            await monitor.subscribe_new_heads(ws)
    finally:
        await producer.stop()


if __name__ == "__main__":
    while True:
        try:
            asyncio.run(main())
            break

        except KeyboardInterrupt:
            bt.logging.info("👋 Monitoring stopped by user")
            break

        except Exception:
            bt.logging.error(
                "❌ Script terminated with an unexpected error:", exc_info=True
            )
            bt.logging.info("⏳ Restarting in 5 seconds...")
            time.sleep(5)
