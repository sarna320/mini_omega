import os
import asyncio
import contextlib
from utils import configure_logging

from notifier import KafkaDiscordNotifier


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
