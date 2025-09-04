import os
import asyncio
import contextlib
from utils import configure_logging

from notifier import KafkaDiscordNotifier
from utils import (
    configure_logging,
    default_formatter,
    parse_bootstrap,
    schedule_swap_coldkey_formatter,
    changed_subnet_formatter,
    added_subnet_formatter,
    removed_subnet_formatter,
)


async def main():
    """
    Env:
      - KAFKA_BOOTSTRAP (comma-separated). From host use: localhost:9093 ; from container: kafka:9092
      - KAFKA_TOPIC (default: "signal")
      - KAFKA_GROUP_ID (default: "discord")
      - DISCORD_WEBHOOK_URL (required)
      - TEST_MODE (default: True) -> if True, only logs the Discord payload
    """
    configure_logging()
    bootstrap = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9093")
    topic = os.getenv("KAFKA_TOPIC", "signal")
    group_id = os.getenv("KAFKA_GROUP_ID", "discord")
    webhook = os.getenv("DISCORD_WEBHOOK_URL", "")
    test_mode = os.getenv("TEST_MODE", "True").strip().lower() == "true"
    if not webhook and not test_mode:
        raise RuntimeError("DISCORD_WEBHOOK_URL is required when TEST_MODE is False")

    # map event -> formatter
    event_formatters = {
        "schedule_swap_coldkey": schedule_swap_coldkey_formatter,
        "changed_subnet": changed_subnet_formatter,
        "added_subnet": added_subnet_formatter,
        "removed_subnet": removed_subnet_formatter,
    }

    notifier = KafkaDiscordNotifier(
        bootstrap_servers=bootstrap,
        topic=topic,
        group_id=group_id,
        webhook_url=webhook,
        test_mode=test_mode,
        event_formatters=event_formatters,  # event-specific formatters
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
