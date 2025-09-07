import os
import asyncio
import contextlib
from bittensor.core.async_subtensor import get_async_subtensor
import bittensor as bt

from utils import configure_logging, parse_ignore_netuids_from_env
from kafka import KafkaSignalConsumer
from auto_staker import AutoStaker
from discord import DiscordAlerter


async def main() -> None:
    """
    Environment variables:
      - TEST_MODE (default: True)
      - KAFKA_BOOTSTRAP (comma-separated). From host use: localhost:9093 ; from container: kafka:9092
      - KAFKA_TOPIC (default: "signal")
      - WALLET_NAME (required)
      - DISCORD_WEBHOOK_URL (optional; not used in this split, add a Notifier if needed)
      - HOTKEY_TO_STAKE (optional; default hardcoded)
      - BALANCE_TO_STAKE (optional; float TAO)
      - SUBTENSOR_ENDPOINT (default: wss://entrypoint-finney.opentensor.ai:443, testnet if TEST_MODE=True)
      - REFRESH_INTERVAL_S (optional; default: 12.0) -> background balance/metrics refresh interval
      - MAX_DELAY_IN_BLOCKS (optional; default: 10)
      - REDIS_URL (optional; default: redis://redis:6379/0) -> enable cache when set
      - MIN_SPACING_IN_BLOCKS (optional; default: 50) -> throttle window per netuid
    """
    configure_logging()

    test_mode = os.getenv("TEST_MODE", "False").strip().lower() == "true"
    endpoint = os.getenv(
        "SUBTENSOR_ENDPOINT", "wss://entrypoint-finney.opentensor.ai:443"
    )
    if test_mode:
        endpoint = "wss://test.finney.opentensor.ai:443"

    subtensor = await get_async_subtensor(
        network=endpoint,
        log_verbose=True,
    )

    bootstrap = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9093")
    topic = os.getenv("KAFKA_TOPIC", "signal")

    wallet_name = os.getenv("WALLET_NAME", "trader")
    if not wallet_name:
        raise RuntimeError("WALLET_NAME is required")

    hotkey_to_stake = os.getenv(
        "HOTKEY_TO_STAKE", "5E2LP6EnZ54m3wS8s1yPvD5c3xo71kQroBw7aUVK32TKeZ5u"
    )
    balance_to_stake_tao = float(os.getenv("BALANCE_TO_STAKE", "0.05"))
    refresh_interval_s = float(os.getenv("REFRESH_INTERVAL_S", "12.0"))
    max_delay_in_blocks = int(os.getenv("MAX_DELAY_IN_BLOCKS", "10"))

    redis_url = os.getenv("REDIS_URL", "redis://redis:6379/0")
    redis_url = os.getenv("REDIS_URL", "redis://localhost:6379/0")
    min_spacing_in_blocks = int(os.getenv("MIN_SPACING_IN_BLOCKS", "7200"))

    ignore_netuids = parse_ignore_netuids_from_env()

    alerts_webhook = os.getenv(
        "DISCORD_WEBHOOK_URL",
        "https://discord.com/api/webhooks/1411814256646819944/xONggF8pZAP0SuVkn3Wv0vwdQ977tqOr10Hj_GBSBJ_4S7WVCeC6axcbtidEjb9nnCFc",
    )
    alerter = DiscordAlerter(
        webhook_url=alerts_webhook.strip(),
        max_retries=5,
        max_concurrent_posts=2,
    )
    alerter.start()

    staker = AutoStaker(
        subtensor=subtensor,
        wallet_name=wallet_name,
        test_mode=test_mode,
        hotkey_to_stake=hotkey_to_stake,
        balance_to_stake_tao=balance_to_stake_tao,
        max_delay_in_blocks=max_delay_in_blocks,
        redis_url=redis_url,
        min_spacing_in_blocks=min_spacing_in_blocks,
        ignore_netuids=ignore_netuids,
        alerter=alerter,
    )

    ok, err = await staker.stake_all_on_root_minus_x_fees(x=10)
    if not ok:
        bt.logging.warning(f"Startup add_stake skipped/failed: {err}")

    client_id = f"auto-staker-{staker.wallet.coldkey.ss58_address}"
    group_id = client_id
    consumer = KafkaSignalConsumer(
        bootstrap_servers=bootstrap,
        topic=topic,
        group_id=group_id,
        client_id=client_id,
        ensure_offsets_topic=True,
        offsets_partitions=50,
    )

    loop = asyncio.get_running_loop()
    stop_event = asyncio.Event()
    for sig in ("SIGINT", "SIGTERM"):
        try:
            loop.add_signal_handler(getattr(__import__("signal"), sig), stop_event.set)
        except (NotImplementedError, AttributeError):
            pass

    # Start background periodic refresh (does NOT block Kafka consumer)
    refresh_task = asyncio.create_task(
        staker.periodic_refresh_loop(
            stop_event, interval_s=refresh_interval_s, initial_delay_s=0.0
        )
    )

    runner = asyncio.create_task(consumer.run(staker.handle_signal))

    try:
        await stop_event.wait()
    finally:
        await consumer.stop()
        runner.cancel()
        refresh_task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await runner
        with contextlib.suppress(asyncio.CancelledError):
            await refresh_task
        if alerter is not None:
            await alerter.stop()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
